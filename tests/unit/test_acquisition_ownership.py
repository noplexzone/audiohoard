from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings
from app.database import Base
from app.jobs import runner
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.acquisition_ownership import (
    claim_catalog_acquisition,
    has_committed_catalog_ownership,
)


async def _owned_track(
    db: AsyncSession, destination: Path
) -> tuple[CatalogAlbum, CatalogAlbumTrack]:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(title="Album", artist=artist)
    catalog_track = CatalogAlbumTrack(album=album, position=1, disc=1, title="Track")
    job = Job(source="slskd", query="old")
    release = Release(job=job, source="slskd", title="Album")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Track",
        catalog_album=album,
        catalog_track=catalog_track,
        import_state=ImportWorkflowState.imported,
    )
    plan = ImportPlan(
        release=release,
        track=track,
        source_path=str(destination),
        destination_path=str(destination),
        status=ImportWorkflowState.imported,
        file_state=LibraryFileState.present,
    )
    db.add_all([artist, album, catalog_track, job, release, track, plan])
    await db.flush()
    return album, catalog_track


async def test_exact_present_import_is_owned(db_session: AsyncSession, tmp_path: Path) -> None:
    library = tmp_path / "library"
    destination = library / "Artist" / "Album" / "01 Track.flac"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"audio")
    album, catalog_track = await _owned_track(db_session, destination)

    assert await has_committed_catalog_ownership(db_session, album.id, catalog_track.id, library)


async def test_missing_import_destination_is_not_owned(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library = tmp_path / "library"
    destination = library / "Artist" / "Album" / "01 Track.flac"
    album, catalog_track = await _owned_track(db_session, destination)

    assert not await has_committed_catalog_ownership(
        db_session, album.id, catalog_track.id, library
    )


async def test_different_release_is_not_owned(db_session: AsyncSession, tmp_path: Path) -> None:
    library = tmp_path / "library"
    destination = library / "Artist" / "Album" / "01 Track.flac"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"audio")
    _, catalog_track = await _owned_track(db_session, destination)
    other_artist = CatalogArtist(name="Other Artist")
    other_album = CatalogAlbum(title="Album Deluxe", artist=other_artist)
    other_track = CatalogAlbumTrack(album=other_album, position=1, disc=1, title="Track")
    db_session.add_all([other_artist, other_album, other_track])
    await db_session.flush()

    assert not await has_committed_catalog_ownership(
        db_session, other_album.id, other_track.id, library
    )


async def test_concurrent_catalog_claims_have_one_winner(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'claims.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as seed:
        artist = CatalogArtist(name="Artist")
        album = CatalogAlbum(title="Album", artist=artist)
        track = CatalogAlbumTrack(album=album, position=1, disc=1, title="Track")
        first_job = Job(source="slskd", query="first")
        second_job = Job(source="slskd", query="second")
        seed.add_all([artist, album, track, first_job, second_job])
        await seed.commit()
        identity = album.id, track.id
        job_ids = first_job.id, second_job.id

    async def claim(job_id: int) -> bool:
        async with factory() as session:
            won = await claim_catalog_acquisition(session, *identity, job_id)
            await session.commit()
            return won

    assert sum(await asyncio.gather(*(claim(job_id) for job_id in job_ids))) == 1
    await engine.dispose()


async def test_owned_exact_release_skips_search_and_enqueue(
    db_session: AsyncSession,
    test_settings: Settings,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    library = tmp_path / "library"
    destination = library / "Artist" / "Album" / "01 Track.flac"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"audio")
    album, catalog_track = await _owned_track(db_session, destination)
    target_job = Job(
        source="slskd",
        query="Artist Track",
        catalog_album_id=album.id,
        catalog_track_id=catalog_track.id,
    )
    db_session.add(target_job)
    await db_session.flush()
    calls = 0

    async def fail_fetch(*args: object, **kwargs: object) -> list[object]:
        nonlocal calls
        calls += 1
        raise AssertionError("owned track must not search")

    monkeypatch.setattr(runner, "_call_fetch_results", fail_fetch)

    await runner._run_job_in_session(target_job.id, db_session, test_settings)

    assert calls == 0
    assert target_job.result_json == (
        '{"skipped": "exact_catalog_track_owned", "tracks_created": 0}'
    )


async def test_same_job_retry_reowns_its_claim(db_session: AsyncSession) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(title="Album", artist=artist)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="Track")
    job = Job(source="slskd", query="retry")
    db_session.add_all([artist, album, track, job])
    await db_session.flush()

    assert await claim_catalog_acquisition(db_session, album.id, track.id, job.id)
    assert await claim_catalog_acquisition(db_session, album.id, track.id, job.id)


async def test_terminal_owner_releases_claim_for_retry(db_session: AsyncSession) -> None:
    from app.models.job import JobStatus

    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(title="Album", artist=artist)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="Track")
    owner = Job(source="slskd", query="owner")
    retry = Job(source="slskd", query="retry")
    db_session.add_all([artist, album, track, owner, retry])
    await db_session.flush()

    assert await claim_catalog_acquisition(db_session, album.id, track.id, owner.id)
    assert not await claim_catalog_acquisition(db_session, album.id, track.id, retry.id)
    owner.status = JobStatus.failed
    await db_session.flush()
    assert await claim_catalog_acquisition(db_session, album.id, track.id, retry.id)


async def test_active_equivalent_job_is_cancelled_not_done(
    db_session: AsyncSession, test_settings: Settings
) -> None:
    from app.models.job import JobStatus

    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(title="Album", artist=artist)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="Track")
    owner = Job(source="slskd", query="owner", catalog_album=album, catalog_track=track)
    duplicate = Job(source="slskd", query="duplicate", catalog_album=album, catalog_track=track)
    db_session.add_all([artist, album, track, owner, duplicate])
    await db_session.flush()
    assert await claim_catalog_acquisition(db_session, album.id, track.id, owner.id)

    await runner._run_job_in_session(duplicate.id, db_session, test_settings)

    assert duplicate.status == JobStatus.cancelled
    assert duplicate.queue_hidden is True
    assert duplicate.result_json == (
        '{"cancelled": "equivalent_acquisition_active", "tracks_created": 0}'
    )
