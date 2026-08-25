from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.models.acquisition_claim import AcquisitionDispatchClaim
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyJobOwnership,
    DiscographyScopeKind,
)
from app.models.job import Job, JobStatus
from app.services.catalog import (
    expand_catalog_album_missing_track_jobs,
    queue_catalog_album_missing_track_jobs,
)
from app.settings_service import QualityProfile

PROFILE = QualityProfile(
    format_preference=["flac", "mp3", "m4a/aac", "ogg", "opus"],
    min_mp3_bitrate=320,
    allow_lower_quality_fallback=True,
)


async def _album(db: AsyncSession, count: int = 1) -> CatalogAlbum:
    artist = CatalogArtist(name="Expansion Artist", monitored=True)
    album = CatalogAlbum(artist=artist, title="Expansion Album", monitored=True, track_count=count)
    album.tracks.extend(
        CatalogAlbumTrack(disc=1, position=n, title=f"Track {n:02d}") for n in range(1, count + 1)
    )
    db.add(artist)
    await db.flush()
    return album


async def _item(db: AsyncSession, album: CatalogAlbum) -> DiscographyBatchItem:
    batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.wanted_selected, scope_json="{}", scope_hash="0" * 64
    )
    item = DiscographyBatchItem(
        batch=batch,
        release_identity=f"catalog_album:{album.id}",
        catalog_album=album,
        artist_name=album.artist.name,
        release_title=album.title,
        state=DiscographyBatchItemState.preview,
    )
    db.add(item)
    await db.flush()
    return item


async def test_ordinary_repeat_and_wrapper_create_once(db_session: AsyncSession) -> None:
    album = await _album(db_session, 2)
    first = await expand_catalog_album_missing_track_jobs(
        db_session, album, quality_profile=PROFILE
    )
    second = await expand_catalog_album_missing_track_jobs(
        db_session, album, quality_profile=PROFILE
    )
    assert len(first.created_job_ids) == 2 and first.observed_job_ids == ()
    assert first.complete_track_ids == frozenset() and first.missing_count == 2
    assert not first.hydration_required
    assert second.created_job_ids == () and second.observed_job_ids == first.created_job_ids
    assert (
        await queue_catalog_album_missing_track_jobs(db_session, album, quality_profile=PROFILE)
        == []
    )
    assert await db_session.scalar(select(func.count(Job.id))) == 2
    assert await db_session.scalar(select(func.count(AcquisitionDispatchClaim.id))) == 2


async def test_active_observed_same_item_repeat_and_album_validation(
    db_session: AsyncSession,
) -> None:
    album = await _album(db_session)
    item = await _item(db_session, album)
    owner = Job(
        source="priority",
        query="existing",
        status=JobStatus.running,
        catalog_album=album,
        catalog_track=album.tracks[0],
    )
    db_session.add(owner)
    await db_session.flush()
    db_session.add(
        AcquisitionDispatchClaim(
            catalog_album_id=album.id, catalog_track_id=album.tracks[0].id, job_id=owner.id
        )
    )
    await db_session.commit()
    first = await expand_catalog_album_missing_track_jobs(
        db_session, album, quality_profile=PROFILE, batch_item_id=item.id
    )
    second = await expand_catalog_album_missing_track_jobs(
        db_session, album, quality_profile=PROFILE, batch_item_id=item.id
    )
    assert first.created_job_ids == () and first.observed_job_ids == (owner.id,)
    assert second.observed_job_ids == (owner.id,)
    links = list(
        (
            await db_session.scalars(
                select(DiscographyBatchItemJob).where(DiscographyBatchItemJob.item_id == item.id)
            )
        ).all()
    )
    assert [(link.job_id, link.ownership) for link in links] == [
        (owner.id, DiscographyJobOwnership.observed)
    ]
    other = await _album(db_session)
    album_id = album.id
    item_id = item.id
    with pytest.raises(ValueError, match="catalog album"):
        await expand_catalog_album_missing_track_jobs(
            db_session, other, quality_profile=PROFILE, batch_item_id=item_id
        )
    current_album = await db_session.get(CatalogAlbum, album_id)
    current_item = await db_session.get(DiscographyBatchItem, item_id)
    assert current_album is not None and current_item is not None
    current_item.state = DiscographyBatchItemState.cancelled
    await db_session.commit()
    with pytest.raises(ValueError, match="not expansion-eligible"):
        await expand_catalog_album_missing_track_jobs(
            db_session, current_album, quality_profile=PROFILE, batch_item_id=item_id
        )


async def test_invalid_manifest_requires_hydration_without_jobs(db_session: AsyncSession) -> None:
    artist = CatalogArtist(name="Unhydrated")
    album = CatalogAlbum(artist=artist, title="Unknown", track_count=2)
    album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Only"))
    db_session.add(artist)
    await db_session.flush()
    outcome = await expand_catalog_album_missing_track_jobs(
        db_session, album, quality_profile=PROFILE
    )
    assert outcome.hydration_required and outcome.created_job_ids == outcome.observed_job_ids == ()
    assert outcome.missing_count == 0
    assert await db_session.scalar(select(func.count(Job.id))) == 0


async def test_terminal_takeover_and_max_25_stable(db_session: AsyncSession) -> None:
    album = await _album(db_session, 30)
    old = Job(
        source="priority",
        query="old",
        status=JobStatus.failed,
        catalog_album=album,
        catalog_track=album.tracks[0],
    )
    db_session.add(old)
    await db_session.flush()
    db_session.add(
        AcquisitionDispatchClaim(
            catalog_album_id=album.id, catalog_track_id=album.tracks[0].id, job_id=old.id
        )
    )
    await db_session.commit()
    first = await expand_catalog_album_missing_track_jobs(
        db_session, album, quality_profile=PROFILE
    )
    second = await expand_catalog_album_missing_track_jobs(
        db_session, album, quality_profile=PROFILE
    )
    assert len(first.created_job_ids) == 25 and first.missing_count == 30
    assert len(second.created_job_ids) == 5 and second.missing_count == 30
    active = list(
        await db_session.scalars(
            select(Job).where(Job.status.in_((JobStatus.pending, JobStatus.running)))
        )
    )
    assert len(active) == 30
    assert [job.catalog_track_id for job in active[:25]] == [
        track.id for track in album.tracks[:25]
    ]
    claim = await db_session.scalar(
        select(AcquisitionDispatchClaim).where(
            AcquisitionDispatchClaim.catalog_track_id == album.tracks[0].id
        )
    )
    assert claim is not None and claim.job_id in first.created_job_ids


async def test_file_backed_concurrent_items_share_job_claim_and_links(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'race.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as seed:
        album = await _album(seed)
        items = [await _item(seed, album), await _item(seed, album)]
        await seed.commit()
        album_id, item_ids = album.id, tuple(item.id for item in items)
    gate = asyncio.Event()
    ready = 0
    mutex = asyncio.Lock()

    async def expand(item_id: int):
        nonlocal ready
        async with factory() as session:
            album = await session.get(CatalogAlbum, album_id)
            assert album is not None
            async with mutex:
                ready += 1
                if ready == 2:
                    gate.set()
            await gate.wait()
            return await expand_catalog_album_missing_track_jobs(
                session, album, quality_profile=PROFILE, batch_item_id=item_id
            )

    outcomes = await asyncio.gather(*(expand(item_id) for item_id in item_ids))
    async with factory() as observer:
        assert sum(len(result.created_job_ids) for result in outcomes) == 1
        assert sum(len(result.observed_job_ids) for result in outcomes) == 1
        assert (
            await observer.scalar(
                select(func.count(Job.id)).where(
                    Job.status.in_((JobStatus.pending, JobStatus.running))
                )
            )
            == 1
        )
        assert await observer.scalar(select(func.count(AcquisitionDispatchClaim.id))) == 1
        links = list(
            (
                await observer.execute(
                    select(DiscographyBatchItemJob.item_id, DiscographyBatchItemJob.ownership)
                )
            ).all()
        )
        assert {item_id for item_id, _ in links} == set(item_ids)
        assert {ownership for _, ownership in links} == {
            DiscographyJobOwnership.created,
            DiscographyJobOwnership.observed,
        }
    await engine.dispose()


async def test_provider_manifest_expectation_prevents_partial_expansion(
    db_session: AsyncSession,
) -> None:
    album = await _album(db_session)
    identity = CatalogArtistIdentity(
        artist=album.artist,
        provider="deezer",
        provider_artist_id="artist-1",
        name=album.artist.name,
    )
    provider_release = CatalogAlbumProvider(
        artist_identity=identity,
        catalog_album=album,
        provider_album_id="release-1",
        title=album.title,
        track_count=2,
    )
    batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.artist,
        scope_json="{}",
        scope_hash="1" * 64,
    )
    item = DiscographyBatchItem(
        batch=batch,
        release_identity="provider:deezer:release-1",
        provider_release=provider_release,
        catalog_album=album,
        artist_name=album.artist.name,
        release_title=album.title,
        state=DiscographyBatchItemState.preview,
    )
    db_session.add(item)
    await db_session.flush()

    outcome = await expand_catalog_album_missing_track_jobs(
        db_session,
        album,
        quality_profile=PROFILE,
        batch_item_id=item.id,
    )

    assert outcome.hydration_required
    assert outcome.created_job_ids == outcome.observed_job_ids == ()
    assert await db_session.scalar(select(func.count(Job.id))) == 0


async def test_locked_commit_retry_resets_attempt_local_outcome(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    album = await _album(db_session)
    original_commit = AsyncSession.commit
    commit_calls = 0

    async def lock_second_commit(session: AsyncSession) -> None:
        nonlocal commit_calls
        commit_calls += 1
        if commit_calls == 2:
            raise OperationalError("COMMIT", {}, Exception("database is locked"))
        await original_commit(session)

    monkeypatch.setattr(AsyncSession, "commit", lock_second_commit)
    outcome = await expand_catalog_album_missing_track_jobs(
        db_session,
        album,
        quality_profile=PROFILE,
    )

    assert commit_calls >= 3
    assert len(outcome.created_job_ids) == 1
    assert outcome.observed_job_ids == ()
    assert await db_session.scalar(select(func.count(Job.id))) == 1
