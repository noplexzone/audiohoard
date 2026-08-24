from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.jobs import runner
from app.metadata import deezer
from app.metadata.base import AlbumDetail, AlbumTrack
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.schemas.search import SearchResult
from app.services import catalog, catalog_metadata
from app.services.catalog import ReleaseProgress
from app.settings_service import QualityProfile


def test_deezer_album_track_preserves_provider_artist_identity() -> None:
    parsed = deezer._parse_album_track(
        {
            "id": 2536769291,
            "title": "The Hanging Tree",
            "track_position": 2,
            "disk_number": 1,
            "artist": {"id": 132936402, "name": "Rachel Zegler"},
        }
    )

    assert parsed.artist_name == "Rachel Zegler"
    assert parsed.artist_provider_id == "132936402"


@pytest.mark.asyncio
async def test_compilation_hydration_persists_album_and_track_artists(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    watched = CatalogArtist(name="Olivia Rodrigo", deezer_id="11152580")
    album = CatalogAlbum(
        artist=watched,
        title="The Ballad of Songbirds & Snakes",
        deezer_id="510964651",
        track_count=2,
        release_type="compilation",
    )
    db_session.add(watched)
    await db_session.flush()

    class Provider:
        async def get_album(self, provider_id: str) -> AlbumDetail:
            return AlbumDetail(
                provider="deezer",
                provider_id=provider_id,
                deezer_id=provider_id,
                title=album.title,
                artist_name="Various Artists",
                artist_provider_id="5080",
                release_type="compilation",
                release_kind="compilation",
                track_count=2,
                tracks=[
                    AlbumTrack(
                        position=1,
                        title="Can't Catch Me Now",
                        artist_name="Olivia Rodrigo",
                        artist_provider_id="11152580",
                    ),
                    AlbumTrack(
                        position=2,
                        title="The Hanging Tree",
                        artist_name="Rachel Zegler",
                        artist_provider_id="132936402",
                    ),
                ],
            )

    monkeypatch.setattr(catalog_metadata, "build_metadata_provider", lambda *_: Provider())

    hydrated = await catalog_metadata.fetch_and_store_album(db_session, test_settings, album)

    assert hydrated.is_compilation is True
    assert hydrated.album_artist_name == "Various Artists"
    assert hydrated.album_artist_provider_id == "5080"
    by_title = {track.title: track for track in hydrated.tracks}
    assert by_title["Can't Catch Me Now"].artist_name == "Olivia Rodrigo"
    assert by_title["Can't Catch Me Now"].artist_provider_id == "11152580"
    assert by_title["The Hanging Tree"].artist_name == "Rachel Zegler"
    assert by_title["The Hanging Tree"].artist_provider_id == "132936402"


@pytest.mark.asyncio
async def test_compilation_queue_uses_each_track_artist_without_dropping_tracks(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    watched = CatalogArtist(name="Olivia Rodrigo")
    album = CatalogAlbum(
        artist=watched,
        title="Soundtrack",
        track_count=2,
        is_compilation=True,
        album_artist_name="Various Artists",
    )
    album.tracks.extend(
        [
            CatalogAlbumTrack(position=1, title="Olivia Song", artist_name="Olivia Rodrigo"),
            CatalogAlbumTrack(position=2, title="Rachel Song", artist_name="Rachel Zegler"),
        ]
    )
    db_session.add(watched)
    await db_session.flush()

    async def no_library(*args: object, **kwargs: object) -> dict[int, ReleaseProgress]:
        return {
            album.id: ReleaseProgress(
                wanted_track_count=2, downloaded_track_count=0, manifest_known=True
            )
        }

    monkeypatch.setattr(catalog, "get_release_progress", no_library)
    profile = QualityProfile(
        format_preference=["flac", "mp3"],
        min_mp3_bitrate=320,
        allow_lower_quality_fallback=True,
    )

    job_ids = await catalog.queue_catalog_album_missing_track_jobs(
        db_session, album, quality_profile=profile
    )
    jobs = list((await db_session.scalars(select(Job).where(Job.id.in_(job_ids)))).all())

    assert len(jobs) == 2
    assert {job.query for job in jobs} == {
        "Olivia Rodrigo Olivia Song",
        "Rachel Zegler Rachel Song",
    }


def test_catalog_tag_identity_uses_compilation_performer_and_album_artist() -> None:
    watched = CatalogArtist(name="Olivia Rodrigo")
    compilation = CatalogAlbum(
        artist=watched,
        title="Soundtrack",
        is_compilation=True,
        album_artist_name="Various Artists",
    )
    rachel = CatalogAlbumTrack(position=2, title="The Hanging Tree", artist_name="Rachel Zegler")
    ordinary = CatalogAlbum(artist=watched, title="SOUR")
    brutal = CatalogAlbumTrack(position=1, title="brutal", artist_name="Guest Artist")

    assert runner._catalog_track_artist_name(compilation, rachel) == "Rachel Zegler"
    assert runner._catalog_album_artist_name(compilation) == "Various Artists"
    assert runner._catalog_track_artist_name(ordinary, brutal) == "Olivia Rodrigo"
    assert runner._catalog_album_artist_name(ordinary) == "Olivia Rodrigo"


@pytest.mark.asyncio
async def test_runner_creates_compilation_track_with_performer_and_album_artist_tags(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    watched = CatalogArtist(name="Olivia Rodrigo")
    album = CatalogAlbum(
        artist=watched,
        title="Soundtrack",
        track_count=1,
        is_compilation=True,
        album_artist_name="Various Artists",
    )
    target = CatalogAlbumTrack(
        position=1,
        disc=1,
        title="The Hanging Tree",
        artist_name="Rachel Zegler",
    )
    album.tracks.append(target)
    job = Job(
        source="priority",
        query="Rachel Zegler The Hanging Tree",
        status=JobStatus.pending,
        catalog_album=album,
        catalog_track=target,
    )
    db_session.add(job)
    await db_session.flush()

    async def fake_fetch(*args: object, **kwargs: object) -> list[SearchResult]:
        return [
            SearchResult(
                source="slskd",
                title="The Hanging Tree",
                artist="Rachel Zegler",
                album="Soundtrack",
                url="slskd://peer/The Hanging Tree.flac",
                metadata={"username": "peer", "filename": "Rachel Zegler - The Hanging Tree.flac"},
            )
        ]

    async def fake_prepare(*args: object, **kwargs: object) -> tuple[str, str]:
        return ("transfer-id", "completed")

    async def noop(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch)
    monkeypatch.setattr(runner, "_call_prepare_acquisition", fake_prepare)
    monkeypatch.setattr(runner, "_enrich_musicbrainz", noop)
    monkeypatch.setattr(runner, "_enrich_deezer", noop)
    monkeypatch.setattr(runner, "_run_fingerprint_and_verify", noop)
    monkeypatch.setattr(runner, "_compute_path_preview", noop)
    monkeypatch.setattr(runner, "_try_auto_import", noop)

    await runner._run_job_in_session(job.id, db_session, test_settings)

    created = await db_session.scalar(select(Track).where(Track.job_id == job.id))
    assert created is not None
    assert created.artist == "Rachel Zegler"
    assert created.album_artist == "Various Artists"


@pytest.mark.asyncio
async def test_runner_refreshes_stale_compilation_tags_on_reused_unimported_track(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    watched = CatalogArtist(name="Olivia Rodrigo")
    album = CatalogAlbum(
        artist=watched,
        title="Soundtrack",
        track_count=1,
        is_compilation=True,
        album_artist_name="Various Artists",
    )
    target = CatalogAlbumTrack(
        position=1,
        disc=1,
        title="The Hanging Tree",
        artist_name="Rachel Zegler",
    )
    album.tracks.append(target)
    job = Job(
        source="priority",
        query="Rachel Zegler The Hanging Tree",
        status=JobStatus.pending,
        catalog_album=album,
        catalog_track=target,
    )
    release = Release(
        job=job,
        source="slskd",
        title="Soundtrack",
        album_artist="Olivia Rodrigo",
        track_count=1,
    )
    stale = Track(
        job=job,
        release=release,
        catalog_album=album,
        catalog_track=target,
        title="The Hanging Tree",
        artist="Olivia Rodrigo",
        album_artist="Olivia Rodrigo",
        album="Soundtrack",
        source="slskd",
    )
    db_session.add(job)
    await db_session.flush()
    stale_id = stale.id

    async def fake_fetch(*args: object, **kwargs: object) -> list[SearchResult]:
        return [
            SearchResult(
                source="slskd",
                title="The Hanging Tree",
                artist="Rachel Zegler",
                album="Soundtrack",
                url="slskd://peer/The Hanging Tree.flac",
                metadata={"username": "peer", "filename": "Rachel Zegler - The Hanging Tree.flac"},
            )
        ]

    async def fake_prepare(*args: object, **kwargs: object) -> tuple[str, str]:
        return ("transfer-id", "completed")

    async def noop(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch)
    monkeypatch.setattr(runner, "_call_prepare_acquisition", fake_prepare)
    monkeypatch.setattr(runner, "_enrich_musicbrainz", noop)
    monkeypatch.setattr(runner, "_enrich_deezer", noop)
    monkeypatch.setattr(runner, "_run_fingerprint_and_verify", noop)
    monkeypatch.setattr(runner, "_compute_path_preview", noop)
    monkeypatch.setattr(runner, "_try_auto_import", noop)

    await runner._run_job_in_session(job.id, db_session, test_settings)

    reused = await db_session.get(Track, stale_id)
    assert reused is not None
    assert reused.artist == "Rachel Zegler"
    assert reused.album_artist == "Various Artists"
