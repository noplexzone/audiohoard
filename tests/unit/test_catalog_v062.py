from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import CatalogAlbum, CatalogArtist
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.catalog import (
    get_library_stats,
    get_watchlisted_artists_page,
    list_distinct_formats,
    list_distinct_sources,
    list_library_tracks,
)


async def test_watchlisted_artists_page_uses_catalog_rows_and_release_counts(
    db_session: AsyncSession,
) -> None:
    watched = CatalogArtist(
        name="Watched Artist", monitored=True, artwork_url="https://images.example/artist.jpg"
    )
    unwatched = CatalogArtist(name="Not Watched", monitored=False)
    watched.albums.extend(
        [
            CatalogAlbum(title="Album", release_type="Album"),
            CatalogAlbum(title="Single", release_type="Single"),
            CatalogAlbum(title="EP", release_type="EP"),
            CatalogAlbum(title="Collection", release_type="Compilation"),
        ]
    )
    db_session.add_all([watched, unwatched])
    await db_session.flush()
    page = await get_watchlisted_artists_page(db_session)
    assert page.total == 1
    item = page.items[0]
    assert (item.id, item.name, item.artwork_url) == (
        watched.id,
        "Watched Artist",
        "https://images.example/artist.jpg",
    )
    assert (
        item.album_count,
        item.single_ep_count,
        item.compilation_count,
        item.total_releases,
    ) == (1, 2, 1, 4)
    assert item.watchlisted is True


async def test_watchlisted_artists_filter_sort_and_pagination(db_session: AsyncSession) -> None:
    alpha = CatalogArtist(name="Alpha", monitored=True)
    beta = CatalogArtist(name="Beta", monitored=True)
    hidden = CatalogArtist(name="Hidden Alpha", monitored=False)
    alpha.albums.append(CatalogAlbum(title="One", release_type="Album"))
    beta.albums.extend(
        [
            CatalogAlbum(title="One", release_type="Album"),
            CatalogAlbum(title="Two", release_type="EP"),
        ]
    )
    db_session.add_all([alpha, beta, hidden])
    await db_session.flush()
    filtered = await get_watchlisted_artists_page(db_session, q="Alpha")
    first = await get_watchlisted_artists_page(db_session, sort="releases", page=1, per_page=1)
    second = await get_watchlisted_artists_page(db_session, sort="releases", page=2, per_page=1)
    assert [artist.name for artist in filtered.items] == ["Alpha"]
    assert first.total == 2 and first.items[0].name == "Beta"
    assert second.items[0].name == "Alpha"


def _track(
    job_id: int,
    title: str,
    state: AcquisitionState,
    path: str | None,
    fmt: str,
    size: int | None = 100,
) -> Track:
    return Track(
        job_id=job_id,
        title=title,
        artist="Artist",
        album="Album",
        source="slskd",
        source_path=path,
        acquisition_state=state,
        import_state=ImportWorkflowState.discovered,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
        file_format=fmt,
        duration_sec=60,
        file_size_bytes=size,
    )


async def test_library_only_counts_downloaded_tracks_with_a_file_path(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="test", status=JobStatus.done)
    db_session.add(job)
    await db_session.flush()
    db_session.add_all(
        [
            _track(
                job.id,
                "Downloaded",
                AcquisitionState.downloaded,
                "/staging/downloaded.flac",
                "flac",
            ),
            _track(job.id, "Failed", AcquisitionState.failed, "/staging/failed.mp3", "mp3"),
            _track(job.id, "Queued", AcquisitionState.queued, "/staging/queued.ogg", "ogg"),
            _track(job.id, "Missing path", AcquisitionState.downloaded, "  ", "wav"),
            _track(
                job.id,
                "Missing file metadata",
                AcquisitionState.downloaded,
                "/staging/unverified.flac",
                "flac",
                size=None,
            ),
        ]
    )
    await db_session.flush()
    page = await list_library_tracks(db_session)
    stats = await get_library_stats(db_session)
    assert [item.title for item in page.items] == ["Downloaded"]
    assert page.items[0].file_path == "/staging/downloaded.flac"
    assert (stats.track_count, stats.total_duration_sec, stats.total_bytes) == (1, 60, 100)
    assert stats.format_breakdown == {"flac": 1}
    assert await list_distinct_sources(db_session) == ["slskd"]
    assert await list_distinct_formats(db_session) == ["flac"]


async def test_library_prefers_imported_destination_path(db_session: AsyncSession) -> None:
    job = Job(source="slskd", query="test", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    track = Track(
        job=job,
        release=release,
        title="Imported",
        artist="Artist",
        album="Album",
        source="slskd",
        source_path="/staging/source.flac",
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.imported,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
        file_size_bytes=123,
    )
    old_plan = ImportPlan(
        release=release,
        track=track,
        source_path="/staging/source.flac",
        destination_path="/music/Artist/Album/00 Old Import.flac",
        status=ImportWorkflowState.imported,
    )
    plan = ImportPlan(
        release=release,
        track=track,
        source_path="/staging/source.flac",
        destination_path="/music/Artist/Album/01 Imported.flac",
        status=ImportWorkflowState.imported,
    )
    db_session.add_all([old_plan, plan])
    await db_session.flush()
    page = await list_library_tracks(db_session)
    assert page.items[0].file_path == "/music/Artist/Album/01 Imported.flac"
