from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.catalog import (
    get_library_artists_page,
    get_library_stats,
    list_distinct_formats,
    list_distinct_sources,
    list_library_tracks,
)


async def test_library_artists_include_watchlisted_and_imported_but_not_staging_only(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="library artists", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Imported release")
    watched = CatalogArtist(name="Watchlisted Only", monitored=True)
    identity = CatalogArtistIdentity(
        provider="musicbrainz", provider_artist_id="watched", name=watched.name
    )
    identity.releases.append(
        CatalogAlbumProvider(provider_album_id="wanted", title="Wanted", release_kind="album")
    )
    watched.identities.append(identity)
    imported = CatalogArtist(name="Imported Only", monitored=False)
    imported_album = CatalogAlbum(title="Imported Album", in_library=False)
    imported.albums.append(imported_album)
    staging = CatalogArtist(name="Staging Only", monitored=False)
    staging_album = CatalogAlbum(title="Staging Album", in_library=False)
    staging.albums.append(staging_album)
    db_session.add_all([job, release, watched, imported, staging])
    await db_session.flush()
    imported_track = _track(
        job.id, "Imported", AcquisitionState.downloaded, "/staging/imported.flac", "flac"
    )
    imported_track.catalog_album_id = imported_album.id
    imported_track.import_state = ImportWorkflowState.imported
    imported_track.import_plans.append(
        ImportPlan(
            release=release,
            source_path="/staging/imported.flac",
            destination_path="/music/Imported Only/Imported.flac",
            status=ImportWorkflowState.imported,
            file_state=LibraryFileState.present,
        )
    )
    staging_track = _track(
        job.id, "Staging", AcquisitionState.downloaded, "/staging/only.flac", "flac"
    )
    staging_track.catalog_album_id = staging_album.id
    db_session.add_all([imported_track, staging_track])
    await db_session.flush()

    page = await get_library_artists_page(db_session)

    assert [item.name for item in page.items] == ["Imported Only", "Watchlisted Only"]
    by_name = {item.name: item for item in page.items}
    assert by_name["Imported Only"].downloaded_file_count == 1
    assert by_name["Imported Only"].wanted_release_count == 1
    assert by_name["Imported Only"].unknown_release_count == 1
    assert by_name["Watchlisted Only"].downloaded_file_count == 0
    assert by_name["Watchlisted Only"].wanted_release_count == 1
    assert by_name["Watchlisted Only"].watchlisted is True


async def test_library_artists_filter_sort_and_pagination(db_session: AsyncSession) -> None:
    alpha = CatalogArtist(name="Alpha", monitored=True)
    beta = CatalogArtist(name="Beta", monitored=True)
    hidden = CatalogArtist(name="Hidden Alpha", monitored=False)
    db_session.add_all([alpha, beta, hidden])
    await db_session.flush()
    filtered = await get_library_artists_page(db_session, q="Alpha")
    first = await get_library_artists_page(db_session, sort="name", page=1, per_page=1)
    second = await get_library_artists_page(db_session, sort="name", page=2, per_page=1)
    assert [artist.name for artist in filtered.items] == ["Alpha"]
    assert first.total == 2 and first.items[0].name == "Alpha"
    assert second.items[0].name == "Beta"


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


async def test_library_artists_keep_unlinked_imports_visible(db_session: AsyncSession) -> None:
    job = Job(source="slskd", query="legacy", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Legacy Album")
    matched_artist = CatalogArtist(name="Matched Artist", monitored=False)
    db_session.add_all([job, release, matched_artist])
    await db_session.flush()

    tracks = []
    for name in ("Legacy Artist", "Matched Artist"):
        track = _track(
            job.id,
            f"{name} Track",
            AcquisitionState.downloaded,
            f"/staging/{name}.flac",
            "flac",
        )
        track.artist = name
        track.album_artist = name
        track.import_state = ImportWorkflowState.imported
        track.import_plans.append(
            ImportPlan(
                release=release,
                source_path=f"/staging/{name}.flac",
                destination_path=f"/music/{name}/Legacy Album (2000)/01 Track.flac",
                status=ImportWorkflowState.imported,
                file_state=LibraryFileState.present,
            )
        )
        tracks.append(track)
    db_session.add_all(tracks)
    await db_session.flush()

    page = await get_library_artists_page(db_session)

    assert [item.name for item in page.items] == ["Legacy Artist", "Matched Artist"]
    by_name = {item.name: item for item in page.items}
    assert by_name["Legacy Artist"].id is None
    assert by_name["Legacy Artist"].detail_url == "/artists/detail?name=Legacy+Artist"
    assert by_name["Legacy Artist"].downloaded_file_count == 1
    assert by_name["Matched Artist"].id == matched_artist.id
    assert by_name["Matched Artist"].detail_url == f"/artists/catalog/{matched_artist.id}"
    assert by_name["Matched Artist"].downloaded_file_count == 1


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
    downloaded = await db_session.scalar(select(Track).where(Track.title == "Downloaded"))
    imported_release = Release(job=job, source="slskd", title="Imported")
    downloaded.import_state = ImportWorkflowState.imported
    db_session.add(
        ImportPlan(
            release=imported_release,
            track=downloaded,
            source_path="/staging/downloaded.flac",
            destination_path="/music/downloaded.flac",
            status=ImportWorkflowState.imported,
            file_state=LibraryFileState.present,
        )
    )
    await db_session.flush()
    page = await list_library_tracks(db_session)
    stats = await get_library_stats(db_session)
    assert [item.title for item in page.items] == ["Downloaded"]
    assert page.items[0].file_path == "/music/downloaded.flac"
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
        file_state=LibraryFileState.present,
    )
    plan = ImportPlan(
        release=release,
        track=track,
        source_path="/staging/source.flac",
        destination_path="/music/Artist/Album/01 Imported.flac",
        status=ImportWorkflowState.imported,
        file_state=LibraryFileState.present,
    )
    db_session.add_all([old_plan, plan])
    await db_session.flush()
    page = await list_library_tracks(db_session)
    assert page.items[0].file_path == "/music/Artist/Album/01 Imported.flac"
