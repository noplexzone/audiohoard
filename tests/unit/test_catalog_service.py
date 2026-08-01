from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.catalog import (
    UNKNOWN,
    LibraryStats,
    Page,
    ReleaseProgress,
    _clear_release_evidence_cache,
    _filesystem_release_evidence,
    _normalize_artist,
    aggregate_artist_release_rollup,
    get_artist_detail,
    get_artists_page,
    get_library_artists_page,
    get_library_stats,
    get_release_progress,
    list_distinct_formats,
    list_library_tracks,
    track_meets_quality,
)
from app.settings_service import QualityProfile, save_runtime_settings


def test_artist_release_rollup_excludes_manifest_unknown_track_totals() -> None:
    rollup = aggregate_artist_release_rollup(
        [
            ReleaseProgress(wanted_track_count=10, downloaded_track_count=10, manifest_known=True),
            ReleaseProgress(wanted_track_count=8, downloaded_track_count=3, manifest_known=True),
            ReleaseProgress(wanted_track_count=0, downloaded_track_count=2, manifest_known=False),
            ReleaseProgress(wanted_track_count=0, downloaded_track_count=0, manifest_known=True),
        ]
    )

    assert rollup.tracks_in_library == 13
    assert rollup.tracks_total == 18
    assert rollup.releases_complete == 1
    assert rollup.releases_total == 3


def _make_track(
    job_id: int,
    *,
    title: str = "T",
    artist: str | None = "A",
    album_artist: str | None = None,
    album: str | None = "Alb",
    year: str | None = "2020",
    source: str = "slskd",
    source_path: str | None = "/music/track.flac",
    duration_sec: int | None = 200,
    file_format: str | None = None,
    file_size_bytes: int | None = 1024,
    release_id: int | None = None,
) -> Track:
    track = Track(
        job_id=job_id,
        title=title,
        artist=artist,
        album_artist=album_artist,
        album=album,
        year=year,
        source=source,
        source_path=source_path,
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.imported,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
        duration_sec=duration_sec,
        file_format=file_format,
        file_size_bytes=file_size_bytes,
        release_id=release_id,
    )
    plan = ImportPlan(
        release_id=release_id or 1,
        track=track,
        source_path=source_path or "/staging/source.flac",
        destination_path=source_path or "/music/track.flac",
        status=ImportWorkflowState.imported,
        file_state=LibraryFileState.present,
    )
    track.import_plans.append(plan)
    return track


@pytest.fixture
async def job(db_session: AsyncSession) -> Job:
    j = Job(source="slskd", query="test", status=JobStatus.done, result_json=None)
    db_session.add(j)
    await db_session.flush()
    return j


# ── Pure-unit helpers ──────────────────────────────────────────────────────────


def test_normalize_artist_uses_album_artist_when_set() -> None:
    t = Track(
        job_id=1,
        source="slskd",
        album_artist="Album Art",
        artist="Art",
        acquisition_state=AcquisitionState.queued,
        import_state=ImportWorkflowState.discovered,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
    )
    assert _normalize_artist(t) == "Album Art"


def test_normalize_artist_falls_back_to_artist() -> None:
    t = Track(
        job_id=1,
        source="slskd",
        album_artist=None,
        artist="Art",
        acquisition_state=AcquisitionState.queued,
        import_state=ImportWorkflowState.discovered,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
    )
    assert _normalize_artist(t) == "Art"


def test_normalize_artist_empty_string_falls_back() -> None:
    t = Track(
        job_id=1,
        source="slskd",
        album_artist="",
        artist="Art",
        acquisition_state=AcquisitionState.queued,
        import_state=ImportWorkflowState.discovered,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
    )
    assert _normalize_artist(t) == "Art"


def test_normalize_artist_both_null_returns_unknown() -> None:
    t = Track(
        job_id=1,
        source="slskd",
        album_artist=None,
        artist=None,
        acquisition_state=AcquisitionState.queued,
        import_state=ImportWorkflowState.discovered,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
    )
    assert _normalize_artist(t) == UNKNOWN


def test_track_fmt_uses_file_format_column() -> None:
    t = Track(
        job_id=1,
        source="slskd",
        file_format="flac",
        acquisition_state=AcquisitionState.queued,
        import_state=ImportWorkflowState.discovered,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
    )
    assert t.file_format == "flac"


def test_track_fmt_null_when_no_format_set() -> None:
    t = Track(
        job_id=1,
        source="slskd",
        file_format=None,
        acquisition_state=AcquisitionState.queued,
        import_state=ImportWorkflowState.discovered,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
    )
    assert t.file_format is None


def test_page_total_pages_basic() -> None:
    p: Page[int] = Page(items=[], total=95, page=1, per_page=50)
    assert p.total_pages == 2


def test_page_total_pages_exact_multiple() -> None:
    p: Page[int] = Page(items=[], total=100, page=1, per_page=50)
    assert p.total_pages == 2


def test_page_total_pages_zero() -> None:
    p: Page[int] = Page(items=[], total=0, page=1, per_page=50)
    assert p.total_pages == 1


def test_page_has_prev_next() -> None:
    p: Page[int] = Page(items=[], total=150, page=2, per_page=50)
    assert p.has_prev is True
    assert p.has_next is True


def test_page_first_page_no_prev() -> None:
    p: Page[int] = Page(items=[], total=100, page=1, per_page=50)
    assert p.has_prev is False
    assert p.has_next is True


def test_page_last_page_no_next() -> None:
    p: Page[int] = Page(items=[], total=100, page=2, per_page=50)
    assert p.has_prev is True
    assert p.has_next is False


def test_track_meets_quality_accepts_preferred_lossless_format() -> None:
    profile = QualityProfile(["flac", "mp3"], 320, True)

    assert track_meets_quality("flac", profile) is True


def test_track_meets_quality_rejects_known_below_threshold_mp3() -> None:
    profile = QualityProfile(["flac", "mp3"], 320, True)

    assert track_meets_quality("mp3 128kbps", profile) is False


def test_track_meets_quality_accepts_unknown_bitrate_mp3() -> None:
    profile = QualityProfile(["flac", "mp3"], 320, True)

    assert track_meets_quality("mp3", profile) is True


# ── DB-backed service tests ────────────────────────────────────────────────────


async def test_library_stats_empty_db(db_session: AsyncSession) -> None:
    stats = await get_library_stats(db_session)
    assert isinstance(stats, LibraryStats)
    assert stats.track_count == 0
    assert stats.artist_count == 0
    assert stats.album_count == 0
    assert stats.total_duration_sec == 0
    assert stats.total_bytes == 0
    assert stats.format_breakdown == {}
    assert stats.source_breakdown == {}


async def test_library_artist_release_count_for_catalog_and_legacy_rows(
    db_session: AsyncSession, job: Job
) -> None:
    artist = CatalogArtist(name="Catalog Artist", monitored=True)
    db_session.add(artist)
    await db_session.flush()
    identity = CatalogArtistIdentity(
        artist_id=artist.id,
        provider="musicbrainz",
        provider_artist_id="catalog-artist",
        name="Catalog Artist",
    )
    db_session.add(identity)
    await db_session.flush()
    db_session.add(
        CatalogAlbumProvider(
            artist_identity_id=identity.id,
            provider_album_id="catalog-release",
            title="Catalog Release",
            release_kind="album",
        )
    )
    db_session.add(_make_track(job.id, artist="Legacy Artist", album_artist=None))
    await db_session.flush()

    page = await get_library_artists_page(db_session)
    rows = {row.name: row for row in page.items}

    assert rows["Catalog Artist"].release_count == 1
    assert rows["Legacy Artist"].release_count == 0


async def test_library_artist_card_uses_runtime_provider_when_primary_unset(
    db_session: AsyncSession,
) -> None:
    await save_runtime_settings(
        db_session,
        [{"name": "slskd", "enabled": True}],
        10,
        metadata_providers=[
            {"name": "musicbrainz", "enabled": True},
            {"name": "deezer", "enabled": True},
        ],
        primary_metadata_provider="deezer",
    )
    artist = CatalogArtist(name="Runtime Default Artist", monitored=True)
    musicbrainz_identity = CatalogArtistIdentity(
        provider="musicbrainz", provider_artist_id="runtime-mb", name=artist.name
    )
    deezer_identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="runtime-deezer", name=artist.name
    )
    artist.identities.extend([musicbrainz_identity, deezer_identity])
    deezer_albums = [
        CatalogAlbum(artist=artist, title="Runtime Deezer One"),
        CatalogAlbum(artist=artist, title="Runtime Deezer Two"),
    ]
    for index, album in enumerate(deezer_albums, start=1):
        deezer_identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id=f"runtime-deezer-{index}",
                title=album.title,
                catalog_album=album,
                release_kind="album",
            )
        )
    for index in range(1, 5):
        musicbrainz_identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id=f"runtime-mb-{index}",
                title=f"Runtime MusicBrainz {index}",
                release_kind="album",
            )
        )
    db_session.add(artist)
    await db_session.flush()

    row = (await get_library_artists_page(db_session)).items[0]

    assert row.primary_metadata_provider == "deezer"
    assert row.release_count == 2


async def test_library_artist_card_uses_explicit_primary_provider_regardless_of_identity_age(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(
        name="Explicit Primary Artist",
        monitored=True,
        watchlist_provider="musicbrainz",
        primary_metadata_provider="deezer",
    )
    musicbrainz_identity = CatalogArtistIdentity(
        provider="musicbrainz", provider_artist_id="explicit-mb", name=artist.name
    )
    deezer_identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="explicit-deezer", name=artist.name
    )
    artist.identities.extend([musicbrainz_identity, deezer_identity])
    for index in range(1, 4):
        musicbrainz_identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id=f"explicit-mb-{index}",
                title=f"Explicit MusicBrainz {index}",
                release_kind="album",
            )
        )
    deezer_album = CatalogAlbum(artist=artist, title="Explicit Deezer One")
    deezer_identity.releases.append(
        CatalogAlbumProvider(
            provider_album_id="explicit-deezer-1",
            title=deezer_album.title,
            catalog_album=deezer_album,
            release_kind="album",
        )
    )
    db_session.add(artist)
    await db_session.flush()

    row = (await get_library_artists_page(db_session)).items[0]

    assert row.primary_metadata_provider == "deezer"
    assert row.release_count == 1


async def test_release_progress_uses_hydrated_manifest_as_denominator(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Juice WRLD")
    album = CatalogAlbum(
        artist=artist,
        title="Goodbye & Good Riddance",
        year="2018",
        track_count=17,
    )
    album.tracks.extend(
        CatalogAlbumTrack(position=position, disc=1, title=f"Track {position}")
        for position in range(1, 16)
    )
    db_session.add(artist)
    await db_session.flush()

    progress = await get_release_progress(db_session, [album.id])

    assert progress[album.id].wanted_track_count == 15
    assert progress[album.id].downloaded_track_count == 0


async def test_release_progress_counts_all_imported_catalog_tracks(
    db_session: AsyncSession, job: Job
) -> None:
    artist = CatalogArtist(name="Juice WRLD & Future")
    album = CatalogAlbum(
        artist=artist,
        title="WRLD ON DRUGS",
        year="2018",
        track_count=16,
        in_library=True,
    )
    album.tracks.extend(
        CatalogAlbumTrack(position=position, disc=1, title=f"Track {position}")
        for position in range(1, 17)
    )
    db_session.add(artist)
    await db_session.flush()
    release = Release(job_id=job.id, source="slskd", title=album.title, year=album.year)
    db_session.add(release)
    await db_session.flush()
    for catalog_track in album.tracks:
        track = _make_track(
            job.id,
            title=catalog_track.title,
            artist=artist.name,
            album_artist=artist.name,
            album=album.title,
            year=album.year,
            source_path=f"/music/{album.title}/{catalog_track.position:02d}.flac",
            release_id=release.id,
        )
        track.catalog_album_id = album.id
        track.catalog_track_id = catalog_track.id
        db_session.add(track)
    await db_session.flush()

    progress = await get_release_progress(db_session, [album.id])

    assert progress[album.id].wanted_track_count == 16
    assert progress[album.id].downloaded_track_count == 16


async def test_release_progress_keeps_clean_and_explicit_ownership_distinct(
    db_session: AsyncSession, job: Job
) -> None:
    artist = CatalogArtist(name="Juice WRLD")
    clean_album = CatalogAlbum(
        artist=artist,
        title="We Don't Get Along",
        year="2024",
        track_count=1,
        content_rating="clean",
    )
    explicit_album = CatalogAlbum(
        artist=artist,
        title="We Don't Get Along",
        year="2024",
        track_count=1,
        content_rating="explicit",
        in_library=True,
    )
    clean_album.tracks.append(
        CatalogAlbumTrack(position=1, disc=1, title="We Don't Get Along", content_rating="clean")
    )
    explicit_album.tracks.append(
        CatalogAlbumTrack(
            position=1,
            disc=1,
            title="We Don't Get Along",
            content_rating="explicit",
        )
    )
    db_session.add(artist)
    await db_session.flush()
    release = Release(
        job_id=job.id, source="slskd", title=explicit_album.title, year=explicit_album.year
    )
    db_session.add(release)
    await db_session.flush()
    imported = _make_track(
        job.id,
        title="We Don't Get Along",
        artist=artist.name,
        album_artist=artist.name,
        album=explicit_album.title,
        year=explicit_album.year,
        source_path="/music/Juice WRLD/We Don't Get Along (2024)/01.flac",
        release_id=release.id,
    )
    imported.catalog_album_id = explicit_album.id
    imported.catalog_track_id = explicit_album.tracks[0].id
    db_session.add(imported)
    await db_session.flush()

    progress = await get_release_progress(db_session, [clean_album.id, explicit_album.id])

    assert progress[clean_album.id].wanted_track_count == 1
    assert progress[clean_album.id].downloaded_track_count == 0
    assert progress[explicit_album.id].wanted_track_count == 1
    assert progress[explicit_album.id].downloaded_track_count == 1


async def test_library_artist_card_counts_watchlist_provider_releases_only(
    db_session: AsyncSession, job: Job
) -> None:
    artist = CatalogArtist(name="Tyler Childers", monitored=True, watchlist_provider="deezer")
    deezer_identity = CatalogArtistIdentity(
        provider="deezer",
        provider_artist_id="tyler-deezer",
        name=artist.name,
    )
    musicbrainz_identity = CatalogArtistIdentity(
        provider="musicbrainz",
        provider_artist_id="tyler-mbid",
        name=artist.name,
    )
    artist.identities.extend([deezer_identity, musicbrainz_identity])
    deezer_albums = [
        CatalogAlbum(artist=artist, title="Can I Take My Hounds to Heaven?", track_count=1),
        CatalogAlbum(artist=artist, title="Rustin' in the Rain", track_count=1),
    ]
    catalog_tracks = [
        CatalogAlbumTrack(album=album, position=1, disc=1, title=f"Song {index}")
        for index, album in enumerate(deezer_albums, start=1)
    ]
    for index, album in enumerate(deezer_albums, start=1):
        db_session.add(
            CatalogAlbumProvider(
                artist_identity=deezer_identity,
                catalog_album=album,
                provider_album_id=f"deezer-{index}",
                title=album.title,
                track_count=1,
                release_kind="album",
            )
        )
    for index in range(1, 3):
        db_session.add(
            CatalogAlbumProvider(
                artist_identity=musicbrainz_identity,
                provider_album_id=f"mb-{index}",
                title=f"Secondary Provider Release {index}",
                track_count=1,
                release_kind="album",
            )
        )
    db_session.add(artist)
    await db_session.flush()
    for index, (album, catalog_track) in enumerate(
        zip(deezer_albums, catalog_tracks, strict=True), start=1
    ):
        track = _make_track(
            job.id,
            title=f"Song {index}",
            artist=artist.name,
            album_artist=artist.name,
            album=album.title,
            source_path=f"/music/Tyler Childers/{album.title}/01.flac",
        )
        track.catalog_album_id = album.id
        track.catalog_track_id = catalog_track.id
        db_session.add(track)
    await db_session.flush()

    page = await get_library_artists_page(db_session)
    row = next(item for item in page.items if item.name == artist.name)

    assert row.release_count == 2
    assert row.complete_release_count == 2
    assert row.partial_release_count == 0
    assert row.unknown_release_count == 0
    assert row.local_release_count == 2
    assert row.wanted_release_count == 0
    assert row.downloaded_file_count == 2


async def test_library_artist_projection_distinguishes_complete_partial_unknown_and_missing(
    db_session: AsyncSession, job: Job
) -> None:
    artist = CatalogArtist(name="Truthful Artist", monitored=True, watchlist_provider="deezer")
    identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="truthful", name=artist.name
    )
    complete = CatalogAlbum(artist=artist, title="Complete", track_count=99)
    partial = CatalogAlbum(artist=artist, title="Partial", track_count=2)
    empty = CatalogAlbum(artist=artist, title="Empty", track_count=1)
    unknown = CatalogAlbum(artist=artist, title="Unknown", track_count=7)
    complete.tracks.extend(
        CatalogAlbumTrack(position=position, disc=1, title=f"Complete {position}")
        for position in (1, 2)
    )
    partial.tracks.extend(
        CatalogAlbumTrack(position=position, disc=1, title=f"Partial {position}")
        for position in (1, 2)
    )
    empty.tracks.append(CatalogAlbumTrack(position=1, disc=1, title="Empty 1"))
    for index, album in enumerate((complete, partial, empty, unknown), start=1):
        identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id=f"release-{index}",
                title=album.title,
                catalog_album=album,
                release_kind="album",
            )
        )
    artist.identities.append(identity)
    db_session.add(artist)
    await db_session.flush()

    for album, catalog_track in (
        (complete, complete.tracks[0]),
        (complete, complete.tracks[1]),
        (partial, partial.tracks[0]),
    ):
        imported = _make_track(job.id, title=catalog_track.title, artist=artist.name)
        imported.catalog_album_id = album.id
        imported.catalog_track_id = catalog_track.id
        db_session.add(imported)
    missing = _make_track(job.id, title="Removed attempt", artist=artist.name)
    missing.catalog_album_id = empty.id
    missing.catalog_track_id = empty.tracks[0].id
    missing.import_plans[0].file_state = LibraryFileState.removed
    missing.import_plans[0].status = ImportWorkflowState.removed
    db_session.add(missing)
    await db_session.flush()

    row = (await get_library_artists_page(db_session)).items[0]

    assert row.release_count == 4
    assert row.complete_release_count == 1
    assert row.partial_release_count == 1
    assert row.unknown_release_count == 1
    assert row.local_release_count == 2
    assert row.wanted_release_count == 3
    assert row.downloaded_file_count == 3


async def test_library_artist_projection_deduplicates_provider_aliases_but_not_canonical_ids(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Alias Artist", monitored=True, watchlist_provider="deezer")
    first = CatalogAlbum(artist=artist, title="Same Title", year="2020")
    second = CatalogAlbum(artist=artist, title="Same Title", year="2020")
    for provider in ("deezer", "musicbrainz"):
        identity = CatalogArtistIdentity(
            provider=provider, provider_artist_id=f"alias-{provider}", name=artist.name
        )
        identity.releases.extend(
            [
                CatalogAlbumProvider(
                    provider_album_id=f"{provider}-first",
                    title=first.title,
                    catalog_album=first,
                    release_kind="album",
                ),
                CatalogAlbumProvider(
                    provider_album_id=f"{provider}-second",
                    title=second.title,
                    catalog_album=second,
                    release_kind="album",
                ),
            ]
        )
        artist.identities.append(identity)
    db_session.add(artist)
    await db_session.flush()

    row = (await get_library_artists_page(db_session)).items[0]

    assert row.release_count == 2
    assert row.unknown_release_count == 2


async def test_release_progress_requires_present_plan_and_hydrated_manifest(
    db_session: AsyncSession, job: Job
) -> None:
    artist = CatalogArtist(name="Manifest Artist")
    hydrated = CatalogAlbum(artist=artist, title="Hydrated", track_count=99)
    hydrated.tracks.append(CatalogAlbumTrack(position=1, disc=1, title="One"))
    unknown = CatalogAlbum(artist=artist, title="Provider Count Only", track_count=1)
    db_session.add(artist)
    await db_session.flush()
    imported = _make_track(job.id, title="One", artist=artist.name)
    imported.catalog_album_id = hydrated.id
    imported.catalog_track_id = hydrated.tracks[0].id
    db_session.add(imported)
    removed_attempt = _make_track(job.id, title="Provider Count Only", artist=artist.name)
    removed_attempt.catalog_album_id = unknown.id
    removed_attempt.import_plans[0].file_state = LibraryFileState.missing
    db_session.add(removed_attempt)
    unknown_local = _make_track(job.id, title="Local Unknown", artist=artist.name)
    unknown_local.catalog_album_id = unknown.id
    db_session.add(unknown_local)
    await db_session.flush()

    progress = await get_release_progress(db_session, [hydrated.id, unknown.id])

    assert progress[hydrated.id].manifest_known is True
    assert progress[hydrated.id].wanted_track_count == 1
    assert progress[hydrated.id].complete is True
    assert progress[hydrated.id].track_state(hydrated.tracks[0].id) == "present"
    assert progress[unknown.id].manifest_known is False
    assert progress[unknown.id].wanted_track_count == 0
    assert progress[unknown.id].downloaded_track_count == 1
    assert progress[unknown.id].complete is False


async def test_library_stats_counts(db_session: AsyncSession, job: Job) -> None:
    tracks = [
        _make_track(
            job.id,
            title="S1",
            album_artist="AA",
            artist="A",
            album="Alb1",
            source="slskd",
            file_format="flac",
            file_size_bytes=10_000_000,
            duration_sec=60,
        ),
        _make_track(
            job.id,
            title="S2",
            album_artist="AA",
            artist="A",
            album="Alb1",
            source="youtube",
            file_format="mp3",
            file_size_bytes=5_000_000,
            duration_sec=120,
        ),
        _make_track(
            job.id,
            title="S3",
            album_artist=None,
            artist="B",
            album="Alb2",
            source="prowlarr",
            file_format="flac",
            file_size_bytes=8_000_000,
            duration_sec=90,
        ),
    ]
    db_session.add_all(tracks)
    await db_session.flush()

    stats = await get_library_stats(db_session)
    assert stats.track_count == 3
    assert stats.artist_count == 2  # AA and B
    assert stats.album_count == 2
    assert stats.total_duration_sec == 270
    assert stats.total_bytes == 23_000_000
    assert stats.format_breakdown == {"flac": 2, "mp3": 1}
    assert stats.source_breakdown["slskd"] == 1
    assert stats.source_breakdown["youtube"] == 1


async def test_library_stats_format_breakdown_uses_db_column(
    db_session: AsyncSession, job: Job
) -> None:
    db_session.add(
        _make_track(
            job.id,
            source_path="/some/path.ogg",  # extension ignored; file_format wins
            file_format="flac",
        )
    )
    await db_session.flush()
    stats = await get_library_stats(db_session)
    assert "flac" in stats.format_breakdown
    assert "ogg" not in stats.format_breakdown


async def test_library_stats_total_bytes_sql_aggregated(
    db_session: AsyncSession, job: Job
) -> None:
    db_session.add(_make_track(job.id, file_size_bytes=1024))
    db_session.add(_make_track(job.id, file_size_bytes=2048))
    db_session.add(_make_track(job.id, file_size_bytes=None))
    await db_session.flush()
    stats = await get_library_stats(db_session)
    assert stats.total_bytes == 3072


async def test_library_stats_counts_unknown_album_group(
    db_session: AsyncSession, job: Job
) -> None:
    tracks = [
        _make_track(job.id, album="Alb1"),
        _make_track(job.id, album=None),
    ]
    db_session.add_all(tracks)
    await db_session.flush()
    stats = await get_library_stats(db_session)
    assert stats.album_count == 2


async def test_library_stats_distinguishes_same_title_by_year(
    db_session: AsyncSession, job: Job
) -> None:
    db_session.add(_make_track(job.id, album="Same", year="2020"))
    db_session.add(_make_track(job.id, album="Same", year="2021"))
    await db_session.flush()
    assert (await get_library_stats(db_session)).album_count == 2


async def test_release_ids_keep_same_title_editions_distinct(
    db_session: AsyncSession, job: Job
) -> None:
    release_a = Release(job_id=job.id, source="slskd", title="Same", year="2020")
    release_b = Release(job_id=job.id, source="slskd", title="Same", year="2020")
    db_session.add_all([release_a, release_b])
    await db_session.flush()
    db_session.add(_make_track(job.id, album="Same", year="2020", release_id=release_a.id))
    db_session.add(_make_track(job.id, album="Same", year="2020", release_id=release_b.id))
    await db_session.flush()

    stats = await get_library_stats(db_session)
    artists = await get_artists_page(db_session)
    detail = await get_artist_detail(db_session, artist_name="A")
    assert stats.album_count == 2
    assert artists.items[0].album_count == 2
    assert detail.album_count == 2
    assert len(detail.albums) == 2


async def test_list_library_tracks_empty(db_session: AsyncSession) -> None:
    page = await list_library_tracks(db_session)
    assert isinstance(page, Page)
    assert page.items == []
    assert page.total == 0


async def test_list_library_tracks_pagination(db_session: AsyncSession, job: Job) -> None:
    for i in range(5):
        db_session.add(_make_track(job.id, title=f"Track {i}"))
    await db_session.flush()

    page1 = await list_library_tracks(db_session, page=1, per_page=2)
    assert len(page1.items) == 2
    assert page1.total == 5
    assert page1.has_next is True
    assert page1.has_prev is False

    page3 = await list_library_tracks(db_session, page=3, per_page=2)
    assert len(page3.items) == 1
    assert page3.has_next is False
    assert page3.has_prev is True


async def test_list_library_tracks_text_filter(db_session: AsyncSession, job: Job) -> None:
    db_session.add(
        _make_track(
            job.id,
            title="Moonlight Sonata",
            artist="Beethoven",
            album="Classics",
        )
    )
    db_session.add(
        _make_track(
            job.id,
            title="Fur Elise",
            artist="Beethoven",
            album="Classics",
        )
    )
    db_session.add(
        _make_track(
            job.id,
            title="Blue in Green",
            artist="Miles Davis",
            album="Kind of Blue",
        )
    )
    await db_session.flush()

    result = await list_library_tracks(db_session, q="Beethoven")
    assert result.total == 2
    titles = {r.title for r in result.items}
    assert "Moonlight Sonata" in titles
    assert "Fur Elise" in titles

    result2 = await list_library_tracks(db_session, q="blue")
    assert result2.total == 1
    assert result2.items[0].title == "Blue in Green"


async def test_list_library_tracks_source_filter(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, source="slskd"))
    db_session.add(_make_track(job.id, source="youtube"))
    db_session.add(_make_track(job.id, source="youtube"))
    await db_session.flush()

    result = await list_library_tracks(db_session, source="youtube")
    assert result.total == 2


async def test_list_library_tracks_fmt_filter(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, file_format="flac"))
    db_session.add(_make_track(job.id, file_format="mp3"))
    db_session.add(_make_track(job.id, file_format="flac"))
    await db_session.flush()

    result = await list_library_tracks(db_session, fmt="flac")
    assert result.total == 2
    assert all(r.fmt == "flac" for r in result.items)


async def test_list_library_tracks_deterministic_sort(db_session: AsyncSession, job: Job) -> None:
    for letter in ["C", "A", "B"]:
        db_session.add(_make_track(job.id, title=letter))
    await db_session.flush()

    r1 = await list_library_tracks(db_session, sort="title")
    r2 = await list_library_tracks(db_session, sort="title")
    assert [t.title for t in r1.items] == [t.title for t in r2.items]
    assert r1.items[0].title == "A"


async def test_list_library_tracks_fallback_artist_normalization(
    db_session: AsyncSession, job: Job
) -> None:
    db_session.add(_make_track(job.id, album_artist=None, artist="Solo Artist"))
    await db_session.flush()

    result = await list_library_tracks(db_session)
    assert result.items[0].artist == "Solo Artist"


async def test_list_library_tracks_page_clamped_to_last(
    db_session: AsyncSession, job: Job
) -> None:
    for i in range(3):
        db_session.add(_make_track(job.id, title=f"T{i}"))
    await db_session.flush()

    result = await list_library_tracks(db_session, page=999, per_page=2)
    assert result.page == 2
    assert len(result.items) > 0


async def test_list_distinct_formats(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, file_format="flac"))
    db_session.add(_make_track(job.id, file_format="mp3"))
    db_session.add(_make_track(job.id, file_format="flac"))
    db_session.add(_make_track(job.id, file_format=None))
    await db_session.flush()

    fmts = await list_distinct_formats(db_session)
    assert fmts == ["flac", "mp3"]


async def test_get_artists_page_empty(db_session: AsyncSession) -> None:
    page = await get_artists_page(db_session)
    assert page.items == []
    assert page.total == 0


async def test_get_artists_page_groups_by_album_artist(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, album_artist="AA", artist="A1", album="Alb1"))
    db_session.add(_make_track(job.id, album_artist="AA", artist="A2", album="Alb2"))
    await db_session.flush()

    page = await get_artists_page(db_session)
    assert page.total == 1
    assert page.items[0].display_name == "AA"
    assert page.items[0].track_count == 2
    assert page.items[0].album_count == 2


async def test_get_artists_page_fallback_to_artist(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, album_artist=None, artist="Solo"))
    await db_session.flush()

    page = await get_artists_page(db_session)
    assert page.total == 1
    assert page.items[0].display_name == "Solo"


async def test_get_artists_page_null_artist_shows_as_unknown(
    db_session: AsyncSession, job: Job
) -> None:
    db_session.add(_make_track(job.id, album_artist=None, artist=None))
    db_session.add(_make_track(job.id, album_artist="Known", artist=None))
    await db_session.flush()

    page = await get_artists_page(db_session)
    assert page.total == 2
    names = {a.display_name for a in page.items}
    assert "Known" in names
    assert UNKNOWN in names


async def test_get_artists_page_search(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, album_artist="Bach"))
    db_session.add(_make_track(job.id, album_artist="Beatles"))
    db_session.add(_make_track(job.id, album_artist="Miles Davis"))
    await db_session.flush()

    result = await get_artists_page(db_session, q="B")
    assert result.total == 2
    names = {a.display_name for a in result.items}
    assert "Bach" in names
    assert "Beatles" in names


async def test_get_artists_page_sort_by_tracks(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, album_artist="One Hit"))
    for _ in range(3):
        db_session.add(_make_track(job.id, album_artist="Big Artist"))
    await db_session.flush()

    result = await get_artists_page(db_session, sort="tracks")
    assert result.items[0].display_name == "Big Artist"
    assert result.items[0].track_count == 3


async def test_get_artists_page_pagination(db_session: AsyncSession, job: Job) -> None:
    for name in ["A", "B", "C"]:
        db_session.add(_make_track(job.id, album_artist=name))
    await db_session.flush()

    page1 = await get_artists_page(db_session, sort="name", page=1, per_page=2)
    assert len(page1.items) == 2
    assert page1.total == 3
    assert page1.has_next is True

    page2 = await get_artists_page(db_session, sort="name", page=2, per_page=2)
    assert len(page2.items) == 1
    assert page2.has_next is False


async def test_get_artists_page_formats_bounded_query(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, album_artist="Artist X", file_format="flac"))
    db_session.add(_make_track(job.id, album_artist="Artist X", file_format="mp3"))
    db_session.add(_make_track(job.id, album_artist="Artist Y", file_format="ogg"))
    await db_session.flush()

    page = await get_artists_page(db_session, sort="name", page=1, per_page=1)
    assert len(page.items) == 1
    assert page.items[0].display_name == "Artist X"
    assert set(page.items[0].formats) == {"flac", "mp3"}


async def test_get_artists_page_page_clamped_to_last(db_session: AsyncSession, job: Job) -> None:
    for name in ["A", "B", "C"]:
        db_session.add(_make_track(job.id, album_artist=name))
    await db_session.flush()

    result = await get_artists_page(db_session, sort="name", page=999, per_page=2)
    assert result.page == 2
    assert len(result.items) > 0


async def test_get_artist_detail_empty(db_session: AsyncSession) -> None:
    detail = await get_artist_detail(db_session, artist_name="Nobody")
    assert detail.track_count == 0
    assert detail.album_count == 0
    assert detail.albums == []


async def test_get_artist_detail_groups_albums(db_session: AsyncSession, job: Job) -> None:
    db_session.add(
        _make_track(
            job.id,
            title="S1",
            album_artist="AA",
            album="Alb1",
            year="2020",
            duration_sec=60,
        )
    )
    db_session.add(
        _make_track(
            job.id,
            title="S2",
            album_artist="AA",
            album="Alb1",
            year="2020",
            duration_sec=90,
        )
    )
    db_session.add(
        _make_track(
            job.id,
            title="S3",
            album_artist="AA",
            album="Alb2",
            year="2021",
            duration_sec=120,
        )
    )
    await db_session.flush()

    detail = await get_artist_detail(db_session, artist_name="AA")
    assert detail.track_count == 3
    assert detail.album_count == 2
    assert detail.total_duration_sec == 270
    album_names = {ag.album for ag in detail.albums}
    assert "Alb1" in album_names
    assert "Alb2" in album_names
    alb1 = next(ag for ag in detail.albums if ag.album == "Alb1")
    assert len(alb1.tracks) == 2


async def test_get_artist_detail_uses_album_artist(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, album_artist="Album Art", artist="Individual"))
    await db_session.flush()

    detail = await get_artist_detail(db_session, artist_name="Album Art")
    assert detail.track_count == 1


async def test_get_artist_detail_fallback_to_artist(db_session: AsyncSession, job: Job) -> None:
    db_session.add(_make_track(job.id, album_artist=None, artist="Solo Art"))
    await db_session.flush()

    detail = await get_artist_detail(db_session, artist_name="Solo Art")
    assert detail.track_count == 1


async def test_get_artist_detail_unknown_artist_finds_null_tracks(
    db_session: AsyncSession, job: Job
) -> None:
    db_session.add(_make_track(job.id, album_artist=None, artist=None, title="Mystery"))
    await db_session.flush()

    detail = await get_artist_detail(db_session, artist_name=UNKNOWN)
    assert detail.track_count == 1
    assert detail.albums[0].tracks[0].title == "Mystery"


async def test_get_artist_detail_pagination(db_session: AsyncSession, job: Job) -> None:
    for i in range(5):
        db_session.add(_make_track(job.id, album_artist="Prolific", title=f"Track {i}"))
    await db_session.flush()

    detail = await get_artist_detail(db_session, artist_name="Prolific", page=1, per_page=2)
    assert detail.track_count == 5
    assert detail.total_track_pages == 3
    assert detail.has_next is True
    assert detail.has_prev is False
    assert sum(len(ag.tracks) for ag in detail.albums) == 2


async def test_get_artist_detail_page_clamped_to_last(db_session: AsyncSession, job: Job) -> None:
    for i in range(3):
        db_session.add(_make_track(job.id, album_artist="Band", title=f"T{i}"))
    await db_session.flush()

    detail = await get_artist_detail(db_session, artist_name="Band", page=999, per_page=2)
    assert detail.page == 2
    assert sum(len(ag.tracks) for ag in detail.albums) > 0


async def test_get_artist_detail_track_row_fields(db_session: AsyncSession, job: Job) -> None:
    db_session.add(
        _make_track(
            job.id,
            album_artist="Band",
            file_format="flac",
            file_size_bytes=8_192_000,
        )
    )
    await db_session.flush()

    detail = await get_artist_detail(db_session, artist_name="Band")
    row = detail.albums[0].tracks[0]
    assert row.fmt == "flac"
    assert row.file_size_bytes == 8_192_000


async def test_library_stats_distinguishes_same_album_across_artists(
    db_session: AsyncSession, job: Job
) -> None:
    db_session.add(_make_track(job.id, artist="Artist A", album="Greatest Hits", year="2020"))
    db_session.add(_make_track(job.id, artist="Artist B", album="Greatest Hits", year="2020"))
    await db_session.flush()
    assert (await get_library_stats(db_session)).album_count == 2


def test_filesystem_release_evidence_reuses_unchanged_directory(tmp_path, monkeypatch) -> None:
    _clear_release_evidence_cache()
    folder = tmp_path / "Artist" / "Album (2020)"
    folder.mkdir(parents=True)
    audio = folder / "01 - Track.flac"
    audio.write_bytes(b"audio")
    albums = [(1, 1, "Album", "2020", "Artist")]

    first = _filesystem_release_evidence(tmp_path, albums)
    original_is_file = Path.is_file

    def reject_rescan(path):
        if path == audio:
            raise AssertionError("cached audio files should not be rescanned")
        return original_is_file(path)

    monkeypatch.setattr(Path, "is_file", reject_rescan)
    second = _filesystem_release_evidence(tmp_path, albums)

    assert first == second


def test_filesystem_release_evidence_rewalks_changed_nested_directory(tmp_path) -> None:
    _clear_release_evidence_cache()
    folder = tmp_path / "Artist" / "Album (2020)"
    disc = folder / "CD1"
    disc.mkdir(parents=True)
    (disc / "01 - Track.flac").write_bytes(b"audio")
    albums = [(1, 2, "Album", "2020", "Artist")]

    first = _filesystem_release_evidence(tmp_path, albums)
    (disc / "02 - Track.flac").write_bytes(b"audio")
    second = _filesystem_release_evidence(tmp_path, albums)

    assert first[1].file_count == 1
    assert second[1].file_count == 2


async def test_release_progress_ignores_filesystem_fallback_for_rated_siblings(
    db_session: AsyncSession, job: Job, tmp_path: Path
) -> None:
    artist = CatalogArtist(name="Juice WRLD")
    clean = CatalogAlbum(
        artist=artist,
        title="We Don’t Get Along",
        year="2026",
        release_type="single",
        track_count=1,
        content_rating="clean",
    )
    explicit = CatalogAlbum(
        artist=artist,
        title="We Don’t Get Along",
        year="2026",
        release_type="single",
        track_count=1,
        content_rating="explicit",
    )
    db_session.add_all([artist, clean, explicit])
    await db_session.flush()

    folder = tmp_path / "Juice WRLD" / "We Don’t Get Along (2026)"
    folder.mkdir(parents=True)
    (folder / "01 - We Don’t Get Along.flac").write_bytes(b"audio")

    progress = await get_release_progress(
        db_session, [clean.id, explicit.id], library_root=tmp_path
    )

    assert progress[clean.id].downloaded_track_count == 0
    assert progress[explicit.id].downloaded_track_count == 0


async def test_release_progress_uses_explicit_import_binding_not_clean_sibling(
    db_session: AsyncSession, job: Job, tmp_path: Path
) -> None:
    artist = CatalogArtist(name="Juice WRLD")
    clean = CatalogAlbum(
        artist=artist,
        title="AGATS2 (Insecure)",
        year="2024",
        release_type="single",
        track_count=1,
        content_rating="clean",
    )
    explicit = CatalogAlbum(
        artist=artist,
        title="AGATS2 (Insecure)",
        year="2024",
        release_type="single",
        track_count=1,
        content_rating="explicit",
    )
    clean_track = CatalogAlbumTrack(album=clean, position=1, title="AGATS2 (Insecure)")
    explicit_track = CatalogAlbumTrack(album=explicit, position=1, title="AGATS2 (Insecure)")
    db_session.add_all([artist, clean, explicit, clean_track, explicit_track])
    await db_session.flush()
    track = _make_track(
        job.id,
        title="AGATS2 (Insecure)",
        album="AGATS2 (Insecure)",
        source_path="/music/Juice WRLD/AGATS2 (Insecure) (2024)/01 - AGATS2 (Insecure).flac",
        release_id=1,
    )
    track.catalog_album_id = explicit.id
    track.catalog_track_id = explicit_track.id
    db_session.add(track)
    await db_session.flush()

    progress = await get_release_progress(
        db_session, [clean.id, explicit.id], library_root=tmp_path
    )

    assert progress[clean.id].downloaded_track_count == 0
    assert progress[explicit.id].downloaded_track_count == 1
