from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.catalog import (
    UNKNOWN,
    LibraryStats,
    Page,
    _normalize_artist,
    get_artist_detail,
    get_artists_page,
    get_library_stats,
    list_distinct_formats,
    list_library_tracks,
)


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
