from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.jobs.runner import _catalog_track_for_result, _spawn_continuation_jobs
from app.metadata.filename_parse import parsed_position_evidence
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
    ReviewDecision,
)
from app.schemas.search import SearchResult
from app.services.acoustid_verification import run_acoustid_verification
from app.services.auto_import import try_auto_import_release
from app.services.slskd_scoring import (
    group_slskd_files_into_folders,
    select_best_folder,
)


def _folder_response(
    username: str, folder: str, ext: str, count: int, bitrate: int
) -> dict[str, object]:
    return {
        "username": username,
        "files": [
            {
                "filename": f"{folder}\\{number:02d} Track {number}.{ext}",
                "size": 10_000 + number,
                "bitRate": bitrate,
            }
            for number in range(1, count + 1)
        ],
    }


def test_slskd_album_folder_selection_keeps_complete_15_track_candidate() -> None:
    folders = group_slskd_files_into_folders(
        [
            _folder_response("partial", "Juice WRLD\\Goodbye & Good Riddance", "mp3", 10, 320),
            _folder_response("complete", "Juice WRLD\\Goodbye & Good Riddance", "mp3", 15, 320),
        ]
    )
    selected = select_best_folder(
        folders,
        catalog_track_count=15,
        catalog_artist="Juice WRLD",
        catalog_album="Goodbye & Good Riddance",
        format_preference=["mp3", "flac"],
        min_mp3_bitrate=256,
        allow_lower_quality_fallback=True,
    )
    assert selected is not None
    assert selected.username == "complete"
    assert len(selected.files) == 15


def test_quality_profile_prefers_complete_flac_over_complete_mp3() -> None:
    folders = group_slskd_files_into_folders(
        [
            _folder_response("mp3", "Artist\\Album", "mp3", 15, 320),
            _folder_response("flac", "Artist\\Album", "flac", 15, 0),
        ]
    )
    selected = select_best_folder(
        folders,
        catalog_track_count=15,
        catalog_artist="Artist",
        catalog_album="Album",
        format_preference=["flac", "mp3"],
        min_mp3_bitrate=320,
        allow_lower_quality_fallback=True,
    )
    assert selected is not None
    assert selected.audio_format == "flac"


def test_quality_profile_accepts_second_ranked_format_without_global_fallback() -> None:
    folders = group_slskd_files_into_folders(
        [_folder_response("mp3", "Artist\\Album", "mp3", 15, 320)]
    )
    selected = select_best_folder(
        folders,
        catalog_track_count=15,
        catalog_artist="Artist",
        catalog_album="Album",
        format_preference=["flac", "mp3"],
        min_mp3_bitrate=320,
        allow_lower_quality_fallback=False,
    )
    assert selected is not None
    assert selected.audio_format == "mp3"


def test_quality_profile_rejects_below_threshold_mp3_without_fallback() -> None:
    folders = group_slskd_files_into_folders(
        [_folder_response("mp3", "Artist\\Album", "mp3", 15, 256)]
    )
    assert (
        select_best_folder(
            folders,
            catalog_track_count=15,
            catalog_artist="Artist",
            catalog_album="Album",
            format_preference=["flac", "mp3", "m4a/aac", "ogg", "opus"],
            min_mp3_bitrate=320,
            allow_lower_quality_fallback=False,
        )
        is None
    )


def test_quality_profile_treats_m4a_and_aac_as_one_ranked_family() -> None:
    folders = group_slskd_files_into_folders(
        [_folder_response("m4a", "Artist\\Album", "m4a", 15, 256)]
    )
    selected = select_best_folder(
        folders,
        catalog_track_count=15,
        catalog_artist="Artist",
        catalog_album="Album",
        format_preference=["flac", "mp3", "m4a/aac", "ogg", "opus"],
        min_mp3_bitrate=320,
        allow_lower_quality_fallback=False,
    )
    assert selected is not None
    assert selected.audio_format == "m4a"


def test_catalog_matching_normalizes_track_prefixes_and_skit_descriptors() -> None:
    catalog = [
        CatalogAlbumTrack(id=1, album_id=1, position=1, disc=1, title="Intro"),
        CatalogAlbumTrack(id=6, album_id=1, position=6, disc=1, title="Betrayal (skit)"),
        CatalogAlbumTrack(id=10, album_id=1, position=10, disc=1, title="Karma (skit)"),
    ]
    for title, expected_id in (("01 Intro", 1), ("06 Betrayal", 6), ("10 Karma", 10)):
        result = SearchResult(source="slskd", title=title)
        assert _catalog_track_for_result(result, catalog, None).id == expected_id


def test_scene_packed_disc_track_prefix_preserves_position_evidence() -> None:
    assert parsed_position_evidence("101-ty_myers_harper_oneill-help_ourselves.mp3") == {
        "disc": 1,
        "track_no": 1,
    }
    assert parsed_position_evidence("210-tom_bukovac-man_on_the_side.mp3") == {
        "disc": 2,
        "track_no": 10,
    }


def test_catalog_matching_uses_scene_packed_disc_track_metadata() -> None:
    catalog = [
        CatalogAlbumTrack(id=1, album_id=1, position=1, disc=1, title="Help Ourselves"),
        CatalogAlbumTrack(id=10, album_id=1, position=10, disc=2, title="Man On The Side"),
    ]
    first = SearchResult(
        source="slskd",
        title="ty myers harper oneill-help ourselves",
        metadata=parsed_position_evidence("101-ty_myers_harper_oneill-help_ourselves.mp3"),
    )
    second = SearchResult(
        source="slskd",
        title="tom bukovac-man on the side",
        metadata=parsed_position_evidence("210-tom_bukovac-man_on_the_side.mp3"),
    )

    assert _catalog_track_for_result(first, catalog, None).id == 1
    assert _catalog_track_for_result(second, catalog, None).id == 10


async def test_acoustid_mismatch_creates_one_durable_review_item(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="album", status=JobStatus.running)
    release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song",
        mbid="11111111-1111-1111-1111-111111111111",
        identity_state=IdentityResolutionState.resolved,
        acquisition_state=AcquisitionState.downloaded,
    )
    db_session.add_all([job, release, track])
    await db_session.flush()
    observed = [
        {
            "score": 0.98,
            "recordings": [{"id": "22222222-2222-2222-2222-222222222222"}],
        }
    ]
    await run_acoustid_verification(
        track, acoustid_raw_results=observed, fingerprint_duration_sec=180, db=db_session
    )
    await run_acoustid_verification(
        track, acoustid_raw_results=observed, fingerprint_duration_sec=180, db=db_session
    )
    items = list((await db_session.scalars(select(StagingReviewItem))).all())
    assert track.acoustid_verification_state == AcoustIDVerificationState.mismatch
    assert len(items) == 1
    assert items[0].review_state == ReviewDecision.pending


async def test_matching_expected_mbid_above_threshold_clears_stale_review(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="album", status=JobStatus.running)
    release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
    expected = "11111111-1111-1111-1111-111111111111"
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song",
        mbid=expected,
        identity_state=IdentityResolutionState.resolved,
        acquisition_state=AcquisitionState.downloaded,
    )
    db_session.add_all([job, release, track])
    await db_session.flush()
    db_session.add(
        StagingReviewItem(
            track_id=track.id,
            release_id=release.id,
            expected_recording_mbid=expected,
            expected_title="Song",
            observed_acoustid_mbids_json="[]",
            acoustid_score=0.9,
            fingerprint_duration_sec=180,
            verification_reason="ambiguous",
            review_state=ReviewDecision.pending,
        )
    )
    await db_session.flush()
    state = await run_acoustid_verification(
        track,
        acoustid_raw_results=[
            {
                "score": 0.95,
                "recordings": [
                    {"id": expected},
                    {"id": "22222222-2222-2222-2222-222222222222"},
                ],
            }
        ],
        fingerprint_duration_sec=180,
        db=db_session,
    )
    assert state == AcoustIDVerificationState.verified
    item = await db_session.scalar(
        select(StagingReviewItem).where(StagingReviewItem.track_id == track.id)
    )
    assert item is None


async def test_auto_import_starts_with_first_verified_track_of_partial_release(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    from app.services import auto_import

    job = Job(source="slskd", query="album", status=JobStatus.partial)
    release = Release(job=job, source="slskd", title="Album", album_artist="Artist", track_count=2)
    first = Track(
        job=job,
        release=release,
        source="slskd",
        catalog_track_id=1,
        source_path=str(tmp_path / "one.flac"),
        acquisition_state=AcquisitionState.downloaded,
        acoustid_verification_state=AcoustIDVerificationState.verified,
    )
    db_session.add_all([job, release, first])
    await db_session.flush()
    calls: list[object] = []

    async def fake_plan(*args, **kwargs):
        calls.append(("plan", kwargs["track_ids"]))
        return [SimpleNamespace(id=1, status=auto_import.ImportWorkflowState.ready)]

    async def fake_execute(*args, **kwargs):
        calls.append("execute")
        release.import_state = ImportWorkflowState.discovered
        first.import_state = ImportWorkflowState.imported
        return []

    monkeypatch.setattr(auto_import, "plan_release_import", fake_plan)
    monkeypatch.setattr(auto_import, "execute_release_import", fake_execute)

    assert await try_auto_import_release(
        db_session, release, library_root=tmp_path, naming_template="{title}.{ext}"
    )
    assert calls == [("plan", {first.id}), "execute"]
    assert release.import_state != ImportWorkflowState.imported


async def test_missing_track_continuations_are_targeted_and_idempotent(
    db_session: AsyncSession, monkeypatch
) -> None:
    from app.jobs import dispatcher as dispatcher_module

    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=2)
    tracks = [
        CatalogAlbumTrack(album=album, position=1, disc=1, title="One"),
        CatalogAlbumTrack(album=album, position=2, disc=1, title="Two"),
    ]
    parent = Job(
        source="slskd", query="Artist Album", status=JobStatus.partial, catalog_album=album
    )
    db_session.add_all([artist, album, *tracks, parent])
    await db_session.flush()
    dispatched: list[int] = []

    async def fake_dispatch(job_id: int):
        dispatched.append(job_id)
        return SimpleNamespace()

    monkeypatch.setattr(dispatcher_module.job_dispatcher, "dispatch", fake_dispatch)
    missing = [tracks[1].id]
    await _spawn_continuation_jobs(parent, missing, album, db_session)
    await _spawn_continuation_jobs(parent, missing, album, db_session)
    children = list(
        (
            await db_session.scalars(
                select(Job).where(
                    Job.parent_job_id == parent.id, Job.catalog_track_id == tracks[1].id
                )
            )
        ).all()
    )
    assert len(children) == 1
    assert children[0].query == "Artist Album Two"
    assert children[0].source == "priority"
    assert dispatched == [children[0].id]


def test_normalize_for_catalog_match_strips_space_separated_prefix() -> None:
    from app.metadata.filename_parse import normalize_for_catalog_match

    assert normalize_for_catalog_match("01 Intro") == normalize_for_catalog_match("Intro")
    assert normalize_for_catalog_match("10 Karma") == normalize_for_catalog_match("Karma")
    assert normalize_for_catalog_match("1 Track") == normalize_for_catalog_match("Track")


def test_catalog_matching_prefers_disc_position_over_duplicate_title() -> None:
    """Compound disc-track prefix (e.g. '2-01 - Title') must bind to the correct disc
    track even when the same title appears on another disc."""
    catalog = [
        CatalogAlbumTrack(id=1, album_id=1, position=1, disc=1, title="Intro"),
        CatalogAlbumTrack(id=11, album_id=1, position=1, disc=2, title="Intro"),
    ]
    # Disc 2, track 1 — compound prefix wins over title-only match to disc 1
    r_d2 = SearchResult(source="slskd", title="2-01 - Intro")
    assert _catalog_track_for_result(r_d2, catalog, None).id == 11

    # Disc 1, track 1 — compound prefix correctly identifies disc 1
    r_d1 = SearchResult(source="slskd", title="1-01 - Intro")
    assert _catalog_track_for_result(r_d1, catalog, None).id == 1


def test_catalog_matching_explicit_metadata_trackno_preferred_over_title_order() -> None:
    """Explicit track_no+disc metadata must win over title-only match order.

    Two catalog rows share title 'Intro' on the same disc at different positions.
    A result carrying {track_no: 2, disc: 1} must bind to position 2, not position 1.
    """
    catalog = [
        CatalogAlbumTrack(id=1, album_id=1, position=1, disc=1, title="Intro"),
        CatalogAlbumTrack(id=2, album_id=1, position=2, disc=1, title="Intro"),
    ]
    result = SearchResult(source="slskd", title="Intro", metadata={"track_no": 2, "disc": 1})
    assert _catalog_track_for_result(result, catalog, None).id == 2


def test_catalog_matching_simple_numbered_prefix_preferred_over_title_order() -> None:
    """Simple numbered prefix '02 Intro' must bind to position 2 before title scan.

    Without the fix, the title scan for 'Intro' returns position 1 (first list entry).
    """
    catalog = [
        CatalogAlbumTrack(id=1, album_id=1, position=1, disc=1, title="Intro"),
        CatalogAlbumTrack(id=2, album_id=1, position=2, disc=1, title="Intro"),
    ]
    result = SearchResult(source="slskd", title="02 Intro")
    assert _catalog_track_for_result(result, catalog, None).id == 2
