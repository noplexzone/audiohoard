from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.jobs.runner import (
    _catalog_disc_total,
    _catalog_track_for_result,
    _fetch_slskd_album_results,
    _ParentTerminalEvidence,
    _spawn_continuation_jobs,
)
from app.metadata.filename_parse import parsed_position_evidence
from app.models.acquisition_claim import AcquisitionDispatchClaim
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchJobRole,
    DiscographyBatchState,
    DiscographyJobOwnership,
    DiscographyScopeKind,
)
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
from app.schemas.search import SearchRequest, SearchResult
from app.services.acoustid_verification import run_acoustid_verification
from app.services.auto_import import try_auto_import_release
from app.services.discography_batches import cancel_discography_batch
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


def test_slskd_album_folder_grouping_combines_sibling_disc_directories() -> None:
    folders = group_slskd_files_into_folders(
        [
            _folder_response("peer", "Morgan Wallen\\Dangerous\\CD1", "flac", 15, 0),
            _folder_response("peer", "Morgan Wallen\\Dangerous\\CD2", "flac", 18, 0),
        ]
    )

    assert len(folders) == 1
    assert folders[0].parent_dir == "Morgan Wallen/Dangerous"
    assert len(folders[0].files) == 33
    assert {item.disc for item in folders[0].files} == {1, 2}


async def test_slskd_album_fetch_preserves_disc_directory_position_metadata(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Morgan Wallen")
    album = CatalogAlbum(artist=artist, title="Dangerous: The Double Album (Bonus)", track_count=4)
    album.tracks.extend(
        [
            CatalogAlbumTrack(id=1, album_id=1, position=1, disc=1, title="Sand In My Boots"),
            CatalogAlbumTrack(id=2, album_id=1, position=2, disc=1, title="Wasted On You"),
            CatalogAlbumTrack(id=3, album_id=1, position=1, disc=2, title="Still Goin Down"),
            CatalogAlbumTrack(
                id=4, album_id=1, position=2, disc=2, title="Rednecks, Red Letters, Red Dirt"
            ),
        ]
    )

    class Adapter:
        async def search_album_folders(self, request: SearchRequest):
            return group_slskd_files_into_folders(
                [
                    _folder_response("peer", "Morgan Wallen\\Dangerous\\CD1", "flac", 2, 0),
                    _folder_response("peer", "Morgan Wallen\\Dangerous\\CD2", "flac", 2, 0),
                ]
            ), []

    runtime = SimpleNamespace(
        quality_profile=SimpleNamespace(
            format_preference=["flac", "mp3"],
            min_mp3_bitrate=320,
            allow_lower_quality_fallback=True,
        )
    )

    results = await _fetch_slskd_album_results(
        Adapter(),
        SearchRequest(query="Dangerous"),
        Job(source="slskd"),
        album,
        runtime,
        db_session,
    )

    assert len(results) == 4
    assert [item.metadata.get("disc") for item in results] == [1, 1, 2, 2]
    assert _catalog_track_for_result(results[2], album.tracks, None).title == "Still Goin Down"


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


def test_catalog_disc_total_detects_multidisc_manifest() -> None:
    tracks = [
        CatalogAlbumTrack(position=1, disc=1, title="One"),
        CatalogAlbumTrack(position=1, disc=2, title="Two"),
        CatalogAlbumTrack(position=1, disc=3, title="Three"),
    ]

    assert _catalog_disc_total(tracks) == 3


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
    sibling = Track(
        job=job,
        release=release,
        source="slskd",
        catalog_track_id=2,
        source_path=str(tmp_path / "two.flac"),
        acquisition_state=AcquisitionState.downloaded,
        acoustid_verification_state=AcoustIDVerificationState.verified,
    )
    db_session.add_all([job, release, first, sibling])
    await db_session.flush()
    calls: list[object] = []

    async def fake_plan(*args, **kwargs):
        calls.append(("plan", kwargs["track_ids"], kwargs["source_artifacts"]))
        first.title = "Persisted before execution"
        await db_session.flush()
        return [SimpleNamespace(id=1, status=auto_import.ImportWorkflowState.ready)]

    async def fake_execute(*args, **kwargs):
        calls.append(("execute", db_session.in_transaction()))
        release.import_state = ImportWorkflowState.discovered
        first.import_state = ImportWorkflowState.imported
        return []

    monkeypatch.setattr(auto_import, "plan_release_import", fake_plan)
    monkeypatch.setattr(auto_import, "execute_release_import", fake_execute)

    artifact = {first.id: (tmp_path / "one.flac", "approved-hash")}
    assert await try_auto_import_release(
        db_session,
        release,
        library_root=tmp_path,
        naming_template="{title}.{ext}",
        source_artifacts=artifact,
    )
    assert calls == [("plan", {first.id}, artifact), ("execute", False)]
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
    await db_session.commit()
    dispatched: list[int] = []

    async def fake_dispatch(job_id: int):
        dispatched.append(job_id)
        return SimpleNamespace()

    monkeypatch.setattr(dispatcher_module.job_dispatcher, "dispatch", fake_dispatch)
    missing = [tracks[1].id]
    first_ids = await _spawn_continuation_jobs(parent.id, missing, album.id, db_session)
    second_ids = await _spawn_continuation_jobs(parent.id, missing, album.id, db_session)
    for continuation_id in [*first_ids, *second_ids]:
        await dispatcher_module.job_dispatcher.dispatch(continuation_id)
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
    assert children[0].query == "Artist Two"
    assert children[0].source == "priority"
    assert dispatched == [children[0].id]


async def test_release_root_continuation_links_all_current_active_batch_observers(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=2)
    tracks = [
        CatalogAlbumTrack(album=album, position=1, disc=1, title="One"),
        CatalogAlbumTrack(album=album, position=2, disc=1, title="Two"),
    ]
    parent = Job(
        source="priority",
        query="Artist Album",
        status=JobStatus.partial,
        catalog_album=album,
        catalog_track_id=None,
    )
    batches = [
        DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json="{}",
            scope_hash=character * 64,
            state=state,
        )
        for character, state in (
            ("a", DiscographyBatchState.running),
            ("b", DiscographyBatchState.running),
            ("c", DiscographyBatchState.cancelled),
            ("d", DiscographyBatchState.running),
        )
    ]
    items = [
        DiscographyBatchItem(
            batch=batch,
            release_identity=f"catalog_album:{index}",
            catalog_album=album,
            artist_name="Artist",
            release_title="Album",
            state=(
                DiscographyBatchItemState.cancelled
                if batch.state == DiscographyBatchState.cancelled
                else DiscographyBatchItemState.waiting
            ),
            execution_generation=2 if index == 4 else 1,
        )
        for index, batch in enumerate(batches, 1)
    ]
    db_session.add_all([artist, album, *tracks, parent, *batches, *items])
    await db_session.flush()
    for index, item in enumerate(items):
        db_session.add(
            DiscographyBatchItemJob(
                item_id=item.id,
                job_id=parent.id,
                generation=1,
                catalog_track_id=None,
                ownership=(
                    DiscographyJobOwnership.created
                    if index == 0
                    else DiscographyJobOwnership.observed
                ),
                role=DiscographyBatchJobRole.release_root,
            )
        )
    await db_session.commit()

    first_ids = await _spawn_continuation_jobs(parent.id, [tracks[1].id], album.id, db_session)
    second_ids = await _spawn_continuation_jobs(parent.id, [tracks[1].id], album.id, db_session)

    assert len(first_ids) == 1
    assert second_ids == []
    child = await db_session.get(Job, first_ids[0])
    assert child is not None and child.catalog_track_id == tracks[1].id
    fallback_links = list(
        (
            await db_session.scalars(
                select(DiscographyBatchItemJob)
                .where(DiscographyBatchItemJob.role == DiscographyBatchJobRole.track_fallback)
                .order_by(DiscographyBatchItemJob.item_id)
            )
        ).all()
    )
    assert [(link.item_id, link.ownership) for link in fallback_links] == [
        (items[0].id, DiscographyJobOwnership.created),
        (items[1].id, DiscographyJobOwnership.observed),
    ]


async def test_cancelled_batch_created_ancestor_blocks_late_continuation(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=1)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
    parent = Job(
        source="slskd",
        query="Artist Album",
        status=JobStatus.partial,
        catalog_album=album,
    )
    batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.wanted_selected,
        scope_json="{}",
        scope_hash="b" * 64,
        state=DiscographyBatchState.cancelled,
    )
    item = DiscographyBatchItem(
        batch=batch,
        release_identity="catalog_album:1",
        catalog_album=album,
        artist_name="Artist",
        release_title="Album",
    )
    db_session.add_all([artist, album, track, parent, batch, item])
    await db_session.flush()
    db_session.add(
        DiscographyBatchItemJob(
            item_id=item.id,
            job_id=parent.id,
            ownership=DiscographyJobOwnership.created,
        )
    )
    await db_session.commit()
    parent_id, track_id, album_id = parent.id, track.id, album.id

    continuation_ids = await _spawn_continuation_jobs(parent_id, [track_id], album_id, db_session)

    assert continuation_ids == []
    assert await db_session.scalar(select(Job.id).where(Job.parent_job_id == parent_id)) is None


async def test_continuation_locked_commit_reconstructs_without_committing_caller_mutation(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=1)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
    parent = Job(
        source="slskd", query="Artist Album", status=JobStatus.partial, catalog_album=album
    )
    db_session.add_all([artist, album, track, parent])
    await db_session.commit()
    parent_id, album_id, track_id = parent.id, album.id, track.id

    # This stale caller-side mutation must be rolled back, not swept into the
    # continuation transaction by its commit.
    parent.status = JobStatus.done
    original_commit = AsyncSession.commit
    injected_lock = False

    async def lock_first_continuation_commit(self: AsyncSession) -> None:
        nonlocal injected_lock
        jobs = [obj for obj in self.sync_session.identity_map.values() if isinstance(obj, Job)]
        if not injected_lock and any(job.parent_job_id == parent_id for job in jobs):
            injected_lock = True
            raise OperationalError("INSERT INTO jobs", {}, Exception("database is locked"))
        await original_commit(self)

    monkeypatch.setattr(AsyncSession, "commit", lock_first_continuation_commit)

    continuation_ids = await _spawn_continuation_jobs(parent_id, [track_id], album_id, db_session)

    db_session.expire_all()
    persisted_parent = await db_session.get(Job, parent_id)
    continuations = list(
        (
            await db_session.scalars(
                select(Job).where(
                    Job.parent_job_id == parent_id,
                    Job.catalog_track_id == track_id,
                )
            )
        ).all()
    )
    assert injected_lock is True
    assert persisted_parent is not None
    assert persisted_parent.status == JobStatus.partial
    assert [job.id for job in continuations] == continuation_ids
    assert len(continuations) == 1


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


async def test_exact_track_continuation_parent_remains_supported(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=1)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
    parent = Job(
        source="priority",
        query="Artist One",
        status=JobStatus.partial,
        catalog_album=album,
        catalog_track=track,
        partial_attempt=1,
    )
    db_session.add_all([artist, album, track, parent])
    await db_session.commit()

    continuation_ids = await _spawn_continuation_jobs(parent.id, [track.id], album.id, db_session)

    assert len(continuation_ids) == 1
    child = await db_session.get(Job, continuation_ids[0])
    assert child is not None
    assert child.parent_job_id == parent.id
    assert child.catalog_track_id == track.id
    assert child.partial_attempt == 2


async def test_batch_fallback_retry_advances_exact_role_link(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=1)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
    root = Job(
        source="priority",
        query="Artist Album",
        status=JobStatus.partial,
        catalog_album=album,
    )
    batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.wanted_selected,
        scope_json="{}",
        scope_hash="e" * 64,
        state=DiscographyBatchState.running,
    )
    item = DiscographyBatchItem(
        batch=batch,
        release_identity="catalog_album:retry",
        catalog_album=album,
        artist_name="Artist",
        release_title="Album",
        state=DiscographyBatchItemState.waiting,
    )
    db_session.add_all([artist, album, track, root, batch, item])
    await db_session.flush()
    db_session.add(
        DiscographyBatchItemJob(
            item_id=item.id,
            job_id=root.id,
            generation=1,
            ownership=DiscographyJobOwnership.created,
            role=DiscographyBatchJobRole.release_root,
        )
    )
    await db_session.commit()
    first_ids = await _spawn_continuation_jobs(root.id, [track.id], album.id, db_session)
    assert len(first_ids) == 1
    first = await db_session.get(Job, first_ids[0])
    assert first is not None
    first.status = JobStatus.partial
    await db_session.commit()

    retry_ids = await _spawn_continuation_jobs(first.id, [track.id], album.id, db_session)

    assert len(retry_ids) == 1
    retry = await db_session.get(Job, retry_ids[0])
    assert retry is not None and retry.parent_job_id == first.id
    fallback_link = await db_session.scalar(
        select(DiscographyBatchItemJob).where(
            DiscographyBatchItemJob.item_id == item.id,
            DiscographyBatchItemJob.role == DiscographyBatchJobRole.track_fallback,
            DiscographyBatchItemJob.catalog_track_id == track.id,
        )
    )
    assert fallback_link is not None
    assert fallback_link.job_id == retry.id
    assert fallback_link.ownership == DiscographyJobOwnership.created


async def test_stale_release_root_generation_cannot_spawn_orphan_fallback(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=1)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
    root = Job(
        source="priority", query="Artist Album", status=JobStatus.partial, catalog_album=album
    )
    batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.wanted_selected,
        scope_json="{}",
        scope_hash="f" * 64,
        state=DiscographyBatchState.running,
    )
    item = DiscographyBatchItem(
        batch=batch,
        release_identity="catalog_album:stale",
        catalog_album=album,
        artist_name="Artist",
        release_title="Album",
        state=DiscographyBatchItemState.waiting,
        execution_generation=2,
    )
    db_session.add_all([artist, album, track, root, batch, item])
    await db_session.flush()
    db_session.add(
        DiscographyBatchItemJob(
            item_id=item.id,
            job_id=root.id,
            generation=1,
            ownership=DiscographyJobOwnership.created,
            role=DiscographyBatchJobRole.release_root,
        )
    )
    await db_session.commit()
    root_id, track_id, album_id = root.id, track.id, album.id

    continuation_ids = await _spawn_continuation_jobs(root_id, [track_id], album_id, db_session)

    assert continuation_ids == []
    assert await db_session.scalar(select(Job.id).where(Job.parent_job_id == root_id)) is None


async def test_parent_retry_between_terminal_commit_and_spawn_creates_nothing(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'parent-retry.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as seed:
            artist = CatalogArtist(name="Artist")
            album = CatalogAlbum(artist=artist, title="Album", track_count=1)
            track = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
            parent = Job(
                source="priority",
                query="Artist Album",
                status=JobStatus.partial,
                result_json='{"missing_catalog_track_ids": [1]}',
                catalog_album=album,
            )
            seed.add_all([artist, album, track, parent])
            await seed.commit()
            parent_id, album_id, track_id = parent.id, album.id, track.id
            evidence = _ParentTerminalEvidence(
                status=parent.status,
                result_json=parent.result_json,
                updated_at=parent.updated_at,
                execution_token="finished-generation",
            )

        async with factory() as retry:
            retried = await retry.get(Job, parent_id)
            assert retried is not None
            retried.status = JobStatus.pending
            retried.result_json = None
            await retry.commit()

        async with factory() as stale:
            continuation_ids = await _spawn_continuation_jobs(
                parent_id,
                [track_id],
                album_id,
                stale,
                terminal_evidence=evidence,
            )

        assert continuation_ids == []
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 1
            assert await observer.scalar(select(func.count(AcquisitionDispatchClaim.id))) == 0
    finally:
        await engine.dispose()


async def test_cancelling_shared_root_creator_transfers_ownership_to_active_observer(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=1)
    track = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
    root = Job(
        source="priority", query="Artist Album", status=JobStatus.pending, catalog_album=album
    )
    batches = [
        DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json="{}",
            scope_hash=character * 64,
            state=DiscographyBatchState.running,
        )
        for character in ("1", "2")
    ]
    items = [
        DiscographyBatchItem(
            batch=batch,
            release_identity=f"catalog_album:shared-{index}",
            catalog_album=album,
            artist_name="Artist",
            release_title="Album",
            state=DiscographyBatchItemState.waiting,
        )
        for index, batch in enumerate(batches)
    ]
    db_session.add_all([artist, album, track, root, *batches, *items])
    await db_session.flush()
    links = [
        DiscographyBatchItemJob(
            item_id=item.id,
            job_id=root.id,
            generation=1,
            ownership=ownership,
            role=DiscographyBatchJobRole.release_root,
        )
        for item, ownership in zip(
            items,
            (DiscographyJobOwnership.created, DiscographyJobOwnership.observed),
            strict=True,
        )
    ]
    db_session.add_all(links)
    await db_session.commit()
    root_id = root.id
    track_id = track.id
    album_id = album.id
    creator_batch_id = batches[0].id
    observer_item_id = items[1].id

    await cancel_discography_batch(db_session, creator_batch_id)
    db_session.expire_all()
    stored_root = await db_session.get(Job, root_id)
    stored_links = list(
        (
            await db_session.scalars(
                select(DiscographyBatchItemJob).order_by(DiscographyBatchItemJob.item_id)
            )
        ).all()
    )
    assert stored_root is not None and stored_root.status == JobStatus.pending
    assert [link.ownership for link in stored_links] == [
        DiscographyJobOwnership.observed,
        DiscographyJobOwnership.created,
    ]

    stored_root.status = JobStatus.partial
    await db_session.commit()
    fallback_ids = await _spawn_continuation_jobs(stored_root.id, [track_id], album_id, db_session)
    assert len(fallback_ids) == 1
    fallback_link = await db_session.scalar(
        select(DiscographyBatchItemJob).where(
            DiscographyBatchItemJob.item_id == observer_item_id,
            DiscographyBatchItemJob.role == DiscographyBatchJobRole.track_fallback,
        )
    )
    assert fallback_link is not None
    assert fallback_link.ownership == DiscographyJobOwnership.created
