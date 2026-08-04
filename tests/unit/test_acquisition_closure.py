from __future__ import annotations

import json
from datetime import UTC
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.jobs.runner import _fetch_slskd_album_results, _without_blocked_slskd_results
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.staging_review import StagingReviewItem
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
)
from app.schemas.search import SearchRequest, SearchResult
from app.services import acquisition_cleanup, acquisition_recovery
from app.services.acoustid_verification import run_acoustid_verification
from app.services.acquisition_cleanup import (
    ImportedSourceCleanup,
    cleanup_imported_sources,
    pending_imported_source_cleanups,
    prune_orphaned_terminal_records,
)
from app.services.acquisition_recovery import recover_approved_downloads
from app.settings_service import get_runtime_settings, save_runtime_settings


async def _verify(db: AsyncSession, score: float, observed: str, expected: str | None = None):
    job = Job(source="slskd", query="x", status=JobStatus.running)
    release = Release(job=job, source="slskd", title="Album")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song",
        catalog_track_id=1,
        mbid=expected,
        identity_state=IdentityResolutionState.resolved,
        acquisition_state=AcquisitionState.downloaded,
    )
    db.add_all([job, release, track])
    await db.flush()
    state = await run_acoustid_verification(
        track,
        acoustid_raw_results=[{"score": score, "recordings": [{"id": observed}]}],
        fingerprint_duration_sec=180,
        db=db,
        acceptance_threshold=0.90,
    )
    return state


async def test_acoustid_strict_threshold_and_mismatch(db_session: AsyncSession) -> None:
    expected = "11111111-1111-1111-1111-111111111111"
    equal = await _verify(db_session, 0.90, expected, expected)
    above = await _verify(db_session, 0.9001, expected, expected)
    mismatch = await _verify(db_session, 0.999, "22222222-2222-2222-2222-222222222222", expected)
    assert equal != AcoustIDVerificationState.verified
    assert above == AcoustIDVerificationState.verified
    assert mismatch == AcoustIDVerificationState.mismatch
    assert len(list((await db_session.scalars(select(StagingReviewItem))).all())) == 2


async def test_runtime_threshold_and_timeout_defaults_persist(db_session: AsyncSession) -> None:
    runtime = await get_runtime_settings(db_session)
    assert runtime.acoustid_acceptance_threshold == 0.90
    assert runtime.slskd_download_timeout_seconds == 1800
    await save_runtime_settings(
        db_session,
        runtime.source_priority,
        runtime.free_text_result_limit,
        metadata_providers=runtime.metadata_providers,
        primary_metadata_provider=runtime.primary_metadata_provider,
        acoustid_acceptance_threshold=0.95,
        slskd_download_timeout_seconds=600,
    )
    runtime = await get_runtime_settings(db_session)
    assert runtime.acoustid_acceptance_threshold == 0.95
    assert runtime.slskd_download_timeout_seconds == 600


async def test_blocked_slskd_folder_is_not_selected(db_session: AsyncSession) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=2)
    album.tracks.extend(
        [
            CatalogAlbumTrack(position=1, disc=1, title="One"),
            CatalogAlbumTrack(position=2, disc=1, title="Two"),
        ]
    )
    job = Job(source="slskd", query="Artist Album", status=JobStatus.pending, catalog_album=album)
    db_session.add_all(
        [
            artist,
            album,
            job,
            SourceCandidateBlock(
                provider="slskd", peer="blocked", filename="Album\\01 One.flac", reason="timeout"
            ),
        ]
    )
    await db_session.flush()

    class Adapter:
        async def search_album_folders(self, request: SearchRequest):
            from app.services.slskd_scoring import group_slskd_files_into_folders

            raw = [
                {
                    "username": "blocked",
                    "files": [
                        {"filename": "Album\\01 One.flac", "size": 1},
                        {"filename": "Album\\02 Two.flac", "size": 1},
                    ],
                },
                {
                    "username": "alternate",
                    "files": [
                        {"filename": "Album\\01 One.flac", "size": 1},
                        {"filename": "Album\\02 Two.flac", "size": 1},
                    ],
                },
            ]
            return group_slskd_files_into_folders(raw), raw

    runtime = SimpleNamespace(
        quality_profile=SimpleNamespace(
            format_preference=["flac"], min_mp3_bitrate=192, allow_lower_quality_fallback=True
        )
    )
    results = await _fetch_slskd_album_results(
        Adapter(), SearchRequest(query="x"), job, album, runtime, db_session
    )  # type: ignore[arg-type]
    assert {r.metadata["username"] for r in results} == {"alternate"}
    assert [r.metadata["track_no"] for r in results] == [1, 2]


async def test_acoustid_without_expected_identity_rejects_ambiguous_recordings(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="x", status=JobStatus.running)
    release = Release(job=job, source="slskd", title="Album")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song",
        catalog_track_id=1,
        identity_state=IdentityResolutionState.resolved,
        acquisition_state=AcquisitionState.downloaded,
    )
    db_session.add_all([job, release, track])
    await db_session.flush()

    state = await run_acoustid_verification(
        track,
        acoustid_raw_results=[
            {
                "score": 0.999,
                "recordings": [
                    {"id": "11111111-1111-1111-1111-111111111111"},
                    {"id": "22222222-2222-2222-2222-222222222222"},
                ],
            }
        ],
        fingerprint_duration_sec=180,
        db=db_session,
        acceptance_threshold=0.90,
    )

    assert state == AcoustIDVerificationState.unavailable
    assert (
        await db_session.scalar(
            select(StagingReviewItem).where(StagingReviewItem.track_id == track.id)
        )
        is not None
    )


async def test_approved_legacy_release_is_mapped_and_recovered_once(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    artist = CatalogArtist(name="Juice WRLD")
    album = CatalogAlbum(artist=artist, title="WRLD ON DRUGS", track_count=2, monitored=True)
    album.tracks.extend(
        [
            CatalogAlbumTrack(position=1, disc=1, title="Jet Lag"),
            CatalogAlbumTrack(position=2, disc=1, title="Astronauts"),
        ]
    )
    job = Job(
        source="slskd",
        query="Juice WRLD WRLD ON DRUGS",
        status=JobStatus.done,
        catalog_album=album,
    )
    release = Release(
        job=job, source="slskd", title=album.title, album_artist=artist.name, track_count=2
    )
    tracks = [
        Track(
            job=job,
            release=release,
            source="slskd",
            title="01 - Jet Lag",
            source_path=str(tmp_path / "01 - Jet Lag.flac"),
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.approved,
        ),
        Track(
            job=job,
            release=release,
            source="slskd",
            title="02 - Astronauts",
            source_path=str(tmp_path / "02 - Astronauts.flac"),
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.approved,
        ),
    ]
    db_session.add_all([artist, album, job, release, *tracks])
    await db_session.flush()
    calls = 0

    async def fake_import(db, candidate_release, **kwargs):  # noqa: ANN001, ANN003
        nonlocal calls
        calls += 1
        candidate_release.import_state = ImportWorkflowState.imported
        return True

    monkeypatch.setattr(acquisition_recovery, "try_auto_import_release", fake_import)
    settings = get_settings().model_copy(
        update={"library_root": tmp_path / "music", "staging_root": tmp_path}
    )

    assert await recover_approved_downloads(db_session, settings) == 1
    await db_session.flush()
    assert [(track.disc, track.track_no, track.catalog_track_id) for track in tracks] == [
        (1, 1, album.tracks[0].id),
        (1, 2, album.tracks[1].id),
    ]
    assert await recover_approved_downloads(db_session, settings) == 0
    assert calls == 1


async def test_committed_slskd_cleanup_removes_provider_row_and_staged_file(
    monkeypatch, tmp_path
) -> None:
    staged = tmp_path / "staged.flac"
    staged.write_bytes(b"audio")
    calls: list[tuple[str, str, str | None]] = []

    class FakeSlskdAdapter:
        def __init__(self, url: str, api_key: str) -> None:
            pass

        async def cancel(
            self, username: str, filename: str, transfer_id: str | None = None
        ) -> None:
            calls.append((username, filename, transfer_id))

    class FakeSessionContext:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    async def fake_effective_settings(db, settings):  # noqa: ANN001
        return SimpleNamespace(slskd_url="", slskd_api_key="")

    monkeypatch.setattr(acquisition_cleanup, "SlskdAdapter", FakeSlskdAdapter)
    monkeypatch.setattr(
        acquisition_cleanup, "get_session_factory", lambda: lambda: FakeSessionContext()
    )
    monkeypatch.setattr(acquisition_cleanup, "build_effective_settings", fake_effective_settings)
    await cleanup_imported_sources(
        (
            ImportedSourceCleanup(
                None,
                staged,
                json.dumps(
                    {
                        "source": "slskd",
                        "username": "peer",
                        "filename": r"Album\01.flac",
                    }
                ),
                "original-transfer-id",
            ),
        )
    )

    assert calls == [("peer", "Album\\01.flac", "original-transfer-id")]
    assert not staged.exists()


async def test_recovery_limit_counts_eligible_releases_not_older_ineligible_rows(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=1)
    catalog_track = CatalogAlbumTrack(position=1, disc=1, title="Song")
    album.tracks.append(catalog_track)
    job = Job(
        source="slskd",
        query="Artist Album",
        status=JobStatus.done,
        catalog_album=album,
    )
    db_session.add_all([artist, album, job])
    await db_session.flush()
    for index in range(25):
        release = Release(job=job, source="slskd", title=f"Pending {index}", track_count=1)
        release.tracks.append(
            Track(
                job=job,
                source="slskd",
                title="Pending",
                acquisition_state=AcquisitionState.queued,
                acoustid_verification_state=AcoustIDVerificationState.approved,
            )
        )
        db_session.add(release)
    eligible = Release(job=job, source="slskd", title="Album", track_count=1)
    eligible.tracks.append(
        Track(
            job=job,
            source="slskd",
            title="01 - Song",
            source_path=str(tmp_path / "01 - Song.flac"),
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.approved,
        )
    )
    db_session.add(eligible)
    await db_session.flush()

    async def fake_import(db, release, **kwargs):  # noqa: ANN001, ANN003
        release.import_state = ImportWorkflowState.imported
        return True

    monkeypatch.setattr(acquisition_recovery, "try_auto_import_release", fake_import)
    settings = get_settings().model_copy(
        update={"library_root": tmp_path / "music", "staging_root": tmp_path}
    )

    assert await recover_approved_downloads(db_session, settings, limit=1) == 1
    assert eligible.import_state == ImportWorkflowState.imported


async def test_acoustid_without_expected_mbid_requires_matching_recording_title(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="x", status=JobStatus.running)
    release = Release(job=job, source="slskd", title="Album")
    compatible = Track(
        job=job,
        release=release,
        source="slskd",
        title="Catalog Song (feat. Guest)",
        catalog_track_id=1,
        acquisition_state=AcquisitionState.downloaded,
    )
    wrong = Track(
        job=job,
        release=release,
        source="slskd",
        title="Different Song",
        catalog_track_id=2,
        acquisition_state=AcquisitionState.downloaded,
    )
    db_session.add_all([job, release, compatible, wrong])
    await db_session.flush()
    raw = [
        {
            "score": 0.9001,
            "recordings": [
                {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "title": "Catalog Song",
                }
            ],
        }
    ]

    compatible_state = await run_acoustid_verification(
        compatible,
        acoustid_raw_results=raw,
        fingerprint_duration_sec=180,
        db=db_session,
        acceptance_threshold=0.90,
    )
    wrong_state = await run_acoustid_verification(
        wrong,
        acoustid_raw_results=raw,
        fingerprint_duration_sec=180,
        db=db_session,
        acceptance_threshold=0.90,
    )

    assert compatible_state == AcoustIDVerificationState.verified
    assert wrong_state == AcoustIDVerificationState.unavailable


async def test_acoustid_without_expected_mbid_accepts_multiple_matching_recording_titles(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="x", status=JobStatus.running)
    release = Release(job=job, source="slskd", title="Album")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="I Don’t Wanna Break Up",
        catalog_track_id=1,
        duration_sec=243,
        acquisition_state=AcquisitionState.downloaded,
    )
    db_session.add_all([job, release, track])
    await db_session.flush()

    state = await run_acoustid_verification(
        track,
        acoustid_raw_results=[
            {
                "score": 1.0,
                "recordings": [
                    {
                        "id": "11111111-1111-1111-1111-111111111111",
                        "title": "I Don’t Wanna Break Up",
                    },
                    {
                        "id": "22222222-2222-2222-2222-222222222222",
                        "title": "I Don't Wanna Break Up",
                    },
                ],
            }
        ],
        fingerprint_duration_sec=243,
        db=db_session,
        acceptance_threshold=0.90,
    )

    assert state == AcoustIDVerificationState.verified
    assert track.mbid is None
    assert (
        await db_session.scalar(
            select(StagingReviewItem).where(StagingReviewItem.track_id == track.id)
        )
        is None
    )


async def test_acoustid_without_expected_mbid_rejects_matching_title_duration_outlier(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="x", status=JobStatus.running)
    release = Release(job=job, source="slskd", title="Album")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="The Rush",
        catalog_track_id=1,
        duration_sec=186,
        acquisition_state=AcquisitionState.downloaded,
    )
    db_session.add_all([job, release, track])
    await db_session.flush()

    state = await run_acoustid_verification(
        track,
        acoustid_raw_results=[
            {
                "score": 1.0,
                "recordings": [
                    {"id": "11111111-1111-1111-1111-111111111111", "title": "The Rush"}
                ],
            }
        ],
        fingerprint_duration_sec=225,
        db=db_session,
        acceptance_threshold=0.90,
    )

    assert state == AcoustIDVerificationState.unavailable
    assert (
        await db_session.scalar(
            select(StagingReviewItem).where(StagingReviewItem.track_id == track.id)
        )
        is not None
    )


async def test_selected_slskd_result_is_discarded_when_durably_blocked(
    db_session: AsyncSession,
) -> None:
    db_session.add(
        SourceCandidateBlock(
            provider="slskd", peer="blocked", filename=r"Album\01.flac", reason="timeout"
        )
    )
    await db_session.flush()
    selected = SearchResult(
        source="slskd",
        title="Song",
        metadata={"username": "blocked", "filename": r"Album\01.flac"},
    )

    assert await _without_blocked_slskd_results([selected], db_session) == []


async def test_low_confidence_title_evidence_cannot_authorize_high_confidence_mbid(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="x", status=JobStatus.running)
    release = Release(job=job, source="slskd", title="Album")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Catalog Song",
        catalog_track_id=1,
        acquisition_state=AcquisitionState.downloaded,
    )
    db_session.add_all([job, release, track])
    await db_session.flush()
    recording_id = "11111111-1111-1111-1111-111111111111"
    state = await run_acoustid_verification(
        track,
        acoustid_raw_results=[
            {"score": 0.99, "recordings": [{"id": recording_id, "title": "Wrong Song"}]},
            {"score": 0.01, "recordings": [{"id": recording_id, "title": "Catalog Song"}]},
        ],
        fingerprint_duration_sec=180,
        db=db_session,
        acceptance_threshold=0.90,
    )
    assert state == AcoustIDVerificationState.unavailable


async def test_recovery_rotates_past_unmappable_eligible_release(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    blocked_job = Job(source="slskd", query="blocked", status=JobStatus.done)
    blocked_release = Release(job=blocked_job, source="slskd", title="No catalog", track_count=1)
    blocked_release.tracks.append(
        Track(
            job=blocked_job,
            source="slskd",
            title="01 - Song",
            source_path=str(tmp_path / "blocked.flac"),
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.approved,
        )
    )
    db_session.add_all([blocked_job, blocked_release])
    await db_session.flush()

    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=1)
    album.tracks.append(CatalogAlbumTrack(position=1, disc=1, title="Song"))
    good_job = Job(source="slskd", query="good", status=JobStatus.done, catalog_album=album)
    good_release = Release(job=good_job, source="slskd", title="Album", track_count=1)
    good_release.tracks.append(
        Track(
            job=good_job,
            source="slskd",
            title="01 - Song",
            source_path=str(tmp_path / "01 - Song.flac"),
            acquisition_state=AcquisitionState.downloaded,
            acoustid_verification_state=AcoustIDVerificationState.approved,
        )
    )
    db_session.add_all([artist, album, good_job, good_release])
    await db_session.flush()

    async def fake_import(db, release, **kwargs):  # noqa: ANN001, ANN003
        release.import_state = ImportWorkflowState.imported
        return True

    monkeypatch.setattr(acquisition_recovery, "try_auto_import_release", fake_import)
    settings = get_settings().model_copy(
        update={"library_root": tmp_path / "music", "staging_root": tmp_path}
    )
    assert await recover_approved_downloads(db_session, settings, limit=1) == 0
    await db_session.commit()
    assert await recover_approved_downloads(db_session, settings, limit=1) == 1
    assert good_release.import_state == ImportWorkflowState.imported


async def test_recovery_promotes_matching_review_and_imports_partial_release(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    expected = "11111111-1111-1111-1111-111111111111"
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=3)
    catalog_tracks = [
        CatalogAlbumTrack(position=index, disc=1, title=f"Song {index}") for index in range(1, 4)
    ]
    album.tracks.extend(catalog_tracks)
    job = Job(source="slskd", query="partial", status=JobStatus.partial, catalog_album=album)
    release = Release(job=job, source="slskd", title="Album", track_count=3)
    source = tmp_path / "01 - Song 1.flac"
    source.write_bytes(b"audio")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="Song 1",
        source_path=str(source),
        catalog_album_id=album.id,
        catalog_track_id=catalog_tracks[0].id,
        mbid=expected,
        acquisition_state=AcquisitionState.downloaded,
        acoustid_verification_state=AcoustIDVerificationState.unavailable,
    )
    db_session.add_all([artist, album, job, release, track])
    await db_session.flush()
    review = StagingReviewItem(
        track_id=track.id,
        release_id=release.id,
        expected_recording_mbid=expected,
        expected_title=track.title,
        observed_acoustid_mbids_json=json.dumps(
            [expected, "22222222-2222-2222-2222-222222222222"]
        ),
        acoustid_score=0.99,
        fingerprint_duration_sec=180,
        verification_reason="ambiguous",
    )
    db_session.add(review)
    await db_session.flush()

    imported: list[int] = []

    async def fake_import(db, candidate_release, **kwargs):  # noqa: ANN001, ANN003
        imported.append(candidate_release.id)
        track.import_state = ImportWorkflowState.imported
        return True

    monkeypatch.setattr(acquisition_recovery, "try_auto_import_release", fake_import)
    settings = get_settings().model_copy(
        update={"library_root": tmp_path / "music", "staging_root": tmp_path}
    )

    assert await recover_approved_downloads(db_session, settings) == 1
    assert imported == [release.id]
    assert track.acoustid_verification_state == AcoustIDVerificationState.verified
    assert await db_session.get(StagingReviewItem, review.id) is None


async def test_pending_cleanup_rotates_unattempted_plan_before_failed_plan(
    db_session: AsyncSession, tmp_path
) -> None:
    job = Job(source="slskd", query="cleanup", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    tracks = []
    plans = []
    for index in range(2):
        track = Track(job=job, release=release, source="slskd", title=f"Song {index}")
        plan = ImportPlan(
            release=release,
            track=track,
            source_path=str(tmp_path / f"source-{index}.flac"),
            staging_path=str(tmp_path / f"stage-{index}.flac"),
            destination_path=str(tmp_path / f"music-{index}.flac"),
            status=ImportWorkflowState.imported,
        )
        tracks.append(track)
        plans.append(plan)
    from datetime import datetime

    plans[0].cleanup_attempted_at = datetime.now(UTC)
    db_session.add_all([job, release, *tracks, *plans])
    await db_session.flush()

    pending = await pending_imported_source_cleanups(db_session, limit=1)
    assert len(pending) == 1
    assert pending[0].plan_id == plans[1].id


async def test_prune_orphaned_terminal_records_removes_only_rows_without_files(
    db_session: AsyncSession, tmp_path
) -> None:
    kept_source = tmp_path / "kept.flac"
    kept_source.write_bytes(b"audio")
    imported_destination = tmp_path / "library" / "imported.flac"
    imported_destination.parent.mkdir()
    imported_destination.write_bytes(b"audio")

    mixed_job = Job(source="slskd", query="mixed", status=JobStatus.partial)
    mixed_release = Release(job=mixed_job, source="slskd", title="Mixed")
    orphan_track = Track(job=mixed_job, release=mixed_release, source="slskd", title="Gone")
    kept_track = Track(
        job=mixed_job,
        release=mixed_release,
        source="slskd",
        title="Kept",
        source_path=str(kept_source),
    )
    imported_track = Track(
        job=mixed_job,
        release=mixed_release,
        source="slskd",
        title="Imported",
        import_state=ImportWorkflowState.imported,
    )
    imported_plan = ImportPlan(
        release=mixed_release,
        track=imported_track,
        source_path=str(tmp_path / "gone-source.flac"),
        destination_path=str(imported_destination),
        status=ImportWorkflowState.imported,
    )
    empty_job = Job(source="slskd", query="empty", status=JobStatus.failed)
    empty_release = Release(job=empty_job, source="slskd", title="Empty")
    empty_track = Track(job=empty_job, release=empty_release, source="slskd", title="Gone")
    active_job = Job(source="slskd", query="active", status=JobStatus.running)
    active_track = Track(job=active_job, source="slskd", title="Still active")
    db_session.add_all(
        [
            mixed_job,
            mixed_release,
            orphan_track,
            kept_track,
            imported_track,
            imported_plan,
            empty_job,
            empty_release,
            empty_track,
            active_job,
            active_track,
        ]
    )
    await db_session.flush()
    ids = {
        "mixed_job": mixed_job.id,
        "orphan_track": orphan_track.id,
        "kept_track": kept_track.id,
        "imported_track": imported_track.id,
        "empty_job": empty_job.id,
        "active_track": active_track.id,
    }

    result = await prune_orphaned_terminal_records(db_session, batch_size=1)

    assert result.tracks == 2
    assert result.releases == 1
    assert result.jobs == 1
    assert await db_session.get(Track, ids["orphan_track"]) is None
    assert await db_session.get(Job, ids["empty_job"]) is None
    assert await db_session.get(Track, ids["kept_track"]) is not None
    assert await db_session.get(Track, ids["imported_track"]) is not None
    assert await db_session.get(Track, ids["active_track"]) is not None
    assert await db_session.get(Job, ids["mixed_job"]) is not None


# ---------------------------------------------------------------------------
# Empty-directory pruning after import cleanup
# ---------------------------------------------------------------------------


async def test_cleanup_prunes_empty_parent_dirs_within_staging_root(
    test_settings, tmp_path
) -> None:
    staging_root = test_settings.staging_root
    nested = staging_root / "Artist" / "Album"
    nested.mkdir(parents=True)
    staged = nested / "track.flac"
    staged.write_bytes(b"audio")

    await cleanup_imported_sources((ImportedSourceCleanup(None, staged, None),))

    assert not staged.exists()
    assert not nested.exists()
    assert not (staging_root / "Artist").exists()
    assert staging_root.exists()


async def test_cleanup_stops_pruning_at_non_empty_parent(test_settings, tmp_path) -> None:
    staging_root = test_settings.staging_root
    artist_dir = staging_root / "Artist"
    album_dir = artist_dir / "Album"
    album_dir.mkdir(parents=True)
    staged = album_dir / "track.flac"
    staged.write_bytes(b"audio")
    sibling = artist_dir / "cover.jpg"
    sibling.write_bytes(b"image")

    await cleanup_imported_sources((ImportedSourceCleanup(None, staged, None),))

    assert not staged.exists()
    assert not album_dir.exists()
    assert artist_dir.exists()
    assert sibling.exists()


async def test_cleanup_never_removes_staging_root(test_settings, tmp_path) -> None:
    staging_root = test_settings.staging_root
    staging_root.mkdir(exist_ok=True)
    staged = staging_root / "track.flac"
    staged.write_bytes(b"audio")

    await cleanup_imported_sources((ImportedSourceCleanup(None, staged, None),))

    assert not staged.exists()
    assert staging_root.exists()


async def test_cleanup_skips_symlink_dir(test_settings, tmp_path) -> None:
    staging_root = test_settings.staging_root
    staging_root.mkdir(exist_ok=True)
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    link_dir = staging_root / "link"
    link_dir.symlink_to(real_dir)
    staged = link_dir / "track.flac"
    staged.write_bytes(b"audio")

    await cleanup_imported_sources((ImportedSourceCleanup(None, staged, None),))

    assert not staged.exists()
    assert link_dir.exists()


async def test_cleanup_does_not_prune_outside_staging_root(test_settings, tmp_path) -> None:
    staging_root = test_settings.staging_root
    staging_root.mkdir(exist_ok=True)
    outside_dir = tmp_path / "other" / "Artist"
    outside_dir.mkdir(parents=True)
    staged = outside_dir / "track.flac"
    staged.write_bytes(b"audio")

    await cleanup_imported_sources((ImportedSourceCleanup(None, staged, None),))

    assert not staged.exists()
    assert outside_dir.exists()


async def test_cleanup_dir_prune_oserror_retains_obligation(
    test_settings, tmp_path, monkeypatch
) -> None:
    # A prune failure keeps staging_path set so startup can retry, even though the
    # file itself is already gone (unlink uses missing_ok=True).
    staging_root = test_settings.staging_root
    staging_root.mkdir(exist_ok=True)
    staged = staging_root / "track.flac"
    staged.write_bytes(b"audio")

    completed_flags: list[bool] = []

    async def fake_mark(plan_id: int | None, *, completed: bool) -> None:
        completed_flags.append(completed)

    def raising_prune(path, root) -> None:  # noqa: ANN001
        raise OSError("simulated dir-prune failure")

    monkeypatch.setattr(acquisition_cleanup, "_mark_cleanup_attempted", fake_mark)
    monkeypatch.setattr(acquisition_cleanup, "_prune_empty_parents", raising_prune)

    await cleanup_imported_sources((ImportedSourceCleanup(None, staged, None),))

    assert not staged.exists()
    assert completed_flags == [False]


async def _pending_cleanup_fixture(
    db_session: AsyncSession, tmp_path
) -> tuple[ImportedSourceCleanup, ImportPlan, Track]:
    staged = tmp_path / "staged.flac"
    staged.write_bytes(b"old-audio")
    job = Job(source="slskd", query="cleanup", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        source_job_id="old-transfer",
        staging_path=str(staged),
        acquisition_provenance_json=json.dumps(
            {"source": "slskd", "username": "peer", "filename": "song.flac"}
        ),
    )
    plan = ImportPlan(
        release=release,
        track=track,
        source_path=str(staged),
        staging_path=str(staged),
        destination_path=str(tmp_path / "library" / "song.flac"),
        status=ImportWorkflowState.imported,
    )
    db_session.add_all([job, release, track, plan])
    await db_session.commit()
    pending = await pending_imported_source_cleanups(db_session)
    assert len(pending) == 1
    return pending[0], plan, track


async def test_stale_cleanup_cannot_unlink_or_clear_reassigned_plan(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    item, plan, _ = await _pending_cleanup_fixture(db_session, tmp_path)
    replacement = tmp_path / "replacement.flac"
    replacement.write_bytes(b"replacement")
    plan.staging_path = str(replacement)
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)

    await cleanup_imported_sources((item,))

    assert item.staged_path.exists()
    async with factory() as verify:
        current = await verify.get(ImportPlan, plan.id)
        assert current is not None
        assert current.staging_path == str(replacement)
        assert current.cleanup_attempted_at is None


async def test_stale_cleanup_cannot_cancel_reassigned_transfer(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    item, _, track = await _pending_cleanup_fixture(db_session, tmp_path)
    track.source_job_id = "replacement-transfer"
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)
    calls: list[str | None] = []

    class FakeSlskdAdapter:
        def __init__(self, url: str, api_key: str) -> None:
            pass

        async def cancel(self, username: str, filename: str, transfer_id: str | None = None):
            calls.append(transfer_id)

    async def fake_effective_settings(db, settings):  # noqa: ANN001
        return SimpleNamespace(slskd_url="", slskd_api_key="")

    monkeypatch.setattr(acquisition_cleanup, "SlskdAdapter", FakeSlskdAdapter)
    monkeypatch.setattr(acquisition_cleanup, "build_effective_settings", fake_effective_settings)

    await cleanup_imported_sources((item,))

    assert calls == []
    assert item.staged_path.exists()


async def test_cleanup_refuses_changed_filesystem_identity(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    item, plan, _ = await _pending_cleanup_fixture(db_session, tmp_path)
    item.staged_path.unlink()
    item.staged_path.write_bytes(b"replacement-audio")
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)

    await cleanup_imported_sources((item,))

    assert item.staged_path.read_bytes() == b"replacement-audio"
    async with factory() as verify:
        current = await verify.get(ImportPlan, plan.id)
        assert current is not None
        assert current.staging_path == str(item.staged_path)


async def test_stale_cleanup_cannot_unlink_active_destination_owner(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    item, stale_plan, _ = await _pending_cleanup_fixture(db_session, tmp_path)
    stale_plan.destination_path = str(item.staged_path)
    active_track = Track(job=stale_plan.release.job, release=stale_plan.release, source="slskd")
    active_plan = ImportPlan(
        release=stale_plan.release,
        track=active_track,
        source_path=str(tmp_path / "other-source.flac"),
        destination_path=str(item.staged_path),
        status=ImportWorkflowState.importing,
        file_state=LibraryFileState.unknown,
    )
    db_session.add_all([active_track, active_plan])
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)

    await cleanup_imported_sources((item,))

    assert item.staged_path.exists()
    async with factory() as verify:
        current = await verify.get(ImportPlan, stale_plan.id)
        assert current is not None
        assert current.staging_path == str(item.staged_path)


async def test_stale_cleanup_cannot_unlink_when_track_staging_path_changes(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    item, plan, track = await _pending_cleanup_fixture(db_session, tmp_path)
    track.staging_path = str(tmp_path / "new-track-stage.flac")
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)

    await cleanup_imported_sources((item,))

    assert item.staged_path.exists()
    async with factory() as verify:
        current = await verify.get(ImportPlan, plan.id)
        assert current is not None
        assert current.staging_path == str(item.staged_path)
        assert current.cleanup_attempted_at is None


async def test_cleanup_marker_retry_refetches_and_clears_current_obligation(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    item, plan, _ = await _pending_cleanup_fixture(db_session, tmp_path)
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)
    original_commit = AsyncSession.commit
    attempts = 0

    async def lock_once(session: AsyncSession) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            from sqlalchemy.exc import OperationalError

            raise OperationalError("UPDATE import_plans", {}, Exception("database is locked"))
        await original_commit(session)

    async def no_sleep(delay: float) -> None:
        assert delay == 0.25

    monkeypatch.setattr(AsyncSession, "commit", lock_once)
    monkeypatch.setattr(acquisition_cleanup.asyncio, "sleep", no_sleep)

    await acquisition_cleanup._mark_cleanup_attempted(item, completed=True)

    assert attempts == 2
    async with factory() as verify:
        current = await verify.get(ImportPlan, plan.id)
        assert current is not None
        assert current.staging_path is None
        assert current.cleanup_attempted_at is not None


async def test_quarantine_cleanup_preserves_replacement_at_original_path(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    item, plan, track = await _pending_cleanup_fixture(db_session, tmp_path)
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)

    class ReplacingAdapter:
        def __init__(self, url: str, api_key: str) -> None:
            pass

        async def cancel(
            self, username: str, filename: str, transfer_id: str | None = None
        ) -> bool:
            item.staged_path.write_bytes(b"replacement")
            return True

    async def fake_effective_settings(db, settings):  # noqa: ANN001
        return SimpleNamespace(slskd_url="", slskd_api_key="")

    monkeypatch.setattr(acquisition_cleanup, "SlskdAdapter", ReplacingAdapter)
    monkeypatch.setattr(acquisition_cleanup, "build_effective_settings", fake_effective_settings)

    await cleanup_imported_sources((item,))

    assert item.staged_path.read_bytes() == b"replacement"
    assert list(tmp_path.glob(".*.audiohoard-cleanup-*")) == []
    async with factory() as verify:
        persisted_plan = await verify.get(ImportPlan, plan.id)
        persisted_track = await verify.get(Track, track.id)
        assert persisted_plan is not None and persisted_plan.staging_path is None
        assert persisted_track is not None and persisted_track.staging_path is None
