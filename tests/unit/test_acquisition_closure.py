from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import UTC
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.jobs.runner import _fetch_slskd_album_results, _without_blocked_slskd_results
from app.models.acquisition_attempt import AcquisitionAttempt, CleanupState
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
from app.services.acoustid_verification import (
    reconcile_matching_acoustid_reviews,
    run_acoustid_verification,
)
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


async def test_acoustid_evidence_persists_sanitized_per_recording_details(
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
        mbid="11111111-1111-1111-1111-111111111111",
        identity_state=IdentityResolutionState.resolved,
        acquisition_state=AcquisitionState.downloaded,
    )
    db_session.add_all([job, release, track])
    await db_session.flush()

    await run_acoustid_verification(
        track,
        acoustid_raw_results=[
            {
                "id": "raw-result-id-must-not-be-copied",
                "score": 0.98,
                "recordings": [
                    {
                        "id": "22222222-2222-2222-2222-222222222222",
                        "title": "Song",
                        "artists": [{"id": "artist-id", "name": "Artist"}],
                        "releasegroups": [{"title": "must not be copied"}],
                    }
                ],
            }
        ],
        fingerprint_duration_sec=180,
        db=db_session,
        acceptance_threshold=0.90,
    )

    evidence = json.loads(track.acoustid_evidence_json or "{}")
    assert evidence["recordings"] == [
        {
            "artist": "Artist",
            "mbid": "22222222-2222-2222-2222-222222222222",
            "score": 0.98,
            "title": "Song",
        }
    ]
    assert "raw-result-id-must-not-be-copied" not in track.acoustid_evidence_json
    assert "releasegroups" not in track.acoustid_evidence_json


async def test_reconciliation_uses_expected_mbids_own_strict_score(
    db_session: AsyncSession,
) -> None:
    expected = "11111111-1111-1111-1111-111111111111"
    unrelated = "22222222-2222-2222-2222-222222222222"
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
    db_session.add_all([job, release, track])
    await db_session.flush()
    await run_acoustid_verification(
        track,
        acoustid_raw_results=[
            {"score": 0.99, "recordings": [{"id": unrelated}]},
            {"score": 0.90, "recordings": [{"id": expected}]},
        ],
        fingerprint_duration_sec=180,
        db=db_session,
        acceptance_threshold=0.90,
    )
    assert await reconcile_matching_acoustid_reviews(db_session, acceptance_threshold=0.90) == 0
    assert (
        await db_session.scalar(
            select(StagingReviewItem).where(StagingReviewItem.track_id == track.id)
        )
        is not None
    )


async def test_non_finite_and_boolean_acoustid_scores_fail_closed(
    db_session: AsyncSession,
) -> None:
    expected = "11111111-1111-1111-1111-111111111111"
    for invalid in (float("inf"), float("nan"), True):
        state = await _verify(db_session, invalid, expected, expected)
        assert state != AcoustIDVerificationState.verified


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


async def test_legacy_slskd_cleanup_retains_unfenced_provider_row_and_staged_file(
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

    assert calls == []
    assert staged.exists()


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
        observed_acoustid_evidence_json=json.dumps(
            [
                {"mbid": expected, "score": 0.99},
                {"mbid": "22222222-2222-2222-2222-222222222222", "score": 0.98},
            ]
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

    review.observed_acoustid_evidence_json = json.dumps([{"mbid": expected, "score": 0.99}])
    assert await recover_approved_downloads(db_session, settings) == 0
    assert await db_session.get(StagingReviewItem, review.id) is review
    review.observed_acoustid_evidence_json = json.dumps(
        [{"mbid": expected, "score": 0.10}, {"mbid": expected, "score": 0.99}]
    )
    assert await recover_approved_downloads(db_session, settings) == 0
    assert await db_session.get(StagingReviewItem, review.id) is review
    review.observed_acoustid_evidence_json = json.dumps(
        [
            {"mbid": expected, "score": 0.99},
            {"mbid": "22222222-2222-2222-2222-222222222222", "score": 0.98},
        ]
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


async def test_failed_bounded_cleanup_rotates_past_low_id_prefix(
    db_session: AsyncSession, tmp_path, monkeypatch
) -> None:
    job = Job(source="slskd", query="cleanup", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    plans: list[ImportPlan] = []
    for index in range(3):
        staged = tmp_path / f"stage-{index}.flac"
        staged.write_bytes(b"audio")
        track = Track(
            job=job,
            release=release,
            source="slskd",
            source_job_id=f"transfer-{index}",
            staging_path=str(staged),
            acquisition_provenance_json=json.dumps(
                {"source": "slskd", "username": "peer", "filename": staged.name}
            ),
        )
        plan = ImportPlan(
            release=release,
            track=track,
            source_path=str(staged),
            staging_path=str(staged),
            destination_path=str(tmp_path / "music" / staged.name),
            status=ImportWorkflowState.imported,
        )
        plans.append(plan)
        db_session.add_all([track, plan])
    db_session.add_all([job, release])
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)

    first = await pending_imported_source_cleanups(db_session, limit=2)
    assert [item.plan_id for item in first] == [plans[0].id, plans[1].id]
    await cleanup_imported_sources(first)

    async with factory() as verify:
        second = await pending_imported_source_cleanups(verify, limit=2)
        assert second[0].plan_id == plans[2].id
        attempted = []
        for plan in plans[:2]:
            persisted = await verify.get(ImportPlan, plan.id)
            assert persisted is not None
            attempted.append(persisted.cleanup_attempted_at)
    assert all(value is not None for value in attempted)


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


async def test_prune_preserves_no_track_job_with_unresolved_attempt_cleanup(
    db_session: AsyncSession,
) -> None:
    job = Job(source="slskd", query="rejected", status=JobStatus.failed)
    attempt = AcquisitionAttempt(
        job=job,
        provider="slskd",
        peer="peer",
        remote_path="Album/01 Song.flac",
        provider_cleanup_state=CleanupState.pending,
        file_cleanup_state=CleanupState.not_required,
    )
    db_session.add_all([job, attempt])
    await db_session.flush()
    job_id, attempt_id = job.id, attempt.id

    result = await prune_orphaned_terminal_records(db_session, batch_size=1)

    assert result.jobs == 0
    assert await db_session.get(Job, job_id) is not None
    assert await db_session.get(AcquisitionAttempt, attempt_id) is not None


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


def test_cleanup_quarantine_path_is_bounded_for_long_source_name(tmp_path: Path) -> None:
    staged = tmp_path / (
        "Panic! at the Disco_A Fever You Can’t Sweat Out_02_"
        "The Only Difference Between Martyrdom and Suicide Is Press Coverage.flac"
    )
    staged.write_bytes(b"old-audio")
    quarantine = acquisition_cleanup._cleanup_quarantine_path(
        staged,
        2405,
        44,
        650207209087006807,
        1785800562000715084,
        69911789,
        acquisition_cleanup._file_sha256(staged),
    )

    assert len(os.fsencode(quarantine.name)) <= os.pathconf(tmp_path, "PC_NAME_MAX")
    assert not quarantine.exists()


def test_cleanup_quarantine_rejects_malformed_claim_before_identity_read(
    tmp_path: Path, monkeypatch
) -> None:
    configured = tmp_path / "staged.flac"
    malformed = tmp_path / ".audiohoard-cleanup-42-invalid"
    malformed.mkdir()

    def unexpected_identity(path: Path):
        raise AssertionError(f"identity read for malformed claim: {path}")

    monkeypatch.setattr(acquisition_cleanup, "_current_identity", unexpected_identity)

    assert not acquisition_cleanup._quarantine_claim_matches(malformed, configured, 42)
    assert not acquisition_cleanup._persisted_quarantine_claim_matches(malformed, 42)


def test_pending_cleanup_finds_legacy_quarantine_name(tmp_path: Path) -> None:
    configured = tmp_path / "[track].flac"
    configured.write_bytes(b"old-audio")
    current = configured.stat()
    digest = acquisition_cleanup._file_sha256(configured)
    marker = (
        f".audiohoard-cleanup-42-{current.st_dev}-{current.st_ino}-"
        f"{current.st_mtime_ns}-{current.st_size}-{digest}"
    )
    legacy = configured.with_name(f".{configured.name}{marker}")
    configured.replace(legacy)
    plan = SimpleNamespace(id=42, staging_path=str(configured), source_path=str(configured))

    assert acquisition_cleanup._pending_cleanup_path_sync(plan) == legacy


def test_cleanup_quarantine_rejects_non_regular_claim_before_hash(
    tmp_path: Path, monkeypatch
) -> None:
    configured = tmp_path / "staged.flac"
    digest = "0" * 64
    candidate = tmp_path / f".audiohoard-cleanup-42-1-2-3-4-{digest}"
    candidate.mkdir()

    def unexpected_hash(path: Path):
        raise AssertionError(f"hash attempted for non-regular claim: {path}")

    monkeypatch.setattr(acquisition_cleanup, "_file_sha256", unexpected_hash)

    assert not acquisition_cleanup._quarantine_claim_matches(candidate, configured, 42)


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
    provenance = json.loads(item.provenance_json or "{}")
    provenance["source_cleanup_completed_at"] = "2026-08-09T00:00:00+00:00"
    serialized = json.dumps(provenance, sort_keys=True)
    track.acquisition_provenance_json = serialized
    item = replace(item, provenance_json=serialized)
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)
    real_revalidate = acquisition_cleanup._revalidate_cleanup_obligation
    replacement_written = False

    async def replace_after_quarantine(claimed, *, protect_destination=True):  # noqa: ANN001
        nonlocal replacement_written
        if not replacement_written:
            item.staged_path.write_bytes(b"replacement")
            replacement_written = True
        return await real_revalidate(claimed, protect_destination=protect_destination)

    monkeypatch.setattr(
        acquisition_cleanup, "_revalidate_cleanup_obligation", replace_after_quarantine
    )

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


async def test_crash_before_quarantine_commit_recovers_owned_inode_and_preserves_replacement(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    item, plan, track = await _pending_cleanup_fixture(db_session, tmp_path)
    provenance = json.loads(item.provenance_json or "{}")
    provenance["source_cleanup_completed_at"] = "2026-08-09T00:00:00+00:00"
    serialized = json.dumps(provenance, sort_keys=True)
    track.acquisition_provenance_json = serialized
    item = replace(item, provenance_json=serialized)
    await db_session.commit()
    assert item.expected_device is not None and item.expected_inode is not None
    assert item.expected_mtime_ns is not None and item.expected_size is not None
    quarantine = acquisition_cleanup._cleanup_quarantine_path(
        item.staged_path,
        plan.id,
        item.expected_device,
        item.expected_inode,
        item.expected_mtime_ns,
        item.expected_size,
        acquisition_cleanup._file_sha256(item.staged_path),
    )
    item.staged_path.replace(quarantine)
    item.staged_path.write_bytes(b"replacement")
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)

    class Adapter:
        def __init__(self, url: str, api_key: str) -> None:
            pass

        async def cancel(
            self, username: str, filename: str, transfer_id: str | None = None
        ) -> bool:
            return True

    async def fake_effective_settings(db, settings):  # noqa: ANN001
        return SimpleNamespace(slskd_url="", slskd_api_key="")

    monkeypatch.setattr(acquisition_cleanup, "SlskdAdapter", Adapter)
    monkeypatch.setattr(acquisition_cleanup, "build_effective_settings", fake_effective_settings)
    async with factory() as load:
        recovered = await pending_imported_source_cleanups(load)
    assert len(recovered) == 1 and recovered[0].staged_path == quarantine

    await cleanup_imported_sources(recovered)

    assert item.staged_path.read_bytes() == b"replacement"
    assert not quarantine.exists()
    async with factory() as verify:
        persisted_plan = await verify.get(ImportPlan, plan.id)
        persisted_track = await verify.get(Track, track.id)
        assert persisted_plan is not None and persisted_plan.staging_path is None
        assert persisted_track is not None and persisted_track.staging_path is None


async def test_provider_failure_retains_quarantined_cleanup_obligation(
    db_session: AsyncSession, monkeypatch, tmp_path
) -> None:
    item, plan, _ = await _pending_cleanup_fixture(db_session, tmp_path)
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(acquisition_cleanup, "get_session_factory", lambda: factory)

    class FailingAdapter:
        def __init__(self, url: str, api_key: str) -> None:
            pass

        async def cancel(
            self, username: str, filename: str, transfer_id: str | None = None
        ) -> bool:
            return False

    async def fake_effective_settings(db, settings):  # noqa: ANN001
        return SimpleNamespace(slskd_url="", slskd_api_key="")

    monkeypatch.setattr(acquisition_cleanup, "SlskdAdapter", FailingAdapter)
    monkeypatch.setattr(acquisition_cleanup, "build_effective_settings", fake_effective_settings)

    await cleanup_imported_sources((item,))

    async with factory() as verify:
        persisted = await verify.get(ImportPlan, plan.id)
        assert persisted is not None and persisted.staging_path is not None
        retained_bytes = await acquisition_cleanup.asyncio.to_thread(
            Path(persisted.staging_path).read_bytes
        )
        assert retained_bytes == b"old-audio"
        pending = await pending_imported_source_cleanups(verify)
        assert len(pending) == 1


async def test_persisted_quarantine_with_replaced_inode_is_rejected(
    db_session: AsyncSession, tmp_path
) -> None:
    item, plan, track = await _pending_cleanup_fixture(db_session, tmp_path)
    assert item.expected_device is not None and item.expected_inode is not None
    assert item.expected_mtime_ns is not None and item.expected_size is not None
    quarantine = acquisition_cleanup._cleanup_quarantine_path(
        item.staged_path,
        plan.id,
        item.expected_device,
        item.expected_inode,
        item.expected_mtime_ns,
        item.expected_size,
        acquisition_cleanup._file_sha256(item.staged_path),
    )
    await acquisition_cleanup.asyncio.to_thread(item.staged_path.replace, quarantine)
    plan.staging_path = str(quarantine)
    track.staging_path = str(quarantine)
    await db_session.commit()
    await acquisition_cleanup.asyncio.to_thread(quarantine.unlink)
    await acquisition_cleanup.asyncio.to_thread(quarantine.write_bytes, b"replacement")

    pending = await pending_imported_source_cleanups(db_session)

    assert pending == ()
    replacement = await acquisition_cleanup.asyncio.to_thread(quarantine.read_bytes)
    assert replacement == b"replacement"
    await db_session.refresh(plan)
    assert plan.staging_path == str(quarantine)


def test_select_best_folder_rejects_disabled_formats_with_strict_profile() -> None:
    from app.services.slskd_scoring import AlbumFolder, SlskdFile, select_best_folder

    folders = [
        AlbumFolder(
            username="peer",
            parent_dir="Artist/Album",
            audio_format="ogg",
            files=[SlskdFile("Artist/Album/01 - Track.ogg", None, None, None)],
        )
    ]

    assert (
        select_best_folder(
            folders,
            catalog_track_count=1,
            catalog_artist="Artist",
            catalog_album="Album",
            format_preference=["flac", "mp3"],
            min_mp3_bitrate=320,
            allow_lower_quality_fallback=False,
        )
        is None
    )
