from __future__ import annotations

import json
from datetime import UTC
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.jobs.runner import _fetch_slskd_album_results, _without_blocked_slskd_results
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan
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
    calls: list[tuple[str, str]] = []

    class FakeSlskdAdapter:
        def __init__(self, url: str, api_key: str) -> None:
            pass

        async def cancel(self, username: str, filename: str) -> None:
            calls.append((username, filename))

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
            ),
        )
    )

    assert calls == [("peer", "Album\\01.flac")]
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
