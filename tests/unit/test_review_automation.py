from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

import app.models  # noqa: F401
from app.database import Base
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import ReviewAutomationAttempt, StagingReviewItem
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
    ReviewDecision,
)
from app.services.acoustid_verification import _create_review_item
from app.services.audio_alignment import AlignmentResult
from app.services.auto_import import try_auto_import_release
from app.services.reference_audio import ExactDeezerReference
from app.services.review_automation import (
    ReviewAutomationScheduler,
    ReviewAutomationService,
)

UNIQUE_OBSERVED = "22222222-2222-2222-2222-222222222222"
ORIGINAL_EXPECTED = "11111111-1111-1111-1111-111111111111"


@pytest_asyncio.fixture
async def automation_db(
    tmp_path: Path,
) -> AsyncIterator[tuple[async_sessionmaker[AsyncSession], AsyncEngine]]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'automation.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory, engine
    await engine.dispose()


async def _seed_review(
    factory: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    *,
    title: str = "Song",
    provider_title: str | None = None,
    observed: list[str] | None = None,
    expected_mbid: str | None = ORIGINAL_EXPECTED,
    catalog_mbid: str | None = ORIGINAL_EXPECTED,
    album_deezer_id: str | None = "55",
    score: float = 0.98,
    fingerprint_duration: int = 180,
    target_duration: int = 181,
    recordings: list[dict[str, object]] | None = None,
    track_content_rating: str = "unknown",
    album_content_rating: str = "unknown",
) -> tuple[int, int, int]:
    del provider_title
    source = tmp_path / f"source-{title.replace('/', '-')}.flac"
    source.write_bytes(b"audio")
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(
        artist=artist,
        title="Album",
        deezer_id=album_deezer_id,
        content_rating=album_content_rating,
    )
    catalog_track = CatalogAlbumTrack(
        album=album,
        position=1,
        disc=1,
        title=title,
        duration_sec=target_duration,
        recording_mbid=catalog_mbid,
        content_rating=track_content_rating,
    )
    job = Job(source="slskd", query=title, status=JobStatus.done, catalog_album=album)
    release = Release(
        job=job,
        source="slskd",
        title="Album",
        import_state=ImportWorkflowState.needs_review,
    )
    observed_mbids = observed or [UNIQUE_OBSERVED]
    recording_evidence = recordings or [
        {"mbid": mbid, "score": score, "title": title, "artist": "Artist"}
        for mbid in observed_mbids
    ]
    track = Track(
        job=job,
        release=release,
        catalog_album=album,
        catalog_track=catalog_track,
        source="slskd",
        title=title,
        artist="Artist",
        duration_sec=target_duration,
        mbid=expected_mbid,
        identity_state=IdentityResolutionState.resolved,
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.needs_review,
        staging_path=str(source),
        deezer_id="999999",
        acoustid_evidence_json=json.dumps({"recordings": recording_evidence}),
        acoustid_verification_state=(
            AcoustIDVerificationState.mismatch
            if expected_mbid
            else AcoustIDVerificationState.unavailable
        ),
    )
    item = StagingReviewItem(
        track=track,
        release=release,
        expected_recording_mbid=expected_mbid,
        expected_title=title,
        observed_acoustid_mbids_json=json.dumps(observed_mbids),
        observed_acoustid_evidence_json=json.dumps(recording_evidence, sort_keys=True),
        fingerprint_duration_sec=fingerprint_duration,
        acoustid_score=score,
        verification_reason="mismatch" if expected_mbid else "no_expected_mbid",
        review_state=ReviewDecision.pending,
    )
    async with factory() as db:
        db.add_all([artist, album, catalog_track, job, release, track, item])
        await db.commit()
        return item.id, track.id, catalog_track.id


def _settings(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        deezer_api_url="https://api.deezer.com",
        acoustid_acceptance_threshold=0.90,
        library_root=tmp_path / "library",
        staging_root=tmp_path,
        naming_template="{album_artist}/{album}/{track_no} - {title}.{ext}",
    )


def _reference(
    *,
    title: str = "Song",
    duration: int = 181,
    track_content_rating: str = "unknown",
    album_content_rating: str = "unknown",
) -> ExactDeezerReference:
    return ExactDeezerReference(
        provider_track_id="101",
        title=title,
        duration_sec=duration,
        preview_url="https://cdnt-preview.dzcdn.net/preview.mp3?hdnea=secret-signature",
        album_title="Album",
        artist_name="Artist",
        track_artist_name="Artist",
        track_content_rating=track_content_rating,
        album_content_rating=album_content_rating,
    )


async def test_high_confidence_exact_preview_approves_preserves_audit_and_imports(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, track_id, _catalog_track_id = await _seed_review(factory, tmp_path)
    imported: list[int] = []

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(_path: Path, _url: str) -> AlignmentResult:
        return AlignmentResult(offset_seconds=42.0, score=0.02, confidence="high")

    async def auto_import(db: AsyncSession, release: Release, **_kwargs) -> bool:
        persisted = await db.get(StagingReviewItem, item_id)
        assert persisted is not None and persisted.review_state == ReviewDecision.approved
        imported.append(release.id)
        return True

    service = ReviewAutomationService(
        factory,
        _settings(tmp_path),
        reference_resolver=reference,
        aligner=align,
        auto_importer=auto_import,
    )
    assert await service.run_cycle(limit=1) == 1

    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        track = await db.get(Track, track_id)
        assert item is not None and track is not None
        assert item.review_state == ReviewDecision.approved
        assert item.automation_state == "approved"
        assert track.acoustid_verification_state == AcoustIDVerificationState.approved
        assert track.mbid == UNIQUE_OBSERVED
        assert track.identity_state == IdentityResolutionState.resolved
        decision = item.automation_decision
        assert decision["original_expected_mbid"] == ORIGINAL_EXPECTED
        assert decision["provider_track_id"] == "101"
        assert decision["alignment_confidence"] == "high"
        assert "preview_url" not in decision
        assert "secret-signature" not in (item.automation_decision_json or "")
        assert decision["acoustid_consensus_shadow"]["decision"] == "would_approve"
        track_evidence = json.loads(track.acoustid_evidence_json or "{}")
        assert track_evidence["review_automation"]["original_expected_mbid"] == ORIGINAL_EXPECTED
    assert imported


async def test_multiple_observed_mbids_clear_only_track_override(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    observed = [UNIQUE_OBSERVED, "33333333-3333-3333-3333-333333333333"]
    item_id, track_id, catalog_track_id = await _seed_review(factory, tmp_path, observed=observed)

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(_path: Path, _url: str) -> AlignmentResult:
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    async def no_import(*_args, **_kwargs) -> bool:
        return True

    service = ReviewAutomationService(
        factory,
        _settings(tmp_path),
        reference_resolver=reference,
        aligner=align,
        auto_importer=no_import,
    )
    await service.run_cycle(limit=1)

    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        track = await db.get(Track, track_id)
        catalog_track = await db.get(CatalogAlbumTrack, catalog_track_id)
        assert item is not None and item.review_state == ReviewDecision.approved
        assert track is not None and track.mbid is None
        assert track.identity_state == IdentityResolutionState.unresolved
        assert catalog_track is not None and catalog_track.recording_mbid == ORIGINAL_EXPECTED


async def test_medium_alignment_and_score_alone_never_approve(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, _track_id, _catalog_track_id = await _seed_review(factory, tmp_path, score=0.9999)

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(_path: Path, _url: str) -> AlignmentResult:
        return AlignmentResult(offset_seconds=1.0, score=0.11, confidence="medium")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    await service.run_cycle(limit=1)

    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None
        assert item.review_state == ReviewDecision.pending
        assert item.automation_state == "rejected"
        assert item.automation_decision["reason"] == "alignment_not_high_confidence"


async def test_identity_qualifier_and_duration_contradictions_skip_alignment(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    qualifier_id, *_ = await _seed_review(factory, tmp_path, title="Song (Live)")
    duration_id, *_ = await _seed_review(
        factory, tmp_path, title="Other Song", album_deezer_id="56"
    )
    align_calls = 0

    async def reference(snapshot, _settings):
        if snapshot.review_id == qualifier_id:
            return _reference(title="Song")
        return _reference(title="Other Song", duration=240)

    async def align(_path: Path, _url: str) -> AlignmentResult:
        nonlocal align_calls
        align_calls += 1
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    assert await service.run_cycle(limit=2) == 2
    assert align_calls == 0

    async with factory() as db:
        qualifier = await db.get(StagingReviewItem, qualifier_id)
        duration = await db.get(StagingReviewItem, duration_id)
        assert qualifier is not None
        assert qualifier.automation_decision["reason"] == "provider_title_mismatch"
        assert duration is not None
        assert duration.automation_decision["reason"] == "provider_duration_mismatch"


async def test_provider_album_and_artist_contradictions_skip_alignment(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    album_id, *_ = await _seed_review(factory, tmp_path, title="Album mismatch")
    artist_id, *_ = await _seed_review(
        factory, tmp_path, title="Artist mismatch", album_deezer_id="56"
    )
    align_calls = 0

    async def reference(snapshot, _settings):
        resolved = _reference(title=snapshot.catalog_title)
        if snapshot.review_id == album_id:
            return ExactDeezerReference(
                provider_track_id=resolved.provider_track_id,
                title=resolved.title,
                duration_sec=resolved.duration_sec,
                preview_url=resolved.preview_url,
                album_title="Different Album",
                artist_name=resolved.artist_name,
                track_artist_name=resolved.track_artist_name,
            )
        return ExactDeezerReference(
            provider_track_id=resolved.provider_track_id,
            title=resolved.title,
            duration_sec=resolved.duration_sec,
            preview_url=resolved.preview_url,
            album_title=resolved.album_title,
            artist_name="Different Artist",
            track_artist_name="Different Artist",
        )

    async def align(_path: Path, _url: str) -> AlignmentResult:
        nonlocal align_calls
        align_calls += 1
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    assert await service.run_cycle(limit=2) == 2
    assert align_calls == 0
    async with factory() as db:
        album_item = await db.get(StagingReviewItem, album_id)
        artist_item = await db.get(StagingReviewItem, artist_id)
        assert album_item is not None
        assert album_item.automation_decision["reason"] == "provider_album_title_mismatch"
        assert artist_item is not None
        assert artist_item.automation_decision["reason"] == "provider_artist_mismatch"


async def test_transient_failure_retries_without_starving_later_rows(
    automation_db, tmp_path: Path, caplog
) -> None:
    factory, _engine = automation_db
    first_id, *_ = await _seed_review(factory, tmp_path, title="First")
    second_id, *_ = await _seed_review(factory, tmp_path, title="Second", album_deezer_id="56")

    async def reference(snapshot, _settings):
        if snapshot.review_id == first_id:
            raise TimeoutError(
                "https://cdnt-preview.dzcdn.net/x?hdnea=exp=999~hmac=sentinel-secret"
            )
        return _reference(title="Second")

    async def align(_path: Path, _url: str) -> AlignmentResult:
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    assert await service.run_cycle(limit=2) == 2

    async with factory() as db:
        first = await db.get(StagingReviewItem, first_id)
        second = await db.get(StagingReviewItem, second_id)
        assert first is not None and second is not None
        assert first.automation_state == "retry"
        assert first.automation_attempt_count == 1
        assert first.automation_next_attempt_at is not None
        assert "sentinel-secret" not in (first.automation_decision_json or "")
        assert second.review_state == ReviewDecision.approved
    assert "sentinel-secret" not in caplog.text


async def test_fuzzy_track_deezer_id_without_catalog_album_id_cannot_approve(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, *_ = await _seed_review(factory, tmp_path, album_deezer_id=None)
    alignment_called = False

    async def reference(snapshot, _settings):
        assert snapshot.catalog_album_deezer_id == ""
        return None

    async def align(_path: Path, _url: str) -> AlignmentResult:
        nonlocal alignment_called
        alignment_called = True
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    assert await service.run_cycle(limit=1) == 1
    assert not alignment_called
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.review_state == ReviewDecision.pending
        assert item.automation_decision["reason"] == "exact_deezer_reference_unavailable"


async def test_source_outside_staging_is_rejected_before_provider_io(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, *_ = await _seed_review(factory, tmp_path)

    async def unexpected_reference(*_args, **_kwargs):
        raise AssertionError("provider must not be called for an out-of-root source")

    settings = _settings(tmp_path)
    settings.staging_root = tmp_path / "different-staging-root"
    service = ReviewAutomationService(factory, settings, reference_resolver=unexpected_reference)
    assert await service.run_cycle(limit=1) == 1
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.review_state == ReviewDecision.pending
        assert item.automation_decision["reason"] == "source_outside_or_missing_staging"


async def test_stale_claim_cannot_overwrite_manual_decision(automation_db, tmp_path: Path) -> None:
    factory, _engine = automation_db
    item_id, *_ = await _seed_review(factory, tmp_path)
    service = ReviewAutomationService(factory, _settings(tmp_path))
    snapshot = await service.claim_next()
    assert snapshot is not None

    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None
        item.review_state = ReviewDecision.denied
        item.reviewed_at = datetime.now(UTC)
        await db.commit()

    applied = await service.apply_result(
        snapshot,
        service.approval_result(
            snapshot,
            _reference(),
            AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high"),
        ),
    )
    assert not applied
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.review_state == ReviewDecision.denied


async def test_true_two_session_manual_denial_wins_while_automation_is_evaluating(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, *_ = await _seed_review(factory, tmp_path)
    evaluating = asyncio.Event()
    resume = asyncio.Event()

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(_path: Path, _url: str) -> AlignmentResult:
        evaluating.set()
        await resume.wait()
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    automation = asyncio.create_task(service.process_next())
    await asyncio.wait_for(evaluating.wait(), timeout=1)
    async with factory() as manual_db:
        await manual_db.execute(text("BEGIN IMMEDIATE"))
        item = await manual_db.get(StagingReviewItem, item_id)
        assert item is not None and item.review_state == ReviewDecision.pending
        item.review_state = ReviewDecision.denied
        item.reviewed_at = datetime.now(UTC)
        item.automation_state = "manual_denied"
        item.automation_claim_token = None
        item.automation_claimed_at = None
        await manual_db.commit()
    resume.set()
    assert await asyncio.wait_for(automation, timeout=1)
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.review_state == ReviewDecision.denied
        assert item.automation_state == "manual_denied"


async def test_stale_claim_is_recoverable_and_retry_cap_is_terminal(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, *_ = await _seed_review(factory, tmp_path)
    service = ReviewAutomationService(factory, _settings(tmp_path), max_attempts=2)

    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None
        item.automation_state = "claimed"
        item.automation_claim_token = "dead-worker"
        item.automation_claimed_at = datetime.now(UTC) - timedelta(hours=1)
        await db.commit()

    assert await service.claim_next() is not None

    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None
        item.automation_state = "retry"
        item.automation_attempt_count = 1
        item.automation_next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
        item.automation_claim_token = None
        item.automation_claimed_at = None
        await db.commit()

    snapshot = await service.claim_next()
    assert snapshot is not None
    await service.apply_result(snapshot, service.transient_failure_result("provider_unavailable"))
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None
        assert item.automation_state == "failed"
        assert item.review_state == ReviewDecision.pending
        assert item.automation_next_attempt_at is None


async def test_only_individually_qualified_mbid_can_be_bound_and_equality_fails_closed(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    low = "33333333-3333-3333-3333-333333333333"
    mixed_id, mixed_track_id, _ = await _seed_review(
        factory,
        tmp_path,
        observed=[UNIQUE_OBSERVED, low],
        recordings=[
            {"mbid": UNIQUE_OBSERVED, "score": 0.98, "title": "Song", "artist": "Artist"},
            {"mbid": low, "score": 0.89, "title": "Song", "artist": "Artist"},
        ],
    )
    equal_id, *_ = await _seed_review(
        factory,
        tmp_path,
        title="Equal",
        album_deezer_id="56",
        observed=[low],
        score=0.90,
        recordings=[{"mbid": low, "score": 0.90, "title": "Equal", "artist": "Artist"}],
    )

    async def reference(snapshot, _settings):
        return _reference(title=snapshot.catalog_title)

    async def align(_path: Path, _url: str) -> AlignmentResult:
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    async def no_import(*_args, **_kwargs) -> bool:
        return True

    service = ReviewAutomationService(
        factory,
        _settings(tmp_path),
        reference_resolver=reference,
        aligner=align,
        auto_importer=no_import,
    )
    assert await service.run_cycle(limit=2) == 2
    async with factory() as db:
        mixed = await db.get(StagingReviewItem, mixed_id)
        mixed_track = await db.get(Track, mixed_track_id)
        equal = await db.get(StagingReviewItem, equal_id)
        assert mixed is not None and mixed.review_state == ReviewDecision.approved
        assert mixed_track is not None and mixed_track.mbid == UNIQUE_OBSERVED
        assert equal is not None and equal.review_state == ReviewDecision.pending
        assert equal.automation_decision["reason"] == "no_mbid_strictly_above_threshold"


async def test_explicit_clean_conflict_rejects_before_alignment(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, *_ = await _seed_review(
        factory, tmp_path, track_content_rating="clean", album_content_rating="clean"
    )
    aligned = False

    async def reference(_snapshot, _settings):
        return _reference(track_content_rating="explicit", album_content_rating="explicit")

    async def align(_path: Path, _url: str) -> AlignmentResult:
        nonlocal aligned
        aligned = True
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    assert await service.run_cycle(limit=1) == 1
    assert not aligned
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None
        assert item.automation_decision["reason"] == "provider_content_rating_mismatch"


async def test_source_swap_after_alignment_fails_closed(automation_db, tmp_path: Path) -> None:
    factory, _engine = automation_db
    item_id, track_id, _ = await _seed_review(factory, tmp_path)
    async with factory() as db:
        track = await db.get(Track, track_id)
        assert track is not None
        source = Path(track.staging_path or track.source_path or "")

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(_path: Path, _url: str) -> AlignmentResult:
        await asyncio.to_thread(source.write_bytes, b"different audio")
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    assert await service.run_cycle(limit=1) == 1
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        track = await db.get(Track, track_id)
        assert item is not None and item.review_state == ReviewDecision.pending
        assert item.automation_decision["reason"] == "source_identity_changed"
        assert track is not None and track.mbid == ORIGINAL_EXPECTED


async def test_alignment_reads_pinned_source_during_aba_path_replacement(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, track_id, _ = await _seed_review(factory, tmp_path)
    async with factory() as db:
        track = await db.get(Track, track_id)
        assert track is not None
        source = Path(track.staging_path or track.source_path or "")
    original_bytes, original_stat = await asyncio.gather(
        asyncio.to_thread(source.read_bytes), asyncio.to_thread(source.stat)
    )

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(pinned_path: Path, _url: str) -> AlignmentResult:
        def replace_aba_and_read_pinned() -> bytes:
            replacement = source.with_name(f"{source.name}.replacement")
            replacement.write_bytes(b"other")
            os.replace(replacement, source)
            analyzed = pinned_path.read_bytes()
            restored = source.with_name(f"{source.name}.restored")
            restored.write_bytes(original_bytes)
            os.utime(
                restored,
                ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
            )
            os.replace(restored, source)
            return analyzed

        assert await asyncio.to_thread(replace_aba_and_read_pinned) == original_bytes
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    assert await service.run_cycle(limit=1) == 1
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None
        assert item.review_state == ReviewDecision.approved
        assert (
            item.automation_decision["source_sha256"] == hashlib.sha256(original_bytes).hexdigest()
        )


async def test_alignment_reads_sealed_snapshot_during_in_place_aba_mutation(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, track_id, _ = await _seed_review(factory, tmp_path)
    async with factory() as db:
        track = await db.get(Track, track_id)
        assert track is not None
        source = Path(track.staging_path or track.source_path or "")
    original_bytes, original_stat = await asyncio.gather(
        asyncio.to_thread(source.read_bytes), asyncio.to_thread(source.stat)
    )

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(snapshot_path: Path, _url: str) -> AlignmentResult:
        def mutate_restore_and_read_snapshot() -> bytes:
            source.write_bytes(b"tampered-in-place")
            analyzed = snapshot_path.read_bytes()
            source.write_bytes(original_bytes)
            os.utime(
                source,
                ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
            )
            return analyzed

        assert await asyncio.to_thread(mutate_restore_and_read_snapshot) == original_bytes
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    assert await service.run_cycle(limit=1) == 1
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.review_state == ReviewDecision.approved


async def test_source_swap_after_approval_blocks_import_dispatch(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, track_id, _ = await _seed_review(factory, tmp_path)
    import_calls = 0

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(_path: Path, _url: str) -> AlignmentResult:
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    async def auto_import(*_args, **_kwargs) -> bool:
        nonlocal import_calls
        import_calls += 1
        return True

    service = ReviewAutomationService(
        factory,
        _settings(tmp_path),
        reference_resolver=reference,
        aligner=align,
        auto_importer=auto_import,
    )
    assert await service.process_next()
    async with factory() as db:
        track = await db.get(Track, track_id)
        assert track is not None and track.staging_path
        source_path = Path(track.staging_path)
    await asyncio.to_thread(source_path.write_bytes, b"changed after approval")
    assert await service.dispatch_pending_imports(limit=1) == 1
    assert import_calls == 0
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.import_dispatch_state == "failed"
        outcome = json.loads(item.import_dispatch_outcome_json or "{}")
        assert outcome["reason"] == "source_identity_changed_after_approval"


async def test_verification_refresh_invalidates_claim_and_fences_old_evidence(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, track_id, _ = await _seed_review(factory, tmp_path)
    service = ReviewAutomationService(factory, _settings(tmp_path))
    old_snapshot = await service.claim_next()
    assert old_snapshot is not None
    replacement = "44444444-4444-4444-4444-444444444444"
    async with factory() as db:
        track = await db.get(Track, track_id)
        assert track is not None
        track.acoustid_evidence_json = json.dumps(
            {
                "recordings": [
                    {"mbid": replacement, "score": 0.99, "title": "Song", "artist": "Artist"}
                ]
            }
        )
        await _create_review_item(
            track,
            observed_mbids=[replacement],
            best_score=0.99,
            duration_sec=180,
            reason="mismatch",
            db=db,
        )
        await db.commit()

    stale_applied = await service.apply_result(
        old_snapshot,
        service.approval_result(
            old_snapshot,
            _reference(),
            AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high"),
        ),
    )
    assert not stale_applied
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        attempt = await db.scalar(
            select(ReviewAutomationAttempt).where(
                ReviewAutomationAttempt.claim_token == old_snapshot.claim_token
            )
        )
        assert item is not None
        assert item.evidence_revision == 2
        assert item.automation_state == "pending"
        assert item.observed_acoustid_mbids == [replacement]
        assert attempt is not None and attempt.state == "abandoned"

    new_snapshot = await service.claim_next()
    assert new_snapshot is not None
    assert new_snapshot.evidence_revision == 2
    assert new_snapshot.qualified_mbids == (replacement,)


async def test_failed_import_is_retried_by_next_cycle_without_restart(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, *_ = await _seed_review(factory, tmp_path)
    current = datetime.now(UTC)
    outcomes = iter((False, True))

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(_path: Path, _url: str) -> AlignmentResult:
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    async def auto_import(*_args, **_kwargs) -> bool:
        return next(outcomes)

    service = ReviewAutomationService(
        factory,
        _settings(tmp_path),
        reference_resolver=reference,
        aligner=align,
        auto_importer=auto_import,
        now=lambda: current,
    )
    assert await service.run_cycle(limit=1) == 1
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.import_dispatch_state == "retry"
        assert item.import_dispatch_attempt_count == 1
    current += timedelta(seconds=16)
    assert await service.run_cycle(limit=1) == 0
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.import_dispatch_state == "completed"
        assert item.import_dispatch_attempt_count == 2
        attempt = await db.scalar(
            select(ReviewAutomationAttempt)
            .where(
                ReviewAutomationAttempt.review_item_id == item_id,
                ReviewAutomationAttempt.input_json.like('%"kind": "import_dispatch"%'),
            )
            .order_by(ReviewAutomationAttempt.id.desc())
        )
        assert attempt is not None
        assert json.loads(attempt.import_outcome_json or "{}")["outcome"] == "succeeded"


async def test_runtime_threshold_is_reloaded_before_claim(automation_db, tmp_path: Path) -> None:
    factory, _engine = automation_db
    item_id, *_ = await _seed_review(factory, tmp_path, score=0.95)
    loaded = 0

    async def settings_provider():
        nonlocal loaded
        loaded += 1
        return _settings(tmp_path), 0.90

    async def reference(_snapshot, _settings):
        return _reference()

    async def align(_path: Path, _url: str) -> AlignmentResult:
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    async def no_import(*_args, **_kwargs) -> bool:
        return True

    service = ReviewAutomationService(
        factory,
        _settings(tmp_path),
        acceptance_threshold=0.99,
        settings_provider=settings_provider,
        reference_resolver=reference,
        aligner=align,
        auto_importer=no_import,
    )
    assert await service.run_cycle(limit=1) == 1
    assert loaded == 1
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.review_state == ReviewDecision.approved


async def test_import_dispatch_claim_prevents_duplicate_concurrent_imports(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, track_id, _ = await _seed_review(factory, tmp_path)
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        track = await db.get(Track, track_id)
        assert item is not None and track is not None
        source = Path(track.staging_path or track.source_path or "")
        stat, source_bytes = await asyncio.gather(
            asyncio.to_thread(source.stat), asyncio.to_thread(source.read_bytes)
        )
        item.review_state = ReviewDecision.approved
        item.automation_state = "manual_approved"
        item.automation_decision_json = json.dumps(
            {
                "decision": "manual_approval",
                "source_path": str(source),
                "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
                "source_size": stat.st_size,
                "source_mtime_ns": stat.st_mtime_ns,
            },
            sort_keys=True,
        )
        item.import_dispatch_state = "pending"
        item.import_dispatch_next_attempt_at = datetime.now(UTC)
        await db.commit()
    calls = 0

    async def auto_import(*_args, **_kwargs) -> bool:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return True

    first = ReviewAutomationService(factory, _settings(tmp_path), auto_importer=auto_import)
    second = ReviewAutomationService(factory, _settings(tmp_path), auto_importer=auto_import)
    processed = await asyncio.gather(
        first.dispatch_pending_imports(limit=1), second.dispatch_pending_imports(limit=1)
    )
    assert sum(processed) == 1
    assert calls == 1
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        assert item is not None and item.import_dispatch_state == "completed"


async def test_dispatcher_binds_importer_to_approved_bytes_after_validation(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    item_id, track_id, _ = await _seed_review(factory, tmp_path)
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        track = await db.get(Track, track_id)
        assert item is not None and track is not None
        source = Path(track.staging_path or track.source_path or "")
        stat, source_bytes = await asyncio.gather(
            asyncio.to_thread(source.stat), asyncio.to_thread(source.read_bytes)
        )
        approved_hash = hashlib.sha256(source_bytes).hexdigest()
        item.review_state = ReviewDecision.approved
        item.automation_state = "manual_approved"
        item.automation_decision_json = json.dumps(
            {
                "decision": "manual_approval",
                "source_path": str(source),
                "source_sha256": approved_hash,
                "source_size": stat.st_size,
                "source_mtime_ns": stat.st_mtime_ns,
            },
            sort_keys=True,
        )
        item.import_dispatch_state = "pending"
        item.import_dispatch_next_attempt_at = datetime.now(UTC)
        await db.commit()

    async def swap_then_import(db, release, **kwargs) -> bool:
        artifacts = kwargs.get("source_artifacts")
        assert artifacts == {track_id: (source, approved_hash)}
        await asyncio.to_thread(source.write_bytes, b"replacement-after-dispatch-validation")
        return await try_auto_import_release(db, release, **kwargs)

    service = ReviewAutomationService(factory, _settings(tmp_path), auto_importer=swap_then_import)
    assert await service.dispatch_pending_imports(limit=1) == 1
    async with factory() as db:
        item = await db.get(StagingReviewItem, item_id)
        track = await db.get(Track, track_id)
        assert item is not None and track is not None
        assert item.import_dispatch_state == "retry"
        assert track.import_state != ImportWorkflowState.imported


async def test_malformed_evidence_is_terminal_and_does_not_starve_later_row(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    malformed_id, *_ = await _seed_review(factory, tmp_path, title="Malformed")
    mismatch_id, *_ = await _seed_review(
        factory,
        tmp_path,
        title="Mismatched evidence",
        album_deezer_id="56",
        observed=[UNIQUE_OBSERVED, ORIGINAL_EXPECTED],
        recordings=[{"mbid": UNIQUE_OBSERVED, "score": 0.99}],
    )
    later_id, *_ = await _seed_review(factory, tmp_path, title="Later", album_deezer_id="57")
    async with factory() as db:
        malformed = await db.get(StagingReviewItem, malformed_id)
        assert malformed is not None
        malformed.observed_acoustid_evidence_json = json.dumps(
            [{"mbid": UNIQUE_OBSERVED, "score": 0.99}, "malformed"]
        )
        await db.commit()

    async def reference(snapshot, _settings):
        return _reference(title=snapshot.catalog_title)

    async def align(_path: Path, _url: str) -> AlignmentResult:
        return AlignmentResult(offset_seconds=1.0, score=0.01, confidence="high")

    service = ReviewAutomationService(
        factory, _settings(tmp_path), reference_resolver=reference, aligner=align
    )
    assert await service.run_cycle(limit=3) == 3
    async with factory() as db:
        malformed = await db.get(StagingReviewItem, malformed_id)
        mismatch = await db.get(StagingReviewItem, mismatch_id)
        later = await db.get(StagingReviewItem, later_id)
        assert malformed is not None and malformed.automation_state == "rejected"
        assert malformed.automation_decision["reason"] == "malformed_observed_acoustid_evidence"
        assert mismatch is not None and mismatch.automation_state == "rejected"
        assert mismatch.automation_decision["reason"] == "malformed_observed_acoustid_evidence"
        assert later is not None and later.review_state == ReviewDecision.approved


async def test_scheduler_start_is_nonblocking_and_stop_cancels_work(
    automation_db, tmp_path: Path
) -> None:
    factory, _engine = automation_db
    await _seed_review(factory, tmp_path)
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def reference(_snapshot, _settings):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    service = ReviewAutomationService(factory, _settings(tmp_path), reference_resolver=reference)
    scheduler = ReviewAutomationScheduler(service, interval_seconds=60, batch_size=1)
    await scheduler.start()
    await asyncio.wait_for(entered.wait(), timeout=1)
    await scheduler.stop()
    assert cancelled.is_set()
