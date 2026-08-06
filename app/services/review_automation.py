from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import re
import unicodedata
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO, Literal

from sqlalchemy import or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.metadata.content_rating import (
    CONTENT_RATING_CLEAN,
    CONTENT_RATING_EXPLICIT,
    CONTENT_RATING_NOT_EXPLICIT,
    normalize_content_rating,
)
from app.metadata.filename_parse import normalize_for_catalog_match, strip_non_identity_descriptors
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.release import Release
from app.models.staging_review import ReviewAutomationAttempt, StagingReviewItem
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import AcoustIDVerificationState, ReviewDecision
from app.services.acoustid_evidence import parse_consistent_acoustid_evidence
from app.services.audio_alignment import AlignmentResult, align_deezer_preview
from app.services.auto_import import try_auto_import_release
from app.services.reference_audio import (
    ExactDeezerReference,
    resolve_exact_deezer_position_reference,
)

logger = logging.getLogger(__name__)

AutomationState = Literal["approved", "rejected", "retry"]


@dataclass(frozen=True)
class ReviewAutomationSnapshot:
    review_id: int
    track_id: int
    release_id: int
    claim_token: str
    attempt_count: int
    verification_reason: str
    original_expected_mbid: str | None
    observed_mbids: tuple[str, ...]
    acoustid_score: float
    evidence_revision: int
    catalog_track_id: int
    qualified_mbids: tuple[str, ...]
    fingerprint_duration_sec: int | None
    source_path: Path
    catalog_album_deezer_id: str
    catalog_disc: int
    catalog_position: int
    catalog_title: str
    catalog_duration_sec: int | None
    catalog_recording_mbid: str | None
    catalog_album_title: str
    catalog_artist_id: int
    catalog_artist_name: str
    catalog_track_content_rating: str
    catalog_album_content_rating: str
    track_artist: str
    acoustid_recordings: tuple[dict[str, object], ...]
    source_sha256: str = ""
    source_size: int = 0
    source_mtime_ns: int = 0


@dataclass(frozen=True)
class ReviewAutomationResult:
    state: AutomationState
    reason: str
    evidence: dict[str, object]


ReferenceResolver = Callable[
    [ReviewAutomationSnapshot, Settings], Awaitable[ExactDeezerReference | None]
]
Aligner = Callable[[Path, str], Awaitable[AlignmentResult | None]]
AutoImporter = Callable[..., Awaitable[bool]]
SettingsProvider = Callable[[], Awaitable[tuple[Settings, float]]]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _identity_title(value: str) -> str:
    """Normalize punctuation while retaining every identity-bearing qualifier."""
    normalized = normalize_for_catalog_match(strip_non_identity_descriptors(value))
    normalized = normalized.replace("'", "")
    normalized = "".join(
        " " if unicodedata.category(character)[0] in {"P", "S"} else character
        for character in normalized
    )
    return re.sub(r"\s+", " ", normalized).strip()


def _duration_matches(observed: int | None, target: int | None) -> bool:
    return observed is None or target is None or abs(observed - target) <= 4


def _source_is_in_staging(source_path: Path, staging_root: Path) -> bool:
    resolved_root = staging_root.resolve()
    resolved_source = source_path.resolve()
    return resolved_source.is_relative_to(resolved_root) and resolved_source.is_file()


def _open_pinned_source(
    source_path: Path, staging_root: Path
) -> tuple[BinaryIO, tuple[str, int, int], Path] | None:
    try:
        resolved_root = staging_root.resolve(strict=True)
        resolved_source = source_path.resolve(strict=True)
    except OSError:
        return None
    if not resolved_source.is_relative_to(resolved_root) or not resolved_source.is_file():
        return None
    digest = hashlib.sha256()
    source_stream: BinaryIO | None = None
    snapshot_stream: BinaryIO | None = None
    try:
        source_stream = resolved_source.open("rb")
        descriptor = source_stream.fileno()
        before_stat = os.fstat(descriptor)
        snapshot_fd = os.memfd_create(
            "audiohoard-review-source",
            os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING,
        )
        snapshot_stream = os.fdopen(snapshot_fd, "w+b")
        for chunk in iter(lambda: source_stream.read(1024 * 1024), b""):
            digest.update(chunk)
            snapshot_stream.write(chunk)
        snapshot_stream.flush()
        after_stat = os.fstat(descriptor)
        path_stat = resolved_source.stat()
        still_resolves_to_same_path = source_path.resolve(strict=True) == resolved_source
    except OSError:
        if source_stream is not None:
            source_stream.close()
        if snapshot_stream is not None:
            snapshot_stream.close()
        return None
    stable = (
        (
            before_stat.st_dev,
            before_stat.st_ino,
            before_stat.st_size,
            before_stat.st_mtime_ns,
        )
        == (
            after_stat.st_dev,
            after_stat.st_ino,
            after_stat.st_size,
            after_stat.st_mtime_ns,
        )
        == (
            path_stat.st_dev,
            path_stat.st_ino,
            path_stat.st_size,
            path_stat.st_mtime_ns,
        )
    )
    if not stable or not still_resolves_to_same_path:
        source_stream.close()
        if snapshot_stream is not None:
            snapshot_stream.close()
        return None
    source_stream.close()
    assert snapshot_stream is not None
    try:
        fcntl.fcntl(
            snapshot_stream.fileno(),
            fcntl.F_ADD_SEALS,
            fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE,
        )
    except OSError:
        snapshot_stream.close()
        return None
    snapshot_stream.seek(0)
    identity = (digest.hexdigest(), path_stat.st_size, path_stat.st_mtime_ns)
    return (
        snapshot_stream,
        identity,
        Path(f"/proc/{os.getpid()}/fd/{snapshot_stream.fileno()}"),
    )


def _source_identity(source_path: Path, staging_root: Path) -> tuple[str, int, int] | None:
    pinned = _open_pinned_source(source_path, staging_root)
    if pinned is None:
        return None
    stream, identity, _pinned_path = pinned
    stream.close()
    return identity


def _rating_conflicts(provider: str, catalog: str) -> bool:
    pair = {normalize_content_rating(provider), normalize_content_rating(catalog)}
    return CONTENT_RATING_EXPLICIT in pair and bool(
        pair & {CONTENT_RATING_CLEAN, CONTENT_RATING_NOT_EXPLICIT}
    )


def _load_acoustid_recordings(raw_evidence: str | None) -> tuple[dict[str, object], ...]:
    try:
        payload = json.loads(raw_evidence or "{}")
    except (TypeError, ValueError):
        return ()
    recordings = payload.get("recordings") if isinstance(payload, dict) else None
    if not isinstance(recordings, list):
        return ()
    return tuple(dict(item) for item in recordings if isinstance(item, dict))


def _merge_track_automation_evidence(track: Track, evidence: dict[str, object]) -> None:
    try:
        payload = json.loads(track.acoustid_evidence_json or "{}")
    except (TypeError, ValueError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    payload["review_automation"] = evidence
    track.acoustid_evidence_json = json.dumps(payload, sort_keys=True)


async def _default_reference_resolver(
    snapshot: ReviewAutomationSnapshot, settings: Settings
) -> ExactDeezerReference | None:
    return await resolve_exact_deezer_position_reference(
        album_deezer_id=snapshot.catalog_album_deezer_id,
        disc=snapshot.catalog_disc,
        position=snapshot.catalog_position,
        settings=settings,
    )


class ReviewAutomationService:
    """Claim, evaluate, and safely apply one exact-preview review decision at a time."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        acceptance_threshold: float | None = None,
        reference_resolver: ReferenceResolver = _default_reference_resolver,
        aligner: Aligner = align_deezer_preview,
        auto_importer: AutoImporter = try_auto_import_release,
        max_attempts: int = 3,
        claim_timeout: timedelta = timedelta(minutes=5),
        now: Callable[[], datetime] = _utcnow,
        settings_provider: SettingsProvider | None = None,
    ) -> None:
        self._factory = session_factory
        self._settings = settings
        self._acceptance_threshold = float(
            acceptance_threshold
            if acceptance_threshold is not None
            else getattr(settings, "acoustid_acceptance_threshold", 0.90)
        )
        self._reference_resolver = reference_resolver
        self._aligner = aligner
        self._auto_importer = auto_importer
        self._max_attempts = max(1, max_attempts)
        self._claim_timeout = claim_timeout
        self._import_claim_timeout = max(claim_timeout, timedelta(hours=6))
        self._now = now
        self._settings_provider = settings_provider

    async def claim_next(self) -> ReviewAutomationSnapshot | None:
        now = self._now()
        stale_before = now - self._claim_timeout
        async with self._factory() as db:
            # SQLite has no SKIP LOCKED. A short immediate transaction makes the
            # candidate read plus claim update one serialized operation.
            await db.execute(text("BEGIN IMMEDIATE"))
            row = (
                await db.execute(
                    select(
                        StagingReviewItem,
                        Track,
                        CatalogAlbumTrack,
                        CatalogAlbum,
                        CatalogArtist,
                    )
                    .join(Track, Track.id == StagingReviewItem.track_id)
                    .join(CatalogAlbumTrack, CatalogAlbumTrack.id == Track.catalog_track_id)
                    .join(CatalogAlbum, CatalogAlbum.id == CatalogAlbumTrack.album_id)
                    .join(CatalogArtist, CatalogArtist.id == CatalogAlbum.artist_id)
                    .where(
                        StagingReviewItem.review_state == ReviewDecision.pending,
                        StagingReviewItem.verification_reason.in_(
                            ("mismatch", "no_expected_mbid")
                        ),
                        StagingReviewItem.observed_acoustid_mbids_json.is_not(None),
                        StagingReviewItem.observed_acoustid_mbids_json.not_in(("", "[]")),
                        or_(
                            Track.staging_path.is_not(None) & (Track.staging_path != ""),
                            Track.source_path.is_not(None) & (Track.source_path != ""),
                        ),
                        or_(
                            StagingReviewItem.automation_state == "pending",
                            (
                                (StagingReviewItem.automation_state == "retry")
                                & or_(
                                    StagingReviewItem.automation_next_attempt_at.is_(None),
                                    StagingReviewItem.automation_next_attempt_at <= now,
                                )
                            ),
                            (
                                (StagingReviewItem.automation_state == "claimed")
                                & StagingReviewItem.automation_claimed_at.is_not(None)
                                & (StagingReviewItem.automation_claimed_at <= stale_before)
                            ),
                        ),
                    )
                    .order_by(StagingReviewItem.id)
                    .limit(1)
                )
            ).one_or_none()
            if row is None:
                await db.commit()
                return None
            item, track, catalog_track, catalog_album, catalog_artist = row
            consistent = parse_consistent_acoustid_evidence(
                item.observed_acoustid_mbids_json,
                item.observed_acoustid_evidence_json,
            )
            observed = tuple(sorted(consistent[0])) if consistent is not None else ()
            per_mbid = consistent[1] if consistent is not None else None
            qualified = tuple(
                sorted(
                    mbid
                    for mbid, score in (per_mbid or {}).items()
                    if score > self._acceptance_threshold and mbid in observed
                )
            )
            source_path = str(track.staging_path or track.source_path or "").strip()
            album_deezer_id = str(catalog_album.deezer_id or "").strip()
            if item.automation_state == "claimed" and item.automation_claim_token:
                stale_attempt = await db.scalar(
                    select(ReviewAutomationAttempt).where(
                        ReviewAutomationAttempt.claim_token == item.automation_claim_token
                    )
                )
                if stale_attempt is not None:
                    stale_attempt.state = "abandoned"
                    stale_attempt.completed_at = now
                    stale_attempt.decision_json = json.dumps(
                        {"decision": "abandoned", "reason": "stale_claim_recovered"},
                        sort_keys=True,
                    )
            token = str(uuid.uuid4())
            item.automation_state = "claimed"
            item.automation_claim_token = token
            item.automation_claimed_at = now
            item.automation_next_attempt_at = None
            item.automation_attempt_count += 1
            db.add(
                ReviewAutomationAttempt(
                    review_item_id=item.id,
                    track_id=track.id,
                    release_id=item.release_id,
                    attempt_number=item.automation_attempt_count,
                    evidence_revision=item.evidence_revision,
                    claim_token=token,
                    state="claimed",
                    claimed_at=now,
                    input_json=json.dumps(
                        {
                            "catalog_track_id": catalog_track.id,
                            "evidence_revision": item.evidence_revision,
                            "expected_mbid": item.expected_recording_mbid,
                            "observed_mbids": list(observed),
                            "per_mbid_evidence": list(per_mbid or ()),
                            "fingerprint_duration_sec": item.fingerprint_duration_sec,
                            "verification_reason": item.verification_reason,
                            "source_path": source_path,
                            "catalog_album_id": catalog_album.id,
                            "catalog_album_deezer_id": album_deezer_id,
                            "catalog_disc": catalog_track.disc,
                            "catalog_position": catalog_track.position,
                            "catalog_title": catalog_track.title,
                            "catalog_duration_sec": catalog_track.duration_sec,
                            "catalog_recording_mbid": catalog_track.recording_mbid,
                            "catalog_album_title": catalog_album.title,
                            "catalog_artist_id": catalog_artist.id,
                            "catalog_artist_name": catalog_artist.name,
                            "catalog_track_content_rating": catalog_track.content_rating,
                            "catalog_album_content_rating": catalog_album.content_rating,
                            "track_artist": str(track.artist or "").strip(),
                            "qualified_mbids": list(qualified),
                        },
                        sort_keys=True,
                    ),
                )
            )
            await db.commit()
            return ReviewAutomationSnapshot(
                review_id=item.id,
                track_id=track.id,
                release_id=item.release_id,
                claim_token=token,
                attempt_count=item.automation_attempt_count,
                verification_reason=item.verification_reason,
                original_expected_mbid=item.expected_recording_mbid,
                observed_mbids=observed,
                acoustid_score=max((per_mbid or {}).values(), default=0.0),
                evidence_revision=item.evidence_revision,
                catalog_track_id=catalog_track.id,
                qualified_mbids=qualified,
                fingerprint_duration_sec=item.fingerprint_duration_sec,
                source_path=Path(source_path),
                catalog_album_deezer_id=album_deezer_id,
                catalog_disc=catalog_track.disc,
                catalog_position=catalog_track.position,
                catalog_title=catalog_track.title,
                catalog_duration_sec=catalog_track.duration_sec,
                catalog_recording_mbid=catalog_track.recording_mbid,
                catalog_album_title=catalog_album.title,
                catalog_artist_id=catalog_artist.id,
                catalog_artist_name=catalog_artist.name,
                catalog_track_content_rating=catalog_track.content_rating,
                catalog_album_content_rating=catalog_album.content_rating,
                track_artist=str(track.artist or "").strip(),
                acoustid_recordings=_load_acoustid_recordings(track.acoustid_evidence_json),
            )

    def acoustid_consensus_shadow(self, snapshot: ReviewAutomationSnapshot) -> dict[str, object]:
        """Log what a strict AcoustID-only rule would do without authorizing it."""
        recordings: list[dict[str, object]] = []
        for recording in snapshot.acoustid_recordings:
            raw_score = recording.get("score")
            if isinstance(raw_score, (int, float)) and raw_score > self._acceptance_threshold:
                recordings.append(recording)
        base: dict[str, object] = {
            "rule": "strict_acoustid_consensus_v1",
            "mode": "shadow",
            "observed_mbids": list(snapshot.observed_mbids),
        }
        if not recordings:
            return {
                **base,
                "decision": "insufficient_evidence",
                "reason": "recordings_unavailable",
            }
        expected_title = _identity_title(snapshot.catalog_title)
        expected_artist = _identity_title(snapshot.track_artist)
        for recording in recordings:
            title = recording.get("title")
            artist = recording.get("artist")
            if not isinstance(title, str) or _identity_title(title) != expected_title:
                return {**base, "decision": "human_review", "reason": "title_contradiction"}
            if not isinstance(artist, str) or not expected_artist:
                return {
                    **base,
                    "decision": "insufficient_evidence",
                    "reason": "artist_unavailable",
                }
            if _identity_title(artist) != expected_artist:
                return {**base, "decision": "human_review", "reason": "artist_contradiction"}
        return {
            **base,
            "decision": "would_approve",
            "reason": "strict_title_artist_consensus",
        }

    def approval_result(
        self,
        snapshot: ReviewAutomationSnapshot,
        reference: ExactDeezerReference,
        alignment: AlignmentResult,
    ) -> ReviewAutomationResult:
        return ReviewAutomationResult(
            state="approved",
            reason="exact_deezer_preview_high_confidence_match",
            evidence={
                "decision": "approved",
                "reason": "exact_deezer_preview_high_confidence_match",
                "original_expected_mbid": snapshot.original_expected_mbid,
                "observed_mbids": list(snapshot.observed_mbids),
                "acoustid_score": snapshot.acoustid_score,
                "provider": "deezer",
                "provider_album_id": snapshot.catalog_album_deezer_id,
                "provider_track_id": reference.provider_track_id,
                "catalog_disc": snapshot.catalog_disc,
                "catalog_position": snapshot.catalog_position,
                "provider_title": reference.title,
                "provider_album_title": reference.album_title,
                "provider_artist_name": reference.artist_name,
                "provider_duration_sec": reference.duration_sec,
                "fingerprint_duration_sec": snapshot.fingerprint_duration_sec,
                "alignment_confidence": alignment.confidence,
                "alignment_score": alignment.score,
                "alignment_offset_seconds": alignment.offset_seconds,
            },
        )

    def transient_failure_result(self, reason: str) -> ReviewAutomationResult:
        return ReviewAutomationResult(
            state="retry",
            reason=reason,
            evidence={"decision": "retry", "reason": reason},
        )

    def _rejection_result(self, reason: str, **evidence: object) -> ReviewAutomationResult:
        return ReviewAutomationResult(
            state="rejected",
            reason=reason,
            evidence={"decision": "human_review", "reason": reason, **evidence},
        )

    async def evaluate(
        self,
        snapshot: ReviewAutomationSnapshot,
        *,
        alignment_source_path: Path | None = None,
    ) -> ReviewAutomationResult:
        if not snapshot.observed_mbids:
            return self._rejection_result("malformed_observed_acoustid_evidence")
        if not snapshot.qualified_mbids:
            return self._rejection_result("no_mbid_strictly_above_threshold")
        if not _duration_matches(snapshot.fingerprint_duration_sec, snapshot.catalog_duration_sec):
            return self._rejection_result(
                "fingerprint_duration_mismatch",
                fingerprint_duration_sec=snapshot.fingerprint_duration_sec,
                catalog_duration_sec=snapshot.catalog_duration_sec,
            )
        source_is_valid = await asyncio.to_thread(
            _source_is_in_staging,
            snapshot.source_path,
            Path(self._settings.staging_root),
        )
        if not source_is_valid:
            return self._rejection_result("source_outside_or_missing_staging")
        try:
            reference = await self._reference_resolver(snapshot, self._settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "Review automation provider resolution failed for review %d (%s)",
                snapshot.review_id,
                "provider_error",
            )
            return self.transient_failure_result("provider_unavailable")
        if reference is None:
            return self.transient_failure_result("exact_deezer_reference_unavailable")
        if _identity_title(reference.title) != _identity_title(snapshot.catalog_title):
            return self._rejection_result(
                "provider_title_mismatch", provider_track_id=reference.provider_track_id
            )
        if _identity_title(reference.album_title) != _identity_title(snapshot.catalog_album_title):
            return self._rejection_result(
                "provider_album_title_mismatch", provider_track_id=reference.provider_track_id
            )
        if _identity_title(reference.artist_name) != _identity_title(snapshot.catalog_artist_name):
            return self._rejection_result(
                "provider_artist_mismatch", provider_track_id=reference.provider_track_id
            )
        target_track_artist = snapshot.track_artist or snapshot.catalog_artist_name
        if _identity_title(reference.track_artist_name) != _identity_title(target_track_artist):
            return self._rejection_result(
                "provider_track_artist_mismatch", provider_track_id=reference.provider_track_id
            )
        if _rating_conflicts(
            reference.track_content_rating, snapshot.catalog_track_content_rating
        ) or _rating_conflicts(
            reference.album_content_rating, snapshot.catalog_album_content_rating
        ):
            return self._rejection_result(
                "provider_content_rating_mismatch",
                provider_track_id=reference.provider_track_id,
            )
        if not _duration_matches(reference.duration_sec, snapshot.catalog_duration_sec):
            return self._rejection_result(
                "provider_duration_mismatch",
                provider_track_id=reference.provider_track_id,
                provider_duration_sec=reference.duration_sec,
                catalog_duration_sec=snapshot.catalog_duration_sec,
            )
        try:
            alignment = await self._aligner(
                alignment_source_path or snapshot.source_path,
                reference.preview_url,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "Review automation alignment failed for review %d (%s)",
                snapshot.review_id,
                "alignment_error",
            )
            return self.transient_failure_result("alignment_unavailable")
        if alignment is None or alignment.confidence != "high":
            return self._rejection_result(
                "alignment_not_high_confidence",
                provider_track_id=reference.provider_track_id,
                alignment_confidence=alignment.confidence if alignment is not None else None,
            )
        return self.approval_result(snapshot, reference, alignment)

    async def apply_result(
        self, snapshot: ReviewAutomationSnapshot, result: ReviewAutomationResult
    ) -> bool:
        now = self._now()
        current_source = None
        if result.state == "approved":
            current_source = await asyncio.to_thread(
                _source_identity, snapshot.source_path, Path(self._settings.staging_root)
            )
            if current_source != (
                snapshot.source_sha256,
                snapshot.source_size,
                snapshot.source_mtime_ns,
            ):
                result = self._rejection_result("source_identity_changed")
        async with self._factory() as db:
            await db.execute(text("BEGIN IMMEDIATE"))
            row = (
                await db.execute(
                    select(
                        StagingReviewItem,
                        Track,
                        CatalogAlbumTrack,
                        CatalogAlbum,
                        CatalogArtist,
                    )
                    .join(Track, Track.id == StagingReviewItem.track_id)
                    .join(CatalogAlbumTrack, CatalogAlbumTrack.id == Track.catalog_track_id)
                    .join(CatalogAlbum, CatalogAlbum.id == CatalogAlbumTrack.album_id)
                    .join(CatalogArtist, CatalogArtist.id == CatalogAlbum.artist_id)
                    .where(StagingReviewItem.id == snapshot.review_id)
                )
            ).one_or_none()
            if row is None:
                await db.rollback()
                return False
            item, track, catalog_track, catalog_album, catalog_artist = row
            fenced = (
                item.review_state == ReviewDecision.pending
                and item.automation_state == "claimed"
                and item.automation_claim_token == snapshot.claim_token
                and item.evidence_revision == snapshot.evidence_revision
                and track.id == snapshot.track_id
                and track.catalog_track_id == snapshot.catalog_track_id
                and str(track.staging_path or track.source_path or "").strip()
                == str(snapshot.source_path)
                and catalog_track.id == snapshot.catalog_track_id
                and catalog_track.disc == snapshot.catalog_disc
                and catalog_track.position == snapshot.catalog_position
                and catalog_track.title == snapshot.catalog_title
                and catalog_track.duration_sec == snapshot.catalog_duration_sec
                and catalog_track.recording_mbid == snapshot.catalog_recording_mbid
                and str(catalog_album.deezer_id or "").strip() == snapshot.catalog_album_deezer_id
                and catalog_album.title == snapshot.catalog_album_title
                and catalog_album.content_rating == snapshot.catalog_album_content_rating
                and catalog_album.artist_id == snapshot.catalog_artist_id
                and catalog_artist.id == snapshot.catalog_artist_id
                and catalog_artist.name == snapshot.catalog_artist_name
                and catalog_track.content_rating == snapshot.catalog_track_content_rating
                and str(track.artist or "").strip() == snapshot.track_artist
                and item.expected_recording_mbid == snapshot.original_expected_mbid
            )
            if not fenced:
                await db.rollback()
                return False
            evidence = {
                **result.evidence,
                "attempt": snapshot.attempt_count,
                "attempted_at": now.isoformat(),
                "evidence_revision": snapshot.evidence_revision,
                "source_path": str(snapshot.source_path),
                "source_sha256": snapshot.source_sha256,
                "source_size": snapshot.source_size,
                "source_mtime_ns": snapshot.source_mtime_ns,
            }
            item.automation_last_attempted_at = now
            item.automation_claim_token = None
            item.automation_claimed_at = None
            item.automation_decision_json = json.dumps(evidence, sort_keys=True)
            if result.state == "approved":
                item.review_state = ReviewDecision.approved
                item.reviewed_at = now
                item.automation_state = "approved"
                item.automation_next_attempt_at = None
                item.import_dispatch_state = "pending"
                item.import_dispatch_next_attempt_at = now
                track.acoustid_verification_state = AcoustIDVerificationState.approved
                track.mbid = (
                    snapshot.qualified_mbids[0] if len(snapshot.qualified_mbids) == 1 else None
                )
                track.identity_state = (
                    IdentityResolutionState.resolved
                    if track.mbid
                    else IdentityResolutionState.unresolved
                )
                evidence["track_mbid_action"] = (
                    "replaced_with_unique_qualified"
                    if track.mbid
                    else "cleared_ambiguous_qualified"
                )
                item.automation_decision_json = json.dumps(evidence, sort_keys=True)
                _merge_track_automation_evidence(track, evidence)
            elif result.state == "retry":
                if snapshot.attempt_count >= self._max_attempts:
                    item.automation_state = "failed"
                    item.automation_next_attempt_at = None
                    evidence["decision"] = "human_review"
                    evidence["reason"] = "retry_limit_reached"
                    evidence["last_transient_reason"] = result.reason
                    item.automation_decision_json = json.dumps(evidence, sort_keys=True)
                else:
                    item.automation_state = "retry"
                    item.automation_next_attempt_at = now + timedelta(
                        seconds=min(300, 15 * (2 ** (snapshot.attempt_count - 1)))
                    )
            else:
                item.automation_state = "rejected"
                item.automation_next_attempt_at = None
            attempt = await db.scalar(
                select(ReviewAutomationAttempt).where(
                    ReviewAutomationAttempt.claim_token == snapshot.claim_token
                )
            )
            if attempt is not None:
                attempt.state = "completed"
                attempt.completed_at = now
                attempt.decision_json = json.dumps(evidence, sort_keys=True)
            await db.commit()
        return True

    async def dispatch_pending_imports(self, *, limit: int = 10) -> int:
        now = self._now()
        stale_before = now - self._import_claim_timeout
        eligible = or_(
            (
                StagingReviewItem.import_dispatch_state.in_(("pending", "retry"))
                & or_(
                    StagingReviewItem.import_dispatch_next_attempt_at.is_(None),
                    StagingReviewItem.import_dispatch_next_attempt_at <= now,
                )
            ),
            (
                (StagingReviewItem.import_dispatch_state == "dispatching")
                & StagingReviewItem.import_dispatch_claimed_at.is_not(None)
                & (StagingReviewItem.import_dispatch_claimed_at <= stale_before)
            ),
        )
        async with self._factory() as db:
            rows = (
                await db.execute(
                    select(StagingReviewItem.id, StagingReviewItem.release_id)
                    .where(
                        StagingReviewItem.review_state == ReviewDecision.approved,
                        eligible,
                    )
                    .order_by(StagingReviewItem.id)
                    .limit(limit)
                )
            ).all()
        processed = 0
        for review_id, release_id in rows:
            dispatch_token = str(uuid.uuid4())
            approved_source_path: Path | None = None
            approved_source_path_matches = False
            approved_source_sha256: str | None = None
            approved_source_size: int | None = None
            approved_source_mtime_ns: int | None = None
            async with self._factory() as db:
                await db.execute(text("BEGIN IMMEDIATE"))
                claimed_id = await db.scalar(
                    update(StagingReviewItem)
                    .where(
                        StagingReviewItem.id == review_id,
                        StagingReviewItem.review_state == ReviewDecision.approved,
                        eligible,
                    )
                    .values(
                        import_dispatch_state="dispatching",
                        import_dispatch_claim_token=dispatch_token,
                        import_dispatch_claimed_at=now,
                        import_dispatch_next_attempt_at=None,
                    )
                    .returning(StagingReviewItem.id)
                )
                if claimed_id is None:
                    await db.rollback()
                    continue
                claimed_item = await db.get(StagingReviewItem, review_id)
                if claimed_item is None:
                    await db.rollback()
                    continue
                previous_dispatch_attempt = await db.scalar(
                    select(ReviewAutomationAttempt).where(
                        ReviewAutomationAttempt.review_item_id == review_id,
                        ReviewAutomationAttempt.state == "claimed",
                        ReviewAutomationAttempt.input_json.like('%"kind": "import_dispatch"%'),
                    )
                )
                if previous_dispatch_attempt is not None:
                    previous_dispatch_attempt.state = "abandoned"
                    previous_dispatch_attempt.completed_at = now
                    previous_dispatch_attempt.import_outcome_json = json.dumps(
                        {"outcome": "abandoned", "reason": "stale_dispatch_claim_replaced"},
                        sort_keys=True,
                    )
                db.add(
                    ReviewAutomationAttempt(
                        review_item_id=review_id,
                        track_id=claimed_item.track_id,
                        release_id=claimed_item.release_id,
                        attempt_number=claimed_item.import_dispatch_attempt_count + 1,
                        evidence_revision=claimed_item.evidence_revision,
                        claim_token=dispatch_token,
                        state="claimed",
                        claimed_at=now,
                        input_json=json.dumps(
                            {
                                "kind": "import_dispatch",
                                "approved_decision": claimed_item.automation_decision,
                            },
                            sort_keys=True,
                        ),
                    )
                )
                claimed_track = await db.get(Track, claimed_item.track_id)
                source_path = str(
                    (claimed_track.staging_path or claimed_track.source_path or "")
                    if claimed_track is not None
                    else ""
                ).strip()
                decision = claimed_item.automation_decision
                source_sha256 = decision.get("source_sha256")
                source_size = decision.get("source_size")
                source_mtime_ns = decision.get("source_mtime_ns")
                expected_source_path = decision.get("source_path")
                approved_source_path = Path(source_path) if source_path else None
                approved_source_path_matches = (
                    isinstance(expected_source_path, str) and expected_source_path == source_path
                )
                approved_source_sha256 = (
                    str(source_sha256) if isinstance(source_sha256, str) else None
                )
                approved_source_size = source_size if isinstance(source_size, int) else None
                approved_source_mtime_ns = (
                    source_mtime_ns if isinstance(source_mtime_ns, int) else None
                )
                await db.commit()
            succeeded = False
            failure_reason: str | None = None
            if not approved_source_path_matches:
                failure_reason = "source_path_changed_after_approval"
            elif (
                approved_source_path is None
                or approved_source_sha256 is None
                or approved_source_size is None
                or approved_source_mtime_ns is None
            ):
                failure_reason = "approved_source_identity_missing"
            else:
                current_identity = (
                    await asyncio.to_thread(
                        _source_identity,
                        approved_source_path,
                        Path(self._settings.staging_root),
                    )
                    if approved_source_path is not None
                    else None
                )
                if current_identity != (
                    approved_source_sha256,
                    approved_source_size,
                    approved_source_mtime_ns,
                ):
                    failure_reason = "source_identity_changed_after_approval"
            if failure_reason is None:
                try:
                    async with self._factory() as db:
                        release = await db.get(Release, release_id)
                        if release is not None:
                            async with asyncio.timeout(30 * 60):
                                succeeded = await self._auto_importer(
                                    db,
                                    release,
                                    library_root=self._settings.library_root,
                                    naming_template=self._settings.naming_template,
                                    source_artifacts={
                                        claimed_item.track_id: (
                                            approved_source_path,
                                            approved_source_sha256,
                                        )
                                    },
                                )
                            await db.commit()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning(
                        "Approved review %d import dispatch failed (%s)",
                        review_id,
                        "import_error",
                    )
            async with self._factory() as db:
                await db.execute(text("BEGIN IMMEDIATE"))
                item = await db.get(StagingReviewItem, review_id)
                if (
                    item is None
                    or item.review_state != ReviewDecision.approved
                    or item.import_dispatch_state != "dispatching"
                    or item.import_dispatch_claim_token != dispatch_token
                ):
                    await db.rollback()
                    continue
                item.import_dispatch_attempt_count += 1
                item.import_dispatch_claim_token = None
                item.import_dispatch_claimed_at = None
                outcome = {
                    "attempt": item.import_dispatch_attempt_count,
                    "outcome": "succeeded" if succeeded else "retry",
                    "attempted_at": now.isoformat(),
                }
                item.import_dispatch_outcome_json = json.dumps(outcome, sort_keys=True)
                if failure_reason is not None:
                    item.import_dispatch_state = "failed"
                    item.import_dispatch_next_attempt_at = None
                    outcome["outcome"] = "failed"
                    outcome["reason"] = failure_reason
                    item.import_dispatch_outcome_json = json.dumps(outcome, sort_keys=True)
                elif succeeded:
                    item.import_dispatch_state = "completed"
                    item.import_dispatch_next_attempt_at = None
                elif item.import_dispatch_attempt_count >= self._max_attempts:
                    item.import_dispatch_state = "failed"
                    item.import_dispatch_next_attempt_at = None
                    outcome["outcome"] = "failed"
                    outcome["reason"] = "retry_limit_reached"
                    item.import_dispatch_outcome_json = json.dumps(outcome, sort_keys=True)
                else:
                    item.import_dispatch_state = "retry"
                    item.import_dispatch_next_attempt_at = now + timedelta(
                        seconds=min(300, 15 * (2 ** (item.import_dispatch_attempt_count - 1)))
                    )
                attempt = await db.scalar(
                    select(ReviewAutomationAttempt).where(
                        ReviewAutomationAttempt.claim_token == dispatch_token
                    )
                )
                if attempt is not None:
                    attempt.state = "completed"
                    attempt.completed_at = self._now()
                    attempt.import_outcome_json = json.dumps(outcome, sort_keys=True)
                await db.commit()
            processed += 1
        return processed

    async def _record_claim_source_identity(self, snapshot: ReviewAutomationSnapshot) -> bool:
        async with self._factory() as db:
            await db.execute(text("BEGIN IMMEDIATE"))
            item = await db.get(StagingReviewItem, snapshot.review_id)
            attempt = await db.scalar(
                select(ReviewAutomationAttempt).where(
                    ReviewAutomationAttempt.claim_token == snapshot.claim_token
                )
            )
            if (
                item is None
                or attempt is None
                or item.automation_state != "claimed"
                or item.automation_claim_token != snapshot.claim_token
                or item.evidence_revision != snapshot.evidence_revision
            ):
                await db.rollback()
                return False
            try:
                inputs = json.loads(attempt.input_json or "{}")
            except (TypeError, ValueError):
                inputs = {}
            if not isinstance(inputs, dict):
                inputs = {}
            inputs.update(
                {
                    "source_sha256": snapshot.source_sha256,
                    "source_size": snapshot.source_size,
                    "source_mtime_ns": snapshot.source_mtime_ns,
                }
            )
            attempt.input_json = json.dumps(inputs, sort_keys=True)
            await db.commit()
            return True

    async def process_next(self) -> bool:
        snapshot = await self.claim_next()
        if snapshot is None:
            return False
        try:
            pinned_source = await asyncio.to_thread(
                _open_pinned_source,
                snapshot.source_path,
                Path(self._settings.staging_root),
            )
            if pinned_source is None:
                result = self._rejection_result("source_outside_or_missing_staging")
            else:
                pinned_stream, source_identity, pinned_path = pinned_source
                snapshot = replace(
                    snapshot,
                    source_sha256=source_identity[0],
                    source_size=source_identity[1],
                    source_mtime_ns=source_identity[2],
                )
                if not await self._record_claim_source_identity(snapshot):
                    await asyncio.to_thread(pinned_stream.close)
                    return True
                try:
                    result = await self.evaluate(
                        snapshot,
                        alignment_source_path=pinned_path,
                    )
                finally:
                    await asyncio.to_thread(pinned_stream.close)
        except asyncio.CancelledError:
            # The durable claim is intentionally left for stale-claim recovery.
            raise
        result = replace(
            result,
            evidence={
                **result.evidence,
                "acoustid_consensus_shadow": self.acoustid_consensus_shadow(snapshot),
            },
        )
        await self.apply_result(snapshot, result)
        return True

    async def run_cycle(self, *, limit: int = 10) -> int:
        if self._settings_provider is not None:
            self._settings, self._acceptance_threshold = await self._settings_provider()
        processed = 0
        for _ in range(max(0, limit)):
            if not await self.process_next():
                break
            processed += 1
        await self.dispatch_pending_imports(limit=limit)
        return processed


class ReviewAutomationScheduler:
    def __init__(
        self,
        service: ReviewAutomationService,
        *,
        interval_seconds: float = 60.0,
        batch_size: int = 10,
    ) -> None:
        self._service = service
        self._interval_seconds = max(0.1, interval_seconds)
        self._batch_size = max(1, batch_size)
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(
                self._run(), name="import-review-automation-scheduler"
            )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._service.run_cycle(limit=self._batch_size)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Import-review automation cycle failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                continue
