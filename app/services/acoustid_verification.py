from __future__ import annotations

import json
import logging
import math
from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.metadata.filename_parse import normalize_for_catalog_match, strip_non_identity_descriptors
from app.models.staging_review import ReviewAutomationAttempt, StagingReviewItem
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import AcoustIDVerificationState, ReviewDecision
from app.services.acoustid_evidence import (
    canonical_recording_mbid,
    parse_consistent_acoustid_evidence,
)

logger = logging.getLogger(__name__)
_MIN_CONFIDENCE = 0.6


def _finite_score(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    try:
        score = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return score if math.isfinite(score) and 0.0 <= score <= 1.0 else None


def _load_sanitized_recordings(raw: str | None) -> list[dict[str, object]]:
    try:
        payload = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return []
    recordings = payload.get("recordings") if isinstance(payload, dict) else None
    return [dict(item) for item in recordings or [] if isinstance(item, dict)]


def _best_acoustid_score(acoustid_results: list[dict[str, object]]) -> float:
    """Return the highest confidence score from AcoustID raw results."""
    best = 0.0
    for result in acoustid_results:
        score = _finite_score(result.get("score", 0.0)) or 0.0
        if score > best:
            best = score
    return best


def _extract_mbid_scores(
    acoustid_results: list[dict[str, object]], *, min_confidence: float
) -> dict[str, float]:
    """Return each observed recording MBID with its own highest result score."""
    scores: dict[str, float] = {}
    for result in acoustid_results:
        score = _finite_score(result.get("score", 0.0)) or 0.0
        recordings = result.get("recordings")
        if score < min_confidence or not isinstance(recordings, list):
            continue
        for recording in recordings:
            if not isinstance(recording, dict):
                continue
            rid = recording.get("id")
            if rid:
                mbid = str(rid)
                scores[mbid] = max(score, scores.get(mbid, 0.0))
    return dict(sorted(scores.items(), key=lambda item: item[1], reverse=True))


def _sanitized_recording_evidence(
    acoustid_results: list[dict[str, object]], *, min_confidence: float
) -> list[dict[str, object]]:
    """Project only bounded identity evidence needed by later review policy."""
    recordings_by_mbid: dict[str, dict[str, object]] = {}
    for result in acoustid_results:
        score = _finite_score(result.get("score", 0.0))
        if score is None:
            continue
        raw_recordings = result.get("recordings")
        if score < min_confidence or not isinstance(raw_recordings, list):
            continue
        for recording in raw_recordings:
            if not isinstance(recording, dict):
                continue
            mbid = str(recording.get("id") or "").strip()
            if not mbid:
                continue
            title = recording.get("title")
            artists = recording.get("artists")
            artist_names = []
            if isinstance(artists, list):
                artist_names = [
                    str(artist.get("name") or "").strip()
                    for artist in artists
                    if isinstance(artist, dict) and str(artist.get("name") or "").strip()
                ]
            candidate: dict[str, object] = {
                "mbid": mbid,
                "score": score,
                "title": str(title).strip() if isinstance(title, str) else None,
                "artist": ", ".join(artist_names) or None,
            }
            previous = recordings_by_mbid.get(mbid)
            if previous is None or _recording_evidence_score(previous) < score:
                recordings_by_mbid[mbid] = candidate
    return sorted(recordings_by_mbid.values(), key=_recording_evidence_score, reverse=True)


def _recording_evidence_score(evidence: dict[str, object]) -> float:
    return _finite_score(evidence.get("score", 0.0)) or 0.0


def _normalize_recording_title(title: str | None) -> str:
    return normalize_for_catalog_match(
        strip_non_identity_descriptors(title or "", preserve_featured_artists=False)
    )


def _duration_matches_target(
    *, fingerprint_duration_sec: int | None, target_duration_sec: int | None
) -> bool:
    if fingerprint_duration_sec is None or target_duration_sec is None:
        return True
    return abs(fingerprint_duration_sec - target_duration_sec) <= 8


def _matching_recording_title_mbids(
    acoustid_results: list[dict[str, object]],
    track_title: str | None,
    acceptance_threshold: float,
) -> list[str]:
    expected_title = _normalize_recording_title(track_title)
    if not expected_title:
        return []
    matching: dict[str, float] = {}
    contradictory_title = False
    for result in acoustid_results:
        result_score = _finite_score(result.get("score"))
        if result_score is None:
            continue
        if result_score <= acceptance_threshold:
            continue
        recordings = result.get("recordings")
        if not isinstance(recordings, list):
            continue
        for recording in recordings:
            if not isinstance(recording, dict):
                continue
            mbid = str(recording.get("id") or "").strip()
            if not mbid:
                continue
            observed_title = recording.get("title")
            if not isinstance(observed_title, str):
                contradictory_title = True
                continue
            if _normalize_recording_title(observed_title) == expected_title:
                matching[mbid] = max(result_score, matching.get(mbid, 0.0))
            else:
                contradictory_title = True
    if contradictory_title:
        return []
    return [
        mbid for mbid, _score in sorted(matching.items(), key=lambda item: item[1], reverse=True)
    ]


async def reconcile_matching_acoustid_reviews(
    db: AsyncSession, *, acceptance_threshold: float
) -> int:
    """Promote persisted reviews whose expected MBID already has acceptable evidence."""
    rows = (
        await db.execute(
            select(StagingReviewItem, Track)
            .join(Track, Track.id == StagingReviewItem.track_id)
            .where(StagingReviewItem.review_state == ReviewDecision.pending)
            .order_by(StagingReviewItem.id)
        )
    ).all()
    reconciled = 0
    for item, track in rows:
        expected = canonical_recording_mbid(item.expected_recording_mbid or track.mbid)
        consistent = parse_consistent_acoustid_evidence(
            item.observed_acoustid_mbids_json,
            item.observed_acoustid_evidence_json,
        )
        scores_by_mbid = consistent[1] if consistent is not None else None
        if (
            expected
            and scores_by_mbid is not None
            and scores_by_mbid.get(expected, 0.0) > acceptance_threshold
        ):
            track.mbid = track.mbid or expected
            track.acoustid_verification_state = AcoustIDVerificationState.verified
            await db.delete(item)
            reconciled += 1
    await db.flush()
    return reconciled


async def run_acoustid_verification(
    track: Track,
    *,
    acoustid_raw_results: list[dict[str, object]],
    fingerprint_duration_sec: int | None,
    db: AsyncSession,
    acceptance_threshold: float = 0.90,
) -> AcoustIDVerificationState:
    """Compare AcoustID lookup results against the track's expected recording MBID.

    Updates track.acoustid_verification_state and track.acoustid_evidence_json.
    Creates a StagingReviewItem when human review is required.
    Returns the resulting verification state.
    """
    mbid_scores = _extract_mbid_scores(
        acoustid_raw_results,
        min_confidence=min(_MIN_CONFIDENCE, acceptance_threshold),
    )
    observed_mbids = list(mbid_scores)
    best_score = _best_acoustid_score(acoustid_raw_results)
    best_recording_score = max(mbid_scores.values(), default=0.0)

    track.acoustid_evidence_json = json.dumps(
        {
            "observed_mbids": observed_mbids,
            "best_score": best_score,
            "result_count": len(acoustid_raw_results),
            "recordings": _sanitized_recording_evidence(
                acoustid_raw_results,
                min_confidence=min(_MIN_CONFIDENCE, acceptance_threshold),
            ),
        },
        sort_keys=True,
    )

    expected_mbid = track.mbid

    if observed_mbids and expected_mbid:
        if mbid_scores.get(expected_mbid, 0.0) > acceptance_threshold:
            track.acoustid_verification_state = AcoustIDVerificationState.verified
            if track.id is not None:
                await db.execute(
                    delete(StagingReviewItem).where(
                        StagingReviewItem.track_id == track.id,
                        StagingReviewItem.review_state == ReviewDecision.pending,
                    )
                )
            return AcoustIDVerificationState.verified
        reason = (
            "low_confidence"
            if expected_mbid in observed_mbids and len(observed_mbids) == 1
            else ("ambiguous" if expected_mbid in observed_mbids else "mismatch")
        )
        state = (
            AcoustIDVerificationState.mismatch
            if reason == "mismatch"
            else AcoustIDVerificationState.unavailable
        )
        track.acoustid_verification_state = state
        await _create_review_item(
            track,
            observed_mbids=observed_mbids,
            best_score=best_score,
            duration_sec=fingerprint_duration_sec,
            reason=reason,
            db=db,
        )
        return state

    if not observed_mbids:
        # No AcoustID result
        if expected_mbid and track.identity_state == IdentityResolutionState.resolved:
            # Known identity from deterministic source (catalog MBID) — treat as
            # requiring review rather than silently approving an unconfirmed match.
            track.acoustid_verification_state = AcoustIDVerificationState.unavailable
            await _create_review_item(
                track,
                observed_mbids=[],
                best_score=0.0,
                duration_sec=fingerprint_duration_sec,
                reason="unavailable",
                db=db,
            )
            return AcoustIDVerificationState.unavailable
        else:
            # No expected MBID and no AcoustID result — cannot verify at all.
            track.acoustid_verification_state = AcoustIDVerificationState.unavailable
            await _create_review_item(
                track,
                observed_mbids=[],
                best_score=0.0,
                duration_sec=fingerprint_duration_sec,
                reason="no_fingerprint_result",
                db=db,
            )
            return AcoustIDVerificationState.unavailable

    # Without an expected MBID, a strict high-confidence fingerprint can verify a
    # catalog-selected track, but never a title-only/unbound download.
    title_matching_mbids = _matching_recording_title_mbids(
        acoustid_raw_results, track.title, acceptance_threshold
    )
    if (
        best_recording_score > acceptance_threshold
        and track.catalog_track_id is not None
        and title_matching_mbids
        and set(observed_mbids).issubset(set(title_matching_mbids))
        and _duration_matches_target(
            fingerprint_duration_sec=fingerprint_duration_sec,
            target_duration_sec=track.duration_sec,
        )
    ):
        if len(title_matching_mbids) == 1:
            track.mbid = track.mbid or title_matching_mbids[0]
        track.acoustid_verification_state = AcoustIDVerificationState.verified
        return AcoustIDVerificationState.verified
    track.acoustid_verification_state = AcoustIDVerificationState.unavailable
    await _create_review_item(
        track,
        observed_mbids=observed_mbids,
        best_score=best_score,
        duration_sec=fingerprint_duration_sec,
        reason="no_expected_mbid",
        db=db,
    )
    return AcoustIDVerificationState.unavailable


async def _create_review_item(
    track: Track,
    *,
    observed_mbids: list[str],
    best_score: float,
    duration_sec: int | None,
    reason: str,
    db: AsyncSession,
) -> StagingReviewItem:
    evidence_by_mbid: dict[str, dict[str, object]] = {}
    for recording in _load_sanitized_recordings(track.acoustid_evidence_json):
        mbid = str(recording.get("mbid") or "").strip()
        score = recording.get("score")
        if mbid and isinstance(score, (int, float)):
            previous = evidence_by_mbid.get(mbid)
            previous_score = previous.get("score") if previous is not None else None
            if previous is None or (
                isinstance(previous_score, (int, float)) and float(score) > float(previous_score)
            ):
                evidence_by_mbid[mbid] = dict(recording)
    observed_json = json.dumps(observed_mbids, sort_keys=True)
    evidence_json = json.dumps(
        [evidence_by_mbid[mbid] for mbid in observed_mbids if mbid in evidence_by_mbid],
        sort_keys=True,
    )
    existing = await db.scalar(
        select(StagingReviewItem).where(
            StagingReviewItem.track_id == track.id,
            StagingReviewItem.review_state == ReviewDecision.pending,
        )
    )
    if existing is not None:
        if existing.automation_claim_token:
            attempt = await db.scalar(
                select(ReviewAutomationAttempt).where(
                    ReviewAutomationAttempt.claim_token == existing.automation_claim_token
                )
            )
            if attempt is not None and attempt.state == "claimed":
                attempt.state = "abandoned"
                attempt.completed_at = datetime.now(UTC)
                attempt.decision_json = json.dumps(
                    {"decision": "abandoned", "reason": "verification_evidence_replaced"},
                    sort_keys=True,
                )
        existing.expected_recording_mbid = track.mbid
        existing.expected_title = track.title
        existing.observed_acoustid_mbids_json = observed_json
        existing.observed_acoustid_evidence_json = evidence_json
        existing.fingerprint_duration_sec = duration_sec
        existing.acoustid_score = best_score if best_score > 0.0 else None
        existing.verification_reason = reason
        existing.evidence_revision += 1
        existing.automation_state = "pending"
        existing.automation_attempt_count = 0
        existing.automation_claim_token = None
        existing.automation_claimed_at = None
        existing.automation_next_attempt_at = None
        existing.automation_decision_json = json.dumps(
            {"decision": "refreshed", "reason": "verification_evidence_replaced"},
            sort_keys=True,
        )
        await db.flush()
        return existing
    item = StagingReviewItem(
        track_id=track.id,
        release_id=track.release_id,
        expected_recording_mbid=track.mbid,
        expected_title=track.title,
        observed_acoustid_mbids_json=observed_json,
        observed_acoustid_evidence_json=evidence_json,
        fingerprint_duration_sec=duration_sec,
        acoustid_score=best_score if best_score > 0.0 else None,
        verification_reason=reason,
        review_state=ReviewDecision.pending,
    )
    db.add(item)
    await db.flush()
    logger.info(
        "Created StagingReviewItem %d for track %d (reason=%s)",
        item.id,
        track.id,
        reason,
    )
    return item
