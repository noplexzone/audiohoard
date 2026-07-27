from __future__ import annotations

import json
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.staging_review import StagingReviewItem
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import AcoustIDVerificationState, ReviewDecision

logger = logging.getLogger(__name__)
_MIN_CONFIDENCE = 0.6


def _best_acoustid_score(acoustid_results: list[dict[str, object]]) -> float:
    """Return the highest confidence score from AcoustID raw results."""
    best = 0.0
    for result in acoustid_results:
        raw_score = result.get("score", 0.0)
        try:
            score = float(raw_score) if isinstance(raw_score, (str, int, float)) else 0.0
        except ValueError:
            score = 0.0
        if score > best:
            best = score
    return best


def _extract_mbids(acoustid_results: list[dict[str, object]]) -> list[str]:
    """Extract all recording MBIDs from AcoustID raw results, sorted by score descending."""
    pairs: list[tuple[float, str]] = []
    for result in acoustid_results:
        raw_score = result.get("score", 0.0)
        try:
            score = float(raw_score) if isinstance(raw_score, (str, int, float)) else 0.0
        except ValueError:
            score = 0.0
        recordings = result.get("recordings")
        if not isinstance(recordings, list):
            continue
        for recording in recordings:
            if not isinstance(recording, dict):
                continue
            rid = recording.get("id")
            if rid and score >= _MIN_CONFIDENCE:
                pairs.append((score, str(rid)))
    pairs.sort(key=lambda x: x[0], reverse=True)
    return list(dict.fromkeys(mbid for _, mbid in pairs))


async def run_acoustid_verification(
    track: Track,
    *,
    acoustid_raw_results: list[dict[str, object]],
    fingerprint_duration_sec: int | None,
    db: AsyncSession,
) -> AcoustIDVerificationState:
    """Compare AcoustID lookup results against the track's expected recording MBID.

    Updates track.acoustid_verification_state and track.acoustid_evidence_json.
    Creates a StagingReviewItem when human review is required.
    Returns the resulting verification state.
    """
    observed_mbids = _extract_mbids(acoustid_raw_results)
    best_score = _best_acoustid_score(acoustid_raw_results)

    track.acoustid_evidence_json = json.dumps(
        {
            "observed_mbids": observed_mbids,
            "best_score": best_score,
            "result_count": len(acoustid_raw_results),
        },
        sort_keys=True,
    )

    expected_mbid = track.mbid

    if observed_mbids and expected_mbid:
        if expected_mbid in observed_mbids and len(observed_mbids) == 1:
            track.acoustid_verification_state = AcoustIDVerificationState.verified
            return AcoustIDVerificationState.verified
        reason = "ambiguous" if expected_mbid in observed_mbids else "mismatch"
        state = (
            AcoustIDVerificationState.unavailable
            if reason == "ambiguous"
            else AcoustIDVerificationState.mismatch
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

    # observed_mbids present but no expected MBID — cannot confirm identity.
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
    existing = await db.scalar(
        select(StagingReviewItem).where(
            StagingReviewItem.track_id == track.id,
            StagingReviewItem.review_state == ReviewDecision.pending,
        )
    )
    if existing is not None:
        return existing
    item = StagingReviewItem(
        track_id=track.id,
        release_id=track.release_id,
        expected_recording_mbid=track.mbid,
        expected_title=track.title,
        observed_acoustid_mbids_json=json.dumps(observed_mbids),
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
