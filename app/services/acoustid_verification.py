from __future__ import annotations

import json
import logging

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.metadata.filename_parse import normalize_for_catalog_match, strip_non_identity_descriptors
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


def _extract_mbid_scores(
    acoustid_results: list[dict[str, object]], *, min_confidence: float
) -> dict[str, float]:
    """Return each observed recording MBID with its own highest result score."""
    scores: dict[str, float] = {}
    for result in acoustid_results:
        raw_score = result.get("score", 0.0)
        try:
            score = float(raw_score) if isinstance(raw_score, (str, int, float)) else 0.0
        except ValueError:
            score = 0.0
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
        raw_score = result.get("score")
        if not isinstance(raw_score, (int, float, str)):
            continue
        try:
            result_score = float(raw_score)
        except ValueError:
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
        expected = str(item.expected_recording_mbid or track.mbid or "").strip()
        observed = {str(value).strip() for value in item.observed_acoustid_mbids}
        if (
            expected
            and expected in observed
            and (item.acoustid_score or 0.0) > acceptance_threshold
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
