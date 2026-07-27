from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.release import Release
from app.models.release_candidate import MatchReviewState, ReleaseCandidate
from app.models.track import Track
from app.models.workflow import ImportWorkflowState

_AUTO_SELECT_THRESHOLD = 0.75
_TIE_EPSILON = 0.001
_DURATION_DRIFT_SECONDS = 15


@dataclass(frozen=True)
class EditionEvidence:
    recording_mbid: str | None = None
    release_mbid: str | None = None
    medium_position: int | None = None
    track_position: int | None = None
    duration_sec: int | None = None
    track_count: int | None = None
    year: str | None = None
    country: str | None = None
    label: str | None = None
    catalog_number: str | None = None
    barcode: str | None = None
    artist: str | None = None
    album_artist: str | None = None
    source: str = "unknown"
    quality: dict[str, object] | None = None


@dataclass(frozen=True)
class _Score:
    value: float
    reasons: list[str]
    contradictions: list[str]
    has_recording_agreement: bool
    has_release_agreement: bool


@dataclass(frozen=True)
class MatchResolution:
    state: ImportWorkflowState
    candidates: list[ReleaseCandidate]
    selected_candidate: ReleaseCandidate | None


def _norm(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.casefold().strip().split())
    return normalized or None


def _score_evidence(release: Release, track: Track, evidence: EditionEvidence) -> _Score:
    score = 0.0
    reasons: list[str] = []
    contradictions: list[str] = []
    has_recording_agreement = False
    has_release_agreement = False

    if evidence.recording_mbid and track.mbid and evidence.recording_mbid == track.mbid:
        score += 0.45
        has_recording_agreement = True
        reasons.append("recording MBID agrees")
    elif evidence.recording_mbid and track.mbid and evidence.recording_mbid != track.mbid:
        contradictions.append("recording MBID contradicts track identity")

    release_mbid_matches = (
        evidence.release_mbid
        and release.release_mbid
        and evidence.release_mbid == release.release_mbid
    )
    release_mbid_conflicts = (
        evidence.release_mbid
        and release.release_mbid
        and evidence.release_mbid != release.release_mbid
    )
    if release_mbid_matches:
        score += 0.35
        has_release_agreement = True
        reasons.append("release MBID agrees")
    elif release_mbid_conflicts:
        contradictions.append("release MBID contradicts release identity")

    if evidence.source == "acoustid" and not has_release_agreement:
        reasons.append("AcoustID evidence identifies recording only, not edition")

    if evidence.medium_position is not None and track.disc is not None:
        if evidence.medium_position == track.disc:
            score += 0.05
            reasons.append("medium position agrees")
        else:
            contradictions.append("medium position contradicts track metadata")

    if evidence.track_position is not None and track.track_no is not None:
        if evidence.track_position == track.track_no:
            score += 0.05
            reasons.append("track position agrees")
        else:
            contradictions.append("track position contradicts track metadata")

    if evidence.duration_sec is not None and track.duration_sec is not None:
        if abs(evidence.duration_sec - track.duration_sec) <= _DURATION_DRIFT_SECONDS:
            score += 0.04
            reasons.append("duration within drift tolerance")
        else:
            contradictions.append("duration exceeds drift tolerance")

    if evidence.track_count is not None and release.track_count is not None:
        if evidence.track_count == release.track_count:
            score += 0.03
            reasons.append("track count agrees")
        else:
            contradictions.append("track count contradicts release metadata")

    for attr, label in (("year", "year"), ("country", "country"), ("label", "label")):
        ev = _norm(getattr(evidence, attr))
        rel = _norm(getattr(release, attr))
        if ev and rel and ev == rel:
            score += 0.02
            reasons.append(f"{label} agrees")
        elif ev and rel and ev != rel:
            contradictions.append(f"{label} contradicts release metadata")

    for attr, label in (("barcode", "barcode"), ("catalog_number", "catalog number")):
        ev = _norm(getattr(evidence, attr))
        rel = _norm(getattr(release, attr))
        if ev and rel and ev == rel:
            score += 0.03
            has_release_agreement = True
            reasons.append(f"{label} agrees")
        elif ev and rel and ev != rel:
            contradictions.append(f"{label} contradicts release metadata")

    if _norm(release.album_artist) == "various artists" and _norm(evidence.artist) == _norm(
        track.artist
    ):
        score += 0.02
        reasons.append("compilation artist evidence accepted")

    return _Score(score, reasons, contradictions, has_recording_agreement, has_release_agreement)


def _candidate_from_evidence(
    release: Release, track: Track, evidence: EditionEvidence, scored: _Score
) -> ReleaseCandidate:
    reasons = [*scored.reasons]
    if scored.contradictions:
        reasons.extend(f"needs review: {reason}" for reason in scored.contradictions)
    elif not scored.has_recording_agreement:
        reasons.append("needs review: missing recording evidence agreement")
    elif not scored.has_release_agreement:
        reasons.append("needs review: missing release edition evidence agreement")

    return ReleaseCandidate(
        release_id=release.id,
        track_id=track.id,
        recording_mbid=evidence.recording_mbid,
        release_mbid=evidence.release_mbid,
        medium_position=evidence.medium_position,
        track_position=evidence.track_position,
        duration_sec=evidence.duration_sec,
        track_count=evidence.track_count,
        year=evidence.year,
        country=evidence.country,
        label=evidence.label,
        catalog_number=evidence.catalog_number,
        barcode=evidence.barcode,
        quality_json=json.dumps(evidence.quality, sort_keys=True) if evidence.quality else None,
        evidence_json=json.dumps(asdict(evidence), sort_keys=True),
        match_score=round(scored.value, 4),
        match_reasons_json=json.dumps(reasons),
        review_state=MatchReviewState.pending,
        selected=False,
    )


def _can_auto_select(candidate: ReleaseCandidate, scored: _Score) -> bool:
    return (
        scored.has_recording_agreement
        and scored.has_release_agreement
        and not scored.contradictions
        and candidate.match_score is not None
        and candidate.match_score >= _AUTO_SELECT_THRESHOLD
    )


async def _manual_select(
    db: AsyncSession,
    release: Release,
    track: Track,
    manual_candidate_id: int,
    reviewer: str | None,
    review_note: str | None,
) -> MatchResolution:
    candidates_result = await db.execute(
        select(ReleaseCandidate).where(
            ReleaseCandidate.release_id == release.id,
            ReleaseCandidate.track_id == track.id,
        )
    )
    candidates = list(candidates_result.scalars().all())
    selected = next(
        (candidate for candidate in candidates if candidate.id == manual_candidate_id),
        None,
    )
    if selected is None:
        raise ValueError("manual candidate does not belong to the requested release track")
    for candidate in candidates:
        candidate.selected = candidate.id == manual_candidate_id
        candidate.review_state = (
            MatchReviewState.manual_selected
            if candidate.id == manual_candidate_id
            else MatchReviewState.rejected
        )
    if selected not in candidates:
        candidates.append(selected)
    selected.selected = True
    selected.review_state = MatchReviewState.manual_selected
    selected.review_audit_json = json.dumps({"reviewer": reviewer, "note": review_note})
    release.import_state = ImportWorkflowState.ready
    track.import_state = ImportWorkflowState.ready
    await db.flush()
    return MatchResolution(ImportWorkflowState.ready, candidates, selected)


async def resolve_release_match(
    db: AsyncSession,
    release: Release,
    track: Track,
    evidence_items: list[EditionEvidence],
    *,
    manual_candidate_id: int | None = None,
    reviewer: str | None = None,
    review_note: str | None = None,
) -> MatchResolution:
    if manual_candidate_id is not None:
        return await _manual_select(db, release, track, manual_candidate_id, reviewer, review_note)

    scored_items = [
        (evidence, _score_evidence(release, track, evidence)) for evidence in evidence_items
    ]
    candidates = [
        _candidate_from_evidence(release, track, evidence, scored)
        for evidence, scored in scored_items
    ]
    db.add_all(candidates)
    await db.flush()

    selectable = [
        (candidate, scored)
        for candidate, (_, scored) in zip(candidates, scored_items, strict=True)
        if _can_auto_select(candidate, scored)
    ]
    selectable.sort(key=lambda item: item[0].match_score or 0.0, reverse=True)

    selected: ReleaseCandidate | None = None
    state = ImportWorkflowState.needs_review
    if selectable:
        top_score = selectable[0][0].match_score or 0.0
        tied = [
            item
            for item in selectable
            if abs((item[0].match_score or 0.0) - top_score) <= _TIE_EPSILON
        ]
        if len(tied) == 1:
            selected = selectable[0][0]
            selected.selected = True
            selected.review_state = MatchReviewState.auto_selected
            state = ImportWorkflowState.ready

    if selected is None:
        for candidate in candidates:
            candidate.selected = False
            candidate.review_state = MatchReviewState.needs_review
        release.error_detail = "release-match review: no unambiguous edition candidate"
        release.review_dismissed_at = None
    else:
        release.error_detail = None

    release.import_state = state
    track.import_state = state
    await db.flush()
    return MatchResolution(state, candidates, selected)
