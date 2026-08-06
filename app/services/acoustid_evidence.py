from __future__ import annotations

import json
import math
from uuid import UUID


def canonical_recording_mbid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    try:
        return str(UUID(candidate))
    except (ValueError, AttributeError):
        return None


def parse_strict_observed_mbids(raw: str | None) -> tuple[str, ...] | None:
    try:
        payload = json.loads(raw or "")
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, list) or not payload:
        return None
    normalized: list[str] = []
    for value in payload:
        mbid = canonical_recording_mbid(value)
        if mbid is None or mbid in normalized:
            return None
        normalized.append(mbid)
    return tuple(normalized)


def parse_strict_recording_evidence(raw: str | None) -> dict[str, float] | None:
    """Parse complete per-recording evidence, rejecting any malformed or duplicate entry."""
    try:
        payload = json.loads(raw or "")
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, list) or not payload:
        return None

    scores: dict[str, float] = {}
    for record in payload:
        if not isinstance(record, dict):
            return None
        mbid = canonical_recording_mbid(record.get("mbid"))
        value = record.get("score")
        if (
            mbid is None
            or mbid in scores
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
        ):
            return None
        try:
            score = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(score) or not 0.0 <= score <= 1.0:
            return None
        scores[mbid] = score
    return scores


def parse_consistent_acoustid_evidence(
    observed_raw: str | None,
    evidence_raw: str | None,
) -> tuple[tuple[str, ...], dict[str, float]] | None:
    """Require strict observed and evidence payloads with identical MBID sets."""
    observed = parse_strict_observed_mbids(observed_raw)
    scores = parse_strict_recording_evidence(evidence_raw)
    if observed is None or scores is None or set(observed) != set(scores):
        return None
    return observed, scores
