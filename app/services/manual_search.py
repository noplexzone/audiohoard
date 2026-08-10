from __future__ import annotations

from dataclasses import dataclass

from app.metadata.filename_parse import normalize_for_catalog_match
from app.schemas.search import SearchRequest, SearchResult

_LOSSLESS = {"flac", "alac", "wav", "aiff", "aif"}


@dataclass(frozen=True)
class RankedManualResult:
    """A scored view over an untouched provider result."""

    result: SearchResult
    score: int
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ManualResultGroup:
    key: str
    options: tuple[RankedManualResult, ...]

    @property
    def best(self) -> RankedManualResult:
        return self.options[0]


def _norm(value: str | None) -> str:
    return normalize_for_catalog_match(value or "")


def _exact(expected: str | None, actual: str | None) -> bool:
    return bool(expected and actual and _norm(expected) == _norm(actual))


def _metadata_int(result: SearchResult, *names: str) -> int | None:
    for name in names:
        value = result.metadata.get(name)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def score_manual_result(
    result: SearchResult,
    request: SearchRequest,
    provider_priority: dict[str, int],
) -> RankedManualResult:
    """Return Audiohoard's deterministic manual-result score.

    Evidence weights: exact artist/title/album +24/+30/+18; duration <=2s/+14,
    <=10s/+8, <=30s/+3, otherwise -8; preferred format +12; any lossless
    format +5; bitrate >=320/+4, >=192/+2, <128/-4; exact release file count
    +12, >=80% +6, <50% -8; first configured provider +6 then one point less
    per position; explicit availability +4 or -12; positive peer evidence up
    to +5; and explicit rejection/block evidence -40. Free-text exact title
    adds +18 when no structured track is supplied. Unknown evidence scores 0.
    """
    score = 0
    reasons: list[str] = []

    if _exact(request.artist, result.artist):
        score += 24
        reasons.append("Exact artist")
    expected_title = request.track or (request.query if not request.album else None)
    if _exact(expected_title, result.title):
        score += 30 if request.track else 18
        reasons.append("Exact title")
    if _exact(request.album, result.album):
        score += 18
        reasons.append("Exact album")

    if request.expected_duration_sec and result.duration_sec:
        delta = abs(request.expected_duration_sec - result.duration_sec)
        if delta <= 2:
            score += 14
            reasons.append("Duration within 2 seconds")
        elif delta <= 10:
            score += 8
            reasons.append("Duration within 10 seconds")
        elif delta <= 30:
            score += 3
            reasons.append("Duration within 30 seconds")
        else:
            score -= 8
            reasons.append("Duration differs")

    audio_format = (result.format or "").casefold().lstrip(".")
    preferred = (request.preferred_format or "").casefold().lstrip(".")
    if preferred and audio_format == preferred:
        score += 12
        reasons.append("Preferred format")
    if audio_format in _LOSSLESS:
        score += 5
        reasons.append("Lossless")

    bitrate = _metadata_int(result, "bitrate", "bit_rate")
    if bitrate is not None:
        bitrate = bitrate // 1000 if bitrate > 10_000 else bitrate
        if bitrate >= 320:
            score += 4
            reasons.append("High bitrate")
        elif bitrate >= 192:
            score += 2
            reasons.append("Good bitrate")
        elif bitrate < 128:
            score -= 4
            reasons.append("Low bitrate")

    expected_count = request.expected_track_count
    actual_count = _metadata_int(result, "track_count", "file_count")
    if expected_count and actual_count is not None:
        ratio = actual_count / expected_count
        if actual_count == expected_count:
            score += 12
            reasons.append("Complete release")
        elif ratio >= 0.8:
            score += 6
            reasons.append("Nearly complete release")
        elif ratio < 0.5:
            score -= 8
            reasons.append("Incomplete release")

    priority = provider_priority.get(result.source)
    if priority is not None:
        bonus = max(0, 6 - priority)
        score += bonus
        if priority == 0:
            reasons.append("Preferred source")

    available = result.metadata.get("available")
    if available is True:
        score += 4
        reasons.append("Available")
    elif available is False:
        score -= 12
        reasons.append("Unavailable")

    peers = _metadata_int(result, "seeders", "peers", "uploads")
    if peers is not None and peers > 0:
        score += min(5, peers)
        reasons.append("Active source")

    if result.metadata.get("rejected") is True or result.metadata.get("blocked") is True:
        score -= 40
        reasons.append("Previously rejected")

    return RankedManualResult(result=result, score=score, reasons=tuple(reasons))


def rank_manual_results(
    results: list[SearchResult], request: SearchRequest, ordered_sources: list[str]
) -> list[RankedManualResult]:
    priority = {name: index for index, name in enumerate(ordered_sources)}
    ranked = [score_manual_result(result, request, priority) for result in results]
    return sorted(
        ranked,
        key=lambda item: (
            -item.score,
            priority.get(item.result.source, 999),
            _norm(item.result.artist),
            _norm(item.result.album),
            _norm(item.result.title),
            item.result.url or "",
            item.result.model_dump_json(),
        ),
    )


def _stable_group_key(result: SearchResult) -> str | None:
    namespace = result.metadata.get("artifact_namespace")
    artifact_id = result.metadata.get("artifact_id")
    if not isinstance(namespace, str) or not namespace.strip():
        return None
    if not isinstance(artifact_id, str) or not artifact_id.strip():
        return None
    return f"artifact:{namespace.strip().casefold()}:{artifact_id.strip()}"


def group_ranked_results(results: list[RankedManualResult]) -> list[ManualResultGroup]:
    """Group only shared stable artifact IDs; never group by fuzzy names."""
    groups: dict[str, list[RankedManualResult]] = {}
    order: list[str] = []
    for index, item in enumerate(results):
        key = _stable_group_key(item.result) or f"result:{index}"
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(item)
    return [ManualResultGroup(key, tuple(groups[key])) for key in order]
