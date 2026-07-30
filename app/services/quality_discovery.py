from __future__ import annotations

import json
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.jobs import runner
from app.models.monitoring import MonitoringRecord
from app.models.release import Release
from app.models.release_candidate import MatchReviewState, ReleaseCandidate
from app.schemas.search import SearchResult
from app.services.monitoring import CheckDiscovery, ProgressCheckpoint

_LOSSLESS_FORMATS = {"flac", "alac", "wav", "aiff", "aif"}


def _number_metadata(metadata: dict[str, object], *keys: str) -> int | None:
    for key in keys:
        value = metadata.get(key)
        if isinstance(value, int | float) and value > 0:
            return int(value)
        if isinstance(value, str):
            try:
                parsed = int(float(value))
            except ValueError:
                continue
            if parsed > 0:
                return parsed
    return None


def _bitrate_kbps(metadata: dict[str, object]) -> int | None:
    value = _number_metadata(metadata, "bitrate_kbps", "bit_rate_kbps")
    if value is not None:
        return value
    value = _number_metadata(metadata, "bit_rate", "bitrate")
    if value is None:
        return None
    return value // 1000 if value > 5000 else value


def _quality_from_result(result: SearchResult) -> dict[str, object]:
    metadata = result.metadata
    codec = result.format or metadata.get("format") or metadata.get("audio_format") or ""
    normalized = str(codec).casefold().lstrip(".")
    if normalized == "m4a":
        normalized = "aac"
    quality: dict[str, object] = {
        "codec": normalized,
        "lossless": normalized in _LOSSLESS_FORMATS,
        "reliability": 1.0,
    }
    bitrate = _bitrate_kbps(metadata)
    if bitrate is not None:
        quality["bitrate_kbps"] = bitrate
    sample_rate = _number_metadata(metadata, "sample_rate_hz", "sample_rate")
    if sample_rate is not None:
        quality["sample_rate_hz"] = sample_rate
    bit_depth = _number_metadata(metadata, "bit_depth", "bits_per_sample")
    if bit_depth is not None:
        quality["bit_depth"] = bit_depth
    channels = _number_metadata(metadata, "channels")
    if channels is not None:
        quality["channels"] = channels
    return quality


def _match_score(result: SearchResult) -> float:
    raw = result.metadata.get("match_score", result.metadata.get("parse_confidence", 1.0))
    if isinstance(raw, int | float):
        return max(0.0, min(float(raw), 1.0))
    return 1.0


def _track_count(result: SearchResult) -> int | None:
    value = result.metadata.get("track_count")
    return value if isinstance(value, int) else None


def _candidate_from_result(release_id: int, result: SearchResult) -> ReleaseCandidate:
    quality = _quality_from_result(result)
    evidence: dict[str, Any] = {
        "source": result.source,
        "title": result.title,
        "artist": result.artist,
        "album": result.album,
        "url": result.url,
        "format": result.format,
        "size_bytes": result.size_bytes,
        "metadata": result.metadata,
    }
    return ReleaseCandidate(
        release_id=release_id,
        track_id=None,
        duration_sec=result.duration_sec,
        track_count=_track_count(result),
        quality_json=json.dumps(quality, sort_keys=True),
        evidence_json=json.dumps(evidence, sort_keys=True),
        match_score=_match_score(result),
        match_reasons_json=json.dumps(["quality upgrade discovery"]),
        review_state=MatchReviewState.auto_selected,
        selected=True,
    )


def build_upgrade_discovery(
    db: AsyncSession,
    cfg: Settings,
    record: MonitoringRecord,
    *,
    checkpoint: ProgressCheckpoint | None = None,
) -> CheckDiscovery:
    async def discover() -> list[ReleaseCandidate]:
        release = await db.get(
            Release,
            record.release_id,
            options=(selectinload(Release.job),),
        )
        if release is None:
            return []
        if checkpoint is None:
            results = await runner._call_fetch_results(release.job, cfg, db)  # noqa: SLF001
        else:
            results = await runner._call_fetch_results(  # noqa: SLF001
                release.job, cfg, db, checkpoint=checkpoint
            )
        candidates = [_candidate_from_result(release.id, result) for result in results]
        db.add_all(candidates)
        await db.flush()
        return candidates

    return discover
