from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release_candidate import MatchReviewState, ReleaseCandidate
from app.models.track import Track
from app.models.workflow import AcquisitionState as _AcquisitionState
from app.models.workflow import ImportWorkflowState
from app.services.library_import import (
    ImportExecutionError,
    MutagenTagWriter,
    execute_release_import,
    plan_release_import,
)
from app.sources.base import CapabilityState as _CapabilityState


class MonitoringCheckAlreadyRunning(RuntimeError):
    pass


@dataclass(frozen=True)
class QualityProfile:
    preferred_codecs: tuple[str, ...] = ("flac", "alac", "wav", "aac", "mp3", "opus")
    minimum_bitrate_kbps: int = 0
    minimum_sample_rate_hz: int = 0
    minimum_bit_depth: int = 0
    minimum_channels: int = 0
    minimum_reliability: float = 0.0
    minimum_match_confidence: float = 0.85

    def to_json(self) -> str:
        payload = asdict(self)
        payload["preferred_codecs"] = list(self.preferred_codecs)
        return json.dumps(payload, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str | None) -> QualityProfile:
        if not raw:
            return cls()
        payload = json.loads(raw)
        payload["preferred_codecs"] = tuple(payload.get("preferred_codecs", ()))
        return cls(**payload)


@dataclass(frozen=True)
class QualityEvaluation:
    score: float
    gain: float
    meaningful: bool
    reasons: tuple[str, ...]


def _number(quality: Mapping[str, Any], key: str) -> float:
    value = quality.get(key, 0)
    return float(value) if isinstance(value, int | float) else 0.0


def _codec_rank(profile: QualityProfile, codec: object) -> int:
    value = str(codec or "").casefold()
    try:
        return len(profile.preferred_codecs) - profile.preferred_codecs.index(value)
    except ValueError:
        return 0


def _quality_score(profile: QualityProfile, quality: Mapping[str, Any]) -> float:
    return (
        (1000 if quality.get("lossless") is True else 0)
        + _codec_rank(profile, quality.get("codec")) * 100
        + _number(quality, "sample_rate_hz") / 1000
        + _number(quality, "bit_depth") * 2
        + _number(quality, "bitrate_kbps") / 10
        + _number(quality, "channels") * 5
        + _number(quality, "reliability") * 100
    )


def evaluate_quality_candidate(
    profile: QualityProfile,
    current: Mapping[str, Any],
    candidate: Mapping[str, Any],
    match_confidence: float,
) -> QualityEvaluation:
    score = _quality_score(profile, candidate)
    gain = score - _quality_score(profile, current)
    reasons: list[str] = []
    if candidate.get("lossless") is True and current.get("lossless") is not True:
        reasons.append("lossless upgrade")
    if _codec_rank(profile, candidate.get("codec")) > _codec_rank(profile, current.get("codec")):
        reasons.append("preferred codec")
    thresholds = (
        ("sample_rate_hz", 8000),
        ("bit_depth", 4),
        ("bitrate_kbps", 32),
        ("channels", 1),
    )
    for key, delta in thresholds:
        if _number(candidate, key) - _number(current, key) >= delta:
            reasons.append(f"higher {key}")
    meets_preferences = (
        (
            candidate.get("lossless") is True
            or _number(candidate, "bitrate_kbps") >= profile.minimum_bitrate_kbps
        )
        and _number(candidate, "sample_rate_hz") >= profile.minimum_sample_rate_hz
        and _number(candidate, "bit_depth") >= profile.minimum_bit_depth
        and _number(candidate, "channels") >= profile.minimum_channels
        and _number(candidate, "reliability") >= profile.minimum_reliability
    )
    reliability_preserved = _number(candidate, "reliability") >= _number(current, "reliability")
    meaningful = (
        bool(reasons)
        and gain > 0
        and meets_preferences
        and reliability_preserved
        and match_confidence >= profile.minimum_match_confidence
    )
    return QualityEvaluation(score, gain, meaningful, tuple(reasons))


_active_checks: set[int] = set()
CheckDiscovery = Callable[[], Awaitable[list[ReleaseCandidate]]]
ScheduledCheck = Callable[[], Awaitable[None]]


def _is_approved_release_candidate(candidate: ReleaseCandidate, record: MonitoringRecord) -> bool:
    return (
        candidate.selected
        and candidate.review_state
        in (MatchReviewState.auto_selected, MatchReviewState.manual_selected)
        and candidate.release_id == record.release_id
        and candidate.track_id is None
    )


class MonitoringScheduler:
    """Small in-process scheduler with one cancellable task per monitoring record."""

    def __init__(self) -> None:
        self._tasks: dict[int, asyncio.Task[None]] = {}

    def schedule(self, record_id: int, interval_seconds: float, check: ScheduledCheck) -> None:
        current = self._tasks.get(record_id)
        if current is not None and not current.done():
            raise MonitoringCheckAlreadyRunning("monitoring schedule is already active")

        async def loop() -> None:
            try:
                while True:
                    await check()
                    await asyncio.sleep(interval_seconds)
            finally:
                self._tasks.pop(record_id, None)

        self._tasks[record_id] = asyncio.create_task(loop())

    async def cancel(self, record_id: int) -> None:
        task = self._tasks.get(record_id)
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def run_monitoring_check(
    db: AsyncSession,
    record: MonitoringRecord,
    current_quality: Mapping[str, Any],
    discover: CheckDiscovery,
) -> ReleaseCandidate | None:
    if record.id is None or record.id in _active_checks:
        raise MonitoringCheckAlreadyRunning("monitoring check is already running")
    _active_checks.add(record.id)
    record.status = MonitoringStatus.checking
    await db.flush()
    outcome = "no_upgrade"
    selected: ReleaseCandidate | None = None
    try:
        profile = QualityProfile.from_json(record.desired_quality_json)
        candidates = await discover()
        ranked: list[tuple[float, ReleaseCandidate]] = []
        for candidate in candidates:
            if not _is_approved_release_candidate(candidate, record):
                continue
            quality = json.loads(candidate.quality_json or "{}")
            evaluation = evaluate_quality_candidate(
                profile, current_quality, quality, candidate.match_score or 0.0
            )
            if evaluation.meaningful:
                ranked.append((evaluation.score, candidate))
        if ranked:
            selected = max(ranked, key=lambda item: item[0])[1]
            record.candidate_id = selected.id
            record.status = MonitoringStatus.candidate_found
            outcome = "candidate_found"
        else:
            record.status = MonitoringStatus.active
        return selected
    except asyncio.CancelledError:
        record.status = MonitoringStatus.active
        outcome = "cancelled"
        raise
    except Exception:
        record.status = MonitoringStatus.failed
        outcome = "failed"
        raise
    finally:
        checked_at = datetime.now(UTC)
        record.last_checked_at = checked_at
        history = json.loads(record.history_json or "[]")
        history.append(
            {
                "checked_at": checked_at.isoformat(),
                "outcome": outcome,
                "candidate_id": selected.id if selected else None,
            }
        )
        record.history_json = json.dumps(history[-100:])
        await db.flush()
        _active_checks.discard(record.id)


async def execute_quality_upgrade(
    db: AsyncSession,
    record: MonitoringRecord,
    candidate: ReleaseCandidate,
    current_quality: Mapping[str, Any],
    *,
    library_root: Path,
    tag_writer: MutagenTagWriter | None = None,
) -> None:
    """Route an approved quality gain through verified import replacement semantics."""
    if (
        not _is_approved_release_candidate(candidate, record)
        or record.candidate_id != candidate.id
    ):
        raise ImportExecutionError("quality upgrade requires the approved selected candidate")
    profile = QualityProfile.from_json(record.desired_quality_json)
    quality = json.loads(candidate.quality_json or "{}")
    evaluation = evaluate_quality_candidate(
        profile, current_quality, quality, candidate.match_score or 0.0
    )
    if not evaluation.meaningful:
        raise ImportExecutionError("quality candidate is not an approved meaningful upgrade")
    try:
        evidence = json.loads(candidate.evidence_json or "{}")
        raw_artifacts = evidence["artifacts"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ImportExecutionError("approved candidate has no valid staged artifacts") from exc
    if not isinstance(raw_artifacts, list):
        raise ImportExecutionError("approved candidate has no valid staged artifacts")
    tracks_result = await db.execute(select(Track).where(Track.release_id == record.release_id))
    tracks = list(tracks_result.scalars().all())
    expected_track_ids = {track.id for track in tracks if track.id is not None}
    artifacts: dict[int, tuple[Path, str]] = {}
    for item in raw_artifacts:
        if not isinstance(item, dict):
            raise ImportExecutionError("candidate artifact evidence is invalid")
        track_id = item.get("track_id")
        staging_path = item.get("staging_path")
        source_hash = item.get("sha256")
        artifact_quality = item.get("quality")
        if (
            not isinstance(track_id, int)
            or track_id in artifacts
            or not isinstance(staging_path, str)
            or not isinstance(source_hash, str)
            or len(source_hash) != 64
            or artifact_quality != quality
        ):
            raise ImportExecutionError("candidate artifact evidence is invalid")
        artifacts[track_id] = (Path(staging_path), source_hash)
    if set(artifacts) != expected_track_ids:
        raise ImportExecutionError("candidate artifacts do not match the monitored release tracks")
    plans = await plan_release_import(
        db, record.release, library_root=library_root, source_artifacts=artifacts
    )
    for plan in plans:
        # Existing preferred paths are expected; all other planner checks remain intact.
        if (
            plan.error_detail
            and "destination already exists with different bytes" not in plan.error_detail
        ):
            raise ImportExecutionError(plan.error_detail)
        plan.status = ImportWorkflowState.ready
        plan.error_detail = None
    await execute_release_import(
        db,
        record.release,
        library_root=library_root,
        tag_writer=tag_writer,
        replace_existing_verified=True,
    )
    history = json.loads(record.history_json or "[]")
    history.append(
        {
            "checked_at": datetime.now(UTC).isoformat(),
            "outcome": "upgraded",
            "candidate_id": candidate.id,
            "quality_gain": evaluation.gain,
        }
    )
    record.history_json = json.dumps(history[-100:])
    record.status = MonitoringStatus.active
    await db.flush()


def map_slskd_transfer_state(state: _CapabilityState) -> _AcquisitionState:
    """Map slskd transfer API state into the acquisition workflow enum."""
    tokens = set(re.findall(r"[a-z]+", (state.reason or "").casefold()))
    if not state.available:
        return _AcquisitionState.failed
    if tokens & {"failed", "errored", "error"}:
        return _AcquisitionState.failed
    if tokens & {"cancelled", "canceled"}:
        return _AcquisitionState.cancelled
    if tokens & {"completed", "complete", "succeeded", "downloaded"}:
        return _AcquisitionState.downloaded
    if tokens & {"queued", "initializing"}:
        return _AcquisitionState.queued
    return _AcquisitionState.acquiring
