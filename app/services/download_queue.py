from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.models.job import Job, JobStatus
from app.models.track import Track
from app.models.workflow import AcquisitionState

ERROR_LABELS: dict[str, str] = {
    "artifact_ambiguous": "Multiple matching files found",
    "artifact_invalid": "Downloaded file was not audio",
    "artifact_missing": "Downloaded file missing",
    "candidate_identity_mismatch": "No matching file found",
    "cancelled": "Download cancelled",
    "catalog_track_missing": "Catalog track missing",
    "catalog_tracks_empty": "No catalog tracks found",
    "catalog_tracks_incomplete": "Track metadata incomplete",
    "catalog_tracks_invalid_positions": "Track metadata positions invalid",
    "catalog_tracks_overfull": "Too many catalog tracks found",
    "dispatch_lost": "Download dispatcher lost the job",
    "init_error": "Download setup failed",
    "job_failed": "Download job failed",
    "path_traversal": "Downloaded path was unsafe",
    "result_processing_failed": "Download result could not be processed",
    "running_transition_error": "Download could not begin",
    "settings_error": "Download settings failed",
    "sources_exhausted": "No sources had this track",
    "transfer_failed": "Download failed mid-transfer",
    "transfer_lost": "Download disappeared from the source",
    "transfer_timeout": "Download timed out",
}

SOURCE_DISPLAY_NAMES: dict[str, str] = {
    "priority": "Priority",
    "prowlarr": "Prowlarr",
    "sabnzbd": "SABnzbd",
    "slskd": "slskd",
    "tidal": "TIDAL",
    "youtube": "YouTube",
}


_ACTIVE = {JobStatus.pending, JobStatus.running}
_RETRYABLE = {JobStatus.failed, JobStatus.partial, JobStatus.cancelled}
_IN_FLIGHT_TRACK_STATES = {AcquisitionState.searching, AcquisitionState.acquiring}
_STATUS_PRIORITY = (
    JobStatus.running,
    JobStatus.pending,
    JobStatus.partial,
    JobStatus.failed,
    JobStatus.cancelled,
    JobStatus.done,
)


@dataclass(frozen=True)
class DownloadAttempt:
    job: Job
    metadata: dict[str, Any]
    source_display: str


@dataclass(frozen=True)
class DownloadGroup:
    key: str
    label: str
    catalog_album_id: int | None
    artwork_url: str | None
    artist_name: str | None
    album_title: str | None
    year: str | None
    release_kind: str | None
    attempts: tuple[DownloadAttempt, ...]
    status: JobStatus
    wanted_track_count: int
    downloaded_track_count: int
    action_attempt: Job | None
    source_display: str

    @property
    def active(self) -> bool:
        return any(attempt.job.status in _ACTIVE for attempt in self.attempts)

    @property
    def action_retryable(self) -> bool:
        if self.action_attempt is None or self.action_attempt.status not in _RETRYABLE:
            return False
        return _metadata_retryable(_metadata(self.action_attempt))


def _metadata(job: Job) -> dict[str, Any]:
    try:
        value = json.loads(job.result_json) if job.result_json else {}
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(value, dict):
        return {}
    metadata = dict(value)
    normalized_errors: list[dict[str, Any]] = []
    raw_error = metadata.get("error")
    if raw_error is not None:
        metadata["error"] = _normalized_error(raw_error)
        normalized_errors.append(metadata["error"])
    raw_errors = metadata.get("errors")
    if raw_errors is not None:
        values = raw_errors if isinstance(raw_errors, list) else [raw_errors]
        metadata["errors"] = [_normalized_error(item) for item in values]
        normalized_errors.extend(metadata["errors"])
    if normalized_errors:
        metadata["error_summary"] = summarize_errors(normalized_errors)
    notice = recovery_notice(metadata)
    if notice:
        metadata["recovery_notice"] = notice
    return metadata


def _metadata_retryable(metadata: dict[str, Any]) -> bool:
    """Return False only when the failure payload explicitly forbids retry."""
    errors: list[object] = []
    if "error" in metadata:
        errors.append(metadata["error"])
    raw_errors = metadata.get("errors")
    if isinstance(raw_errors, list):
        errors.extend(raw_errors)
    elif raw_errors is not None:
        errors.append(raw_errors)
    return not any(isinstance(error, dict) and error.get("retryable") is False for error in errors)


def recovery_notice(metadata: dict[str, Any]) -> str | None:
    if isinstance(metadata.get("watchdog_recovery"), dict):
        return "Recovered after dispatcher stall; queued again by watchdog."
    recovery = metadata.get("recovery")
    if not isinstance(recovery, dict):
        return None
    code = str(recovery.get("code") or "")
    if code == "interrupted_by_restart":
        return "Recovered after app restart; queued again."
    if code:
        return f"Recovered after {code.replace('_', ' ')}."
    return "Recovered and queued again."


def error_label(code: object) -> str:
    normalized = str(code or "error")
    label = ERROR_LABELS.get(normalized)
    if label is not None:
        return label
    return normalized.replace("_", " ").strip().capitalize() or "Error"


def source_display_name(source: object) -> str:
    normalized = str(source or "").strip()
    return SOURCE_DISPLAY_NAMES.get(normalized.casefold(), normalized or "—")


def summarize_errors(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for error in errors:
        code = str(error.get("code") or "error")
        item = summary.get(code)
        if item is None:
            item = {
                "code": code,
                "label": error_label(code),
                "detail": error.get("detail"),
                "count": 0,
            }
            summary[code] = item
        if not item.get("detail") and error.get("detail"):
            item["detail"] = error.get("detail")
        item["count"] += 1
    return list(summary.values())


def _normalized_error(value: object) -> dict[str, Any]:
    """Normalize current and historical failure payloads for safe presentation."""
    candidate = value
    if isinstance(candidate, str):
        try:
            decoded = json.loads(candidate)
        except (json.JSONDecodeError, TypeError):
            decoded = candidate
        candidate = decoded
    if isinstance(candidate, dict):
        normalized = dict(candidate)
        normalized.setdefault("code", "error")
        return normalized
    if candidate is None:
        return {"code": "error"}
    return {"code": str(candidate)}


def _root_id(job: Job, parents: dict[int, int | None]) -> int:
    current = job.id
    parent = job.parent_job_id
    seen = {current}
    while parent is not None and parent not in seen:
        current = parent
        seen.add(current)
        parent = parents.get(current)
    return current


def _track_identity(track: Track) -> tuple[object, ...]:
    if track.catalog_track_id is not None:
        return ("catalog", track.catalog_track_id)
    if track.mbid:
        return ("mbid", track.mbid.casefold())
    return (
        "metadata",
        (track.album_artist or track.artist or "").strip().casefold(),
        (track.album or "").strip().casefold(),
        track.disc or 1,
        track.track_no,
        (track.title or "").strip().casefold(),
    )


def _group_status(jobs: list[Job]) -> JobStatus:
    if any(
        track.acquisition_state in _IN_FLIGHT_TRACK_STATES for job in jobs for track in job.tracks
    ):
        return JobStatus.running
    statuses = {job.status for job in jobs}
    for status in _STATUS_PRIORITY:
        if status in statuses:
            return status
    return jobs[0].status


def _effective_source(jobs: list[Job]) -> str:
    track_sources: list[str] = []
    for state in (
        AcquisitionState.acquiring,
        AcquisitionState.searching,
        AcquisitionState.downloaded,
        AcquisitionState.failed,
        AcquisitionState.cancelled,
    ):
        track_sources.extend(
            track.source
            for job in jobs
            for track in job.tracks
            if track.source and track.acquisition_state == state
        )
    for source in track_sources:
        if source != "priority":
            return source
    for job in jobs:
        if job.source != "priority":
            return job.source
    return jobs[0].source


def _action_attempt(jobs: list[Job]) -> Job | None:
    active = next((job for job in jobs if job.status in _ACTIVE), None)
    if active is not None:
        return active
    return next((job for job in jobs if job.status in _RETRYABLE), None)


def project_download_groups(
    jobs: list[Job], parents: dict[int, int | None]
) -> list[DownloadGroup]:
    """Project visible job history into deterministic operational queue groups."""
    buckets: dict[str, list[Job]] = {}
    for job in jobs:
        if job.catalog_album_id is not None:
            key = f"album:{job.catalog_album_id}"
        elif (
            job.parent_job_id is not None
            or parents.get(job.id) is not None
            or job.id in parents.values()
        ):
            key = f"chain:{_root_id(job, parents)}"
        else:
            key = f"job:{job.id}"
        buckets.setdefault(key, []).append(job)

    groups: list[DownloadGroup] = []
    for key, grouped_jobs in buckets.items():
        grouped_jobs.sort(key=lambda job: (job.created_at, job.id), reverse=True)
        visible_jobs = [job for job in grouped_jobs if not job.queue_hidden]
        if not visible_jobs:
            continue
        album = next((job.catalog_album for job in grouped_jobs if job.catalog_album), None)
        wanted: set[tuple[object, ...]]
        if album is not None:
            label = f"{album.artist.name} — {album.title}"
            wanted = {("catalog", track.id) for track in album.tracks}
            wanted_count = max(album.track_count or 0, len(wanted))
            catalog_album_id = album.id
            artwork_url = album.artwork_url
            artist_name = album.artist.name
            album_title = album.title
            year = str(album.year) if album.year is not None else None
            release_kind = album.release_type
        else:
            label = grouped_jobs[-1].query
            wanted = {_track_identity(track) for job in grouped_jobs for track in job.tracks}
            wanted_count = len(wanted)
            catalog_album_id = None
            artwork_url = None
            artist_name = None
            album_title = None
            year = None
            release_kind = None
        downloaded = {
            _track_identity(track)
            for job in grouped_jobs
            for track in job.tracks
            if track.acquisition_state == AcquisitionState.downloaded
        }
        if not wanted:
            wanted = downloaded
            wanted_count = max(wanted_count, len(wanted))
        groups.append(
            DownloadGroup(
                key=key,
                label=label,
                catalog_album_id=catalog_album_id,
                artwork_url=artwork_url,
                artist_name=artist_name,
                album_title=album_title,
                year=year,
                release_kind=release_kind,
                attempts=tuple(
                    DownloadAttempt(
                        job=job,
                        metadata=_metadata(job),
                        source_display=source_display_name(job.source),
                    )
                    for job in visible_jobs
                ),
                status=_group_status(visible_jobs),
                wanted_track_count=wanted_count,
                downloaded_track_count=min(len(downloaded & wanted), wanted_count),
                action_attempt=_action_attempt(visible_jobs),
                source_display=source_display_name(_effective_source(grouped_jobs)),
            )
        )
    groups.sort(
        key=lambda group: (group.attempts[0].job.created_at, group.attempts[0].job.id),
        reverse=True,
    )
    return groups
