from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.models.job import Job, JobStatus
from app.models.track import Track
from app.models.workflow import AcquisitionState

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


def _metadata(job: Job) -> dict[str, Any]:
    try:
        value = json.loads(job.result_json) if job.result_json else {}
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(value, dict):
        return {}
    metadata = dict(value)
    raw_errors = metadata.get("errors")
    if raw_errors is not None:
        values = raw_errors if isinstance(raw_errors, list) else [raw_errors]
        metadata["errors"] = [_normalized_error(item) for item in values]
    raw_error = metadata.get("error")
    if raw_error is not None:
        metadata["error"] = _normalized_error(raw_error)
    return metadata


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
                    DownloadAttempt(job=job, metadata=_metadata(job)) for job in visible_jobs
                ),
                status=_group_status(visible_jobs),
                wanted_track_count=wanted_count,
                downloaded_track_count=min(len(downloaded & wanted), wanted_count),
                action_attempt=_action_attempt(visible_jobs),
                source_display=_effective_source(grouped_jobs),
            )
        )
    groups.sort(
        key=lambda group: (group.attempts[0].job.created_at, group.attempts[0].job.id),
        reverse=True,
    )
    return groups
