from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from sqlalchemy import case, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.database import get_session_factory
from app.models.catalog_entities import CatalogAlbumTrack
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.settings_service import build_effective_settings
from app.sources.slskd import SlskdAdapter

logger = logging.getLogger(__name__)

_SOURCE_CLEANUP_COMPLETED_AT = "source_cleanup_completed_at"
_TERMINAL_CLEANUP_LOCK = asyncio.Lock()


def _provenance(provenance_json: str | None) -> dict[str, object]:
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        value = json.loads(provenance_json or "{}")
        if isinstance(value, dict):
            return value
    return {}


def _source_cleanup_completed(provenance_json: str | None) -> bool:
    return bool(_provenance(provenance_json).get(_SOURCE_CLEANUP_COMPLETED_AT))


@dataclass(frozen=True)
class ImportedSourceCleanup:
    plan_id: int | None
    staged_path: Path
    provenance_json: str | None
    source_job_id: str | None = None


class SlskdCleanupAdapter(Protocol):
    async def cancel(
        self, username: str, filename: str, transfer_id: str | None = None
    ) -> bool | None: ...


def _slskd_identity(provenance_json: str | None) -> tuple[str, str] | None:
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        provenance = json.loads(provenance_json or "{}")
        if (
            provenance.get("source") == "slskd"
            and provenance.get("username")
            and provenance.get("filename")
        ):
            return str(provenance["username"]), str(provenance["filename"])
    return None


async def hide_completed_and_timed_out_jobs(
    session_factory: async_sessionmaker[AsyncSession],
    job_ids: set[int] | None = None,
) -> list[int]:
    """Hide durable successful/timeout attempts without deleting acquisition history."""
    terminal = {JobStatus.done, JobStatus.failed, JobStatus.partial, JobStatus.cancelled}
    async with session_factory() as db:
        query = (
            select(Job)
            .where(Job.queue_hidden.is_(False), Job.status.in_(terminal))
            .options(selectinload(Job.tracks))
            .order_by(Job.id)
        )
        if job_ids is not None:
            query = query.where(Job.id.in_(job_ids))
        jobs = list((await db.scalars(query)).all())
        hidden: set[int] = set()
        for job in jobs:
            timed_out = any(track.source_status == "transfer_timeout" for track in job.tracks)
            has_other_incomplete = any(
                track.acquisition_state != AcquisitionState.downloaded
                and track.source_status != "transfer_timeout"
                for track in job.tracks
            )
            completed_standalone = job.status == JobStatus.done and job.catalog_album_id is None
            timed_out_standalone = (
                job.catalog_album_id is None and timed_out and not has_other_incomplete
            )
            if completed_standalone or timed_out_standalone:
                job.queue_hidden = True
                hidden.add(job.id)

        album_ids = {job.catalog_album_id for job in jobs if job.catalog_album_id is not None}
        if album_ids:
            wanted_rows = await db.execute(
                select(CatalogAlbumTrack.album_id, CatalogAlbumTrack.id).where(
                    CatalogAlbumTrack.album_id.in_(album_ids)
                )
            )
            imported_rows = await db.execute(
                select(Track.catalog_album_id, Track.catalog_track_id).where(
                    Track.catalog_album_id.in_(album_ids),
                    Track.catalog_track_id.is_not(None),
                    Track.import_state == ImportWorkflowState.imported,
                )
            )
            wanted: dict[int, set[int]] = {}
            imported: dict[int, set[int]] = {}
            for album_id, track_id in wanted_rows:
                wanted.setdefault(album_id, set()).add(track_id)
            for album_id, track_id in imported_rows:
                if album_id is not None and track_id is not None:
                    imported.setdefault(album_id, set()).add(track_id)
            completed_album_ids = {
                album_id
                for album_id, track_ids in wanted.items()
                if track_ids and track_ids.issubset(imported.get(album_id, set()))
            }
            if completed_album_ids:
                completed_jobs = list(
                    (
                        await db.scalars(
                            select(Job).where(
                                Job.catalog_album_id.in_(completed_album_ids),
                                Job.queue_hidden.is_(False),
                                Job.status.in_(terminal),
                            )
                        )
                    ).all()
                )
                for job in completed_jobs:
                    job.queue_hidden = True
                    hidden.add(job.id)
        await db.commit()
        return sorted(hidden)


async def cleanup_durable_slskd_transfers(
    session_factory: async_sessionmaker[AsyncSession],
    adapter: SlskdCleanupAdapter,
    job_ids: set[int] | None = None,
) -> int:
    """Remove durable completed/timeout transfers with no DB transaction held during I/O."""
    async with session_factory() as db:
        query = select(Track).where(
            Track.source == "slskd",
            Track.source_job_id.is_not(None),
            Track.acquisition_provenance_json.is_not(None),
        )
        if job_ids is not None:
            query = query.where(Track.job_id.in_(job_ids))
        tracks = list((await db.scalars(query.order_by(Track.id))).all())
        attempts: dict[tuple[str, str], list[tuple[int, str]]] = {}
        for track in tracks:
            if _source_cleanup_completed(track.acquisition_provenance_json):
                continue
            durable = (
                track.acquisition_state == AcquisitionState.downloaded and bool(track.staging_path)
            ) or track.source_status == "transfer_timeout"
            identity = _slskd_identity(track.acquisition_provenance_json)
            source_job_id = track.source_job_id
            if durable and identity is not None and source_job_id is not None:
                attempts.setdefault(identity, []).append((track.id, source_job_id))

    completed = 0
    cleaned_track_identities: dict[int, tuple[tuple[str, str], str]] = {}
    for identity, track_refs in sorted(attempts.items()):
        # Revalidate immediately before provider I/O. The adapter additionally targets
        # the exact provider transfer ID, so a replacement with the same peer/path
        # cannot be removed after this transaction is released.
        async with session_factory() as db:
            current_tracks = list(
                (
                    await db.scalars(
                        select(Track).where(Track.id.in_([ref[0] for ref in track_refs]))
                    )
                ).all()
            )
            current_refs = [
                (track.id, expected_source_job_id)
                for track in current_tracks
                for expected_track_id, expected_source_job_id in track_refs
                if track.id == expected_track_id
                and track.source_job_id == expected_source_job_id
                and _slskd_identity(track.acquisition_provenance_json) == identity
                and not _source_cleanup_completed(track.acquisition_provenance_json)
                and (
                    (
                        track.acquisition_state == AcquisitionState.downloaded
                        and bool(track.staging_path)
                    )
                    or track.source_status == "transfer_timeout"
                )
            ]
        if not current_refs:
            continue
        # Every row grouped under an identity should refer to the same provider
        # transfer. Refuse ambiguous historical groups rather than deleting by path.
        transfer_ids = {source_job_id for _, source_job_id in current_refs}
        if len(transfer_ids) != 1:
            logger.warning("skipping ambiguous durable slskd transfer cleanup identity")
            continue
        transfer_id = next(iter(transfer_ids))
        try:
            cleanup_result = await adapter.cancel(*identity, transfer_id)
            if cleanup_result is False:
                continue
        except Exception:
            logger.exception("durable slskd transfer cleanup failed")
        else:
            completed += 1
            cleaned_track_identities.update(
                {track_id: (identity, source_job_id) for track_id, source_job_id in current_refs}
            )

    if cleaned_track_identities:
        async with session_factory() as db:
            cleaned_tracks = list(
                (
                    await db.scalars(select(Track).where(Track.id.in_(cleaned_track_identities)))
                ).all()
            )
            completed_at = datetime.now(UTC).isoformat()
            for track in cleaned_tracks:
                expected_identity, expected_source_job_id = cleaned_track_identities[track.id]
                if (
                    track.source_job_id != expected_source_job_id
                    or _slskd_identity(track.acquisition_provenance_json) != expected_identity
                ):
                    continue
                provenance = _provenance(track.acquisition_provenance_json)
                provenance[_SOURCE_CLEANUP_COMPLETED_AT] = completed_at
                track.acquisition_provenance_json = json.dumps(provenance, sort_keys=True)
            await db.commit()
    return completed


def _transient_cleanup_error(exc: BaseException) -> bool:
    if isinstance(exc, SQLAlchemyTimeoutError):
        return True
    return isinstance(exc, OperationalError) and any(
        marker in str(exc).casefold() for marker in ("locked", "busy")
    )


async def cleanup_terminal_acquisitions(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    slskd_url: str,
    slskd_api_key: str,
    job_ids: set[int] | None = None,
    max_attempts: int = 3,
) -> tuple[list[int], int]:
    """Serialize and retry idempotent terminal cleanup after transient contention."""
    async with _TERMINAL_CLEANUP_LOCK:
        for attempt in range(1, max_attempts + 1):
            try:
                hidden = await hide_completed_and_timed_out_jobs(session_factory, job_ids)
                removed = await cleanup_durable_slskd_transfers(
                    session_factory,
                    SlskdAdapter(slskd_url, slskd_api_key),
                    job_ids,
                )
                return hidden, removed
            except Exception as exc:
                if attempt == max_attempts or not _transient_cleanup_error(exc):
                    raise
                delay = 0.25 * (2 ** (attempt - 1))
                logger.warning("Terminal acquisition cleanup contention; retrying in %.2fs", delay)
                await asyncio.sleep(delay)
    raise RuntimeError("terminal cleanup retry loop exited unexpectedly")


@dataclass(frozen=True)
class OrphanPruneResult:
    tracks: int = 0
    releases: int = 0
    jobs: int = 0


def _prune_empty_parents(path: Path, staging_root: Path) -> None:
    """Remove empty ancestors up to, but never including, the staging root."""
    root = staging_root.absolute()
    current = path.absolute().parent
    while current != root:
        if not current.is_relative_to(root) or current.is_symlink():
            return
        try:
            current.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            return
        current = current.parent


def _track_has_file(track: Track) -> bool:
    paths = [track.staging_path, track.source_path]
    paths.extend(plan.destination_path for plan in track.import_plans if plan.destination_path)
    return any(Path(raw).is_file() for raw in paths if raw)


async def prune_orphaned_terminal_records(
    db: AsyncSession, *, batch_size: int = 500
) -> OrphanPruneResult:
    """Remove all terminal acquisition history that has no surviving file artifact."""
    terminal = {JobStatus.done, JobStatus.failed, JobStatus.partial, JobStatus.cancelled}
    removed_tracks = 0
    last_track_id = 0
    while True:
        tracks = list(
            (
                await db.scalars(
                    select(Track)
                    .join(Job, Job.id == Track.job_id)
                    .where(Job.status.in_(terminal), Track.id > last_track_id)
                    .options(selectinload(Track.import_plans))
                    .order_by(Track.id)
                    .limit(batch_size)
                )
            ).all()
        )
        if not tracks:
            break
        last_track_id = tracks[-1].id
        has_files = await asyncio.gather(
            *(asyncio.to_thread(_track_has_file, track) for track in tracks)
        )
        for track, has_file in zip(tracks, has_files, strict=True):
            if not has_file:
                await db.delete(track)
                removed_tracks += 1
        await db.flush()
        db.expire_all()

    removed_releases = 0
    while True:
        releases = list(
            (
                await db.scalars(
                    select(Release)
                    .join(Job, Job.id == Release.job_id)
                    .where(
                        Job.status.in_(terminal),
                        ~Release.tracks.any(),
                        ~Release.monitoring_records.any(),
                    )
                    .order_by(Release.id)
                    .limit(batch_size)
                )
            ).all()
        )
        if not releases:
            break
        for release in releases:
            await db.delete(release)
        removed_releases += len(releases)
        await db.flush()
        db.expire_all()

    removed_jobs = 0
    while True:
        jobs = list(
            (
                await db.scalars(
                    select(Job)
                    .where(
                        Job.status.in_(terminal),
                        ~Job.tracks.any(),
                        ~Job.releases.any(),
                    )
                    .order_by(Job.id)
                    .limit(batch_size)
                )
            ).all()
        )
        if not jobs:
            break
        for job in jobs:
            await db.delete(job)
        removed_jobs += len(jobs)
        await db.flush()
        db.expire_all()
    return OrphanPruneResult(removed_tracks, removed_releases, removed_jobs)


async def pending_imported_source_cleanups(
    db: AsyncSession, *, limit: int = 500
) -> tuple[ImportedSourceCleanup, ...]:
    """Load durable post-import obligations represented by a retained staging path."""
    plans = list(
        (
            await db.scalars(
                select(ImportPlan)
                .where(
                    ImportPlan.status == ImportWorkflowState.imported,
                    ImportPlan.staging_path.is_not(None),
                    ImportPlan.staging_path != "",
                )
                .options(selectinload(ImportPlan.track))
                .order_by(
                    case((ImportPlan.cleanup_attempted_at.is_(None), 0), else_=1),
                    ImportPlan.cleanup_attempted_at,
                    ImportPlan.id,
                )
                .limit(limit)
            )
        ).all()
    )
    return tuple(
        ImportedSourceCleanup(
            plan.id,
            Path(plan.staging_path or plan.source_path),
            plan.track.acquisition_provenance_json if plan.track else None,
            plan.track.source_job_id if plan.track else None,
        )
        for plan in plans
    )


async def _mark_cleanup_attempted(plan_id: int | None, *, completed: bool) -> None:
    if plan_id is None:
        return
    async with get_session_factory()() as db:
        plan = await db.get(ImportPlan, plan_id)
        if plan is not None and plan.status == ImportWorkflowState.imported:
            plan.cleanup_attempted_at = datetime.now(UTC)
            if completed:
                plan.staging_path = None
            await db.commit()


async def cleanup_imported_sources(items: tuple[ImportedSourceCleanup, ...]) -> None:
    """Idempotently finish durable cleanup obligations after an import commit."""
    staging_root = get_settings().staging_root
    adapter = None
    if any(_slskd_identity(item.provenance_json) for item in items):
        try:
            async with get_session_factory()() as db:
                settings = await build_effective_settings(db, get_settings())
            adapter = SlskdAdapter(settings.slskd_url, settings.slskd_api_key)
        except Exception:
            logger.exception("post-import slskd cleanup setup failed")

    for item in items:
        failed = False
        identity = _slskd_identity(item.provenance_json)
        if identity is not None:
            if adapter is None:
                failed = True
            else:
                try:
                    if item.source_job_id is None:
                        raise RuntimeError("slskd cleanup requires an exact transfer ID")
                    cleanup_result = await adapter.cancel(*identity, item.source_job_id)
                    if cleanup_result is False:
                        failed = True
                except Exception:
                    failed = True
                    logger.exception("post-import slskd transfer cleanup failed")
        try:
            await asyncio.to_thread(item.staged_path.unlink, missing_ok=True)
        except OSError:
            failed = True
            logger.exception("post-import staging cleanup failed for %s", item.staged_path)
        else:
            try:
                await asyncio.to_thread(_prune_empty_parents, item.staged_path, staging_root)
            except Exception:
                failed = True
                logger.warning("post-import directory prune failed for %s", item.staged_path)
        try:
            await _mark_cleanup_attempted(item.plan_id, completed=not failed)
        except Exception:
            logger.exception("failed to record post-import cleanup attempt")


def schedule_imported_source_cleanup(items: tuple[ImportedSourceCleanup, ...]) -> None:
    if not items:
        return
    task = asyncio.get_running_loop().create_task(cleanup_imported_sources(items))
    task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
