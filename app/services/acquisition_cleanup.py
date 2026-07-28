from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import case, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.database import get_session_factory
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.settings_service import build_effective_settings
from app.sources.slskd import SlskdAdapter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ImportedSourceCleanup:
    plan_id: int | None
    staged_path: Path
    provenance_json: str | None


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
                    await adapter.cancel(*identity)
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
