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
    settings = None
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
        try:
            await _mark_cleanup_attempted(item.plan_id, completed=not failed)
        except Exception:
            logger.exception("failed to record post-import cleanup attempt")


def schedule_imported_source_cleanup(items: tuple[ImportedSourceCleanup, ...]) -> None:
    if not items:
        return
    task = asyncio.get_running_loop().create_task(cleanup_imported_sources(items))
    task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
