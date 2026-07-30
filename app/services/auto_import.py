from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import AcoustIDVerificationState, ImportWorkflowState, ReviewDecision
from app.services.library_import import (
    ImportExecutionError,
    ImportPlanningError,
    execute_release_import,
    plan_release_import,
)

logger = logging.getLogger(__name__)


async def try_auto_import_release(
    db: AsyncSession,
    release: Release,
    *,
    library_root: Path,
    naming_template: str,
) -> bool:
    """Import every verified file-backed track without waiting for album closure."""
    from app.models.workflow import AcquisitionState

    tracks = list((await db.scalars(select(Track).where(Track.release_id == release.id))).all())
    if not tracks:
        logger.warning("auto_import: release %d has no tracks, skipping", release.id)
        return False

    eligible = [
        track
        for track in tracks
        if track.acquisition_state == AcquisitionState.downloaded
        and track.import_state != ImportWorkflowState.imported
        and track.acoustid_verification_state
        in {AcoustIDVerificationState.verified, AcoustIDVerificationState.approved}
        and bool(track.staging_path or track.source_path)
    ]
    if not eligible:
        unresolved = [
            track
            for track in tracks
            if track.acquisition_state == AcquisitionState.downloaded
            and track.import_state != ImportWorkflowState.imported
            and bool(track.staging_path or track.source_path)
        ]
        if unresolved:
            release.import_state = ImportWorkflowState.needs_review
            release.review_dismissed_at = None
            first = unresolved[0]
            track_label = first.track_no or first.id or "unknown"
            release.error_detail = (
                f"AcoustID mismatch on track {track_label}"
                if first.acoustid_verification_state == AcoustIDVerificationState.mismatch
                else f"AcoustID verification unavailable on track {track_label}"
            )
            await db.flush()
        return False

    logger.info(
        "auto_import: planning %d verified track(s) for release %d",
        len(eligible),
        release.id,
    )
    try:
        plans = await plan_release_import(
            db,
            release,
            library_root=library_root,
            naming_template=naming_template,
            track_ids={track.id for track in eligible if track.id is not None},
        )
    except (ImportPlanningError, OSError) as exc:
        logger.error("auto_import: planning failed for release %d: %s", release.id, exc)
        release.import_state = ImportWorkflowState.failed
        release.review_dismissed_at = None
        release.error_detail = f"import planning error: {exc}"
        await db.flush()
        return False

    ready = [plan for plan in plans if plan.status == ImportWorkflowState.ready]
    if not ready:
        first_plan = plans[0] if plans else None
        detail = first_plan.error_detail if first_plan is not None else "no ready import plan"
        release.error_detail = f"import planning error: {detail}"
        release.import_state = ImportWorkflowState.needs_review
        release.review_dismissed_at = None
        await db.flush()
        return False

    # Planning flushes import-plan writes. Commit that checkpoint before execution
    # fetches artwork or performs other external I/O so SQLite does not retain a
    # writer lock for the duration of a provider timeout.
    await db.commit()

    logger.info(
        "auto_import: executing %d ready track import(s) for release %d", len(ready), release.id
    )
    try:
        await execute_release_import(
            db,
            release,
            library_root=library_root,
            plan_ids={plan.id for plan in ready if plan.id is not None},
        )
        pending_reviews = list(
            (
                await db.scalars(
                    select(StagingReviewItem)
                    .where(
                        StagingReviewItem.release_id == release.id,
                        StagingReviewItem.review_state == ReviewDecision.pending,
                    )
                    .options(selectinload(StagingReviewItem.track))
                )
            ).all()
        )
        unresolved = [
            item.track
            for item in pending_reviews
            if item.track is not None
            and item.track.import_state != ImportWorkflowState.imported
            and bool(item.track.staging_path or item.track.source_path)
        ]
        if release.import_state == ImportWorkflowState.imported or not unresolved:
            release.error_detail = None
            release.rollback_detail = None
            if not unresolved and release.import_state == ImportWorkflowState.needs_review:
                release.import_state = ImportWorkflowState.discovered
        else:
            release.error_detail = f"{len(unresolved)} downloaded track(s) still require review"
        logger.info("auto_import: imported %d track(s) for release %d", len(ready), release.id)
        return True
    except ImportExecutionError as exc:
        logger.error("auto_import: execution failed for release %d: %s", release.id, exc)
        release.error_detail = f"import execution error: {exc}"
        release.review_dismissed_at = None
        await db.flush()
        return False
