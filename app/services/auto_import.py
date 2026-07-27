from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcoustIDVerificationState, ImportWorkflowState
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
    """Automatically plan and execute a release import when all tracks are verified.

    Returns True if the import was successfully executed, False if any track is
    still pending verification or under review.

    Does NOT raise on planning/execution errors — those are persisted to the
    release/plans and logged so the operator can investigate.
    """
    tracks = list((await db.scalars(select(Track).where(Track.release_id == release.id))).all())
    if not tracks:
        logger.warning("auto_import: release %d has no tracks, skipping", release.id)
        return False

    # Every downloaded track must be either verified or approved before we proceed.
    from app.models.workflow import AcquisitionState

    downloaded = [t for t in tracks if t.acquisition_state == AcquisitionState.downloaded]
    if not downloaded:
        return False

    expected_count = release.track_count or 0
    catalog_ids = {t.catalog_track_id for t in downloaded if t.catalog_track_id is not None}
    if expected_count and len(catalog_ids) < expected_count:
        logger.debug(
            "auto_import: release %d is incomplete (%d/%d catalog tracks)",
            release.id,
            len(catalog_ids),
            expected_count,
        )
        release.import_state = ImportWorkflowState.needs_review
        release.review_dismissed_at = None
        release.error_detail = (
            f"track-count mismatch: expected {expected_count}, found {len(catalog_ids)}"
        )
        await db.flush()
        return False

    unresolved = [
        t
        for t in downloaded
        if t.acoustid_verification_state
        not in {AcoustIDVerificationState.verified, AcoustIDVerificationState.approved}
    ]
    if unresolved:
        logger.debug(
            "auto_import: release %d has %d unverified track(s), deferring",
            release.id,
            len(unresolved),
        )
        release.import_state = ImportWorkflowState.needs_review
        release.review_dismissed_at = None
        first = unresolved[0]
        track_label = first.track_no or first.id or "unknown"
        if first.acoustid_verification_state == AcoustIDVerificationState.mismatch:
            release.error_detail = f"AcoustID mismatch on track {track_label}"
        else:
            release.error_detail = f"AcoustID verification unavailable on track {track_label}"
        await db.flush()
        return False

    # All tracks verified — proceed to plan and execute.
    logger.info("auto_import: planning import for release %d", release.id)
    try:
        plans = await plan_release_import(
            db,
            release,
            library_root=library_root,
            naming_template=naming_template,
        )
    except (ImportPlanningError, OSError) as exc:
        logger.error("auto_import: planning failed for release %d: %s", release.id, exc)
        release.import_state = ImportWorkflowState.failed
        release.review_dismissed_at = None
        release.error_detail = f"import planning error: {exc}"
        await db.flush()
        return False

    not_ready = [p for p in plans if p.status != ImportWorkflowState.ready]
    if not_ready:
        logger.info(
            "auto_import: release %d has %d plan(s) needing review, deferring import",
            release.id,
            len(not_ready),
        )
        first_plan = not_ready[0]
        detail = first_plan.error_detail or "import plan requires review"
        if detail == "source path is not a regular file" or "no staged source path" in detail:
            release.error_detail = f"missing staged source: {first_plan.source_path or detail}"
        else:
            release.error_detail = f"import planning error: {detail}"
        release.import_state = ImportWorkflowState.needs_review
        release.review_dismissed_at = None
        await db.flush()
        return False

    logger.info("auto_import: executing import for release %d", release.id)
    try:
        await execute_release_import(db, release, library_root=library_root)
        release.error_detail = None
        release.rollback_detail = None
        logger.info("auto_import: release %d imported successfully", release.id)
        return True
    except ImportExecutionError as exc:
        logger.error("auto_import: execution failed for release %d: %s", release.id, exc)
        release.error_detail = f"import execution error: {exc}"
        release.review_dismissed_at = None
        await db.flush()
        return False
