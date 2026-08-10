from __future__ import annotations

import asyncio
import stat
from pathlib import Path

from sqlalchemy import exists, or_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import run_with_sqlite_lock_retry
from app.models.acquisition_claim import AcquisitionDispatchClaim
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.track import Track
from app.models.workflow import ImportWorkflowState


def _is_present_regular_library_file(raw_path: str, library_root: Path) -> bool:
    path = Path(raw_path)
    if not raw_path.strip() or path.is_symlink():
        return False
    try:
        root = library_root.resolve(strict=True)
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        return stat.S_ISREG(resolved.stat().st_mode)
    except (OSError, ValueError):
        return False


async def has_committed_catalog_ownership(
    db: AsyncSession,
    catalog_album_id: int | None,
    catalog_track_id: int | None,
    library_root: Path,
) -> bool:
    """Return whether the exact catalog release/track owns a present committed file."""
    if catalog_album_id is None or catalog_track_id is None:
        return False
    destinations = list(
        (
            await db.scalars(
                select(ImportPlan.destination_path)
                .join(Track, ImportPlan.track_id == Track.id)
                .where(
                    Track.catalog_album_id == catalog_album_id,
                    Track.catalog_track_id == catalog_track_id,
                    Track.import_state == ImportWorkflowState.imported,
                    ImportPlan.status == ImportWorkflowState.imported,
                    ImportPlan.file_state == LibraryFileState.present,
                    ImportPlan.destination_path != "",
                )
            )
        ).all()
    )
    for destination in destinations:
        if await asyncio.to_thread(_is_present_regular_library_file, destination, library_root):
            return True
    return False


async def claim_catalog_acquisition(
    db: AsyncSession, catalog_album_id: int, catalog_track_id: int, job_id: int
) -> bool:
    """Atomically claim an exact catalog track, taking over only terminal owners."""
    terminal = (JobStatus.done, JobStatus.failed, JobStatus.partial, JobStatus.cancelled)
    claimed = False

    async def operation() -> None:
        nonlocal claimed
        statement = (
            sqlite_insert(AcquisitionDispatchClaim)
            .values(
                catalog_album_id=catalog_album_id,
                catalog_track_id=catalog_track_id,
                job_id=job_id,
            )
            .on_conflict_do_update(
                index_elements=["catalog_album_id", "catalog_track_id"],
                set_={"job_id": job_id},
                where=or_(
                    AcquisitionDispatchClaim.job_id == job_id,
                    exists(
                        select(Job.id).where(
                            Job.id == AcquisitionDispatchClaim.job_id,
                            Job.status.in_(terminal),
                        )
                    ),
                ),
            )
            .returning(AcquisitionDispatchClaim.job_id)
        )
        claimed = (await db.execute(statement)).scalar_one_or_none() == job_id

    await run_with_sqlite_lock_retry(db, operation)
    return claimed
