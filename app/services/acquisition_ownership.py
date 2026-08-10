from __future__ import annotations

import asyncio
import stat
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

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
    """Fence equivalent runners with one short SQLite uniqueness claim."""
    existing = await db.scalar(
        select(AcquisitionDispatchClaim).where(
            AcquisitionDispatchClaim.catalog_album_id == catalog_album_id,
            AcquisitionDispatchClaim.catalog_track_id == catalog_track_id,
        )
    )
    if existing is not None and existing.job_id == job_id:
        return True
    terminal = (JobStatus.done, JobStatus.failed, JobStatus.partial, JobStatus.cancelled)
    if existing is not None:
        owner_status = await db.scalar(select(Job.status).where(Job.id == existing.job_id))
        if owner_status not in terminal:
            return False
        await db.delete(existing)
        await db.flush()
    await db.execute(
        sqlite_insert(AcquisitionDispatchClaim)
        .values(
            catalog_album_id=catalog_album_id,
            catalog_track_id=catalog_track_id,
            job_id=job_id,
        )
        .on_conflict_do_nothing(index_elements=["catalog_album_id", "catalog_track_id"])
    )
    owner_job_id = await db.scalar(
        select(AcquisitionDispatchClaim.job_id).where(
            AcquisitionDispatchClaim.catalog_album_id == catalog_album_id,
            AcquisitionDispatchClaim.catalog_track_id == catalog_track_id,
        )
    )
    return owner_job_id == job_id
