from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.import_plan import ImportPlan
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.maintenance_state import DuplicateAlbumSummary, DuplicateScanSummary
from app.services.quality_upgrade import QualityDuplicateResult, reconcile_album_quality_duplicates
from app.settings_service import QualityProfile


async def duplicate_candidate_album_ids(db: AsyncSession) -> list[int]:
    duplicate_groups = (
        select(Track.catalog_album_id.label("album_id"))
        .join(ImportPlan, ImportPlan.track_id == Track.id)
        .where(
            Track.catalog_album_id.is_not(None),
            Track.catalog_track_id.is_not(None),
            Track.import_state == ImportWorkflowState.imported,
            ImportPlan.status == ImportWorkflowState.imported,
            ImportPlan.destination_path != "",
        )
        .group_by(Track.catalog_album_id, Track.catalog_track_id)
        .having(func.count(ImportPlan.id) > 1)
        .subquery()
    )
    rows = await db.scalars(select(duplicate_groups.c.album_id).distinct())
    return [int(album_id) for album_id in rows if album_id is not None]


def summarize_duplicate_results(
    album_results: list[tuple[int, QualityDuplicateResult]],
) -> DuplicateScanSummary:
    paths: list[str] = []
    for _album_id, result in album_results:
        paths.extend(result.would_delete_paths)
    return DuplicateScanSummary(
        deleted_files=sum(result.deleted_files for _album_id, result in album_results),
        review_required=sum(result.review_required for _album_id, result in album_results),
        would_delete_paths=tuple(dict.fromkeys(paths)),
        albums=tuple(
            DuplicateAlbumSummary(album_id=album_id, result=result)
            for album_id, result in album_results
        ),
        scanned_at=datetime.now(UTC),
    )


async def scan_library_duplicates(
    db: AsyncSession, *, library_root: Path, quality_profile: QualityProfile
) -> DuplicateScanSummary:
    album_results: list[tuple[int, QualityDuplicateResult]] = []
    for album_id in await duplicate_candidate_album_ids(db):
        result = await reconcile_album_quality_duplicates(
            db,
            album_id,
            library_root=library_root,
            quality_profile=quality_profile,
            dry_run=True,
        )
        album_results.append((album_id, result))
    return summarize_duplicate_results(album_results)


async def clean_safe_library_duplicates(
    db: AsyncSession,
    *,
    library_root: Path,
    quality_profile: QualityProfile,
    latest_scan: DuplicateScanSummary,
) -> QualityDuplicateResult:
    deleted = 0
    review = 0
    for album_id in latest_scan.safe_album_ids:
        result = await reconcile_album_quality_duplicates(
            db,
            album_id,
            library_root=library_root,
            quality_profile=quality_profile,
            defer_filesystem_delete=True,
        )
        deleted += result.deleted_files
        review += result.review_required
    return QualityDuplicateResult(deleted_files=deleted, review_required=review)
