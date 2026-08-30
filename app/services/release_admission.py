from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import run_with_sqlite_lock_retry
from app.models.acquisition_claim import (
    AcquisitionDispatchClaim,
    CatalogReleaseAcquisitionClaim,
)
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumProvider, CatalogAlbumTrack
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchJobRole,
    DiscographyBatchState,
    DiscographyJobOwnership,
)
from app.models.job import Job, JobStatus
from app.services.catalog import (
    DiscographyLeaseLostError,
    get_release_progress,
    project_catalog_album_queue_targets,
)
from app.services.catalog_artist_credits import catalog_album_artist_name
from app.services.catalog_manifest import catalog_manifest_issue
from app.services.session_contract import reject_pending_orm_changes
from app.settings_service import QualityProfile

_ACTIVE_JOB_STATUSES = (JobStatus.pending, JobStatus.running)
_ACTIVE_BATCH_STATES = (DiscographyBatchState.queued, DiscographyBatchState.running)


class ReleaseRootAdmissionStatus(StrEnum):
    """Truthful outcome of one release-root reservation attempt."""

    no_work = "no_work"
    created = "created"
    observed = "observed"
    waiting_for_tracks = "waiting_for_tracks"


@dataclass(frozen=True, slots=True)
class ReleaseRootAdmissionResult:
    """Committed release-root admission state safe for post-commit dispatch."""

    status: ReleaseRootAdmissionStatus
    job_id: int | None
    target_track_ids: tuple[int, ...]
    blocking_job_ids: tuple[int, ...] = ()


async def _validate_preprojection_lease(
    db: AsyncSession,
    item_id: int,
    execution_generation: int,
    batch_lease_token: str,
) -> int:
    row = (
        await db.execute(
            select(
                DiscographyBatchItem.catalog_album_id,
                DiscographyBatchItem.execution_generation,
                DiscographyBatchItem.lease_token,
                DiscographyBatchItem.state,
                DiscographyBatch.state,
            )
            .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
            .where(DiscographyBatchItem.id == item_id)
        )
    ).one_or_none()
    if row is None:
        raise ValueError("discography batch item does not exist")
    album_id, generation, lease_token, item_state, batch_state = row
    if generation != execution_generation:
        raise DiscographyLeaseLostError("discography batch execution generation is stale")
    if (
        lease_token != batch_lease_token
        or item_state != DiscographyBatchItemState.expanding
        or batch_state not in _ACTIVE_BATCH_STATES
    ):
        raise DiscographyLeaseLostError("discography batch lease is no longer active")
    if album_id is None:
        raise ValueError("discography batch item has no catalog album")
    return int(album_id)


async def _add_release_root_link(
    db: AsyncSession,
    *,
    item_id: int,
    execution_generation: int,
    job_id: int,
    ownership: DiscographyJobOwnership,
) -> None:
    db.add(
        DiscographyBatchItemJob(
            item_id=item_id,
            job_id=job_id,
            generation=execution_generation,
            catalog_track_id=None,
            ownership=ownership,
            role=DiscographyBatchJobRole.release_root,
        )
    )


async def materialize_batch_release_root_job(
    db: AsyncSession,
    batch_item_id: int,
    *,
    execution_generation: int,
    batch_lease_token: str,
    quality_profile: QualityProfile,
    library_root: Path | None = None,
) -> ReleaseRootAdmissionResult:
    """Atomically create or observe the release root for an owned batch item.

    The quality projection happens before the SQLite writer reservation because it may
    inspect destination-path existence. Every database-dependent fact is reconstructed
    and revalidated inside ``BEGIN IMMEDIATE`` before any mutation, and only committed
    identifiers are returned.
    """
    reject_pending_orm_changes(db)
    album_id = await _validate_preprojection_lease(
        db, batch_item_id, execution_generation, batch_lease_token
    )
    projection = (
        await project_catalog_album_queue_targets(
            db,
            [album_id],
            library_root=library_root,
            quality_profile=quality_profile,
        )
    )[album_id]
    projected_quality_targets = set(projection.target_track_ids) & set(
        projection.imported_track_ids
    )
    await db.commit()

    result: ReleaseRootAdmissionResult | None = None

    async def reserve() -> None:
        nonlocal result
        result = None
        await db.execute(text("BEGIN IMMEDIATE"))
        row = (
            await db.execute(
                select(DiscographyBatchItem, DiscographyBatch)
                .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
                .where(DiscographyBatchItem.id == batch_item_id)
            )
        ).one_or_none()
        if row is None:
            raise ValueError("discography batch item does not exist")
        item, batch = row
        if item.execution_generation != execution_generation:
            raise DiscographyLeaseLostError("discography batch execution generation is stale")
        if (
            item.lease_token != batch_lease_token
            or item.state != DiscographyBatchItemState.expanding
            or batch.state not in _ACTIVE_BATCH_STATES
        ):
            raise DiscographyLeaseLostError("discography batch lease is no longer active")
        if item.catalog_album_id != album_id:
            raise ValueError("discography batch item catalog album changed")

        album = await db.scalar(
            select(CatalogAlbum)
            .where(CatalogAlbum.id == album_id)
            .options(selectinload(CatalogAlbum.artist))
        )
        if album is None:
            raise ValueError("catalog album does not exist")
        tracks = list(
            (
                await db.scalars(
                    select(CatalogAlbumTrack)
                    .where(CatalogAlbumTrack.album_id == album_id)
                    .order_by(
                        CatalogAlbumTrack.disc,
                        CatalogAlbumTrack.position,
                        CatalogAlbumTrack.id,
                    )
                )
            ).all()
        )
        expected_count = max(album.track_count or 0, item.expected_track_count or 0) or None
        if item.provider_release_id is not None:
            provider_expected = await db.scalar(
                select(CatalogAlbumProvider.track_count).where(
                    CatalogAlbumProvider.id == item.provider_release_id
                )
            )
            expected_count = max(expected_count or 0, provider_expected or 0) or None
        manifest_issue = catalog_manifest_issue(tracks, expected_count)
        if manifest_issue is not None:
            raise ValueError(f"catalog manifest is structurally invalid: {manifest_issue}")

        imported_ids = set(
            (await get_release_progress(db, [album_id]))[album_id].downloaded_catalog_track_ids
        )
        target_track_ids = tuple(
            track.id
            for track in tracks
            if track.id not in imported_ids or track.id in projected_quality_targets
        )
        if not target_track_ids:
            await db.commit()
            result = ReleaseRootAdmissionResult(
                status=ReleaseRootAdmissionStatus.no_work,
                job_id=None,
                target_track_ids=(),
            )
            return

        existing_job_id = await db.scalar(
            select(DiscographyBatchItemJob.job_id).where(
                DiscographyBatchItemJob.item_id == batch_item_id,
                DiscographyBatchItemJob.generation == execution_generation,
                DiscographyBatchItemJob.role == DiscographyBatchJobRole.release_root,
            )
        )
        if existing_job_id is not None:
            await db.commit()
            result = ReleaseRootAdmissionResult(
                status=ReleaseRootAdmissionStatus.observed,
                job_id=int(existing_job_id),
                target_track_ids=target_track_ids,
            )
            return

        claim_row = (
            await db.execute(
                select(CatalogReleaseAcquisitionClaim, Job)
                .outerjoin(Job, Job.id == CatalogReleaseAcquisitionClaim.job_id)
                .where(CatalogReleaseAcquisitionClaim.catalog_album_id == album_id)
            )
        ).one_or_none()
        if claim_row is not None:
            claim, owner = claim_row
            if owner is not None and owner.status in _ACTIVE_JOB_STATUSES:
                if owner.catalog_album_id != album_id or owner.catalog_track_id is not None:
                    raise ValueError(
                        "active catalog release claim does not own an exact release root"
                    )
                await _add_release_root_link(
                    db,
                    item_id=batch_item_id,
                    execution_generation=execution_generation,
                    job_id=owner.id,
                    ownership=DiscographyJobOwnership.observed,
                )
                await db.commit()
                result = ReleaseRootAdmissionResult(
                    status=ReleaseRootAdmissionStatus.observed,
                    job_id=owner.id,
                    target_track_ids=target_track_ids,
                )
                return
            if owner is None:
                await db.delete(claim)
                await db.flush()
                claim_row = None

        blocking_job_ids = tuple(
            int(job_id)
            for job_id in (
                await db.scalars(
                    select(Job.id)
                    .join(AcquisitionDispatchClaim, AcquisitionDispatchClaim.job_id == Job.id)
                    .where(
                        AcquisitionDispatchClaim.catalog_album_id == album_id,
                        Job.catalog_album_id == AcquisitionDispatchClaim.catalog_album_id,
                        Job.catalog_track_id == AcquisitionDispatchClaim.catalog_track_id,
                        Job.status.in_(_ACTIVE_JOB_STATUSES),
                    )
                    .order_by(Job.id)
                )
            ).all()
        )
        if blocking_job_ids:
            await db.commit()
            result = ReleaseRootAdmissionResult(
                status=ReleaseRootAdmissionStatus.waiting_for_tracks,
                job_id=None,
                target_track_ids=target_track_ids,
                blocking_job_ids=blocking_job_ids,
            )
            return

        query = " ".join(
            part for part in (catalog_album_artist_name(album), album.title.strip()) if part
        )
        root = Job(
            source="priority",
            query=query,
            status=JobStatus.pending,
            catalog_album_id=album_id,
            catalog_track_id=None,
            parent_job_id=None,
            partial_attempt=0,
        )
        db.add(root)
        await db.flush()
        if claim_row is None:
            db.add(CatalogReleaseAcquisitionClaim(catalog_album_id=album_id, job_id=root.id))
        else:
            terminal_claim, _terminal_owner = claim_row
            terminal_claim.job_id = root.id
        await _add_release_root_link(
            db,
            item_id=batch_item_id,
            execution_generation=execution_generation,
            job_id=root.id,
            ownership=DiscographyJobOwnership.created,
        )
        await db.commit()
        result = ReleaseRootAdmissionResult(
            status=ReleaseRootAdmissionStatus.created,
            job_id=root.id,
            target_track_ids=target_track_ids,
        )

    try:
        await run_with_sqlite_lock_retry(db, reserve, attempts=6, delay_seconds=0.2)
    except Exception:
        await db.rollback()
        raise
    if result is None:
        raise RuntimeError("release-root admission completed without a committed result")
    return result
