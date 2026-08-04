from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.metadata.filename_parse import normalize_for_catalog_match, strip_non_identity_descriptors
from app.models.catalog_entities import CatalogAlbumTrack
from app.models.import_plan import CollisionState, ImportPlan, LibraryFileState
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
    ReviewDecision,
)


@dataclass(frozen=True)
class ImportBacklogReconciliationReport:
    dry_run: bool
    pending_identity_reviews: int
    identity_candidates: tuple[int, ...]
    destination_review_plans: int
    destination_candidates: tuple[int, ...]
    stale_projection_candidates: tuple[int, ...]
    artifact_missing_plans: tuple[int, ...]
    no_plan_tracks: tuple[int, ...]
    cross_track_hash_conflict_plans: tuple[int, ...]
    identity_repaired: int = 0
    destinations_closed: int = 0
    stale_projections_normalized: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _normalized_title(value: str | None) -> str:
    return normalize_for_catalog_match(
        strip_non_identity_descriptors(value or "", preserve_featured_artists=False)
    )


def _is_unimported_download(track: Track) -> bool:
    return (
        track.acquisition_state == AcquisitionState.downloaded
        and track.import_state != ImportWorkflowState.imported
    )


def _path_is_file(raw: str) -> bool:
    return Path(raw).is_file()


def _source_exists(track: Track) -> bool:
    raw = track.staging_path or track.source_path
    return bool(raw and _path_is_file(raw))


async def reconcile_import_backlog(
    db: AsyncSession,
    *,
    acceptance_threshold: float,
    apply: bool = False,
    require_existing_files: bool = True,
) -> ImportBacklogReconciliationReport:
    """Classify the historical import backlog and apply only identity-safe repairs.

    Dry-run is the default. The caller owns commit/rollback; this function only
    flushes when ``apply`` is true.
    """
    review_rows = list(
        (
            await db.execute(
                select(StagingReviewItem, Track, CatalogAlbumTrack)
                .join(Track, Track.id == StagingReviewItem.track_id)
                .outerjoin(CatalogAlbumTrack, CatalogAlbumTrack.id == Track.catalog_track_id)
                .where(StagingReviewItem.review_state == ReviewDecision.pending)
                .order_by(StagingReviewItem.id)
            )
        ).all()
    )
    identity_candidates: list[tuple[StagingReviewItem, Track, CatalogAlbumTrack]] = []
    for item, track, catalog_track in review_rows:
        if (
            item.verification_reason != "no_expected_mbid"
            or catalog_track is None
            or not catalog_track.recording_mbid
            or not _is_unimported_download(track)
            or require_existing_files
            and not _source_exists(track)
        ):
            continue
        observed = tuple(
            dict.fromkeys(value.strip() for value in item.observed_acoustid_mbids if value.strip())
        )
        if observed != (catalog_track.recording_mbid,):
            continue
        if (item.acoustid_score or 0.0) <= acceptance_threshold:
            continue
        if _normalized_title(item.expected_title or track.title) != _normalized_title(
            catalog_track.title
        ):
            continue
        if (
            item.fingerprint_duration_sec is None
            or catalog_track.duration_sec is None
            or abs(item.fingerprint_duration_sec - catalog_track.duration_sec) > 8
        ):
            continue
        identity_candidates.append((item, track, catalog_track))

    destination_plans = list(
        (
            await db.scalars(
                select(ImportPlan)
                .where(
                    ImportPlan.status == ImportWorkflowState.needs_review,
                    ImportPlan.collision_state == CollisionState.conflict,
                )
                .options(
                    selectinload(ImportPlan.track),
                    selectinload(ImportPlan.release).selectinload(Release.tracks),
                )
                .order_by(ImportPlan.id)
            )
        ).all()
    )
    owners = list(
        (
            await db.scalars(
                select(ImportPlan)
                .where(
                    ImportPlan.status == ImportWorkflowState.imported,
                    ImportPlan.file_state == LibraryFileState.present,
                )
                .options(selectinload(ImportPlan.track))
                .order_by(ImportPlan.id)
            )
        ).all()
    )
    owners_by_path: dict[str, list[ImportPlan]] = {}
    for owner in owners:
        owners_by_path.setdefault(owner.destination_path, []).append(owner)

    destination_candidates: list[ImportPlan] = []
    for plan in destination_plans:
        identity = plan.track.catalog_track_id if plan.track is not None else None
        matching_owners = [
            owner
            for owner in owners_by_path.get(plan.destination_path, [])
            if owner.id != plan.id
            and owner.track is not None
            and identity is not None
            and owner.track.catalog_track_id == identity
        ]
        if len(matching_owners) != 1:
            continue
        if require_existing_files and not _path_is_file(plan.destination_path):
            continue
        destination_candidates.append(plan)

    unimported_tracks = list(
        (
            await db.scalars(
                select(Track)
                .where(
                    Track.acquisition_state == AcquisitionState.downloaded,
                    Track.import_state != ImportWorkflowState.imported,
                )
                .options(
                    selectinload(Track.import_plans)
                    .selectinload(ImportPlan.release)
                    .selectinload(Release.tracks)
                )
                .order_by(Track.id)
            )
        ).all()
    )
    pending_review_track_ids = {track.id for _item, track, _catalog in review_rows}
    pending_review_release_ids = {item.release_id for item, _track, _catalog in review_rows}
    no_plans: list[int] = []
    artifact_missing_plans: list[int] = []
    hash_conflict_plans: list[int] = []
    stale_candidates: list[ImportPlan] = []
    for track in unimported_tracks:
        if track.id in pending_review_track_ids:
            continue
        latest = max(track.import_plans, key=lambda plan: plan.id, default=None)
        if latest is None:
            no_plans.append(track.id)
            continue
        if latest.status == ImportWorkflowState.needs_review:
            if latest.collision_state == CollisionState.needs_review:
                artifact_missing_plans.append(latest.id)
            elif latest.collision_state == CollisionState.duplicate:
                hash_conflict_plans.append(latest.id)
            continue
        if (
            latest.status == ImportWorkflowState.rolled_back
            and latest.release.review_dismissed_at is None
            and latest.release_id not in pending_review_release_ids
            and all(
                other.id == track.id
                or other.acquisition_state != AcquisitionState.downloaded
                or other.import_state
                in {ImportWorkflowState.imported, ImportWorkflowState.rolled_back}
                for other in latest.release.tracks
            )
        ):
            stale_candidates.append(latest)

    if apply:
        for item, track, catalog_track in identity_candidates:
            track.mbid = catalog_track.recording_mbid
            track.identity_state = IdentityResolutionState.resolved
            track.acoustid_verification_state = AcoustIDVerificationState.verified
            await db.delete(item)
        now = datetime.now(UTC).replace(tzinfo=None)
        for plan in destination_candidates:
            plan.status = ImportWorkflowState.rolled_back
            plan.collision_state = CollisionState.duplicate
            plan.error_detail = None
            plan.rollback_detail = (
                "duplicate projection closed: destination owned by same catalog track"
            )
            if plan.track is not None and plan.track.import_state != ImportWorkflowState.imported:
                plan.track.import_state = ImportWorkflowState.rolled_back
            if plan.release_id not in pending_review_release_ids and all(
                track.acquisition_state != AcquisitionState.downloaded
                or track.import_state
                in {ImportWorkflowState.imported, ImportWorkflowState.rolled_back}
                for track in plan.release.tracks
            ):
                plan.release.review_dismissed_at = now
        for plan in stale_candidates:
            if plan.track is not None:
                plan.track.import_state = ImportWorkflowState.rolled_back
            plan.release.review_dismissed_at = now
            plan.error_detail = None
            plan.rollback_detail = (
                plan.rollback_detail or "resolved rolled-back projection dismissed"
            )
        await db.flush()

    return ImportBacklogReconciliationReport(
        dry_run=not apply,
        pending_identity_reviews=len(review_rows),
        identity_candidates=tuple(item.id for item, _track, _catalog in identity_candidates),
        destination_review_plans=len(destination_plans),
        destination_candidates=tuple(plan.id for plan in destination_candidates),
        stale_projection_candidates=tuple(plan.id for plan in stale_candidates),
        artifact_missing_plans=tuple(artifact_missing_plans),
        no_plan_tracks=tuple(no_plans),
        cross_track_hash_conflict_plans=tuple(hash_conflict_plans),
        identity_repaired=len(identity_candidates) if apply else 0,
        destinations_closed=len(destination_candidates) if apply else 0,
        stale_projections_normalized=len(stale_candidates) if apply else 0,
    )
