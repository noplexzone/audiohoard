from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select
from sqlalchemy.sql.selectable import ScalarSelect

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyJobOwnership,
)
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import ReviewDecision
from app.services.catalog import _present_library_artifact_filter


@dataclass(frozen=True)
class PreparingDownload:
    release_identity: str
    artist_name: str
    release_title: str
    estimated_job_count: int
    catalog_album_id: int | None


def _preparing_downloads_query() -> Select[tuple[str, str, str, int, datetime]]:
    return (
        select(
            DiscographyBatchItem.release_identity.label("release_identity"),
            func.max(DiscographyBatchItem.artist_name).label("artist_name"),
            func.max(DiscographyBatchItem.release_title).label("release_title"),
            func.max(DiscographyBatchItem.estimated_job_count).label("estimated_job_count"),
            func.min(DiscographyBatchItem.created_at).label("created_at"),
        )
        .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
        .outerjoin(
            DiscographyBatchItemJob,
            and_(
                DiscographyBatchItemJob.item_id == DiscographyBatchItem.id,
                DiscographyBatchItemJob.ownership == DiscographyJobOwnership.created,
            ),
        )
        .where(
            DiscographyBatch.state.in_(
                (DiscographyBatchState.queued, DiscographyBatchState.running)
            ),
            DiscographyBatchItem.state.in_(
                (
                    DiscographyBatchItemState.pending,
                    DiscographyBatchItemState.hydrating,
                    DiscographyBatchItemState.expanding,
                )
            ),
            DiscographyBatchItem.estimated_job_count > 0,
        )
        .group_by(DiscographyBatchItem.release_identity)
        .having(func.count(DiscographyBatchItemJob.id) == 0)
    )


async def get_preparing_downloads(db: AsyncSession) -> list[PreparingDownload]:
    rows = (await db.execute(_preparing_downloads_query().order_by("created_at"))).all()
    downloads: list[PreparingDownload] = []
    for row in rows:
        identity = str(row.release_identity)
        album_id: int | None = None
        if identity.startswith("catalog_album:"):
            try:
                album_id = int(identity.removeprefix("catalog_album:"))
            except ValueError:
                album_id = None
        downloads.append(
            PreparingDownload(
                release_identity=identity,
                artist_name=str(row.artist_name),
                release_title=str(row.release_title),
                estimated_job_count=int(row.estimated_job_count),
                catalog_album_id=album_id,
            )
        )
    return downloads


@dataclass(frozen=True, slots=True)
class BatchFailure:
    batch_id: int
    item_id: int
    artist_name: str
    release_title: str
    error_detail: str


def _batch_failures_query() -> Select[tuple[int, int, str, str, str | None, datetime]]:
    return (
        select(
            DiscographyBatchItem.batch_id,
            DiscographyBatchItem.id.label("item_id"),
            DiscographyBatchItem.artist_name,
            DiscographyBatchItem.release_title,
            DiscographyBatchItem.error_detail,
            DiscographyBatchItem.updated_at,
        )
        .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
        .where(
            DiscographyBatch.state != DiscographyBatchState.cancelled,
            DiscographyBatchItem.state == DiscographyBatchItemState.failed,
            ~DiscographyBatchItem.job_links.any(
                DiscographyBatchItemJob.ownership == DiscographyJobOwnership.created
            ),
        )
    )


@dataclass(frozen=True, slots=True)
class BatchFailurePage:
    items: list[BatchFailure]
    total: int
    page: int
    per_page: int

    @property
    def pages(self) -> int:
        return max(1, (self.total + self.per_page - 1) // self.per_page)


async def get_batch_failure_page(
    db: AsyncSession, *, page: int = 1, per_page: int = 20
) -> BatchFailurePage:
    page = max(1, page)
    per_page = max(1, min(per_page, 100))
    query = _batch_failures_query()
    total = int(await db.scalar(select(func.count()).select_from(query.subquery())) or 0)
    rows = (
        await db.execute(
            query.order_by(DiscographyBatchItem.updated_at.desc(), DiscographyBatchItem.id.desc())
            .offset((page - 1) * per_page)
            .limit(per_page)
        )
    ).all()
    items = [
        BatchFailure(
            batch_id=int(row.batch_id),
            item_id=int(row.item_id),
            artist_name=str(row.artist_name),
            release_title=str(row.release_title),
            error_detail=str(row.error_detail or "Download preparation failed."),
        )
        for row in rows
    ]
    return BatchFailurePage(items=items, total=total, page=page, per_page=per_page)


@dataclass(frozen=True, slots=True)
class ActivitySummary:
    """Bounded overview counts for acquisition work and navigation attention."""

    wanted: int
    active_downloads: int
    acquisition_issues: int
    awaiting_review: int
    rejected_sources: int

    @property
    def attention(self) -> int:
        """Items requiring a decision or recovery, excluding informational history."""
        return self.acquisition_issues + self.awaiting_review


def _wanted_count_query() -> ScalarSelect[int]:
    manifest_count = (
        select(func.count(CatalogAlbumTrack.id))
        .where(CatalogAlbumTrack.album_id == CatalogAlbum.id)
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    downloaded_count = (
        select(func.count(func.distinct(Track.catalog_track_id)))
        .where(
            Track.catalog_album_id == CatalogAlbum.id,
            Track.catalog_track_id.is_not(None),
            _present_library_artifact_filter(),
        )
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    wanted_releases = (
        select(CatalogAlbum.id)
        .join(CatalogArtist, CatalogArtist.id == CatalogAlbum.artist_id)
        .where(
            CatalogArtist.monitored.is_(True),
            CatalogAlbum.monitored.is_(True),
            or_(manifest_count == 0, downloaded_count < manifest_count),
        )
        .subquery()
    )
    return select(func.count()).select_from(wanted_releases).scalar_subquery()


async def get_activity_summary(db: AsyncSession) -> ActivitySummary:
    """Return all Activity counts with one database round trip and no entity loading."""
    active_jobs = (
        select(func.count(Job.id))
        .where(
            Job.queue_hidden.is_(False),
            Job.status.in_((JobStatus.pending, JobStatus.running)),
        )
        .scalar_subquery()
    )
    preparing_rows = _preparing_downloads_query().subquery()
    preparing_downloads = (
        select(func.coalesce(func.sum(preparing_rows.c.estimated_job_count), 0))
        .select_from(preparing_rows)
        .scalar_subquery()
    )
    active_downloads = active_jobs + preparing_downloads
    failed_jobs = (
        select(func.count(Job.id))
        .where(
            Job.queue_hidden.is_(False),
            Job.status.in_((JobStatus.failed, JobStatus.partial)),
        )
        .scalar_subquery()
    )
    failed_batch_items = (
        select(func.count()).select_from(_batch_failures_query().subquery()).scalar_subquery()
    )
    acquisition_issues = failed_jobs + failed_batch_items
    awaiting_review = (
        select(func.count(StagingReviewItem.id))
        .join(Release, StagingReviewItem.release_id == Release.id)
        .where(
            StagingReviewItem.review_state == ReviewDecision.pending,
            Release.review_dismissed_at.is_(None),
        )
        .scalar_subquery()
    )
    now = datetime.now(UTC)
    rejected_sources = (
        select(func.count(SourceCandidateBlock.id))
        .where(
            or_(
                SourceCandidateBlock.blocked_until.is_(None),
                SourceCandidateBlock.blocked_until > now,
            )
        )
        .scalar_subquery()
    )
    row = (
        await db.execute(
            select(
                _wanted_count_query().label("wanted"),
                active_downloads.label("active_downloads"),
                acquisition_issues.label("acquisition_issues"),
                awaiting_review.label("awaiting_review"),
                rejected_sources.label("rejected_sources"),
            )
        )
    ).one()
    return ActivitySummary(
        wanted=int(row.wanted or 0),
        active_downloads=int(row.active_downloads or 0),
        acquisition_issues=int(row.acquisition_issues or 0),
        awaiting_review=int(row.awaiting_review or 0),
        rejected_sources=int(row.rejected_sources or 0),
    )
