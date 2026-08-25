from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
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
    preparing_downloads = (
        select(func.coalesce(func.sum(DiscographyBatchItem.estimated_job_count), 0))
        .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
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
            ~DiscographyBatchItem.job_links.any(
                DiscographyBatchItemJob.ownership == DiscographyJobOwnership.created
            ),
        )
        .scalar_subquery()
    )
    active_downloads = active_jobs + preparing_downloads
    acquisition_issues = (
        select(func.count(Job.id))
        .where(
            Job.queue_hidden.is_(False),
            Job.status.in_((JobStatus.failed, JobStatus.partial)),
        )
        .scalar_subquery()
    )
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
