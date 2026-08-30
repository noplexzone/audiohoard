from __future__ import annotations

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import CatalogAlbum, CatalogArtist
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyJobOwnership,
    DiscographyScopeKind,
)
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import ReviewDecision
from app.services.activity import ActivitySummary, get_activity_summary


async def test_activity_summary_counts_actionable_work_in_one_query(
    db_session: AsyncSession,
) -> None:
    wanted_artist = CatalogArtist(name="Wanted artist", monitored=True)
    wanted_album = CatalogAlbum(artist=wanted_artist, title="Wanted album", monitored=True)
    ignored_artist = CatalogArtist(name="Ignored artist", monitored=False)
    ignored_album = CatalogAlbum(artist=ignored_artist, title="Ignored album", monitored=True)
    running = Job(source="slskd", query="active", status=JobStatus.running)
    pending = Job(source="slskd", query="queued", status=JobStatus.pending)
    queued_batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.wanted_selected,
        scope_json="{}",
        scope_hash="queued-activity",
        state=DiscographyBatchState.queued,
        estimated_job_count=3,
    )
    queued_batch.items.append(
        DiscographyBatchItem(
            release_identity="catalog_album:activity",
            artist_name="Queued artist",
            release_title="Queued album",
            state=DiscographyBatchItemState.pending,
            target_count=3,
            estimated_job_count=3,
        )
    )
    materialized_item = DiscographyBatchItem(
        release_identity="catalog_album:materialized",
        artist_name="Materialized artist",
        release_title="Materialized album",
        state=DiscographyBatchItemState.expanding,
        target_count=2,
        estimated_job_count=2,
    )
    materialized_item.job_links.append(
        DiscographyBatchItemJob(job=pending, ownership=DiscographyJobOwnership.created)
    )
    queued_batch.items.append(materialized_item)
    duplicate_batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.wanted_page,
        scope_json="{}",
        scope_hash="queued-activity-duplicate",
        state=DiscographyBatchState.running,
        estimated_job_count=3,
    )
    duplicate_batch.items.append(
        DiscographyBatchItem(
            release_identity="catalog_album:activity",
            artist_name="Queued artist",
            release_title="Queued album",
            state=DiscographyBatchItemState.hydrating,
            target_count=3,
            estimated_job_count=3,
        )
    )
    failed_batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.artist,
        scope_json="{}",
        scope_hash="failed-activity",
        state=DiscographyBatchState.completed_with_failures,
    )
    failed_batch.items.append(
        DiscographyBatchItem(
            release_identity="catalog_album:failed-activity",
            artist_name="Failed artist",
            release_title="Failed album",
            state=DiscographyBatchItemState.failed,
            error_detail="Provider hydration failed",
        )
    )
    cancelled_batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.wanted_all_matching,
        scope_json="{}",
        scope_hash="cancelled-failure-history",
        state=DiscographyBatchState.cancelled,
    )
    cancelled_batch.items.append(
        DiscographyBatchItem(
            release_identity="catalog_album:cancelled-failure-history",
            artist_name="Cancelled artist",
            release_title="Cancelled album",
            state=DiscographyBatchItemState.failed,
            error_detail="Historical failure before cancellation",
        )
    )
    failed = Job(source="slskd", query="failed", status=JobStatus.failed)
    partial = Job(source="slskd", query="retrying", status=JobStatus.partial)
    hidden_failed = Job(source="slskd", query="hidden", status=JobStatus.failed, queue_hidden=True)
    review_job = Job(source="slskd", query="review", status=JobStatus.done)
    release = Release(job=review_job, source="slskd", title="Review release")
    track = Track(job=review_job, release=release, source="slskd", title="Review track")
    pending_review = StagingReviewItem(
        track=track,
        release=release,
        expected_title="Review track",
        review_state=ReviewDecision.pending,
    )
    decided_review = StagingReviewItem(
        track=track,
        release=release,
        expected_title="Review track",
        review_state=ReviewDecision.approved,
    )
    rejected = SourceCandidateBlock(
        provider="slskd", peer="peer", filename="Album/track.flac", reason="denied"
    )
    db_session.add_all(
        [
            wanted_album,
            ignored_album,
            running,
            pending,
            queued_batch,
            duplicate_batch,
            failed_batch,
            cancelled_batch,
            failed,
            partial,
            hidden_failed,
            pending_review,
            decided_review,
            rejected,
        ]
    )
    await db_session.commit()

    statements = 0

    def count_statement(*_args: object, **_kwargs: object) -> None:
        nonlocal statements
        statements += 1

    assert db_session.bind is not None
    sync_engine = db_session.bind.sync_engine
    event.listen(sync_engine, "before_cursor_execute", count_statement)
    try:
        summary = await get_activity_summary(db_session)
    finally:
        event.remove(sync_engine, "before_cursor_execute", count_statement)

    assert statements == 1
    assert summary.wanted == 1
    assert summary.active_downloads == 5
    assert summary.acquisition_issues == 3
    assert summary.awaiting_review == 1
    assert summary.rejected_sources == 1
    assert summary.attention == 4


def test_activity_summary_attention_excludes_informational_counts() -> None:
    summary = ActivitySummary(
        wanted=12,
        active_downloads=5,
        acquisition_issues=2,
        awaiting_review=3,
        rejected_sources=40,
    )

    assert summary.attention == 5
