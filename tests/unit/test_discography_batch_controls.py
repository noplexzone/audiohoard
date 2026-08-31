from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
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
from app.services import discography_batches
from app.services.discography_batches import (
    cancel_discography_batch,
    confirm_discography_batch,
    create_discography_batch_preview,
    pause_discography_batch,
    retry_discography_batch_items,
)
from app.settings_service import QualityProfile

PROFILE = QualityProfile(
    format_preference=["flac", "mp3", "m4a/aac", "ogg", "opus"],
    min_mp3_bitrate=320,
    allow_lower_quality_fallback=True,
)


async def _album(db: AsyncSession, title: str = "Controls") -> CatalogAlbum:
    artist = CatalogArtist(name="Controls", monitored=True)
    album = CatalogAlbum(artist=artist, title=title, monitored=True, track_count=1)
    album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Track"))
    db.add(artist)
    await db.flush()
    return album


async def test_confirmation_scope_change_rebuilds_same_preview_without_jobs(
    db_session: AsyncSession,
) -> None:
    first = await _album(db_session, "First")
    preview = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_all_matching,
        {"q": "", "sort": "title", "status": "all"},
        quality_profile=PROFILE,
    )
    await _album(db_session, "Second")
    await db_session.commit()

    result = await confirm_discography_batch(db_session, preview.id, quality_profile=PROFILE)

    assert result.scope_changed
    assert result.batch.id == preview.id
    assert result.batch.state == DiscographyBatchState.preview
    assert result.batch.matching_count == 2
    assert await db_session.scalar(select(func.count(Job.id))) == 0
    assert first.id is not None


async def test_confirmation_unchanged_queues_and_controls_cancel_created_pending_only(
    db_session: AsyncSession,
) -> None:
    album = await _album(db_session)
    album.track_count = 3
    album.tracks.extend(
        [
            CatalogAlbumTrack(disc=1, position=2, title="Track Two"),
            CatalogAlbumTrack(disc=1, position=3, title="Track Three"),
        ]
    )
    await db_session.flush()
    preview = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_selected,
        {"album_ids": [album.id]},
        quality_profile=PROFILE,
    )
    confirmation = await confirm_discography_batch(db_session, preview.id, quality_profile=PROFILE)
    assert not confirmation.scope_changed
    assert confirmation.batch.state == DiscographyBatchState.queued
    item = await db_session.scalar(
        select(DiscographyBatchItem).where(DiscographyBatchItem.batch_id == preview.id)
    )
    assert item is not None and item.state == DiscographyBatchItemState.pending

    catalog_tracks = sorted(album.tracks, key=lambda track: track.position)
    created_pending = Job(
        source="priority",
        query="created",
        status=JobStatus.pending,
        catalog_album=album,
        catalog_track=catalog_tracks[0],
    )
    created_running = Job(
        source="priority",
        query="running",
        status=JobStatus.running,
        catalog_album=album,
        catalog_track=catalog_tracks[1],
    )
    observed_pending = Job(
        source="priority",
        query="observed",
        status=JobStatus.pending,
        catalog_album=album,
        catalog_track=catalog_tracks[2],
    )
    db_session.add_all([created_pending, created_running, observed_pending])
    await db_session.flush()
    created_descendant = Job(
        source="priority",
        query="created descendant",
        status=JobStatus.pending,
        parent_job_id=created_running.id,
    )
    observed_descendant = Job(
        source="priority",
        query="observed descendant",
        status=JobStatus.pending,
        parent_job_id=observed_pending.id,
    )
    db_session.add_all([created_descendant, observed_descendant])
    await db_session.flush()
    db_session.add_all(
        [
            DiscographyBatchItemJob(
                item_id=item.id,
                job_id=created_pending.id,
                catalog_track_id=catalog_tracks[0].id,
                ownership=DiscographyJobOwnership.created,
            ),
            DiscographyBatchItemJob(
                item_id=item.id,
                job_id=created_running.id,
                catalog_track_id=catalog_tracks[1].id,
                ownership=DiscographyJobOwnership.created,
            ),
            DiscographyBatchItemJob(
                item_id=item.id,
                job_id=observed_pending.id,
                catalog_track_id=catalog_tracks[2].id,
                ownership=DiscographyJobOwnership.observed,
            ),
        ]
    )
    await db_session.commit()
    await pause_discography_batch(db_session, preview.id)
    cancelled = await cancel_discography_batch(db_session, preview.id)
    assert cancelled.cancel_job_ids == tuple(sorted((created_pending.id, created_descendant.id)))
    assert (await db_session.get(Job, created_pending.id)).status == JobStatus.cancelled  # type: ignore[union-attr]
    assert (await db_session.get(Job, created_pending.id)).queue_hidden is True  # type: ignore[union-attr]
    assert (await db_session.get(Job, created_running.id)).status == JobStatus.running  # type: ignore[union-attr]
    assert (await db_session.get(Job, created_running.id)).queue_hidden is False  # type: ignore[union-attr]
    assert (await db_session.get(Job, observed_pending.id)).status == JobStatus.pending  # type: ignore[union-attr]
    assert (await db_session.get(Job, observed_pending.id)).queue_hidden is False  # type: ignore[union-attr]
    assert (await db_session.get(Job, created_descendant.id)).status == JobStatus.cancelled  # type: ignore[union-attr]
    assert (await db_session.get(Job, created_descendant.id)).queue_hidden is True  # type: ignore[union-attr]
    assert (await db_session.get(Job, observed_descendant.id)).status == JobStatus.pending  # type: ignore[union-attr]
    assert (await db_session.get(Job, observed_descendant.id)).queue_hidden is False  # type: ignore[union-attr]
    assert await db_session.scalar(select(func.count(DiscographyBatchItemJob.id))) == 3


async def test_retry_resets_only_selected_retryable_item(db_session: AsyncSession) -> None:
    album = await _album(db_session)
    batch = DiscographyBatch(
        scope_kind=DiscographyScopeKind.wanted_selected,
        scope_json="{}",
        scope_hash="0" * 64,
        state=DiscographyBatchState.completed_with_failures,
    )
    failed = DiscographyBatchItem(
        batch=batch,
        release_identity="failed",
        catalog_album=album,
        artist_name="Controls",
        release_title="Failed",
        state=DiscographyBatchItemState.failed,
    )
    sibling = DiscographyBatchItem(
        batch=batch,
        release_identity="sibling",
        catalog_album=album,
        artist_name="Controls",
        release_title="Sibling",
        state=DiscographyBatchItemState.failed,
    )
    db_session.add_all([failed, sibling])
    await db_session.commit()
    result = await retry_discography_batch_items(db_session, batch.id, [failed.id])
    assert result.reset_item_ids == (failed.id,)
    assert failed.state == DiscographyBatchItemState.pending
    assert failed.execution_generation == 2
    assert sibling.state == DiscographyBatchItemState.failed
    assert sibling.execution_generation == 1


async def test_confirmation_completes_empty_batch_without_queueing(
    db_session: AsyncSession,
) -> None:
    preview = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_selected,
        {"album_ids": []},
        quality_profile=PROFILE,
    )
    confirmation = await confirm_discography_batch(db_session, preview.id, quality_profile=PROFILE)
    assert not confirmation.scope_changed
    assert confirmation.batch.state == DiscographyBatchState.completed


async def test_confirmation_completes_batch_when_every_item_is_already_complete(
    db_session: AsyncSession, monkeypatch
) -> None:
    album = await _album(db_session)
    preview = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_selected,
        {"album_ids": [album.id]},
        quality_profile=PROFILE,
    )
    item = await db_session.scalar(
        select(DiscographyBatchItem).where(DiscographyBatchItem.batch_id == preview.id)
    )
    assert item is not None
    item.state = DiscographyBatchItemState.complete
    item.reason_code = "verified_complete"
    await db_session.commit()

    async def preserve_complete(*_args) -> None:
        return None

    monkeypatch.setattr(discography_batches, "_populate_batch_items", preserve_complete)
    confirmation = await confirm_discography_batch(db_session, preview.id, quality_profile=PROFILE)
    assert not confirmation.scope_changed
    assert confirmation.batch.state == DiscographyBatchState.completed
