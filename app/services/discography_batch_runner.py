from __future__ import annotations

import asyncio
import inspect
import logging
import re
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from sqlalchemy import exists, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.database import is_sqlite_database_locked
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumProvider
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchJobRole,
    DiscographyBatchState,
)
from app.models.job import Job, JobStatus
from app.services.catalog import DiscographyLeaseLostError, project_catalog_album_queue_targets
from app.services.catalog_manifest import catalog_manifest_issue
from app.services.release_admission import (
    ReleaseRootAdmissionStatus,
    materialize_batch_release_root_job,
)
from app.settings_service import QualityProfile, get_runtime_settings

logger = logging.getLogger(__name__)

Dispatcher = Callable[[int], Awaitable[object] | object]
Hydrator = Callable[[int, str], Awaitable[None]]
_ACTIVE_BATCHES = (DiscographyBatchState.queued, DiscographyBatchState.running)
_WORKING_ITEMS = (DiscographyBatchItemState.hydrating, DiscographyBatchItemState.expanding)
_TERMINAL_ITEMS = (
    DiscographyBatchItemState.complete,
    DiscographyBatchItemState.skipped,
    DiscographyBatchItemState.failed,
    DiscographyBatchItemState.cancelled,
)
_RETRYABLE_HYDRATION_REASONS = {
    "catalog_manifest_missing",
    "catalog_manifest_incomplete",
    "catalog_manifest_overfull",
    "catalog_manifest_invalid_positions",
}


def _clean_error(exc: BaseException) -> str:
    value = re.sub(r"https?://\S+", "[provider]", str(exc)).replace("\n", " ").strip()
    return (value or exc.__class__.__name__)[:500]


class DiscographyBatchRunner:
    """Serial, wakeable, bounded materializer for durable discography batches."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        dispatcher: Dispatcher,
        hydration_callback: Hydrator | None = None,
        quality_profile: QualityProfile | None = None,
        library_root: Path | None = None,
        interval_seconds: float = 2.0,
        lease_seconds: int = 300,
    ) -> None:
        self._session_factory = session_factory
        self._dispatcher = dispatcher
        self._hydrator = hydration_callback or self._missing_hydrator
        self._quality_profile = quality_profile
        self._library_root = library_root
        self._interval_seconds = interval_seconds
        self._lease = timedelta(seconds=max(30, min(lease_seconds, 3600)))
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._initial_cycle_complete = asyncio.Event()

    async def _missing_hydrator(self, _item_id: int, _lease_token: str) -> None:
        raise RuntimeError("no safe metadata hydration callback is configured")

    async def start(self, *, wait_for_initial_cycle: bool = False) -> None:
        if self._task is None or self._task.done():
            self._initial_cycle_complete.clear()
            self._task = asyncio.create_task(self._loop(), name="discography-batch-runner")
        if wait_for_initial_cycle:
            await self._initial_cycle_complete.wait()

    def wake(self) -> None:
        self._wake.set()

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                while await self.run_once():
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("discography batch runner iteration failed; will retry")
            finally:
                self._initial_cycle_complete.set()
            self._wake.clear()
            with suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._interval_seconds)

    async def run_once(self) -> bool:
        """Recover work, inspect at most 25 waiting items, and create at most 25 jobs."""
        now = datetime.now(UTC)
        await self._recover_expired(now)
        claimed = await self._claim_pending(now)
        if claimed is None:
            waiting_ids = await self._next_waiting_items()
            if waiting_ids:
                changed = False
                for waiting_id in waiting_ids:
                    fallback_ids = await self._repair_partial_release_fallbacks(waiting_id)
                    for job_id in fallback_ids:
                        if not await self._may_dispatch_waiting_fallback(job_id):
                            continue
                        dispatch_result = self._dispatcher(job_id)
                        if inspect.isawaitable(dispatch_result):
                            await dispatch_result
                    if fallback_ids:
                        changed = True
                    async with self._session_factory() as db:
                        item_changed = await self._reconcile_item(db, waiting_id, attempted=True)
                        if item_changed:
                            await self._finish_batch_for_item(db, waiting_id)
                            changed = True
                        else:
                            # Persist a restart-safe rotation marker without claiming a lease.
                            # This keeps a bounded pass from repeatedly selecting the same
                            # unchanged active jobs while later waiting rows starve.
                            await db.execute(
                                update(DiscographyBatchItem)
                                .where(
                                    DiscographyBatchItem.id == waiting_id,
                                    DiscographyBatchItem.state
                                    == DiscographyBatchItemState.waiting,
                                )
                                .values(updated_at=datetime.now(UTC))
                            )
                        await db.commit()
                return changed
            return await self._reconcile_idle_batch()
        item_id, requires_hydration, lease_token = claimed
        try:
            if requires_hydration:
                # The claim transaction is committed and its session is closed before provider I/O.
                await self._hydrator(item_id, lease_token)
                async with self._session_factory() as db:
                    result = await db.execute(
                        update(DiscographyBatchItem)
                        .where(
                            DiscographyBatchItem.id == item_id,
                            DiscographyBatchItem.state == DiscographyBatchItemState.hydrating,
                            DiscographyBatchItem.lease_token == lease_token,
                            DiscographyBatchItem.batch_id.in_(
                                select(DiscographyBatch.id).where(
                                    DiscographyBatch.state.in_(_ACTIVE_BATCHES)
                                )
                            ),
                        )
                        .values(
                            state=DiscographyBatchItemState.expanding,
                            heartbeat_at=datetime.now(UTC),
                            reason_code=None,
                        )
                    )
                    if not isinstance(result, CursorResult) or result.rowcount != 1:
                        await db.rollback()
                        return True
                    await db.commit()

            async with self._session_factory() as db:
                item = await self._owned_item(
                    db, item_id, lease_token, (DiscographyBatchItemState.expanding,)
                )
                if item is None:
                    return True
                if item.catalog_album_id is None:
                    item.state = DiscographyBatchItemState.skipped
                    item.reason_code = item.reason_code or "catalog_release_unbound"
                    item.lease_token = None
                    item.heartbeat_at = None
                    await self._finish_batch(db, item.batch_id)
                    await db.commit()
                    return True
                album = await db.get(CatalogAlbum, item.catalog_album_id)
                if album is None:
                    item.state = DiscographyBatchItemState.skipped
                    item.reason_code = "catalog_release_unbound"
                    item.lease_token = None
                    item.heartbeat_at = None
                    await self._finish_batch(db, item.batch_id)
                    await db.commit()
                    return True
                renotify_observed = item.reason_code == "resume_pending_dispatch"
                profile = self._quality_profile or (await get_runtime_settings(db)).quality_profile
                outcome = await materialize_batch_release_root_job(
                    db,
                    item_id,
                    execution_generation=item.execution_generation,
                    batch_lease_token=lease_token,
                    quality_profile=profile,
                    library_root=self._library_root,
                )
                root_dispatch_ids = (
                    (outcome.job_id,)
                    if outcome.job_id is not None
                    and (
                        outcome.status == ReleaseRootAdmissionStatus.created
                        or (
                            renotify_observed
                            and outcome.status == ReleaseRootAdmissionStatus.observed
                        )
                    )
                    else ()
                )
                fallback_dispatch_ids: tuple[int, ...] = ()
                if renotify_observed and outcome.job_id is not None:
                    root_status = await db.scalar(
                        select(Job.status).where(Job.id == outcome.job_id)
                    )
                    if root_status == JobStatus.partial:
                        fallback_dispatch_ids = tuple(
                            int(value)
                            for value in (
                                await db.scalars(
                                    select(DiscographyBatchItemJob.job_id)
                                    .join(Job, Job.id == DiscographyBatchItemJob.job_id)
                                    .where(
                                        DiscographyBatchItemJob.item_id == item.id,
                                        DiscographyBatchItemJob.generation
                                        == item.execution_generation,
                                        DiscographyBatchItemJob.role
                                        == DiscographyBatchJobRole.track_fallback,
                                        Job.status == JobStatus.pending,
                                    )
                                    .order_by(DiscographyBatchItemJob.id)
                                )
                            ).all()
                        )
                dispatch_ids = tuple(dict.fromkeys((*root_dispatch_ids, *fallback_dispatch_ids)))

            # Expansion commits links and jobs. Dispatch is deliberately
            # after the session closes. Pending observed jobs are included so
            # pause/resume and restart can re-notify durable work without
            # consuming a new execution generation.
            for job_id in dispatch_ids:
                if not await self._may_dispatch(item_id, lease_token, job_id):
                    continue
                dispatch_result = self._dispatcher(job_id)
                if inspect.isawaitable(dispatch_result):
                    await dispatch_result

            async with self._session_factory() as db:
                reconciled = await self._reconcile_item(
                    db, item_id, attempted=True, expected_lease_token=lease_token
                )
                if reconciled:
                    await self._finish_batch_for_item(db, item_id)
                await db.commit()
        except asyncio.CancelledError:
            raise
        except DiscographyLeaseLostError:
            return True
        except Exception as exc:
            async with self._session_factory() as db:
                item = await self._owned_item(db, item_id, lease_token, _WORKING_ITEMS)
                if item is not None:
                    item.state = DiscographyBatchItemState.failed
                    item.reason_code = (
                        "hydration_failed" if requires_hydration else "expansion_failed"
                    )
                    item.error_detail = _clean_error(exc)
                    item.lease_token = None
                    item.heartbeat_at = None
                    item.completed_at = datetime.now(UTC)
                    await self._finish_batch(db, item.batch_id)
                    await db.commit()
            logger.warning("discography batch item %s failed: %s", item_id, _clean_error(exc))
        return True

    async def _recover_expired(self, now: datetime) -> None:
        cutoff = now - self._lease
        async with self._session_factory() as db:
            await db.execute(
                update(DiscographyBatchItem)
                .where(
                    DiscographyBatchItem.state.in_(_WORKING_ITEMS),
                    DiscographyBatchItem.heartbeat_at < cutoff,
                    DiscographyBatchItem.batch_id.in_(
                        select(DiscographyBatch.id).where(
                            DiscographyBatch.state.in_(_ACTIVE_BATCHES)
                        )
                    ),
                )
                .values(
                    state=DiscographyBatchItemState.pending,
                    lease_token=None,
                    heartbeat_at=None,
                )
            )
            await db.commit()

    async def _next_waiting_items(self) -> list[int]:
        async with self._session_factory() as db:
            values = await db.scalars(
                select(DiscographyBatchItem.id)
                .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
                .where(
                    DiscographyBatch.state.in_(_ACTIVE_BATCHES),
                    DiscographyBatchItem.state == DiscographyBatchItemState.waiting,
                )
                .order_by(DiscographyBatchItem.updated_at, DiscographyBatchItem.id)
                .limit(25)
            )
            return [int(value) for value in values]

    async def _repair_partial_release_fallbacks(self, item_id: int) -> tuple[int, ...]:
        """Repair the post-terminal gap without repeating release-level provider work."""
        async with self._session_factory() as db:
            item = await db.scalar(
                select(DiscographyBatchItem)
                .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
                .where(
                    DiscographyBatchItem.id == item_id,
                    DiscographyBatchItem.state == DiscographyBatchItemState.waiting,
                    DiscographyBatch.state.in_(_ACTIVE_BATCHES),
                )
                .options(
                    selectinload(DiscographyBatchItem.catalog_album).selectinload(
                        CatalogAlbum.tracks
                    )
                )
            )
            if item is None or item.catalog_album is None:
                return ()
            rows = (
                await db.execute(
                    select(DiscographyBatchItemJob, Job)
                    .outerjoin(Job, Job.id == DiscographyBatchItemJob.job_id)
                    .where(
                        DiscographyBatchItemJob.item_id == item.id,
                        DiscographyBatchItemJob.generation == item.execution_generation,
                    )
                    .order_by(DiscographyBatchItemJob.id)
                )
            ).all()
            root_rows = [
                (link, job)
                for link, job in rows
                if link.role == DiscographyBatchJobRole.release_root
            ]
            fallback_rows = [
                (link, job)
                for link, job in rows
                if link.role == DiscographyBatchJobRole.track_fallback
            ]
            if len(root_rows) != 1:
                return ()
            root_link, root_job = root_rows[0]
            album = item.catalog_album
            if (
                root_job is None
                or root_link.catalog_track_id is not None
                or root_job.catalog_album_id != album.id
                or root_job.catalog_track_id is not None
                or root_job.status != JobStatus.partial
            ):
                return ()
            album_track_ids = {track.id for track in album.tracks}
            fallback_jobs: dict[int, Job] = {}
            for link, job in fallback_rows:
                if (
                    job is None
                    or link.catalog_track_id is None
                    or link.catalog_track_id not in album_track_ids
                    or job.catalog_album_id != album.id
                    or job.catalog_track_id != link.catalog_track_id
                ):
                    return ()
                fallback_jobs[int(link.catalog_track_id)] = job
            profile = self._quality_profile or (await get_runtime_settings(db)).quality_profile
            projection = (
                await project_catalog_album_queue_targets(
                    db,
                    [album.id],
                    quality_profile=profile,
                    library_root=self._library_root,
                )
            )[album.id]
            targets = set(projection.target_track_ids)
            runtime = await get_runtime_settings(db)
            max_attempts = runtime.max_partial_attempts
            from app.jobs.runner import _ParentTerminalEvidence, _spawn_continuation_jobs

            def terminal_evidence(job: Job) -> _ParentTerminalEvidence:
                return _ParentTerminalEvidence(
                    status=job.status,
                    result_json=job.result_json,
                    updated_at=job.updated_at,
                    execution_token=job.execution_token,
                )

            repair_requests: list[tuple[int, list[int], _ParentTerminalEvidence]] = []
            root_targets = sorted(targets - set(fallback_jobs))
            if root_targets and root_job.partial_attempt < max_attempts:
                repair_requests.append((root_job.id, root_targets, terminal_evidence(root_job)))
            for track_id, fallback_job in sorted(fallback_jobs.items()):
                if (
                    track_id in targets
                    and fallback_job.status == JobStatus.partial
                    and fallback_job.partial_attempt < max_attempts
                ):
                    repair_requests.append(
                        (fallback_job.id, [track_id], terminal_evidence(fallback_job))
                    )
            if not repair_requests:
                return ()

            committed: list[int] = []
            for parent_id, track_ids, evidence in repair_requests:
                committed.extend(
                    await _spawn_continuation_jobs(
                        parent_id,
                        track_ids,
                        album.id,
                        db,
                        terminal_evidence=evidence,
                    )
                )
            return tuple(dict.fromkeys(committed))

    async def _may_dispatch_waiting_fallback(self, job_id: int) -> bool:
        async with self._session_factory() as db:
            return bool(
                await db.scalar(
                    select(DiscographyBatchItemJob.id)
                    .join(
                        DiscographyBatchItem,
                        DiscographyBatchItem.id == DiscographyBatchItemJob.item_id,
                    )
                    .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
                    .join(Job, Job.id == DiscographyBatchItemJob.job_id)
                    .where(
                        DiscographyBatchItemJob.job_id == job_id,
                        DiscographyBatchItemJob.generation
                        == DiscographyBatchItem.execution_generation,
                        DiscographyBatchItemJob.role == DiscographyBatchJobRole.track_fallback,
                        DiscographyBatchItem.state == DiscographyBatchItemState.waiting,
                        DiscographyBatch.state.in_(_ACTIVE_BATCHES),
                        Job.status == JobStatus.pending,
                    )
                )
            )

    async def _claim_pending(self, now: datetime) -> tuple[int, bool, str] | None:
        for attempt in range(6):
            try:
                async with self._session_factory() as db:
                    candidate = await db.scalar(
                        select(DiscographyBatchItem.id)
                        .join(
                            DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id
                        )
                        .where(
                            DiscographyBatch.state.in_(_ACTIVE_BATCHES),
                            DiscographyBatchItem.state == DiscographyBatchItemState.pending,
                        )
                        .order_by(DiscographyBatchItem.id)
                        .limit(1)
                    )
                    if candidate is None:
                        return None
                    token = uuid.uuid4().hex
                    reason = await db.scalar(
                        select(DiscographyBatchItem.reason_code).where(
                            DiscographyBatchItem.id == candidate
                        )
                    )
                    hydrating = reason in _RETRYABLE_HYDRATION_REASONS
                    claimed_state = (
                        DiscographyBatchItemState.hydrating
                        if hydrating
                        else DiscographyBatchItemState.expanding
                    )
                    result = await db.execute(
                        update(DiscographyBatchItem)
                        .where(
                            DiscographyBatchItem.id == candidate,
                            DiscographyBatchItem.state == DiscographyBatchItemState.pending,
                        )
                        .values(
                            state=claimed_state,
                            lease_token=token,
                            heartbeat_at=now,
                            started_at=now,
                            attempt_count=DiscographyBatchItem.attempt_count + 1,
                            error_detail=None,
                        )
                    )
                    if not isinstance(result, CursorResult) or result.rowcount != 1:
                        await db.rollback()
                        continue
                    item = await db.get(DiscographyBatchItem, candidate)
                    assert item is not None
                    await db.execute(
                        update(DiscographyBatch)
                        .where(
                            DiscographyBatch.id == item.batch_id,
                            DiscographyBatch.state == DiscographyBatchState.queued,
                        )
                        .values(state=DiscographyBatchState.running, started_at=now)
                    )
                    await db.commit()
                    return int(candidate), hydrating, token
            except Exception as exc:
                if not is_sqlite_database_locked(exc) or attempt == 5:
                    raise
                await asyncio.sleep(0.05 * (attempt + 1))
        return None

    async def _owned_item(
        self,
        db: AsyncSession,
        item_id: int,
        lease_token: str,
        states: tuple[DiscographyBatchItemState, ...],
    ) -> DiscographyBatchItem | None:
        return cast(
            DiscographyBatchItem | None,
            await db.scalar(
                select(DiscographyBatchItem)
                .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
                .where(
                    DiscographyBatchItem.id == item_id,
                    DiscographyBatchItem.lease_token == lease_token,
                    DiscographyBatchItem.state.in_(states),
                    DiscographyBatch.state.in_(_ACTIVE_BATCHES),
                )
            ),
        )

    async def _may_dispatch(self, item_id: int, lease_token: str, job_id: int) -> bool:
        async with self._session_factory() as db:
            row = (
                await db.execute(
                    select(DiscographyBatchItem, DiscographyBatch.state)
                    .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
                    .where(DiscographyBatchItem.id == item_id)
                )
            ).one_or_none()
            if row is None:
                return False
            item, batch_state = row
            if item.lease_token != lease_token or item.state not in (
                DiscographyBatchItemState.expanding,
                DiscographyBatchItemState.waiting,
            ):
                return False
            if batch_state == DiscographyBatchState.paused:
                item.state = DiscographyBatchItemState.pending
                item.lease_token = None
                item.heartbeat_at = None
                await db.commit()
                return False
            if batch_state not in _ACTIVE_BATCHES:
                return False
            return await db.scalar(select(Job.status).where(Job.id == job_id)) == JobStatus.pending

    async def _reconcile_idle_batch(self) -> bool:
        async with self._session_factory() as db:
            batch_id = await db.scalar(
                select(DiscographyBatch.id)
                .where(
                    DiscographyBatch.state.in_(_ACTIVE_BATCHES),
                    ~exists(
                        select(DiscographyBatchItem.id).where(
                            DiscographyBatchItem.batch_id == DiscographyBatch.id,
                            DiscographyBatchItem.state.not_in(_TERMINAL_ITEMS),
                        )
                    ),
                )
                .order_by(DiscographyBatch.id)
                .limit(1)
            )
            if batch_id is None:
                return False
            await self._finish_batch(db, int(batch_id))
            await db.commit()
            return True

    async def _reconcile_item(
        self,
        db: AsyncSession,
        item_id: int,
        *,
        attempted: bool,
        expected_lease_token: str | None = None,
    ) -> bool:
        filters = [
            DiscographyBatchItem.id == item_id,
            DiscographyBatch.state.in_(_ACTIVE_BATCHES),
        ]
        if expected_lease_token is not None:
            filters.extend(
                [
                    DiscographyBatchItem.lease_token == expected_lease_token,
                    DiscographyBatchItem.state.in_(_WORKING_ITEMS),
                ]
            )
        item = await db.scalar(
            select(DiscographyBatchItem)
            .join(DiscographyBatch, DiscographyBatch.id == DiscographyBatchItem.batch_id)
            .where(*filters)
            .options(
                selectinload(DiscographyBatchItem.catalog_album).selectinload(CatalogAlbum.tracks)
            )
        )
        if item is None:
            return False
        now = datetime.now(UTC)
        item.lease_token = None
        item.heartbeat_at = None
        if item.catalog_album is None:
            item.state = DiscographyBatchItemState.skipped
            item.reason_code = "catalog_release_unbound"
            item.completed_at = now
            return db.is_modified(item, include_collections=False)
        expected = max(item.expected_track_count or 0, item.catalog_album.track_count or 0) or None
        if item.provider_release_id is not None:
            provider_expected = await db.scalar(
                select(CatalogAlbumProvider.track_count).where(
                    CatalogAlbumProvider.id == item.provider_release_id
                )
            )
            expected = max(expected or 0, provider_expected or 0) or None
        issue = catalog_manifest_issue(item.catalog_album.tracks, expected)
        if issue is not None:
            item.state = (
                DiscographyBatchItemState.failed
                if attempted
                else DiscographyBatchItemState.pending
            )
            item.reason_code = {
                "catalog_tracks_empty": "catalog_manifest_missing",
                "catalog_tracks_incomplete": "catalog_manifest_incomplete",
                "catalog_tracks_overfull": "catalog_manifest_overfull",
                "catalog_tracks_invalid_positions": "catalog_manifest_invalid_positions",
            }[issue]
            return db.is_modified(item, include_collections=False)
        profile = self._quality_profile or (await get_runtime_settings(db)).quality_profile
        projection = (
            await project_catalog_album_queue_targets(
                db,
                [item.catalog_album.id],
                quality_profile=profile,
                library_root=self._library_root,
            )
        )[item.catalog_album.id]
        targets = set(projection.target_track_ids)
        generation_rows = (
            await db.execute(
                select(DiscographyBatchItemJob, Job)
                .outerjoin(Job, Job.id == DiscographyBatchItemJob.job_id)
                .where(
                    DiscographyBatchItemJob.item_id == item.id,
                    DiscographyBatchItemJob.generation == item.execution_generation,
                )
                .order_by(DiscographyBatchItemJob.id)
            )
        ).all()
        roles = {link.role for link, _job in generation_rows}
        role_aware = bool(
            roles
            & {
                DiscographyBatchJobRole.release_root,
                DiscographyBatchJobRole.track_fallback,
            }
        )
        item.target_count = len(targets)
        if role_aware:
            malformed = DiscographyBatchJobRole.legacy_track in roles
            root_rows = [
                (link, job)
                for link, job in generation_rows
                if link.role == DiscographyBatchJobRole.release_root
            ]
            fallback_rows = [
                (link, job)
                for link, job in generation_rows
                if link.role == DiscographyBatchJobRole.track_fallback
            ]
            if len(root_rows) != 1:
                malformed = True
            album_track_ids = {track.id for track in item.catalog_album.tracks}
            if root_rows:
                root_link, root_job = root_rows[0]
                malformed = malformed or (
                    root_job is None
                    or root_link.catalog_track_id is not None
                    or root_job.catalog_album_id != item.catalog_album.id
                    or root_job.catalog_track_id is not None
                )
                if (
                    root_job is not None
                    and root_job.status in (JobStatus.pending, JobStatus.running)
                    and fallback_rows
                ):
                    malformed = True
            else:
                root_job = None
            fallback_statuses: dict[int, JobStatus] = {}
            for link, job in fallback_rows:
                if (
                    job is None
                    or link.catalog_track_id is None
                    or link.catalog_track_id not in album_track_ids
                    or job.catalog_album_id != item.catalog_album.id
                    or job.catalog_track_id != link.catalog_track_id
                ):
                    malformed = True
                    continue
                fallback_statuses[int(link.catalog_track_id)] = job.status
            if malformed:
                item.state = DiscographyBatchItemState.failed
                item.reason_code = "malformed_batch_job_evidence"
                item.error_detail = "current-generation batch job evidence is malformed"
                item.active_count = 0
                item.estimated_job_count = len(targets)
                item.completed_at = now
            elif root_job is not None and root_job.status in (
                JobStatus.pending,
                JobStatus.running,
            ):
                item.state = DiscographyBatchItemState.waiting
                item.reason_code = "active_release_root"
                item.error_detail = None
                item.active_count = 1
                item.estimated_job_count = 0
                item.completed_at = None
            elif not targets:
                item.state = DiscographyBatchItemState.complete
                item.reason_code = "verified_complete"
                item.error_detail = None
                item.active_count = 0
                item.estimated_job_count = 0
                item.completed_at = now
            else:
                attempted_track_ids = set(fallback_statuses) & targets
                active = {
                    track_id
                    for track_id, status in fallback_statuses.items()
                    if track_id in targets and status in (JobStatus.pending, JobStatus.running)
                }
                item.active_count = len(active)
                item.estimated_job_count = len(targets - attempted_track_ids)
                if active:
                    item.state = DiscographyBatchItemState.waiting
                    item.reason_code = "active_jobs"
                    item.error_detail = None
                    item.completed_at = None
                else:
                    item.state = DiscographyBatchItemState.failed
                    item.reason_code = "current_generation_attempts_exhausted"
                    item.error_detail = (
                        "release-root and fallback work ended without verified library artifacts"
                    )
                    item.completed_at = now
        else:
            legacy_rows = [
                (link.catalog_track_id, job.status)
                for link, job in generation_rows
                if job is not None and link.catalog_track_id in targets
            ]
            attempted_track_ids = {
                int(track_id) for track_id, _status in legacy_rows if track_id is not None
            }
            active = {
                int(track_id)
                for track_id, status in legacy_rows
                if track_id is not None and status in (JobStatus.pending, JobStatus.running)
            }
            unattempted = targets - attempted_track_ids
            item.active_count = len(active)
            item.estimated_job_count = len(unattempted)
            if not targets:
                item.state = DiscographyBatchItemState.complete
                item.reason_code = "verified_complete"
                item.completed_at = now
            elif unattempted:
                item.state = DiscographyBatchItemState.pending
                item.reason_code = "targets_remain"
            elif active:
                item.state = DiscographyBatchItemState.waiting
                item.reason_code = "active_jobs"
            elif attempted_track_ids:
                item.state = DiscographyBatchItemState.failed
                item.reason_code = "current_generation_attempts_exhausted"
                item.error_detail = "acquisition work ended without verified library artifacts"
                item.completed_at = now
            elif attempted:
                item.state = DiscographyBatchItemState.failed
                item.reason_code = "targets_remain_without_active_jobs"
                item.error_detail = "acquisition work ended without verified library artifacts"
                item.completed_at = now
            else:
                item.state = DiscographyBatchItemState.pending
                item.reason_code = None
        return db.is_modified(item, include_collections=False)

    async def _finish_batch_for_item(self, db: AsyncSession, item_id: int) -> None:
        batch_id = await db.scalar(
            select(DiscographyBatchItem.batch_id).where(DiscographyBatchItem.id == item_id)
        )
        if batch_id is not None:
            await self._finish_batch(db, int(batch_id))

    async def _finish_batch(self, db: AsyncSession, batch_id: int) -> None:
        batch = await db.get(DiscographyBatch, batch_id)
        if batch is None or batch.state in {
            DiscographyBatchState.paused,
            DiscographyBatchState.cancelled,
        }:
            return
        states = list(
            (
                await db.scalars(
                    select(DiscographyBatchItem.state).where(
                        DiscographyBatchItem.batch_id == batch_id
                    )
                )
            ).all()
        )
        if all(state in _TERMINAL_ITEMS for state in states):
            failed = any(
                state in {DiscographyBatchItemState.failed, DiscographyBatchItemState.cancelled}
                for state in states
            )
            batch.state = (
                DiscographyBatchState.completed_with_failures
                if failed
                else DiscographyBatchState.completed
            )
            batch.completed_at = datetime.now(UTC)
        elif any(
            state in _WORKING_ITEMS or state == DiscographyBatchItemState.waiting
            for state in states
        ):
            batch.state = DiscographyBatchState.running
