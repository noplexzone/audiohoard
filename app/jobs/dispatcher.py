from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Coroutine
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import get_session_factory, run_with_sqlite_lock_retry
from app.models.job import Job, JobStatus

logger = logging.getLogger(__name__)


class JobNotFoundError(LookupError):
    def __init__(self, job_id: int) -> None:
        self.job_id = job_id
        super().__init__(f"Job {job_id} not found")


class JobStateError(ValueError):
    def __init__(self, job_id: int, status: JobStatus) -> None:
        self.job_id = job_id
        self.status = status
        super().__init__(f"Job in {status.value} state cannot be changed")


class JobNotRetryableError(JobStateError):
    pass


def _default_runner(job_id: int) -> Coroutine[Any, Any, None]:  # pragma: no cover
    from app.jobs.runner import run_job

    return run_job(job_id)


_current_acquisition_permit: ContextVar[AcquisitionPermit | None] = ContextVar(
    "current_acquisition_permit", default=None
)


def current_acquisition_permit() -> AcquisitionPermit | None:
    return _current_acquisition_permit.get()


class AcquisitionPermit:
    """Task-local lease over the dispatcher's runtime-resizable limit."""

    def __init__(self, dispatcher: JobDispatcher) -> None:
        self._dispatcher = dispatcher
        self._held = False

    async def acquire(self) -> None:
        if self._held:
            return
        await self._dispatcher._acquire_slot()
        self._held = True

    async def yield_permit(self) -> None:
        if not self._held:
            return
        self._held = False
        await self._dispatcher._release_slot()

    async def release(self) -> None:
        await self.yield_permit()


class JobDispatcher:
    def __init__(
        self,
        runner: Callable[[int], Coroutine[Any, Any, None]] | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        max_concurrent_jobs: int | None = None,
        max_inflight_jobs: int | None = None,
    ) -> None:
        if max_inflight_jobs is not None and max_inflight_jobs < 1:
            raise ValueError("In-flight acquisition limit must be at least 1")
        self._runner: Callable[[int], Coroutine[Any, Any, None]] = (
            runner if runner is not None else _default_runner
        )
        self._session_factory = session_factory
        self._max_concurrent_jobs = max_concurrent_jobs
        self._max_inflight_jobs = max_inflight_jobs
        self._derived_inflight_limit = max_inflight_jobs is None
        self._active_jobs = 0
        self._inflight_jobs = 0
        self._limit_condition = asyncio.Condition()
        self._inflight_condition = asyncio.Condition()
        self._tasks: dict[int, asyncio.Task[None]] = {}
        self._watchdog_task: asyncio.Task[None] | None = None
        self._cleanup_task: asyncio.Task[None] | None = None

    def _factory(self) -> async_sessionmaker[AsyncSession]:
        return self._session_factory or get_session_factory()

    def _configured_limit(self) -> int:
        if self._max_concurrent_jobs is None:
            from app.config import get_settings

            self._max_concurrent_jobs = get_settings().max_concurrent_jobs
        return self._max_concurrent_jobs

    def _configured_inflight_limit(self) -> int:
        if self._derived_inflight_limit:
            local_limit = self._configured_limit()
            return max(local_limit, local_limit * 5)
        assert self._max_inflight_jobs is not None
        return self._max_inflight_jobs

    @property
    def active_jobs(self) -> int:
        return self._active_jobs

    @property
    def inflight_jobs(self) -> int:
        return self._inflight_jobs

    @property
    def max_inflight_jobs(self) -> int:
        return self._configured_inflight_limit()

    async def set_max_concurrent_jobs(self, value: int) -> None:
        if not 1 <= value <= 16:
            raise ValueError("Parallel acquisition limit must be between 1 and 16")
        async with self._limit_condition:
            self._max_concurrent_jobs = value
            self._limit_condition.notify_all()
        if self._derived_inflight_limit:
            async with self._inflight_condition:
                self._inflight_condition.notify_all()

    async def _acquire_slot(self) -> None:
        async with self._limit_condition:
            await self._limit_condition.wait_for(
                lambda: self._active_jobs < self._configured_limit()
            )
            self._active_jobs += 1

    async def _release_slot(self) -> None:
        async with self._limit_condition:
            self._active_jobs -= 1
            self._limit_condition.notify_all()

    async def _acquire_inflight(self) -> None:
        async with self._inflight_condition:
            await self._inflight_condition.wait_for(
                lambda: self._inflight_jobs < self._configured_inflight_limit()
            )
            self._inflight_jobs += 1

    async def _release_inflight(self) -> None:
        async with self._inflight_condition:
            self._inflight_jobs -= 1
            self._inflight_condition.notify_all()

    async def _run_with_limit(self, job_id: int) -> None:
        await self._acquire_inflight()
        permit = AcquisitionPermit(self)
        token: Any = None
        try:
            await permit.acquire()
            token = _current_acquisition_permit.set(permit)
            await self._runner(job_id)
        finally:
            if token is not None:
                _current_acquisition_permit.reset(token)
            try:
                await permit.release()
            finally:
                await self._release_inflight()

    async def dispatch(self, job_id: int) -> asyncio.Task[None]:
        existing = self._tasks.get(job_id)
        if existing is not None and not existing.done():
            return existing

        task = asyncio.create_task(self._run_with_limit(job_id), name=f"job-{job_id}")

        def _remove(done_task: asyncio.Task[None]) -> None:
            if self._tasks.get(job_id) is done_task:
                del self._tasks[job_id]

        def _log_exception(done_task: asyncio.Task[None]) -> None:
            if done_task.cancelled():
                return
            exc = done_task.exception()
            if exc is not None:
                logger.error("Job %d task raised unhandled exception: %s", job_id, exc)

        task.add_done_callback(_remove)
        task.add_done_callback(_log_exception)
        self._tasks[job_id] = task
        return task

    def cancel(self, job_id: int) -> bool:
        task = self._tasks.get(job_id)
        if task is None or task.done():
            return False
        task.cancel()
        return True

    async def cancel_job(self, job_id: int) -> None:
        async with self._factory()() as db:
            job = await db.get(Job, job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            if job.status not in {JobStatus.pending, JobStatus.running}:
                raise JobStateError(job_id, job.status)
            from app.jobs.runner import _job_error_result, _persist_job_envelope

            transitioned = await _persist_job_envelope(
                db,
                job_id,
                expected_statuses={JobStatus.pending, JobStatus.running},
                status=JobStatus.cancelled,
                result_json=_job_error_result("cancelled", "job", retryable=True),
                cancel_active_tracks=True,
            )
            if not transitioned:
                await db.refresh(job)
                raise JobStateError(job_id, job.status)
        self.cancel(job_id)

    async def retry(
        self,
        job_id: int,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> asyncio.Task[None]:
        factory = session_factory or self._factory()
        async with factory() as db:
            job = await db.get(Job, job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            if job.status not in {
                JobStatus.failed,
                JobStatus.partial,
                JobStatus.cancelled,
            }:
                raise JobNotRetryableError(job_id, job.status)
            job.status = JobStatus.pending
            job.queue_hidden = False
            job.result_json = None
            job.updated_at = datetime.now(UTC)
            await db.commit()
        return await self.dispatch(job_id)

    async def remove(self, job_id: int) -> None:
        async with self._factory()() as db:
            job = await db.get(Job, job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            if job.status in {JobStatus.pending, JobStatus.running}:
                raise JobStateError(job_id, job.status)
            job.queue_hidden = True
            await db.commit()

    async def clear(self, statuses: set[JobStatus]) -> int:
        terminal = {
            JobStatus.done,
            JobStatus.failed,
            JobStatus.partial,
            JobStatus.cancelled,
        }
        if not statuses or not statuses.issubset(terminal):
            raise ValueError("Only terminal job states can be cleared")
        async with self._factory()() as db:
            jobs = list(
                (
                    await db.scalars(
                        select(Job).where(
                            Job.status.in_(statuses),
                            Job.queue_hidden.is_(False),
                        )
                    )
                ).all()
            )
            for job in jobs:
                job.queue_hidden = True
            await db.commit()
            return len(jobs)

    async def recover(
        self,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> list[int]:
        factory = session_factory or self._factory()
        recovered_ids: list[int] = []
        async with factory() as db:

            async def recover_jobs() -> None:
                attempt_ids: list[int] = []
                rows = (
                    await db.execute(
                        select(
                            Job.id,
                            Job.status,
                            Job.result_json,
                            Job.updated_at,
                        ).where(Job.status.in_([JobStatus.pending, JobStatus.running]))
                    )
                ).all()
                for job_id, status, result_json, updated_at in rows:
                    result_match = (
                        Job.result_json.is_(None)
                        if result_json is None
                        else Job.result_json == result_json
                    )
                    if status == JobStatus.pending:
                        result = await db.execute(
                            update(Job)
                            .where(
                                Job.id == job_id,
                                Job.status == JobStatus.pending,
                                func.julianday(Job.updated_at) == func.julianday(updated_at),
                                result_match,
                            )
                            .values(updated_at=datetime.now(UTC))
                            .execution_options(synchronize_session=False)
                        )
                        if isinstance(result, CursorResult) and result.rowcount == 1:
                            attempt_ids.append(job_id)
                        continue
                    try:
                        payload = json.loads(result_json) if result_json else {}
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                    if not isinstance(payload, dict):
                        payload = {}
                    payload["recovery"] = {
                        "code": "interrupted_by_restart",
                        "retryable": True,
                    }
                    result = await db.execute(
                        update(Job)
                        .where(
                            Job.id == job_id,
                            Job.status == JobStatus.running,
                            func.julianday(Job.updated_at) == func.julianday(updated_at),
                        )
                        .values(
                            status=JobStatus.pending,
                            result_json=json.dumps(payload),
                            updated_at=datetime.now(UTC),
                        )
                        .execution_options(synchronize_session=False)
                    )
                    if isinstance(result, CursorResult) and result.rowcount == 1:
                        attempt_ids.append(job_id)
                await db.commit()
                recovered_ids[:] = attempt_ids

            await run_with_sqlite_lock_retry(db, recover_jobs)
        for job_id in recovered_ids:
            await self.dispatch(job_id)
        return recovered_ids

    async def _watchdog_tick(self, threshold_seconds: int) -> None:
        threshold_dt = datetime.now(UTC) - timedelta(seconds=threshold_seconds)
        committed_actions: list[tuple[int, bool]] = []
        async with self._factory()() as db:

            async def claim_stale_jobs() -> None:
                attempt_actions: list[tuple[int, bool]] = []
                rows = (
                    await db.execute(
                        select(
                            Job.id,
                            Job.status,
                            Job.result_json,
                            Job.updated_at,
                        ).where(
                            Job.status.in_([JobStatus.pending, JobStatus.running]),
                            Job.updated_at < threshold_dt,
                        )
                    )
                ).all()
                for job_id, status, result_json, updated_at in rows:
                    live = self._tasks.get(job_id)
                    if live is not None and not live.done():
                        continue
                    try:
                        current: dict[str, Any] = json.loads(result_json) if result_json else {}
                    except (json.JSONDecodeError, TypeError):
                        current = {}
                    recurrent = "watchdog_recovery" in current
                    if recurrent:
                        next_status = JobStatus.failed
                        next_result = json.dumps(
                            {
                                "error": {
                                    "code": "dispatch_lost",
                                    "operation": "watchdog",
                                    "retryable": False,
                                }
                            }
                        )
                    else:
                        next_status = JobStatus.pending if status == JobStatus.running else status
                        current["watchdog_recovery"] = {"attempt": 1}
                        next_result = json.dumps(current)
                    result_match = (
                        Job.result_json.is_(None)
                        if result_json is None
                        else Job.result_json == result_json
                    )
                    result = await db.execute(
                        update(Job)
                        .where(
                            Job.id == job_id,
                            Job.status == status,
                            func.julianday(Job.updated_at) == func.julianday(updated_at),
                            Job.updated_at < threshold_dt,
                            result_match,
                        )
                        .values(
                            status=next_status,
                            result_json=next_result,
                            updated_at=datetime.now(UTC),
                        )
                        .execution_options(synchronize_session=False)
                    )
                    if isinstance(result, CursorResult) and result.rowcount == 1:
                        attempt_actions.append((job_id, recurrent))
                await db.commit()
                committed_actions[:] = attempt_actions

            await run_with_sqlite_lock_retry(db, claim_stale_jobs)
        for job_id, recurrent in committed_actions:
            if recurrent:
                logger.error(
                    "Job %d lost after watchdog recovery attempt; marking dispatch_lost",
                    job_id,
                )
                continue
            logger.warning("Job %d stale with no live task; dispatching watchdog recovery", job_id)
            await self.dispatch(job_id)

    async def _watchdog_loop(self, threshold_seconds: int, interval_seconds: int) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await self._watchdog_tick(threshold_seconds)
            except Exception:
                logger.exception("Watchdog tick failed")

    async def _cleanup_reconcile_tick(self) -> None:
        from app.config import get_settings
        from app.services.acquisition_cleanup import cleanup_terminal_acquisitions
        from app.settings_service import build_effective_settings

        factory = self._factory()
        async with factory() as db:
            settings = await build_effective_settings(db, get_settings())
        await cleanup_terminal_acquisitions(
            factory,
            slskd_url=settings.slskd_url,
            slskd_api_key=settings.slskd_api_key,
            slskd_complete_root=settings.slskd_complete_root,
            slskd_incomplete_root=settings.slskd_incomplete_root,
            partial_minimum_age=timedelta(seconds=settings.slskd_directory_sweep_min_age_seconds),
        )
        from app.services.acquisition_cleanup import (
            cleanup_imported_sources,
            pending_imported_source_cleanups,
            prune_orphaned_terminal_records,
        )

        async with factory() as db:
            await prune_orphaned_terminal_records(
                db, batch_size=1, commit_batches=True, max_batches=100
            )

        async with factory() as db:
            pending_cleanups = await pending_imported_source_cleanups(db)
        await cleanup_imported_sources(
            pending_cleanups,
            complete_root=settings.slskd_complete_root,
            incomplete_root=settings.slskd_incomplete_root,
        )
        sweep_roots = tuple(
            root
            for root in (settings.slskd_complete_root, settings.slskd_incomplete_root)
            if root is not None
        )
        if sweep_roots and settings.slskd_configured:
            from app.services.acquisition_cleanup import sweep_empty_slskd_directories
            from app.sources.slskd import SlskdAdapter

            await sweep_empty_slskd_directories(
                SlskdAdapter(settings.slskd_url, settings.slskd_api_key),
                sweep_roots,
                minimum_age=timedelta(seconds=settings.slskd_directory_sweep_min_age_seconds),
            )

    async def _cleanup_reconcile_loop(self, interval_seconds: int) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await self._cleanup_reconcile_tick()
            except Exception:
                logger.exception("Periodic terminal acquisition cleanup failed")

    async def start_cleanup_reconciler(self, interval_seconds: int = 300) -> None:
        if self._cleanup_task is not None and not self._cleanup_task.done():
            return
        self._cleanup_task = asyncio.create_task(
            self._cleanup_reconcile_loop(interval_seconds),
            name="terminal-cleanup-reconciler",
        )

    async def stop_cleanup_reconciler(self) -> None:
        if self._cleanup_task is not None and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
        self._cleanup_task = None

    async def start_watchdog(
        self, threshold_seconds: int = 300, interval_seconds: int = 60
    ) -> None:
        if self._watchdog_task is not None and not self._watchdog_task.done():
            return
        self._watchdog_task = asyncio.create_task(
            self._watchdog_loop(threshold_seconds, interval_seconds),
            name="job-watchdog",
        )

    async def stop_watchdog(self) -> None:
        if self._watchdog_task is not None and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watchdog_task
        self._watchdog_task = None

    async def shutdown(self) -> None:
        await self.stop_cleanup_reconciler()
        await self.stop_watchdog()
        tasks = list(self._tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


job_dispatcher = JobDispatcher()
