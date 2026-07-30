from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Coroutine
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import get_session_factory
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


class _AcquisitionSlotLease:
    def __init__(self, dispatcher: JobDispatcher) -> None:
        self._dispatcher = dispatcher
        self.held = False

    async def acquire(self) -> bool:
        if self.held:
            return False
        await self._dispatcher._acquire_slot()
        self.held = True
        return True

    async def release(self) -> bool:
        if not self.held:
            return False
        await self._dispatcher._release_slot()
        self.held = False
        return True


_current_acquisition_lease: ContextVar[_AcquisitionSlotLease | None] = ContextVar(
    "current_acquisition_lease", default=None
)


async def release_current_acquisition_slot() -> bool:
    lease = _current_acquisition_lease.get()
    return await lease.release() if lease is not None else False


async def reacquire_current_acquisition_slot() -> bool:
    lease = _current_acquisition_lease.get()
    return await lease.acquire() if lease is not None else False


def _default_runner(job_id: int) -> Coroutine[Any, Any, None]:  # pragma: no cover
    from app.jobs.runner import run_job

    return run_job(job_id)


class JobDispatcher:
    def __init__(
        self,
        runner: Callable[[int], Coroutine[Any, Any, None]] | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        max_concurrent_jobs: int | None = None,
    ) -> None:
        self._runner: Callable[[int], Coroutine[Any, Any, None]] = (
            runner if runner is not None else _default_runner
        )
        self._session_factory = session_factory
        self._max_concurrent_jobs = max_concurrent_jobs
        self._active_jobs = 0
        self._limit_condition = asyncio.Condition()
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

    async def set_max_concurrent_jobs(self, value: int) -> None:
        if not 1 <= value <= 16:
            raise ValueError("Parallel acquisition limit must be between 1 and 16")
        async with self._limit_condition:
            self._max_concurrent_jobs = value
            self._limit_condition.notify_all()

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

    async def _run_with_limit(self, job_id: int) -> None:
        lease = _AcquisitionSlotLease(self)
        await lease.acquire()
        token = _current_acquisition_lease.set(lease)
        try:
            await self._runner(job_id)
        finally:
            _current_acquisition_lease.reset(token)
            await lease.release()

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
            if not self.cancel(job_id):
                job.status = JobStatus.cancelled
                job.result_json = json.dumps(
                    {"error": {"code": "cancelled", "operation": "job", "retryable": True}}
                )
                job.updated_at = datetime.now(UTC)
                await db.commit()

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
        try:
            from app.config import get_settings
            from app.services.acquisition_cleanup import cleanup_terminal_acquisitions
            from app.settings_service import build_effective_settings

            async with factory() as cleanup_db:
                settings = await build_effective_settings(cleanup_db, get_settings())
            await cleanup_terminal_acquisitions(
                factory,
                slskd_url=settings.slskd_url,
                slskd_api_key=settings.slskd_api_key,
            )
        except Exception:
            logger.exception("Startup terminal acquisition cleanup failed")
        async with factory() as db:
            result = await db.execute(
                select(Job).where(Job.status.in_([JobStatus.pending, JobStatus.running]))
            )
            jobs = list(result.scalars().all())
            for job in jobs:
                if job.status == JobStatus.running:
                    job.status = JobStatus.pending
                    try:
                        payload = json.loads(job.result_json) if job.result_json else {}
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                    if not isinstance(payload, dict):
                        payload = {}
                    payload["recovery"] = {
                        "code": "interrupted_by_restart",
                        "retryable": True,
                    }
                    job.result_json = json.dumps(payload)
                    job.updated_at = datetime.now(UTC)
            await db.commit()
        for job in jobs:
            await self.dispatch(job.id)
        return [job.id for job in jobs]

    async def _watchdog_tick(self, threshold_seconds: int) -> None:
        threshold_dt = datetime.now(UTC) - timedelta(seconds=threshold_seconds)
        to_dispatch: list[int] = []
        async with self._factory()() as db:
            result = await db.execute(
                select(Job).where(
                    Job.status.in_([JobStatus.pending, JobStatus.running]),
                    Job.updated_at < threshold_dt,
                )
            )
            jobs = list(result.scalars().all())
            for job in jobs:
                live = self._tasks.get(job.id)
                if live is not None and not live.done():
                    continue
                try:
                    current: dict[str, Any] = (
                        json.loads(job.result_json) if job.result_json else {}
                    )
                except (json.JSONDecodeError, TypeError):
                    current = {}
                if "watchdog_recovery" in current:
                    job.status = JobStatus.failed
                    job.result_json = json.dumps(
                        {
                            "error": {
                                "code": "dispatch_lost",
                                "operation": "watchdog",
                                "retryable": False,
                            }
                        }
                    )
                    job.updated_at = datetime.now(UTC)
                    logger.error(
                        "Job %d lost after watchdog recovery attempt; marking dispatch_lost",
                        job.id,
                    )
                else:
                    if job.status == JobStatus.running:
                        job.status = JobStatus.pending
                    current["watchdog_recovery"] = {"attempt": 1}
                    job.result_json = json.dumps(current)
                    job.updated_at = datetime.now(UTC)
                    to_dispatch.append(job.id)
                    logger.warning(
                        "Job %d stale with no live task; dispatching watchdog recovery", job.id
                    )
            await db.commit()
        for job_id in to_dispatch:
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
