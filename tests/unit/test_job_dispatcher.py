from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

os.environ.setdefault("SECRET_KEY", "test-secret")

from app.database import Base
from app.jobs.dispatcher import JobDispatcher, JobNotFoundError, JobNotRetryableError
from app.models.job import Job, JobStatus

_TEST_DB = "sqlite+aiosqlite:///:memory:"


@pytest.fixture
def runner() -> AsyncMock:
    mock = AsyncMock()

    async def side_effect(job_id: int) -> None:
        await asyncio.sleep(10)

    mock.side_effect = side_effect
    return mock


@pytest.fixture
def dispatcher(runner: AsyncMock) -> JobDispatcher:
    return JobDispatcher(runner=runner)


@pytest_asyncio.fixture
async def session_factory() -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine(_TEST_DB)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def _make_job(factory: async_sessionmaker[AsyncSession], *, status: JobStatus) -> Job:
    async with factory() as session:
        job = Job(source="youtube", query="test", status=status)
        session.add(job)
        await session.commit()
        await session.refresh(job)
        return job


class TestDispatchDeduplication:
    async def test_dispatch_creates_task(
        self, dispatcher: JobDispatcher, runner: AsyncMock
    ) -> None:
        task = await dispatcher.dispatch(1)
        assert task is not None
        assert not task.done()
        await dispatcher.shutdown()

    async def test_dispatch_same_id_returns_same_task(self, dispatcher: JobDispatcher) -> None:
        task_a = await dispatcher.dispatch(42)
        task_b = await dispatcher.dispatch(42)
        assert task_a is task_b
        await dispatcher.shutdown()

    async def test_dispatch_different_ids_return_different_tasks(
        self, dispatcher: JobDispatcher
    ) -> None:
        task_a = await dispatcher.dispatch(1)
        task_b = await dispatcher.dispatch(2)
        assert task_a is not task_b
        await dispatcher.shutdown()

    async def test_dispatch_after_done_creates_new_task(self, runner: AsyncMock) -> None:
        async def instant(job_id: int) -> None:
            pass

        dispatcher = JobDispatcher(runner=AsyncMock(side_effect=instant))
        task_a = await dispatcher.dispatch(99)
        await task_a
        task_b = await dispatcher.dispatch(99)
        assert task_a is not task_b
        await dispatcher.shutdown()


class TestCancel:
    async def test_cancel_active_task_returns_true(self, dispatcher: JobDispatcher) -> None:
        await dispatcher.dispatch(5)
        result = dispatcher.cancel(5)
        assert result is True

    async def test_cancel_absent_job_returns_false(self, dispatcher: JobDispatcher) -> None:
        result = dispatcher.cancel(999)
        assert result is False

    async def test_cancel_done_task_returns_false(self, runner: AsyncMock) -> None:
        async def instant(job_id: int) -> None:
            pass

        dispatcher = JobDispatcher(runner=AsyncMock(side_effect=instant))
        task = await dispatcher.dispatch(7)
        await task
        result = dispatcher.cancel(7)
        assert result is False
        await dispatcher.shutdown()

    async def test_cancelled_task_is_actually_cancelled(self, dispatcher: JobDispatcher) -> None:
        task = await dispatcher.dispatch(10)
        dispatcher.cancel(10)
        await asyncio.sleep(0)
        assert task.cancelled()


class TestShutdown:
    async def test_shutdown_cancels_all_active_tasks(self, dispatcher: JobDispatcher) -> None:
        task_a = await dispatcher.dispatch(1)
        task_b = await dispatcher.dispatch(2)
        await dispatcher.shutdown()
        assert task_a.cancelled()
        assert task_b.cancelled()

    async def test_shutdown_is_idempotent(self, dispatcher: JobDispatcher) -> None:
        await dispatcher.dispatch(1)
        await dispatcher.shutdown()
        await dispatcher.shutdown()


class TestRecovery:
    async def test_stale_running_jobs_reset_to_pending(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.running)
        await dispatcher.recover(session_factory)
        async with session_factory() as s:
            refreshed = await s.get(Job, job.id)
            assert refreshed is not None
            assert refreshed.status == JobStatus.pending
        await dispatcher.shutdown()

    async def test_running_job_dispatched_after_reset(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.running)
        await dispatcher.recover(session_factory)
        assert job.id in dispatcher._tasks
        await dispatcher.shutdown()

    async def test_pending_jobs_dispatched_on_recover(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.pending)
        await dispatcher.recover(session_factory)
        assert job.id in dispatcher._tasks
        await dispatcher.shutdown()

    async def test_idempotent_recovery_no_duplicate_tasks(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.pending)
        await dispatcher.recover(session_factory)
        task_first = dispatcher._tasks.get(job.id)
        await dispatcher.recover(session_factory)
        task_second = dispatcher._tasks.get(job.id)
        assert task_first is task_second
        await dispatcher.shutdown()

    async def test_done_jobs_not_dispatched(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.done)
        await dispatcher.recover(session_factory)
        assert job.id not in dispatcher._tasks


class TestRetry:
    async def test_retry_failed_dispatches(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.failed)
        await dispatcher.retry(job.id, session_factory)
        assert job.id in dispatcher._tasks
        await dispatcher.shutdown()

    async def test_retry_partial_dispatches(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.partial)
        await dispatcher.retry(job.id, session_factory)
        assert job.id in dispatcher._tasks
        await dispatcher.shutdown()

    async def test_retry_cancelled_dispatches(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.cancelled)
        await dispatcher.retry(job.id, session_factory)
        assert job.id in dispatcher._tasks
        await dispatcher.shutdown()

    async def test_retry_resets_status_to_pending(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.failed)
        await dispatcher.retry(job.id, session_factory)
        async with session_factory() as s:
            refreshed = await s.get(Job, job.id)
            assert refreshed is not None
            assert refreshed.status == JobStatus.pending
        await dispatcher.shutdown()

    async def test_retry_clears_result_json(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        async with session_factory() as s:
            job = Job(
                source="youtube",
                query="test",
                status=JobStatus.failed,
                result_json='{"error":1}',
            )
            s.add(job)
            await s.commit()
            await s.refresh(job)
        await dispatcher.retry(job.id, session_factory)
        async with session_factory() as s:
            refreshed = await s.get(Job, job.id)
            assert refreshed is not None
            assert refreshed.result_json is None
        await dispatcher.shutdown()

    async def test_retry_absent_raises_not_found(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        with pytest.raises(JobNotFoundError) as exc_info:
            await dispatcher.retry(9999, session_factory)
        assert exc_info.value.job_id == 9999

    async def test_retry_pending_raises_not_retryable(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.pending)
        with pytest.raises(JobNotRetryableError) as exc_info:
            await dispatcher.retry(job.id, session_factory)
        assert exc_info.value.job_id == job.id

    async def test_retry_running_raises_not_retryable(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.running)
        with pytest.raises(JobNotRetryableError):
            await dispatcher.retry(job.id, session_factory)

    async def test_retry_done_raises_not_retryable(
        self,
        dispatcher: JobDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        job = await _make_job(session_factory, status=JobStatus.done)
        with pytest.raises(JobNotRetryableError):
            await dispatcher.retry(job.id, session_factory)
