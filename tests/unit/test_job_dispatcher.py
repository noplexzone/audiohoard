from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

os.environ.setdefault("SECRET_KEY", "test-secret")

from app.database import Base
from app.jobs.dispatcher import JobDispatcher, JobNotFoundError, JobNotRetryableError
from app.models.job import Job, JobStatus
from app.models.track import Track
from app.models.workflow import AcquisitionState
from app.services.acquisition_cleanup import (
    cleanup_durable_slskd_transfers,
    hide_completed_and_timed_out_jobs,
)

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


class TestAutomaticTerminalCleanup:
    async def test_hides_done_and_timeout_only_after_terminal_rows_are_committed(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            done = Job(source="youtube", query="done", status=JobStatus.done)
            timeout = Job(source="slskd", query="timeout", status=JobStatus.failed)
            ordinary = Job(source="slskd", query="ordinary failure", status=JobStatus.failed)
            mixed = Job(source="slskd", query="mixed timeout", status=JobStatus.partial)
            session.add_all([done, timeout, ordinary, mixed])
            await session.flush()
            continuation = Job(
                source="priority",
                query="alternate source",
                status=JobStatus.pending,
                parent_job_id=timeout.id,
            )
            session.add(continuation)
            session.add_all(
                [
                    Track(
                        job_id=timeout.id,
                        source="slskd",
                        source_job_id="timeout-transfer",
                        source_status="transfer_timeout",
                        acquisition_state=AcquisitionState.failed,
                    ),
                    Track(
                        job_id=ordinary.id,
                        source="slskd",
                        source_job_id="failed-transfer",
                        source_status="transfer_failed",
                        acquisition_state=AcquisitionState.failed,
                    ),
                    Track(
                        job_id=mixed.id,
                        source="slskd",
                        source_job_id="mixed-timeout-transfer",
                        source_status="transfer_timeout",
                        acquisition_state=AcquisitionState.failed,
                    ),
                    Track(
                        job_id=mixed.id,
                        source="youtube",
                        source_status="result_processing_failed",
                        acquisition_state=AcquisitionState.failed,
                    ),
                ]
            )
            await session.commit()
            ids = (done.id, timeout.id, ordinary.id, mixed.id, continuation.id)

        hidden = await hide_completed_and_timed_out_jobs(session_factory)

        assert hidden == [ids[0], ids[1]]
        async with session_factory() as session:
            rows = [await session.get(Job, job_id) for job_id in ids]
            assert [row.queue_hidden for row in rows if row is not None] == [
                True,
                True,
                False,
                False,
                False,
            ]

    async def test_slskd_cleanup_is_idempotent_and_runs_without_database_transaction(
        self, session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
    ) -> None:
        staged = tmp_path / "song.flac"
        staged.write_bytes(b"audio")
        async with session_factory() as session:
            success_job = Job(source="slskd", query="success", status=JobStatus.done)
            timeout_job = Job(source="slskd", query="timeout", status=JobStatus.failed)
            failed_job = Job(source="slskd", query="failed", status=JobStatus.failed)
            session.add_all([success_job, timeout_job, failed_job])
            await session.flush()
            session.add_all(
                [
                    Track(
                        job_id=success_job.id,
                        source="slskd",
                        source_job_id="success-transfer",
                        source_status="downloaded",
                        staging_path=str(staged),
                        acquisition_provenance_json=json.dumps(
                            {
                                "source": "slskd",
                                "username": "success-peer",
                                "filename": "success.flac",
                            }
                        ),
                        acquisition_state=AcquisitionState.downloaded,
                    ),
                    Track(
                        job_id=timeout_job.id,
                        source="slskd",
                        source_job_id="timeout-transfer",
                        source_status="transfer_timeout",
                        acquisition_provenance_json=json.dumps(
                            {
                                "source": "slskd",
                                "username": "timeout-peer",
                                "filename": "timeout.flac",
                            }
                        ),
                        acquisition_state=AcquisitionState.failed,
                    ),
                    Track(
                        job_id=failed_job.id,
                        source="slskd",
                        source_job_id="failed-transfer",
                        source_status="transfer_failed",
                        acquisition_provenance_json=json.dumps(
                            {
                                "source": "slskd",
                                "username": "failed-peer",
                                "filename": "failed.flac",
                            }
                        ),
                        acquisition_state=AcquisitionState.failed,
                    ),
                ]
            )
            await session.commit()

        calls: list[tuple[str, str]] = []

        class FakeAdapter:
            async def cancel(self, username: str, filename: str) -> None:
                # A separate write proves the candidate-read session is closed before HTTP I/O.
                async with session_factory() as writer:
                    row = await writer.get(Job, failed_job.id)
                    assert row is not None
                    row.query = "provider call completed"
                    await writer.commit()
                calls.append((username, filename))

        first = await cleanup_durable_slskd_transfers(session_factory, FakeAdapter())
        second = await cleanup_durable_slskd_transfers(session_factory, FakeAdapter())

        assert first == 2
        assert second == 0
        assert calls == [
            ("success-peer", "success.flac"),
            ("timeout-peer", "timeout.flac"),
        ]
        async with session_factory() as session:
            cleaned = list(
                (
                    await session.scalars(
                        select(Track).where(Track.job_id.in_([success_job.id, timeout_job.id]))
                    )
                ).all()
            )
            assert all(
                json.loads(track.acquisition_provenance_json or "{}").get(
                    "source_cleanup_completed_at"
                )
                for track in cleaned
            )

    async def test_slskd_cleanup_does_not_mark_a_concurrently_reassigned_transfer(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            job = Job(source="slskd", query="race", status=JobStatus.failed)
            session.add(job)
            await session.flush()
            track = Track(
                job_id=job.id,
                source="slskd",
                source_job_id="old-transfer",
                source_status="transfer_timeout",
                acquisition_provenance_json=json.dumps(
                    {"source": "slskd", "username": "old-peer", "filename": "old.flac"}
                ),
                acquisition_state=AcquisitionState.failed,
            )
            session.add(track)
            await session.commit()
            track_id = track.id

        class ReassigningAdapter:
            async def cancel(self, username: str, filename: str) -> None:
                assert (username, filename) == ("old-peer", "old.flac")
                async with session_factory() as writer:
                    current = await writer.get(Track, track_id)
                    assert current is not None
                    current.source_job_id = "new-transfer"
                    current.source_status = "acquiring"
                    current.acquisition_state = AcquisitionState.acquiring
                    current.acquisition_provenance_json = json.dumps(
                        {
                            "source": "slskd",
                            "username": "old-peer",
                            "filename": "old.flac",
                        }
                    )
                    await writer.commit()

        removed = await cleanup_durable_slskd_transfers(session_factory, ReassigningAdapter())

        assert removed == 1
        async with session_factory() as session:
            current = await session.get(Track, track_id)
            assert current is not None
            provenance = json.loads(current.acquisition_provenance_json or "{}")
            assert current.source_job_id == "new-transfer"
            assert provenance["username"] == "old-peer"
            assert "source_cleanup_completed_at" not in provenance


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
