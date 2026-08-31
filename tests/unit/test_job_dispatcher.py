from __future__ import annotations

import asyncio
import gc
import json
import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

os.environ.setdefault("SECRET_KEY", "test-secret")

from app.database import Base
from app.jobs import runner as job_runner
from app.jobs.dispatcher import (
    JobDispatcher,
    JobNotFoundError,
    JobNotRetryableError,
    current_acquisition_permit,
)
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.acquisition_cleanup import (
    cleanup_durable_slskd_transfers,
    cleanup_terminal_acquisitions,
    hide_completed_and_timed_out_jobs,
)
from app.sources.base import CapabilityState

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

    async def test_does_not_hide_done_catalog_job_before_durable_import(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            artist = CatalogArtist(name="Downloaded Artist")
            album = CatalogAlbum(artist=artist, title="Downloaded Album", track_count=1)
            catalog_track = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
            job = Job(
                source="slskd", query="downloaded", status=JobStatus.done, catalog_album=album
            )
            session.add_all([artist, album, catalog_track, job])
            await session.flush()
            session.add(
                Track(
                    job_id=job.id,
                    source="slskd",
                    catalog_album_id=album.id,
                    catalog_track_id=catalog_track.id,
                    acquisition_state=AcquisitionState.downloaded,
                    import_state=ImportWorkflowState.ready,
                )
            )
            await session.commit()
            job_id = job.id

        hidden = await hide_completed_and_timed_out_jobs(session_factory, {job_id})

        assert hidden == []
        async with session_factory() as session:
            row = await session.get(Job, job_id)
            assert row is not None and row.queue_hidden is False

    async def test_does_not_hide_timed_out_catalog_job_before_durable_import(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            artist = CatalogArtist(name="Timeout Artist")
            album = CatalogAlbum(artist=artist, title="Timeout Album", track_count=1)
            catalog_track = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
            job = Job(
                source="slskd", query="timeout", status=JobStatus.failed, catalog_album=album
            )
            session.add_all([artist, album, catalog_track, job])
            await session.flush()
            session.add(
                Track(
                    job_id=job.id,
                    source="slskd",
                    catalog_album_id=album.id,
                    catalog_track_id=catalog_track.id,
                    source_status="transfer_timeout",
                    acquisition_state=AcquisitionState.failed,
                    import_state=ImportWorkflowState.ready,
                )
            )
            await session.commit()
            job_id = job.id

        hidden = await hide_completed_and_timed_out_jobs(session_factory, {job_id})

        assert hidden == []
        async with session_factory() as session:
            row = await session.get(Job, job_id)
            assert row is not None and row.queue_hidden is False

    async def test_hides_all_terminal_album_attempts_only_after_full_import(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            artist = CatalogArtist(name="Complete Artist")
            album = CatalogAlbum(artist=artist, title="Complete Album", track_count=2)
            first = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
            second = CatalogAlbumTrack(album=album, position=2, disc=1, title="Two")
            root = Job(source="slskd", query="root", status=JobStatus.partial, catalog_album=album)
            continuation = Job(
                source="youtube",
                query="continuation",
                status=JobStatus.done,
                catalog_album=album,
            )
            active = Job(
                source="priority", query="active", status=JobStatus.running, catalog_album=album
            )
            session.add_all([artist, album, first, second, root, continuation, active])
            await session.flush()
            session.add_all(
                [
                    Track(
                        job_id=root.id,
                        source="slskd",
                        catalog_album_id=album.id,
                        catalog_track_id=first.id,
                        acquisition_state=AcquisitionState.downloaded,
                        import_state=ImportWorkflowState.imported,
                    ),
                    Track(
                        job_id=continuation.id,
                        source="youtube",
                        catalog_album_id=album.id,
                        catalog_track_id=second.id,
                        acquisition_state=AcquisitionState.downloaded,
                        import_state=ImportWorkflowState.imported,
                    ),
                ]
            )
            await session.commit()
            ids = root.id, continuation.id, active.id

        hidden = await hide_completed_and_timed_out_jobs(session_factory, {ids[1]})

        assert hidden == [ids[0], ids[1]]
        async with session_factory() as session:
            rows = [await session.get(Job, job_id) for job_id in ids]
            assert [row.queue_hidden for row in rows if row is not None] == [True, True, False]

    async def test_does_not_hide_partial_album_until_every_track_is_imported(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            artist = CatalogArtist(name="Incomplete Artist")
            album = CatalogAlbum(artist=artist, title="Incomplete Album", track_count=2)
            first = CatalogAlbumTrack(album=album, position=1, disc=1, title="One")
            second = CatalogAlbumTrack(album=album, position=2, disc=1, title="Two")
            root = Job(source="slskd", query="root", status=JobStatus.partial, catalog_album=album)
            session.add_all([artist, album, first, second, root])
            await session.flush()
            session.add_all(
                [
                    Track(
                        job_id=root.id,
                        source="slskd",
                        catalog_album_id=album.id,
                        catalog_track_id=first.id,
                        acquisition_state=AcquisitionState.downloaded,
                        import_state=ImportWorkflowState.imported,
                    ),
                    Track(
                        job_id=root.id,
                        source="slskd",
                        catalog_album_id=album.id,
                        catalog_track_id=second.id,
                        acquisition_state=AcquisitionState.downloaded,
                        import_state=ImportWorkflowState.ready,
                    ),
                ]
            )
            await session.commit()
            root_id = root.id

        hidden = await hide_completed_and_timed_out_jobs(session_factory, {root_id})

        assert hidden == []
        async with session_factory() as session:
            row = await session.get(Job, root_id)
            assert row is not None and row.queue_hidden is False

    async def test_legacy_slskd_rows_remain_report_only_without_provider_io(
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
            async def cancel(
                self, username: str, filename: str, transfer_id: str | None = None
            ) -> None:
                # A separate write proves the candidate-read session is closed before HTTP I/O.
                async with session_factory() as writer:
                    row = await writer.get(Job, failed_job.id)
                    assert row is not None
                    row.query = "provider call completed"
                    await writer.commit()
                assert transfer_id is not None
                calls.append((username, filename))

        first = await cleanup_durable_slskd_transfers(session_factory, FakeAdapter())
        second = await cleanup_durable_slskd_transfers(session_factory, FakeAdapter())

        assert first == 0
        assert second == 0
        assert calls == []
        async with session_factory() as session:
            cleaned = list(
                (
                    await session.scalars(
                        select(Track).where(Track.job_id.in_([success_job.id, timeout_job.id]))
                    )
                ).all()
            )
            assert all(
                "source_cleanup_completed_at"
                not in json.loads(track.acquisition_provenance_json or "{}")
                for track in cleaned
            )

    async def test_legacy_slskd_cleanup_does_not_touch_unfenced_transfer(
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
            async def cancel(
                self, username: str, filename: str, transfer_id: str | None = None
            ) -> None:
                assert (username, filename, transfer_id) == (
                    "old-peer",
                    "old.flac",
                    "old-transfer",
                )
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

        assert removed == 0
        async with session_factory() as session:
            current = await session.get(Track, track_id)
            assert current is not None
            provenance = json.loads(current.acquisition_provenance_json or "{}")
            assert current.source_job_id == "old-transfer"
            assert provenance["username"] == "old-peer"
            assert "source_cleanup_completed_at" not in provenance


async def test_legacy_slskd_cleanup_does_not_create_completion_marker(
    session_factory: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    async with session_factory() as session:
        job = Job(source="slskd", query="marker-lock", status=JobStatus.failed)
        session.add(job)
        await session.flush()
        track = Track(
            job_id=job.id,
            source="slskd",
            source_job_id="exact-transfer",
            source_status="transfer_timeout",
            acquisition_provenance_json=json.dumps(
                {"source": "slskd", "username": "peer", "filename": "song.flac"}
            ),
            acquisition_state=AcquisitionState.failed,
        )
        session.add(track)
        await session.commit()
        track_id = track.id

    cancel_calls = 0

    class FakeAdapter:
        async def cancel(
            self, username: str, filename: str, transfer_id: str | None = None
        ) -> None:
            nonlocal cancel_calls
            cancel_calls += 1
            assert (username, filename, transfer_id) == ("peer", "song.flac", "exact-transfer")

    original_commit = AsyncSession.commit
    marker_commits = 0

    async def lock_first_marker_commit(session: AsyncSession) -> None:
        nonlocal marker_commits
        marking = any(
            isinstance(row, Track)
            and "source_cleanup_completed_at" in (row.acquisition_provenance_json or "")
            for row in session.dirty
        )
        if marking:
            marker_commits += 1
            if marker_commits == 1:
                raise OperationalError("UPDATE tracks", {}, Exception("database is locked"))
        await original_commit(session)

    async def no_sleep(delay: float) -> None:
        assert delay == 0.25

    monkeypatch.setattr(AsyncSession, "commit", lock_first_marker_commit)
    monkeypatch.setattr(asyncio, "sleep", no_sleep)

    assert await cleanup_durable_slskd_transfers(session_factory, FakeAdapter()) == 0

    assert cancel_calls == 0
    assert marker_commits == 0
    async with session_factory() as session:
        current = await session.get(Track, track_id)
        assert current is not None
        assert "source_cleanup_completed_at" not in json.loads(
            current.acquisition_provenance_json or "{}"
        )


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


async def test_terminal_cleanup_retries_transient_sqlite_lock(
    session_factory: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import acquisition_cleanup

    attempts = 0

    async def flaky_hide(factory, job_ids=None):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError("UPDATE jobs", {}, Exception("database is locked"))
        return []

    async def no_transfers(factory, adapter, job_ids=None, *, max_attempts=3, **_kwargs):
        return 0

    async def no_sleep(delay):
        assert delay == 0.25

    monkeypatch.setattr(acquisition_cleanup, "_hide_completed_and_timed_out_jobs_once", flaky_hide)
    monkeypatch.setattr(acquisition_cleanup, "cleanup_durable_slskd_transfers", no_transfers)
    monkeypatch.setattr(acquisition_cleanup.asyncio, "sleep", no_sleep)

    result = await cleanup_terminal_acquisitions(session_factory, slskd_url="", slskd_api_key="")

    assert result == ([], 0)
    assert attempts == 2


async def test_cleanup_reconciler_has_single_owned_task(
    dispatcher: JobDispatcher,
) -> None:
    await dispatcher.start_cleanup_reconciler(interval_seconds=3600)
    first = dispatcher._cleanup_task
    await dispatcher.start_cleanup_reconciler(interval_seconds=3600)
    assert dispatcher._cleanup_task is first
    await dispatcher.shutdown()
    assert dispatcher._cleanup_task is None


async def test_periodic_cleanup_revisits_imported_attempt_files_with_effective_roots(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path, monkeypatch
) -> None:
    import hashlib

    from app import settings_service
    from app.config import get_settings
    from app.models.acquisition_attempt import (
        AcquisitionAttempt,
        ArtifactState,
        AttemptOutcome,
        CleanupState,
        ProviderTransferState,
        RetentionDisposition,
    )
    from app.models.import_plan import ImportPlan
    from app.models.release import Release
    from app.services import acquisition_cleanup

    complete = tmp_path / "db-complete"
    complete.mkdir()
    incomplete = tmp_path / "db-incomplete"
    staged = complete / "song.flac"
    staged.write_bytes(b"owned audio")
    current = staged.stat()
    async with session_factory() as db:
        job = Job(source="slskd", query="cleanup", status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Album")
        track = Track(
            job=job,
            release=release,
            source="slskd",
            source_job_id="2d93899b-cf9a-4567-8f10-993610f274cf",
            staging_path=str(staged),
            acquisition_provenance_json=json.dumps(
                {"source": "slskd", "username": "peer", "filename": "Album/song.flac"}
            ),
        )
        plan = ImportPlan(
            release=release,
            track=track,
            source_path=str(staged),
            staging_path=str(staged),
            destination_path=str(tmp_path / "library" / "song.flac"),
            status=ImportWorkflowState.imported,
        )
        attempt = AcquisitionAttempt(
            job=job,
            track=track,
            provider="slskd",
            peer="peer",
            remote_path="Album/song.flac",
            provider_uuid="2d93899b-cf9a-4567-8f10-993610f274cf",
            provider_state=ProviderTransferState.completed,
            provider_cleanup_state=CleanupState.failed,
            staged_path=str(staged),
            artifact_state=ArtifactState.staged,
            artifact_device=current.st_dev,
            artifact_inode=current.st_ino,
            artifact_mtime_ns=current.st_mtime_ns,
            artifact_size=current.st_size,
            artifact_sha256=hashlib.sha256(staged.read_bytes()).hexdigest(),
            outcome=AttemptOutcome.imported,
            file_cleanup_eligible=True,
            retention_disposition=RetentionDisposition.cleanup_eligible,
        )
        db.add_all([job, release, track, plan, attempt])
        await db.commit()
        plan_id, attempt_id = plan.id, attempt.id

    effective = get_settings().model_copy(
        update={
            "staging_root": tmp_path / "wrong-static-root",
            "slskd_complete_root": complete,
            "slskd_incomplete_root": incomplete,
            "slskd_url": "",
            "slskd_api_key": "",
        }
    )

    async def effective_settings(db, configured):  # noqa: ANN001
        return effective

    async def terminal_cleanup(factory, **kwargs):  # noqa: ANN001
        async with factory() as db:
            current_attempt = await db.get(AcquisitionAttempt, attempt_id)
            assert current_attempt is not None
            current_attempt.provider_cleanup_state = CleanupState.completed
            await db.commit()
        return [], 1

    monkeypatch.setattr(settings_service, "build_effective_settings", effective_settings)
    monkeypatch.setattr(acquisition_cleanup, "build_effective_settings", effective_settings)
    prune_orphans = AsyncMock()
    monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", terminal_cleanup)
    monkeypatch.setattr(acquisition_cleanup, "prune_orphaned_terminal_records", prune_orphans)
    dispatcher = JobDispatcher(runner=AsyncMock(), session_factory=session_factory)

    await dispatcher._cleanup_reconcile_tick()

    prune_orphans.assert_awaited_once()
    assert prune_orphans.await_args.kwargs == {
        "batch_size": 1,
        "commit_batches": True,
        "max_batches": 100,
    }
    assert not staged.exists()
    async with session_factory() as db:
        persisted_plan = await db.get(ImportPlan, plan_id)
        persisted_attempt = await db.get(AcquisitionAttempt, attempt_id)
        assert persisted_plan is not None and persisted_plan.staging_path is None
        assert persisted_plan.cleanup_attempted_at is not None
        assert persisted_attempt is not None
        assert persisted_attempt.file_cleanup_state is CleanupState.completed


async def test_dispatcher_respects_max_concurrent_jobs() -> None:
    current = 0
    peak = 0
    started_two = asyncio.Event()
    release = asyncio.Event()

    async def slow_runner(job_id: int) -> None:
        nonlocal current, peak
        current += 1
        peak = max(peak, current)
        if current == 2:
            started_two.set()
        await release.wait()
        current -= 1

    dispatcher = JobDispatcher(runner=slow_runner, max_concurrent_jobs=2)
    tasks = [await dispatcher.dispatch(job_id) for job_id in range(5)]
    await asyncio.wait_for(started_two.wait(), timeout=1)
    await asyncio.sleep(0.05)

    assert peak == 2
    assert current == 2

    release.set()
    await asyncio.gather(*tasks)
    await dispatcher.shutdown()


async def _assert_dispatcher_counts(
    dispatcher: JobDispatcher, *, active: int, inflight: int
) -> None:
    await asyncio.sleep(0.02)
    assert dispatcher.active_jobs == active
    assert dispatcher.inflight_jobs == inflight


async def test_derived_inflight_limit_bounds_yielded_workflows() -> None:
    entered: list[int] = []
    five_entered = asyncio.Event()
    sixth_entered = asyncio.Event()
    releases = {job_id: asyncio.Event() for job_id in range(1, 7)}

    async def yielded_runner(job_id: int) -> None:
        permit = current_acquisition_permit()
        assert permit is not None
        await permit.yield_permit()
        entered.append(job_id)
        if len(entered) == 5:
            five_entered.set()
        if job_id == 6:
            sixth_entered.set()
        await releases[job_id].wait()

    dispatcher = JobDispatcher(runner=yielded_runner, max_concurrent_jobs=1)
    tasks = [await dispatcher.dispatch(job_id) for job_id in range(1, 7)]

    await asyncio.wait_for(five_entered.wait(), timeout=1)
    await asyncio.sleep(0.02)
    assert 6 not in entered
    assert not sixth_entered.is_set()
    assert dispatcher.active_jobs == 0
    assert dispatcher.inflight_jobs == dispatcher.max_inflight_jobs == 5

    releases[entered[0]].set()
    await asyncio.wait_for(sixth_entered.wait(), timeout=1)

    for release in releases.values():
        release.set()
    await asyncio.gather(*tasks)
    assert dispatcher.active_jobs == dispatcher.inflight_jobs == 0


async def test_yielded_provider_wait_is_inflight_but_not_active_local() -> None:
    yielded = asyncio.Event()
    release = asyncio.Event()

    async def yielded_runner(job_id: int) -> None:
        permit = current_acquisition_permit()
        assert permit is not None
        await permit.yield_permit()
        yielded.set()
        await release.wait()

    dispatcher = JobDispatcher(runner=yielded_runner, max_concurrent_jobs=1)
    task = await dispatcher.dispatch(1)
    await asyncio.wait_for(yielded.wait(), timeout=1)

    assert dispatcher.active_jobs == 0
    assert dispatcher.inflight_jobs == 1

    release.set()
    await task
    assert dispatcher.active_jobs == dispatcher.inflight_jobs == 0


async def test_local_resize_updates_derived_inflight_limit_and_admission() -> None:
    entered: list[int] = []
    five_entered = asyncio.Event()
    ten_entered = asyncio.Event()
    eleventh_entered = asyncio.Event()
    releases = {job_id: asyncio.Event() for job_id in range(1, 12)}

    async def yielded_runner(job_id: int) -> None:
        permit = current_acquisition_permit()
        assert permit is not None
        await permit.yield_permit()
        entered.append(job_id)
        if len(entered) == 5:
            five_entered.set()
        if len(entered) == 10:
            ten_entered.set()
        if job_id == 11:
            eleventh_entered.set()
        await releases[job_id].wait()

    dispatcher = JobDispatcher(runner=yielded_runner, max_concurrent_jobs=1)
    tasks = [await dispatcher.dispatch(job_id) for job_id in range(1, 11)]
    await asyncio.wait_for(five_entered.wait(), timeout=1)
    assert dispatcher.max_inflight_jobs == dispatcher.inflight_jobs == 5

    await dispatcher.set_max_concurrent_jobs(2)
    await asyncio.wait_for(ten_entered.wait(), timeout=1)
    assert dispatcher.max_inflight_jobs == dispatcher.inflight_jobs == 10

    await dispatcher.set_max_concurrent_jobs(1)
    eleventh = await dispatcher.dispatch(11)
    await asyncio.sleep(0.02)
    assert dispatcher.max_inflight_jobs == 5
    assert dispatcher.inflight_jobs == 10
    assert not eleventh_entered.is_set()

    for job_id in entered[:5]:
        releases[job_id].set()
    await asyncio.gather(*(tasks[job_id - 1] for job_id in entered[:5]))
    assert dispatcher.inflight_jobs == 5
    assert not eleventh_entered.is_set()

    releases[entered[5]].set()
    await asyncio.wait_for(eleventh_entered.wait(), timeout=1)

    for release in releases.values():
        release.set()
    await asyncio.gather(*tasks, eleventh)
    assert dispatcher.active_jobs == dispatcher.inflight_jobs == 0


async def test_cancellation_during_inflight_finalizer_cannot_leak_capacity() -> None:
    runner_entered = asyncio.Event()
    finalizer_entered = asyncio.Event()
    release_finalizer = asyncio.Event()
    later_ran = asyncio.Event()

    async def runner(job_id: int) -> None:
        if job_id == 1:
            runner_entered.set()
            await asyncio.Event().wait()
        else:
            later_ran.set()

    dispatcher = JobDispatcher(runner=runner, max_concurrent_jobs=1, max_inflight_jobs=1)
    original_release_inflight = dispatcher._release_inflight

    async def blocked_release_inflight() -> None:
        finalizer_entered.set()
        await release_finalizer.wait()
        await original_release_inflight()

    dispatcher._release_inflight = blocked_release_inflight  # type: ignore[method-assign]
    task = await dispatcher.dispatch(1)
    await asyncio.wait_for(runner_entered.wait(), timeout=1)
    task.cancel()
    await asyncio.wait_for(finalizer_entered.wait(), timeout=1)

    task.cancel()
    await asyncio.sleep(0)
    assert dispatcher.active_jobs == 0
    assert dispatcher.inflight_jobs == 1

    release_finalizer.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert dispatcher.active_jobs == 0
    assert dispatcher.inflight_jobs == 0

    later = await dispatcher.dispatch(2)
    await asyncio.wait_for(later, timeout=1)
    assert later_ran.is_set()
    assert dispatcher.active_jobs == 0
    assert dispatcher.inflight_jobs == 0


async def test_explicit_inflight_limit_stays_fixed_across_local_resize() -> None:
    entered: list[int] = []
    two_entered = asyncio.Event()
    third_entered = asyncio.Event()
    releases = {job_id: asyncio.Event() for job_id in (1, 2, 3)}

    async def yielded_runner(job_id: int) -> None:
        permit = current_acquisition_permit()
        assert permit is not None
        await permit.yield_permit()
        entered.append(job_id)
        if len(entered) == 2:
            two_entered.set()
        if job_id == 3:
            third_entered.set()
        await releases[job_id].wait()

    dispatcher = JobDispatcher(runner=yielded_runner, max_concurrent_jobs=1, max_inflight_jobs=2)
    tasks = [await dispatcher.dispatch(job_id) for job_id in (1, 2, 3)]
    await asyncio.wait_for(two_entered.wait(), timeout=1)

    await dispatcher.set_max_concurrent_jobs(2)
    await asyncio.sleep(0.02)
    assert dispatcher.max_inflight_jobs == dispatcher.inflight_jobs == 2
    assert not third_entered.is_set()

    releases[entered[0]].set()
    await asyncio.wait_for(third_entered.wait(), timeout=1)
    for release in releases.values():
        release.set()
    await asyncio.gather(*tasks)
    assert dispatcher.active_jobs == dispatcher.inflight_jobs == 0


@pytest.mark.parametrize("boundary", ["workflow", "local", "yielded", "runner"])
async def test_cancellation_at_admission_boundaries_releases_owned_capacity(
    boundary: str,
) -> None:
    first_started = asyncio.Event()
    yielded = asyncio.Event()
    release_first = asyncio.Event()
    later_ran = asyncio.Event()

    async def controlled_runner(job_id: int) -> None:
        if job_id == 99:
            later_ran.set()
            return
        if job_id == 1:
            first_started.set()
        if boundary == "yielded":
            permit = current_acquisition_permit()
            assert permit is not None
            await permit.yield_permit()
            yielded.set()
        await release_first.wait()

    explicit_inflight = 1 if boundary == "workflow" else 2
    dispatcher = JobDispatcher(
        runner=controlled_runner,
        max_concurrent_jobs=1,
        max_inflight_jobs=explicit_inflight,
    )
    first = await dispatcher.dispatch(1)
    await asyncio.wait_for(first_started.wait(), timeout=1)

    if boundary == "workflow":
        target = await dispatcher.dispatch(2)
        await _assert_dispatcher_counts(dispatcher, active=1, inflight=1)
    elif boundary == "local":
        target = await dispatcher.dispatch(2)
        await _assert_dispatcher_counts(dispatcher, active=1, inflight=2)
    else:
        target = first
        if boundary == "yielded":
            await asyncio.wait_for(yielded.wait(), timeout=1)
            await _assert_dispatcher_counts(dispatcher, active=0, inflight=1)
        else:
            await _assert_dispatcher_counts(dispatcher, active=1, inflight=1)

    target.cancel()
    with pytest.raises(asyncio.CancelledError):
        await target
    release_first.set()
    if first is not target:
        await first
    await _assert_dispatcher_counts(dispatcher, active=0, inflight=0)

    later = await dispatcher.dispatch(99)
    await asyncio.wait_for(later, timeout=1)
    assert later_ran.is_set()
    assert dispatcher.active_jobs == dispatcher.inflight_jobs == 0


async def test_all_yielded_inflight_workflows_do_not_block_local_reacquisition() -> None:
    yielded_jobs: set[int] = set()
    all_yielded = asyncio.Event()
    reacquire = asyncio.Event()
    reacquired = asyncio.Event()
    release_others = asyncio.Event()

    async def yielded_runner(job_id: int) -> None:
        permit = current_acquisition_permit()
        assert permit is not None
        await permit.yield_permit()
        yielded_jobs.add(job_id)
        if len(yielded_jobs) == 3:
            all_yielded.set()
        if job_id == 1:
            await reacquire.wait()
            await permit.acquire()
            reacquired.set()
            return
        await release_others.wait()

    dispatcher = JobDispatcher(runner=yielded_runner, max_concurrent_jobs=1, max_inflight_jobs=3)
    tasks = [await dispatcher.dispatch(job_id) for job_id in (1, 2, 3)]
    await asyncio.wait_for(all_yielded.wait(), timeout=1)
    assert dispatcher.active_jobs == 0
    assert dispatcher.inflight_jobs == 3

    reacquire.set()
    await asyncio.wait_for(reacquired.wait(), timeout=1)
    await tasks[0]
    assert dispatcher.inflight_jobs == 2

    release_others.set()
    await asyncio.gather(*tasks)
    assert dispatcher.active_jobs == dispatcher.inflight_jobs == 0


async def test_increasing_parallel_limit_starts_waiting_jobs() -> None:
    started: list[int] = []
    two_started = asyncio.Event()
    release = asyncio.Event()

    async def slow_runner(job_id: int) -> None:
        started.append(job_id)
        if len(started) == 2:
            two_started.set()
        await release.wait()

    dispatcher = JobDispatcher(runner=slow_runner, max_concurrent_jobs=1)
    tasks = [await dispatcher.dispatch(job_id) for job_id in (1, 2)]
    await asyncio.sleep(0.05)
    assert started == [1]

    await dispatcher.set_max_concurrent_jobs(2)
    await asyncio.wait_for(two_started.wait(), timeout=1)
    assert started == [1, 2]

    release.set()
    await asyncio.gather(*tasks)
    await dispatcher.shutdown()


async def test_queued_transfer_yields_slot_so_later_job_starts(tmp_path: Path) -> None:
    first_queued = asyncio.Event()
    allow_first_to_finish = asyncio.Event()
    second_started = asyncio.Event()
    staged = tmp_path / "song.flac"
    staged.write_bytes(b"audio")

    class QueuedAdapter:
        async def status(self, transfer_id: str) -> CapabilityState:
            assert transfer_id == "transfer-1"
            if not allow_first_to_finish.is_set():
                first_queued.set()
                return CapabilityState(True, "Queued")
            return CapabilityState(True, "Completed")

        async def cancel(
            self, username: str, filename: str, transfer_id: str | None = None
        ) -> bool:
            return True

    async def runner(job_id: int) -> None:
        if job_id == 1:
            await job_runner._poll_slskd_transfer(
                transfer_id="transfer-1",
                username="peer",
                filename=staged.name,
                adapter=QueuedAdapter(),  # type: ignore[arg-type]
                staging_root=tmp_path,
                poll_interval=0.001,
                poll_timeout=1,
            )
        else:
            second_started.set()

    dispatcher = JobDispatcher(runner=runner, max_concurrent_jobs=1)
    first = await dispatcher.dispatch(1)
    second = await dispatcher.dispatch(2)

    await asyncio.wait_for(first_queued.wait(), timeout=1)
    await asyncio.wait_for(second_started.wait(), timeout=1)
    assert not first.done()

    allow_first_to_finish.set()
    await asyncio.wait_for(second_started.wait(), timeout=1)
    await asyncio.gather(first, second)
    await dispatcher.shutdown()


async def test_queued_transfer_reacquires_before_active_completion(tmp_path: Path) -> None:
    queued = asyncio.Event()
    allow_active = asyncio.Event()
    second_holds_slot = asyncio.Event()
    release_second = asyncio.Event()
    staged = tmp_path / "song.flac"
    staged.write_bytes(b"audio")
    status_calls = 0

    class Adapter:
        async def status(self, transfer_id: str) -> CapabilityState:
            nonlocal status_calls
            status_calls += 1
            if status_calls == 1:
                queued.set()
                return CapabilityState(True, "Queued")
            await allow_active.wait()
            return CapabilityState(True, "Completed")

        async def cancel(self, *args, **kwargs) -> bool:
            return True

    async def controlled_runner(job_id: int) -> None:
        if job_id == 1:
            await job_runner._poll_slskd_transfer(
                "transfer-1", "peer", staged.name, Adapter(), tmp_path, 0.001, 1
            )
        else:
            second_holds_slot.set()
            await release_second.wait()

    dispatcher = JobDispatcher(runner=controlled_runner, max_concurrent_jobs=1)
    first = await dispatcher.dispatch(1)
    second = await dispatcher.dispatch(2)
    await asyncio.wait_for(queued.wait(), timeout=1)
    await asyncio.wait_for(second_holds_slot.wait(), timeout=1)
    allow_active.set()
    await asyncio.sleep(0.02)
    assert not first.done()
    release_second.set()
    await asyncio.gather(first, second)
    assert dispatcher._active_jobs == 0


@pytest.mark.parametrize("transition_state", ["InProgress", "Completed"])
async def test_queued_transfer_reacquires_before_provider_id_checkpoint(
    tmp_path: Path, transition_state: str
) -> None:
    queued = asyncio.Event()
    allow_transition = asyncio.Event()
    second_holds_slot = asyncio.Event()
    release_second = asyncio.Event()
    provider_id_checkpointed = asyncio.Event()
    checkpoint_active_jobs: list[int] = []
    staged = tmp_path / "song.flac"
    staged.write_bytes(b"audio")
    status_calls = 0

    class Adapter:
        async def status(self, transfer_id: str) -> CapabilityState:
            nonlocal status_calls
            status_calls += 1
            if status_calls == 1:
                queued.set()
                return CapabilityState(True, "Queued")
            await allow_transition.wait()
            if status_calls == 2:
                return CapabilityState(True, transition_state, {"id": "canonical-provider-uuid"})
            return CapabilityState(True, "Completed", {"id": "canonical-provider-uuid"})

        async def cancel(self, *args, **kwargs) -> bool:
            return True

    dispatcher: JobDispatcher

    async def on_provider_id(provider_id: str) -> None:
        assert provider_id == "canonical-provider-uuid"
        checkpoint_active_jobs.append(dispatcher._active_jobs)
        provider_id_checkpointed.set()

    async def controlled_runner(job_id: int) -> None:
        if job_id == 1:
            await job_runner._poll_slskd_transfer(
                "transfer-1",
                "peer",
                staged.name,
                Adapter(),  # type: ignore[arg-type]
                tmp_path,
                0.001,
                1,
                on_provider_id,
            )
        else:
            second_holds_slot.set()
            await release_second.wait()

    dispatcher = JobDispatcher(runner=controlled_runner, max_concurrent_jobs=1)
    first = await dispatcher.dispatch(1)
    second = await dispatcher.dispatch(2)
    await asyncio.wait_for(queued.wait(), timeout=1)
    await asyncio.wait_for(second_holds_slot.wait(), timeout=1)
    allow_transition.set()
    await asyncio.sleep(0.02)

    assert not provider_id_checkpointed.is_set()
    assert not first.done()

    release_second.set()
    await asyncio.gather(first, second)
    assert checkpoint_active_jobs == [1]
    assert dispatcher._active_jobs == 0


async def test_queued_transfer_reacquires_before_non_cancellation_error(tmp_path: Path) -> None:
    queued = asyncio.Event()
    fail = asyncio.Event()
    second_holds_slot = asyncio.Event()
    release_second = asyncio.Event()
    calls = 0

    class Adapter:
        async def status(self, transfer_id: str) -> CapabilityState:
            nonlocal calls
            calls += 1
            if calls == 1:
                queued.set()
                return CapabilityState(True, "Queued")
            await fail.wait()
            raise RuntimeError("provider failed")

        async def cancel(self, *args, **kwargs) -> bool:
            return True

    async def controlled_runner(job_id: int) -> None:
        if job_id == 1:
            await job_runner._poll_slskd_transfer(
                "transfer-1", "peer", "song.flac", Adapter(), tmp_path, 0.001, 1
            )
        else:
            second_holds_slot.set()
            await release_second.wait()

    dispatcher = JobDispatcher(runner=controlled_runner, max_concurrent_jobs=1)
    first = await dispatcher.dispatch(1)
    second = await dispatcher.dispatch(2)
    await asyncio.wait_for(queued.wait(), timeout=1)
    await asyncio.wait_for(second_holds_slot.wait(), timeout=1)
    fail.set()
    await asyncio.sleep(0.02)
    assert not first.done()
    release_second.set()
    with pytest.raises(RuntimeError, match="provider failed"):
        await first
    await second
    assert dispatcher._active_jobs == 0


async def test_cancellation_while_queue_permit_is_yielded_does_not_deadlock(
    tmp_path: Path,
) -> None:
    queued = asyncio.Event()
    second_holds_slot = asyncio.Event()
    release_second = asyncio.Event()

    class Adapter:
        async def status(self, transfer_id: str) -> CapabilityState:
            queued.set()
            return CapabilityState(True, "Queued")

        async def cancel(self, *args, **kwargs) -> bool:
            return True

    async def controlled_runner(job_id: int) -> None:
        if job_id == 1:
            await job_runner._poll_slskd_transfer(
                "transfer-1", "peer", "song.flac", Adapter(), tmp_path, 1, 10
            )
        else:
            second_holds_slot.set()
            await release_second.wait()

    dispatcher = JobDispatcher(runner=controlled_runner, max_concurrent_jobs=1)
    first = await dispatcher.dispatch(1)
    second = await dispatcher.dispatch(2)
    await asyncio.wait_for(queued.wait(), timeout=1)
    await asyncio.wait_for(second_holds_slot.wait(), timeout=1)
    assert dispatcher._active_jobs == dispatcher._configured_limit() == 1
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(first, timeout=0.2)
    assert dispatcher._active_jobs == dispatcher._configured_limit() == 1
    release_second.set()
    await second
    assert dispatcher._active_jobs == 0


async def test_decreasing_parallel_limit_does_not_cancel_active_jobs() -> None:
    current = 0
    peak_after_decrease = 0
    first_two_started = asyncio.Event()
    release_first_two = asyncio.Event()
    third_started = asyncio.Event()

    async def controlled_runner(job_id: int) -> None:
        nonlocal current, peak_after_decrease
        current += 1
        if job_id in {1, 2} and current == 2:
            first_two_started.set()
        if job_id == 3:
            peak_after_decrease = current
            third_started.set()
        if job_id in {1, 2}:
            await release_first_two.wait()
        current -= 1

    dispatcher = JobDispatcher(runner=controlled_runner, max_concurrent_jobs=2)
    tasks = [await dispatcher.dispatch(job_id) for job_id in (1, 2, 3)]
    await asyncio.wait_for(first_two_started.wait(), timeout=1)

    await dispatcher.set_max_concurrent_jobs(1)
    assert current == 2
    assert not third_started.is_set()

    release_first_two.set()
    await asyncio.wait_for(third_started.wait(), timeout=1)
    assert peak_after_decrease == 1

    await asyncio.gather(*tasks)
    await dispatcher.shutdown()


async def test_dispatcher_waiting_job_is_not_watchdog_redispatched(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    started: list[int] = []

    async def gated_runner(job_id: int) -> None:
        started.append(job_id)
        if len(started) == 1:
            first_started.set()
            await release_first.wait()

    async with session_factory() as session:
        first = Job(source="youtube", query="first", status=JobStatus.pending)
        second = Job(source="youtube", query="second", status=JobStatus.pending)
        session.add_all([first, second])
        await session.commit()
        first_id = first.id
        second_id = second.id

    dispatcher = JobDispatcher(
        runner=gated_runner, session_factory=session_factory, max_concurrent_jobs=1
    )
    first_task = await dispatcher.dispatch(first_id)
    second_task = await dispatcher.dispatch(second_id)
    await asyncio.wait_for(first_started.wait(), timeout=1)
    async with session_factory() as session:
        waiting = await session.get(Job, second_id)
        assert waiting is not None
        waiting.updated_at = datetime.now(UTC) - timedelta(seconds=600)
        await session.commit()

    await dispatcher._watchdog_tick(threshold_seconds=300)

    release_first.set()
    await asyncio.gather(first_task, second_task)

    assert started == [first_id, second_id]
    async with session_factory() as session:
        waiting = await session.get(Job, second_id)
        assert waiting is not None
        assert waiting.result_json is None
    await dispatcher.shutdown()


async def test_startup_running_recovery_retries_complete_transaction_after_sqlite_lock(
    session_factory: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import acquisition_cleanup

    job = await _make_job(session_factory, status=JobStatus.running)
    original_commit = AsyncSession.commit
    original_execute = AsyncSession.execute
    commit_attempts = 0
    recovery_reads = 0
    dispatched: list[int] = []

    async def no_cleanup(*args, **kwargs):
        return [], 0

    async def locked_once(session, *args, **kwargs):
        nonlocal commit_attempts
        commit_attempts += 1
        if commit_attempts == 1:
            raise OperationalError("UPDATE jobs", {}, Exception("database is locked"))
        return await original_commit(session, *args, **kwargs)

    async def count_recovery_reads(session, statement, *args, **kwargs):
        nonlocal recovery_reads
        rendered = str(statement)
        if "FROM jobs" in rendered and "jobs.status IN" in rendered:
            recovery_reads += 1
        return await original_execute(session, statement, *args, **kwargs)

    async def record_dispatch(job_id: int):
        dispatched.append(job_id)

    monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", no_cleanup)
    monkeypatch.setattr(AsyncSession, "commit", locked_once)
    monkeypatch.setattr(AsyncSession, "execute", count_recovery_reads)
    dispatcher = JobDispatcher(runner=AsyncMock(), session_factory=session_factory)
    monkeypatch.setattr(dispatcher, "dispatch", record_dispatch)

    recovered = await dispatcher.recover()

    assert commit_attempts == 2
    assert recovery_reads == 2
    assert recovered == [job.id]
    assert dispatched == [job.id]
    async with session_factory() as session:
        current = await session.get(Job, job.id)
        assert current is not None
        assert current.status == JobStatus.pending
        assert json.loads(current.result_json or "{}")["recovery"] == {
            "code": "interrupted_by_restart",
            "retryable": True,
        }


async def test_watchdog_dispatches_once_only_after_successful_retry_commit(
    session_factory: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    job = await _make_job(session_factory, status=JobStatus.running)
    async with session_factory() as session:
        current = await session.get(Job, job.id)
        assert current is not None
        current.updated_at = datetime.now(UTC) - timedelta(seconds=600)
        await session.commit()

    original_commit = AsyncSession.commit
    original_execute = AsyncSession.execute
    commit_attempts = 0
    watchdog_reads = 0
    dispatched: list[int] = []

    async def locked_once(session, *args, **kwargs):
        nonlocal commit_attempts
        commit_attempts += 1
        if commit_attempts == 1:
            raise OperationalError("UPDATE jobs", {}, Exception("database is locked"))
        return await original_commit(session, *args, **kwargs)

    async def count_watchdog_reads(session, statement, *args, **kwargs):
        nonlocal watchdog_reads
        rendered = str(statement)
        if "FROM jobs" in rendered and "jobs.updated_at <" in rendered:
            watchdog_reads += 1
        return await original_execute(session, statement, *args, **kwargs)

    async def record_committed_dispatch(job_id: int):
        async with session_factory() as session:
            committed = await session.get(Job, job_id)
            assert committed is not None
            assert committed.status == JobStatus.pending
            assert json.loads(committed.result_json or "{}")["watchdog_recovery"] == {"attempt": 1}
        dispatched.append(job_id)

    monkeypatch.setattr(AsyncSession, "commit", locked_once)
    monkeypatch.setattr(AsyncSession, "execute", count_watchdog_reads)
    dispatcher = JobDispatcher(runner=AsyncMock(), session_factory=session_factory)
    monkeypatch.setattr(dispatcher, "dispatch", record_committed_dispatch)

    await dispatcher._watchdog_tick(threshold_seconds=300)

    assert commit_attempts == 2
    assert watchdog_reads == 2
    assert dispatched == [job.id]


@pytest.mark.parametrize("concurrent_change", ["heartbeat", "status"])
async def test_watchdog_cas_does_not_overwrite_concurrent_job_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, concurrent_change: str
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'watchdog-cas.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)
    try:
        job = await _make_job(factory, status=JobStatus.running)
        stale_at = datetime.now(UTC) - timedelta(seconds=600)
        async with factory() as session:
            current = await session.get(Job, job.id)
            assert current is not None
            current.updated_at = stale_at
            await session.commit()

        original_execute = AsyncSession.execute
        raced = False
        dispatched: list[int] = []

        async def race_before_watchdog_update(session, statement, *args, **kwargs):
            nonlocal raced
            rendered = str(statement)
            if not raced and rendered.startswith("UPDATE jobs SET"):
                raced = True
                async with factory() as writer:
                    current = await writer.get(Job, job.id)
                    assert current is not None
                    if concurrent_change == "heartbeat":
                        current.result_json = json.dumps({"heartbeat": 1})
                        current.updated_at = datetime.now(UTC)
                    else:
                        current.status = JobStatus.done
                        current.updated_at = stale_at
                    await writer.commit()
            return await original_execute(session, statement, *args, **kwargs)

        async def record_dispatch(job_id: int):
            dispatched.append(job_id)

        monkeypatch.setattr(AsyncSession, "execute", race_before_watchdog_update)
        dispatcher = JobDispatcher(runner=AsyncMock(), session_factory=factory)
        monkeypatch.setattr(dispatcher, "dispatch", record_dispatch)

        await dispatcher._watchdog_tick(threshold_seconds=300)

        assert raced is True
        assert dispatched == []
        async with factory() as session:
            current = await session.get(Job, job.id)
            assert current is not None
            if concurrent_change == "heartbeat":
                assert current.status == JobStatus.running
                assert json.loads(current.result_json or "{}") == {"heartbeat": 1}
            else:
                assert current.status == JobStatus.done
                assert current.result_json is None
    finally:
        await engine.dispose()


async def test_watchdog_fences_recurrent_stale_recovery_as_dispatch_lost(
    session_factory: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    async with session_factory() as session:
        job = Job(
            source="youtube",
            query="lost twice",
            status=JobStatus.pending,
            result_json=json.dumps({"watchdog_recovery": {"attempt": 1}}),
            updated_at=datetime.now(UTC) - timedelta(seconds=600),
        )
        session.add(job)
        await session.commit()
        job_id = job.id

    dispatched: list[int] = []

    async def record_dispatch(job_id: int):
        dispatched.append(job_id)

    dispatcher = JobDispatcher(runner=AsyncMock(), session_factory=session_factory)
    monkeypatch.setattr(dispatcher, "dispatch", record_dispatch)

    await dispatcher._watchdog_tick(threshold_seconds=300)

    assert dispatched == []
    async with session_factory() as session:
        current = await session.get(Job, job_id)
        assert current is not None
        assert current.status == JobStatus.failed
        assert json.loads(current.result_json or "{}") == {
            "error": {
                "code": "dispatch_lost",
                "operation": "watchdog",
                "retryable": False,
            }
        }


async def test_startup_pending_recovery_does_not_dispatch_concurrent_terminal_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.services import acquisition_cleanup

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'startup-pending-cas.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)
    try:
        job = await _make_job(factory, status=JobStatus.pending)
        original_execute = AsyncSession.execute
        raced = False
        dispatched: list[int] = []

        async def no_cleanup(*args, **kwargs):
            return [], 0

        async def finish_before_recovery_claim(session, statement, *args, **kwargs):
            nonlocal raced
            rendered = str(statement)
            if not raced and rendered.startswith("UPDATE jobs SET"):
                raced = True
                async with factory() as writer:
                    current = await writer.get(Job, job.id)
                    assert current is not None
                    current.status = JobStatus.done
                    current.updated_at = datetime.now(UTC)
                    await writer.commit()
            return await original_execute(session, statement, *args, **kwargs)

        async def record_dispatch(job_id: int):
            dispatched.append(job_id)

        monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", no_cleanup)
        monkeypatch.setattr(AsyncSession, "execute", finish_before_recovery_claim)
        dispatcher = JobDispatcher(runner=AsyncMock(), session_factory=factory)
        monkeypatch.setattr(dispatcher, "dispatch", record_dispatch)

        recovered = await dispatcher.recover()

        assert raced is True
        assert recovered == []
        assert dispatched == []
        async with factory() as session:
            current = await session.get(Job, job.id)
            assert current is not None
            assert current.status == JobStatus.done
    finally:
        await engine.dispose()


async def test_cancel_waiting_job_persists_cancelled_before_task_exit(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first = await _make_job(session_factory, status=JobStatus.pending)
    second = await _make_job(session_factory, status=JobStatus.pending)
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def runner(job_id: int) -> None:
        if job_id == first.id:
            first_started.set()
            await release_first.wait()

    dispatcher = JobDispatcher(
        runner=runner,
        session_factory=session_factory,
        max_concurrent_jobs=1,
    )
    first_task = await dispatcher.dispatch(first.id)
    second_task = await dispatcher.dispatch(second.id)
    await asyncio.wait_for(first_started.wait(), timeout=1)

    await dispatcher.cancel_job(second.id)
    with pytest.raises(asyncio.CancelledError):
        await second_task

    async with session_factory() as db:
        persisted = await db.get(Job, second.id)
        assert persisted is not None
        assert persisted.status == JobStatus.cancelled

    release_first.set()
    await first_task
    await dispatcher.shutdown()


async def test_dispatcher_logs_and_consumes_execution_lease_loss(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def lost_runner(job_id: int) -> None:
        raise job_runner.ExecutionLeaseLost(f"job {job_id} lease lost")

    dispatcher = JobDispatcher(runner=lost_runner)
    with caplog.at_level("ERROR"):
        task = await dispatcher.dispatch(73)
        await asyncio.gather(task, return_exceptions=True)
    assert isinstance(task.exception(), job_runner.ExecutionLeaseLost)
    assert "Job 73 task raised unhandled exception" in caplog.text


async def test_fire_and_forget_dispatch_consumes_logged_lease_loss() -> None:
    contexts: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: contexts.append(context))

    async def lost_runner(job_id: int) -> None:
        raise job_runner.ExecutionLeaseLost(f"job {job_id} lease lost")

    try:
        dispatcher = JobDispatcher(runner=lost_runner)
        task = await dispatcher.dispatch(74)
        await asyncio.wait({task})
        await asyncio.sleep(0)
        del task
        gc.collect()
        await asyncio.sleep(0)
        assert not any(
            context.get("message") == "Task exception was never retrieved" for context in contexts
        )
    finally:
        loop.set_exception_handler(previous_handler)
