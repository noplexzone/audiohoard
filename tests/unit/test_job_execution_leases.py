from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import func, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings
from app.database import Base
from app.jobs import runner
from app.jobs.dispatcher import JobDispatcher
from app.models.acquisition_attempt import AcquisitionAttempt, ProviderTransferState
from app.models.job import Job, JobStatus
from app.models.track import Track


@pytest_asyncio.fixture
async def lease_factory(tmp_path: Path) -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'leases.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


async def _seed(factory: async_sessionmaker[AsyncSession]) -> int:
    async with factory() as session:
        job = Job(source="youtube", query="lease test", status=JobStatus.pending)
        session.add(job)
        await session.commit()
        return job.id


def _fast_settings(settings: Settings) -> Settings:
    return settings.model_copy(update={"job_watchdog_threshold_seconds": 0.18})


async def _disable_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services import acquisition_cleanup

    async def noop(*args: object, **kwargs: object) -> tuple[list[int], int]:
        return [], 0

    monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", noop)


async def test_simultaneous_background_claims_execute_once_and_persist_one_token(
    lease_factory: async_sessionmaker[AsyncSession],
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = await _seed(lease_factory)
    provider_tokens: list[str] = []
    release = asyncio.Event()

    async def execute(
        current_job_id: int,
        session: AsyncSession,
        cfg: Settings,
        *,
        commit_progress: bool = False,
        expected_token: str | None = None,
    ) -> None:
        assert commit_progress and expected_token is not None
        provider_tokens.append(expected_token)
        release.set()
        await asyncio.sleep(0.05)
        current = await session.get(Job, current_job_id)
        assert current is not None
        current.status = JobStatus.done
        await runner._commit_job_progress(session, current, expected_token)

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", execute)
    await _disable_cleanup(monkeypatch)

    await asyncio.gather(
        runner.run_job(job_id, settings=_fast_settings(test_settings)),
        runner.run_job(job_id, settings=_fast_settings(test_settings)),
    )

    assert release.is_set()
    assert len(provider_tokens) == 1
    assert len(provider_tokens[0]) == 36
    async with lease_factory() as observer:
        persisted = await observer.get(Job, job_id)
        assert persisted is not None
        assert persisted.status == JobStatus.done
        assert persisted.execution_token is None
        assert persisted.execution_lease_expires_at is None


async def test_heartbeat_extends_lease_and_keeps_updated_at_fresh_for_current_watchdog(
    lease_factory: async_sessionmaker[AsyncSession],
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = await _seed(lease_factory)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def block(
        current_job_id: int,
        session: AsyncSession,
        cfg: Settings,
        *,
        commit_progress: bool = False,
        expected_token: str | None = None,
    ) -> None:
        assert expected_token is not None
        entered.set()
        await release.wait()
        current = await session.get(Job, current_job_id)
        assert current is not None
        current.status = JobStatus.done
        await runner._commit_job_progress(session, current, expected_token)

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", block)
    await _disable_cleanup(monkeypatch)
    cfg = _fast_settings(test_settings)
    task = asyncio.create_task(runner.run_job(job_id, settings=cfg))
    await asyncio.wait_for(entered.wait(), 2)
    async with lease_factory() as observer:
        first = await observer.get(Job, job_id)
        assert first is not None and first.execution_lease_expires_at is not None
        original_expiry = first.execution_lease_expires_at
    await asyncio.sleep(0.42)
    async with lease_factory() as observer:
        current = await observer.get(Job, job_id)
        assert current is not None and current.execution_lease_expires_at is not None
        assert current.execution_lease_expires_at > original_expiry + timedelta(seconds=0.18)
        assert current.updated_at > datetime.now(UTC).replace(tzinfo=None) - timedelta(
            seconds=0.18
        )
    dispatcher = JobDispatcher(session_factory=lease_factory)
    await dispatcher._watchdog_tick(1)
    async with lease_factory() as observer:
        current = await observer.get(Job, job_id)
        assert current is not None
        assert current.status == JobStatus.running
    release.set()
    await task


async def test_watchdog_takes_over_only_expired_lease_and_preserves_active_attempt(
    lease_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    async with lease_factory() as session:
        job = Job(
            source="slskd",
            query="expired lease",
            status=JobStatus.running,
            execution_token="expired-owner",
            execution_lease_expires_at=now - timedelta(seconds=1),
            updated_at=now,
        )
        session.add(job)
        await session.flush()
        attempt = AcquisitionAttempt(
            job_id=job.id,
            provider="slskd",
            peer="peer",
            remote_path="Album/01.flac",
            provider_uuid="11111111-1111-1111-1111-111111111111",
            provider_state=ProviderTransferState.queued,
        )
        session.add(attempt)
        await session.commit()
        job_id = job.id
        attempt_id = attempt.id

    dispatched: list[int] = []
    dispatcher = JobDispatcher(session_factory=lease_factory)

    async def record_dispatch(current_job_id: int) -> None:
        dispatched.append(current_job_id)

    monkeypatch.setattr(dispatcher, "dispatch", record_dispatch)
    await dispatcher._watchdog_tick(threshold_seconds=300)

    assert dispatched == [job_id]
    async with lease_factory() as observer:
        current = await observer.get(Job, job_id)
        attempt = await observer.get(AcquisitionAttempt, attempt_id)
        assert current is not None
        assert current.status == JobStatus.pending
        assert current.execution_token is None
        assert current.execution_lease_expires_at is None
        assert '"attempt": 1' in (current.result_json or "")
        assert attempt is not None
        assert attempt.provider_state == ProviderTransferState.queued
        assert attempt.provider_uuid == "11111111-1111-1111-1111-111111111111"


async def test_watchdog_does_not_take_over_unexpired_lease_even_when_updated_at_is_stale(
    lease_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    async with lease_factory() as session:
        job = Job(
            source="slskd",
            query="healthy lease",
            status=JobStatus.running,
            execution_token="healthy-owner",
            execution_lease_expires_at=now + timedelta(minutes=5),
            updated_at=now - timedelta(hours=1),
        )
        session.add(job)
        await session.commit()
        job_id = job.id

    dispatched: list[int] = []
    dispatcher = JobDispatcher(session_factory=lease_factory)

    async def record_dispatch(current_job_id: int) -> None:
        dispatched.append(current_job_id)

    monkeypatch.setattr(dispatcher, "dispatch", record_dispatch)
    await dispatcher._watchdog_tick(threshold_seconds=1)

    assert dispatched == []
    async with lease_factory() as observer:
        current = await observer.get(Job, job_id)
        assert current is not None
        assert current.status == JobStatus.running
        assert current.execution_token == "healthy-owner"


async def test_repeated_expired_tokenized_recovery_remains_retryable(
    lease_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    async with lease_factory() as session:
        job = Job(
            source="slskd",
            query="second recovery",
            status=JobStatus.running,
            result_json='{"watchdog_recovery": {"attempt": 1}}',
            execution_token="second-expired-owner",
            execution_lease_expires_at=now - timedelta(seconds=1),
            updated_at=now,
        )
        session.add(job)
        await session.commit()
        job_id = job.id

    dispatched: list[int] = []
    dispatcher = JobDispatcher(session_factory=lease_factory)

    async def record_dispatch(current_job_id: int) -> None:
        dispatched.append(current_job_id)

    monkeypatch.setattr(dispatcher, "dispatch", record_dispatch)
    await dispatcher._watchdog_tick(threshold_seconds=300)

    assert dispatched == [job_id]
    async with lease_factory() as observer:
        current = await observer.get(Job, job_id)
        assert current is not None
        assert current.status == JobStatus.pending
        assert '"attempt": 2' in (current.result_json or "")
        assert "dispatch_lost" not in (current.result_json or "")


async def test_startup_recovery_waits_for_live_lease_and_reclaims_expired_owner(
    lease_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    async with lease_factory() as session:
        live = Job(
            source="slskd",
            query="live lease",
            status=JobStatus.running,
            execution_token="live-owner",
            execution_lease_expires_at=now + timedelta(minutes=5),
        )
        expired = Job(
            source="slskd",
            query="expired owner",
            status=JobStatus.running,
            execution_token="expired-owner",
            execution_lease_expires_at=now - timedelta(seconds=1),
        )
        session.add_all([live, expired])
        await session.commit()
        live_id, expired_id = live.id, expired.id

    dispatched: list[int] = []
    dispatcher = JobDispatcher(session_factory=lease_factory)

    async def record_dispatch(current_job_id: int) -> None:
        dispatched.append(current_job_id)

    monkeypatch.setattr(dispatcher, "dispatch", record_dispatch)
    recovered = await dispatcher.recover()

    assert recovered == [expired_id]
    assert dispatched == [expired_id]
    async with lease_factory() as observer:
        live = await observer.get(Job, live_id)
        expired = await observer.get(Job, expired_id)
        assert live is not None and live.status == JobStatus.running
        assert live.execution_token == "live-owner"
        assert expired is not None and expired.status == JobStatus.pending
        assert expired.execution_token is None
        assert expired.execution_lease_expires_at is None


async def test_token_replacement_before_progress_rolls_back_all_execution_writes(
    lease_factory: async_sessionmaker[AsyncSession],
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = await _seed(lease_factory)

    async def stale_progress(
        current_job_id: int,
        session: AsyncSession,
        cfg: Settings,
        *,
        commit_progress: bool = False,
        expected_token: str | None = None,
    ) -> None:
        assert expected_token is not None
        session.add_all(
            [
                Job(source="youtube", query="stale sibling", status=JobStatus.pending),
                AcquisitionAttempt(job_id=current_job_id, provider="youtube"),
                Track(job_id=current_job_id, source="youtube", title="stale track"),
            ]
        )
        async with lease_factory() as takeover:
            await takeover.execute(
                update(Job)
                .where(Job.id == current_job_id)
                .values(execution_token="replacement-token", updated_at=datetime.now(UTC))
            )
            await takeover.commit()
        current = await session.get(Job, current_job_id)
        assert current is not None
        await runner._commit_job_progress(session, current, expected_token)

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", stale_progress)
    await _disable_cleanup(monkeypatch)

    with pytest.raises(runner.ExecutionLeaseLost):
        await runner.run_job(job_id, settings=_fast_settings(test_settings))

    async with lease_factory() as observer:
        assert await observer.scalar(select(func.count(Job.id))) == 1
        assert await observer.scalar(select(func.count(AcquisitionAttempt.id))) == 0
        assert await observer.scalar(select(func.count(Track.id))) == 0
        current = await observer.get(Job, job_id)
        assert current is not None
        assert current.status == JobStatus.running
        assert current.execution_token == "replacement-token"


async def test_token_replacement_before_terminal_commit_rolls_back_entire_transaction(
    lease_factory: async_sessionmaker[AsyncSession],
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = await _seed(lease_factory)

    async def stale_terminal(
        current_job_id: int,
        session: AsyncSession,
        cfg: Settings,
        *,
        commit_progress: bool = False,
        expected_token: str | None = None,
    ) -> None:
        assert expected_token is not None
        current = await session.get(Job, current_job_id)
        assert current is not None
        current.status = JobStatus.done
        current.result_json = '{"stale": "terminal"}'
        session.add_all(
            [
                Job(source="youtube", query="stale terminal sibling", status=JobStatus.pending),
                AcquisitionAttempt(job_id=current_job_id, provider="youtube"),
                Track(job_id=current_job_id, source="youtube", title="stale terminal track"),
            ]
        )
        async with lease_factory() as takeover:
            await takeover.execute(
                update(Job)
                .where(Job.id == current_job_id)
                .values(execution_token="replacement-token", updated_at=datetime.now(UTC))
            )
            await takeover.commit()
        await runner._commit_job_progress(session, current, expected_token)

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", stale_terminal)
    await _disable_cleanup(monkeypatch)

    with pytest.raises(runner.ExecutionLeaseLost):
        await runner.run_job(job_id, settings=_fast_settings(test_settings))

    async with lease_factory() as observer:
        assert await observer.scalar(select(func.count(Job.id))) == 1
        assert await observer.scalar(select(func.count(AcquisitionAttempt.id))) == 0
        assert await observer.scalar(select(func.count(Track.id))) == 0
        current = await observer.get(Job, job_id)
        assert current is not None
        assert current.status == JobStatus.running
        assert current.execution_token == "replacement-token"
        assert current.result_json is None


@pytest.mark.parametrize("terminal_status", [JobStatus.done, JobStatus.failed])
async def test_terminal_transaction_clears_lease_atomically(
    lease_factory: async_sessionmaker[AsyncSession],
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    terminal_status: JobStatus,
) -> None:
    job_id = await _seed(lease_factory)

    async def terminal(
        current_job_id: int,
        session: AsyncSession,
        cfg: Settings,
        *,
        commit_progress: bool = False,
        expected_token: str | None = None,
    ) -> None:
        current = await session.get(Job, current_job_id)
        assert current is not None and expected_token is not None
        current.status = terminal_status
        await runner._commit_job_progress(session, current, expected_token)

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", terminal)
    await _disable_cleanup(monkeypatch)
    await runner.run_job(job_id, settings=_fast_settings(test_settings))

    async with lease_factory() as observer:
        current = await observer.get(Job, job_id)
        assert current is not None
        assert current.status == terminal_status
        assert current.execution_token is None
        assert current.execution_lease_expires_at is None


async def test_direct_run_remains_tokenless_and_does_not_force_commit(
    lease_factory: async_sessionmaker[AsyncSession],
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = await _seed(lease_factory)
    async with lease_factory() as direct:
        calls: list[tuple[bool, str | None]] = []

        async def execute(
            current_job_id: int,
            session: AsyncSession,
            cfg: Settings,
            *,
            commit_progress: bool = False,
            expected_token: str | None = None,
        ) -> None:
            calls.append((commit_progress, expected_token))
            current = await session.get(Job, current_job_id)
            assert current is not None
            current.status = JobStatus.done
            await session.flush()

        monkeypatch.setattr(runner, "_run_job_in_session", execute)
        await runner.run_job(job_id, db=direct, settings=test_settings)
        assert calls == [(False, None)]
        current = await direct.get(Job, job_id)
        assert current is not None
        assert current.execution_token is None
        async with lease_factory() as observer:
            persisted = await observer.get(Job, job_id)
            assert persisted is not None
            assert persisted.status == JobStatus.pending


async def test_stale_token_then_ordinary_failure_skips_cleanup_and_surfaces_lease_loss(
    lease_factory, test_settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id = await _seed(lease_factory)
    cleanup_calls = []

    async def fail(current_job_id, session, cfg, **kwargs):
        async with lease_factory() as takeover:
            await takeover.execute(
                update(Job)
                .where(Job.id == current_job_id)
                .values(execution_token="replacement-token")
            )
            await takeover.commit()
        raise RuntimeError("provider failed after takeover")

    async def cleanup(*args, job_ids, **kwargs):
        cleanup_calls.append(job_ids)

    from app.services import acquisition_cleanup

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", fail)
    monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", cleanup)
    with pytest.raises(runner.ExecutionLeaseLost):
        await runner.run_job(job_id, settings=_fast_settings(test_settings))
    assert cleanup_calls == []


async def test_stale_token_then_cancellation_retains_cancellation_without_cleanup(
    lease_factory, test_settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id = await _seed(lease_factory)
    cleanup_calls = []

    async def cancel(current_job_id, session, cfg, **kwargs):
        async with lease_factory() as takeover:
            await takeover.execute(
                update(Job)
                .where(Job.id == current_job_id)
                .values(execution_token="replacement-token")
            )
            await takeover.commit()
        raise asyncio.CancelledError

    async def cleanup(*args, job_ids, **kwargs):
        cleanup_calls.append(job_ids)

    from app.services import acquisition_cleanup

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", cancel)
    monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", cleanup)
    with pytest.raises(asyncio.CancelledError):
        await runner.run_job(job_id, settings=_fast_settings(test_settings))
    assert cleanup_calls == []


async def test_real_sqlite_fence_lock_retries_without_replaying_execution(lease_factory) -> None:
    job_id = await _seed(lease_factory)
    token = "owned-token"
    async with lease_factory() as owner:
        current = await owner.get(Job, job_id)
        current.status = JobStatus.running
        current.execution_token = token
        await owner.commit()

    provider_calls = 0
    locked = asyncio.Event()
    release_lock = asyncio.Event()

    async def competing_writer():
        async with lease_factory() as blocker:
            connection = await blocker.connection()
            await connection.exec_driver_sql("PRAGMA busy_timeout=0")
            await connection.exec_driver_sql("BEGIN IMMEDIATE")
            locked.set()
            await release_lock.wait()
            await blocker.rollback()

    blocker = asyncio.create_task(competing_writer())
    await asyncio.wait_for(locked.wait(), 1)
    async with lease_factory() as execution:
        current = await execution.get(Job, job_id)
        provider_calls += 1
        current.status = JobStatus.done
        execution.add(Job(source="youtube", query="preserved sibling", status=JobStatus.pending))
        asyncio.get_running_loop().call_later(1.2, release_lock.set)
        await runner._commit_job_progress(execution, current, token)
    await blocker
    assert provider_calls == 1
    async with lease_factory() as observer:
        current = await observer.get(Job, job_id)
        assert current.status == JobStatus.done and current.execution_token is None
        assert (
            await observer.scalar(
                select(func.count(Job.id)).where(Job.query == "preserved sibling")
            )
            == 1
        )


async def test_heartbeat_retries_transient_sqlite_lock(lease_factory, monkeypatch) -> None:
    job_id = await _seed(lease_factory)
    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    token = "heartbeat-token"
    async with lease_factory() as session:
        current = await session.get(Job, job_id)
        current.status = JobStatus.running
        current.execution_token = token
        await session.commit()
    original_execute = AsyncSession.execute
    attempts = 0
    retried = asyncio.Event()

    async def lock_once(session, statement, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError("UPDATE jobs", {}, Exception("database is locked"))
        retried.set()
        return await original_execute(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", lock_once)
    stop = asyncio.Event()
    heartbeat = asyncio.create_task(
        runner._heartbeat_execution_lease(job_id, token, timedelta(seconds=0.03), stop)
    )
    await asyncio.wait_for(retried.wait(), 1)
    stop.set()
    await asyncio.wait_for(heartbeat, 1)
    assert attempts >= 2


async def test_terminal_heartbeat_failure_cancels_execution_and_surfaces_lease_loss(
    lease_factory, test_settings, monkeypatch
) -> None:
    job_id = await _seed(lease_factory)
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    cleanup_calls = []

    async def blocked(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def failed_heartbeat(*args, **kwargs):
        await entered.wait()
        raise RuntimeError("heartbeat database failed")

    async def cleanup(*args, job_ids, **kwargs):
        cleanup_calls.append(job_ids)

    from app.services import acquisition_cleanup

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", blocked)
    monkeypatch.setattr(runner, "_heartbeat_execution_lease", failed_heartbeat)
    monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", cleanup)
    with pytest.raises(runner.ExecutionLeaseLost, match="heartbeat"):
        await runner.run_job(job_id, settings=_fast_settings(test_settings))
    assert cancelled.is_set()
    assert cleanup_calls == []


async def test_tokenized_watchdog_recovery_provenance_survives_pending_gap(
    lease_factory, monkeypatch
) -> None:
    now = datetime.now(UTC)
    async with lease_factory() as session:
        job = Job(
            source="slskd",
            query="gap",
            status=JobStatus.running,
            execution_token="expired",
            execution_lease_expires_at=now - timedelta(seconds=1),
            updated_at=now,
        )
        session.add(job)
        await session.commit()
        job_id = job.id
    dispatcher = JobDispatcher(session_factory=lease_factory)
    dispatched = []

    async def record(job_id):
        dispatched.append(job_id)

    monkeypatch.setattr(dispatcher, "dispatch", record)
    await dispatcher._watchdog_tick(1)
    async with lease_factory() as session:
        current = await session.get(Job, job_id)
        payload = __import__("json").loads(current.result_json)
        assert payload["watchdog_recovery"]["origin"] == "tokenized"
        current.updated_at = now - timedelta(seconds=5)
        await session.commit()
    await dispatcher._watchdog_tick(1)
    async with lease_factory() as session:
        current = await session.get(Job, job_id)
        payload = __import__("json").loads(current.result_json)
        assert current.status == JobStatus.pending
        assert payload["watchdog_recovery"] == {"attempt": 2, "origin": "tokenized"}
    assert dispatched == [job_id, job_id]


async def test_malformed_watchdog_attempt_does_not_poison_other_recovery(
    lease_factory, monkeypatch
) -> None:
    now = datetime.now(UTC)
    async with lease_factory() as session:
        jobs = [
            Job(
                source="slskd",
                query="bad",
                status=JobStatus.running,
                result_json='{\\"watchdog_recovery\\":{\\"attempt\\":null}}',
                execution_token="bad-token",
                execution_lease_expires_at=now - timedelta(seconds=1),
            ),
            Job(
                source="slskd",
                query="good",
                status=JobStatus.running,
                execution_token="good-token",
                execution_lease_expires_at=now - timedelta(seconds=1),
            ),
        ]
        session.add_all(jobs)
        await session.commit()
        ids = [job.id for job in jobs]
    dispatcher = JobDispatcher(session_factory=lease_factory)

    async def ignore(job_id):
        pass

    monkeypatch.setattr(dispatcher, "dispatch", ignore)
    await dispatcher._watchdog_tick(1)
    async with lease_factory() as session:
        rows = [await session.get(Job, job_id) for job_id in ids]
        assert all(row.status == JobStatus.pending for row in rows)
        assert all(
            __import__("json").loads(row.result_json)["watchdog_recovery"]["attempt"] == 1
            for row in rows
        )


async def test_startup_repairs_pending_row_with_stranded_token(lease_factory, monkeypatch) -> None:
    async with lease_factory() as session:
        job = Job(
            source="youtube",
            query="stranded",
            status=JobStatus.pending,
            execution_token="malformed-token",
            execution_lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        session.add(job)
        await session.commit()
        job_id = job.id
    dispatcher = JobDispatcher(session_factory=lease_factory)
    dispatched = []

    async def record(current_job_id):
        dispatched.append(current_job_id)

    monkeypatch.setattr(dispatcher, "dispatch", record)
    assert await dispatcher.recover() == [job_id]
    async with lease_factory() as session:
        current = await session.get(Job, job_id)
        assert current.execution_token is None and current.execution_lease_expires_at is None
    assert dispatched == [job_id]


async def test_heartbeat_ownership_loss_cancels_blocked_execution(
    lease_factory, test_settings, monkeypatch
) -> None:
    job_id = await _seed(lease_factory)
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    cleanup_calls = []

    async def blocked(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def cleanup(*args, job_ids, **kwargs):
        cleanup_calls.append(job_ids)

    from app.services import acquisition_cleanup

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", blocked)
    monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", cleanup)
    run = asyncio.create_task(runner.run_job(job_id, settings=_fast_settings(test_settings)))
    await asyncio.wait_for(entered.wait(), 1)
    async with lease_factory() as takeover:
        await takeover.execute(
            update(Job).where(Job.id == job_id).values(execution_token="replacement-token")
        )
        await takeover.commit()
    with pytest.raises(runner.ExecutionLeaseLost, match="heartbeat"):
        await asyncio.wait_for(run, 1)
    assert cancelled.is_set()
    assert cleanup_calls == []


@pytest.mark.parametrize("cancelled", [False, True])
async def test_terminal_persistence_failure_never_enables_cleanup_or_masks_cancellation(
    lease_factory, test_settings, monkeypatch, cancelled: bool
) -> None:
    job_id = await _seed(lease_factory)
    cleanup_calls = []

    async def fail(*args, **kwargs):
        if cancelled:
            raise asyncio.CancelledError
        raise RuntimeError("original execution failure")

    async def persistence_failure(*args, **kwargs):
        raise RuntimeError("terminal persistence failed")

    async def cleanup(*args, job_ids, **kwargs):
        cleanup_calls.append(job_ids)

    from app.services import acquisition_cleanup

    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", fail)
    monkeypatch.setattr(runner, "_persist_job_envelope", persistence_failure)
    monkeypatch.setattr(acquisition_cleanup, "cleanup_terminal_acquisitions", cleanup)
    expected = asyncio.CancelledError if cancelled else runner.ExecutionLeaseLost
    with pytest.raises(expected):
        await runner.run_job(job_id, settings=_fast_settings(test_settings))
    assert cleanup_calls == []
    async with lease_factory() as observer:
        current = await observer.get(Job, job_id)
        assert current is not None and current.status == JobStatus.running


async def test_startup_tokenized_recovery_provenance_survives_watchdog_gaps(
    lease_factory, monkeypatch
) -> None:
    now = datetime.now(UTC)
    async with lease_factory() as session:
        job = Job(
            source="slskd",
            query="startup gap",
            status=JobStatus.running,
            execution_token="expired-startup-owner",
            execution_lease_expires_at=now - timedelta(seconds=1),
            updated_at=now,
        )
        session.add(job)
        await session.commit()
        job_id = job.id
    dispatcher = JobDispatcher(session_factory=lease_factory)
    dispatched = []

    async def record(current_job_id):
        dispatched.append(current_job_id)

    monkeypatch.setattr(dispatcher, "dispatch", record)
    assert await dispatcher.recover() == [job_id]
    for expected_attempt in (1, 2):
        async with lease_factory() as session:
            current = await session.get(Job, job_id)
            current.updated_at = datetime.now(UTC) - timedelta(seconds=5)
            await session.commit()
        await dispatcher._watchdog_tick(1)
        async with lease_factory() as session:
            current = await session.get(Job, job_id)
            payload = __import__("json").loads(current.result_json)
            assert current.status == JobStatus.pending
            assert payload["watchdog_recovery"] == {
                "attempt": expected_attempt,
                "origin": "tokenized",
            }
    assert dispatched == [job_id, job_id, job_id]


async def test_normal_terminal_token_clear_cannot_cancel_continuation_creation(
    lease_factory, test_settings, monkeypatch
) -> None:
    job_id = await _seed(lease_factory)
    terminal_committed = asyncio.Event()
    allow_return = asyncio.Event()
    spawned = asyncio.Event()
    dispatched: list[int] = []

    async def terminal_then_pause(
        current_job_id,
        session,
        cfg,
        *,
        commit_progress=False,
        expected_token=None,
        heartbeat_stop=None,
    ):
        assert expected_token is not None and heartbeat_stop is not None
        current = await session.get(Job, current_job_id)
        assert current is not None
        current.status = JobStatus.partial
        await runner._commit_job_progress(
            session,
            current,
            expected_token,
            heartbeat_stop=heartbeat_stop,
        )
        terminal_committed.set()
        await allow_return.wait()
        return runner._ContinuationRequest(
            parent_job_id=current_job_id,
            catalog_album_id=123,
            missing_catalog_track_ids=(456,),
        )

    async def spawn(*args, **kwargs):
        spawned.set()
        return [789]

    async def dispatch(ids):
        dispatched.extend(ids)

    await _disable_cleanup(monkeypatch)
    monkeypatch.setattr(runner, "get_session_factory", lambda: lease_factory)
    monkeypatch.setattr(runner, "_run_job_in_session", terminal_then_pause)
    monkeypatch.setattr(runner, "_spawn_continuation_jobs", spawn)
    monkeypatch.setattr(runner, "_dispatch_continuation_jobs", dispatch)

    run = asyncio.create_task(runner.run_job(job_id, settings=_fast_settings(test_settings)))
    await asyncio.wait_for(terminal_committed.wait(), 1)
    await asyncio.sleep(0.25)
    assert not run.done()
    allow_return.set()
    await asyncio.wait_for(run, 1)
    assert spawned.is_set()
    assert dispatched == [789]
    async with lease_factory() as observer:
        current = await observer.get(Job, job_id)
        assert current is not None
        assert current.status == JobStatus.partial
        assert current.execution_token is None
