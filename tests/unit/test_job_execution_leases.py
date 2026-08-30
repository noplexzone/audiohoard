from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings
from app.database import Base
from app.jobs import runner
from app.jobs.dispatcher import JobDispatcher
from app.models.acquisition_attempt import AcquisitionAttempt
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
