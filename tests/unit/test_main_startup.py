from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import main
from app.config import Settings


class _Service:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


async def test_startup_ownership_reconciliation_does_not_delay_readiness(
    db_session: AsyncSession,
    monkeypatch,
) -> None:
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    startup_order: list[str] = []

    class _Reconciliation(_Service):
        async def startup_reconcile(self) -> int:
            startup_order.append("reconcile")
            await asyncio.Event().wait()
            return 0

    async def blocked_reconciliation(*_args, **_kwargs) -> int:
        started.set()
        await asyncio.Event().wait()
        return 0

    monkeypatch.setattr(main, "get_session_factory", lambda: factory)
    monkeypatch.setattr(
        main,
        "get_runtime_settings",
        AsyncMock(
            return_value=SimpleNamespace(
                max_parallel_acquisitions=1,
                acoustid_acceptance_threshold=0.91,
            )
        ),
    )

    async def recover_deletions(*_args, **_kwargs) -> None:
        startup_order.append("recover")

    async def prune(_db):
        startup_order.append("prune")
        return SimpleNamespace(tracks=0, releases=0, jobs=0)

    monkeypatch.setattr(main, "recover_deletion_operations", recover_deletions)
    monkeypatch.setattr(main, "prune_orphaned_terminal_records", prune)
    monkeypatch.setattr(main, "reconcile_duplicate_catalog_artists", AsyncMock(return_value=0))
    monkeypatch.setattr(main, "reconcile_deezer_release_snapshots", AsyncMock(return_value=0))
    monkeypatch.setattr(
        main,
        "build_effective_settings",
        AsyncMock(return_value=Settings(secret_key="test-secret")),
    )
    monkeypatch.setattr(main, "recover_approved_downloads", AsyncMock(return_value=0))

    def schedule_blocked_cleanup(*_args, **_kwargs):
        cleanup_started.set()
        raise AssertionError("startup cleanup should not be scheduled before readiness")

    monkeypatch.setattr(
        main, "schedule_imported_source_cleanup", schedule_blocked_cleanup, raising=False
    )
    monkeypatch.setattr(main, "reconcile_deezer_catalog_ownership", blocked_reconciliation)
    monkeypatch.setattr(main, "DiscographyRefreshScheduler", _Service)
    monkeypatch.setattr(main, "MaintenanceScheduler", _Service)
    monkeypatch.setattr(main, "MonitoringScheduler", _Service)
    monkeypatch.setattr(main, "QualityUpgradeCycleScheduler", _Service)
    monkeypatch.setattr(main, "ReviewAutomationScheduler", _Service)
    monkeypatch.setattr(main, "LibraryReconciliationService", _Reconciliation)
    monkeypatch.setattr(main, "get_health_status_service", lambda: _Service())
    monkeypatch.setattr(main.job_dispatcher, "set_max_concurrent_jobs", AsyncMock())
    monkeypatch.setattr(main.job_dispatcher, "recover", AsyncMock())
    monkeypatch.setattr(main.job_dispatcher, "start_watchdog", AsyncMock())
    monkeypatch.setattr(main.job_dispatcher, "start_cleanup_reconciler", AsyncMock())
    monkeypatch.setattr(main.job_dispatcher, "shutdown", AsyncMock())

    app = FastAPI()
    async with asyncio.timeout(1):
        async with main.lifespan(app):
            await started.wait()
            task = app.state.catalog_ownership_reconciliation_task
            library_task = app.state.library_reconciliation_startup_task
            cleanup_task = app.state.startup_imported_source_cleanup_task
            assert task.done() is False
            assert library_task.done() is False
            assert cleanup_task is None
            assert cleanup_started.is_set() is False

    assert task.cancelled()
    assert library_task.cancelled()
    assert startup_order == ["recover", "prune", "reconcile"]
