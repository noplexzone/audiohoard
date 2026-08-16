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
    pending_cleanup = SimpleNamespace(session_factory=factory)
    monkeypatch.setattr(
        main, "pending_imported_source_cleanups", AsyncMock(return_value=(pending_cleanup,))
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
        async def blocked_cleanup() -> None:
            cleanup_started.set()
            await asyncio.Event().wait()

        return asyncio.create_task(blocked_cleanup())

    monkeypatch.setattr(main, "schedule_imported_source_cleanup", schedule_blocked_cleanup)
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
            await cleanup_started.wait()
            task = app.state.catalog_ownership_reconciliation_task
            cleanup_task = app.state.startup_imported_source_cleanup_task
            assert task.done() is False
            assert cleanup_task.done() is False

    assert task.cancelled()
    assert cleanup_task.cancelled() or cleanup_task.done()
    assert startup_order == ["recover", "reconcile", "prune"]


async def test_startup_imported_cleanup_failure_does_not_fail_shutdown(
    db_session: AsyncSession,
    monkeypatch,
) -> None:
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    cleanup_started = asyncio.Event()

    class _Reconciliation(_Service):
        async def startup_reconcile(self) -> int:
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
    pending_cleanup = SimpleNamespace(session_factory=factory)
    monkeypatch.setattr(
        main, "pending_imported_source_cleanups", AsyncMock(return_value=(pending_cleanup,))
    )
    monkeypatch.setattr(main, "recover_deletion_operations", AsyncMock())
    monkeypatch.setattr(
        main,
        "prune_orphaned_terminal_records",
        AsyncMock(return_value=SimpleNamespace(tracks=0, releases=0, jobs=0)),
    )
    monkeypatch.setattr(main, "reconcile_duplicate_catalog_artists", AsyncMock(return_value=0))
    monkeypatch.setattr(main, "reconcile_deezer_release_snapshots", AsyncMock(return_value=0))
    monkeypatch.setattr(main, "reconcile_release_monitoring", AsyncMock(return_value=0))
    monkeypatch.setattr(
        main,
        "build_effective_settings",
        AsyncMock(return_value=Settings(secret_key="test-secret")),
    )
    monkeypatch.setattr(main, "recover_approved_downloads", AsyncMock(return_value=0))

    def schedule_failed_cleanup(*_args, **_kwargs):
        async def failed_cleanup() -> None:
            cleanup_started.set()
            raise RuntimeError("cleanup failed after startup")

        return asyncio.create_task(failed_cleanup())

    monkeypatch.setattr(main, "schedule_imported_source_cleanup", schedule_failed_cleanup)
    monkeypatch.setattr(main, "reconcile_deezer_catalog_ownership", AsyncMock(return_value=0))
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
    cleanup_done = asyncio.Event()
    async with main.lifespan(app):
        await cleanup_started.wait()
        app.state.startup_imported_source_cleanup_task.add_done_callback(
            lambda _task: cleanup_done.set()
        )
        await cleanup_done.wait()

    assert app.state.startup_imported_source_cleanup_task.done()
