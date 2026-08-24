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


async def test_serialized_startup_recovery_does_not_delay_readiness(
    db_session: AsyncSession,
    monkeypatch,
) -> None:
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    ownership_started = asyncio.Event()
    maintenance_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    startup_order: list[str] = []

    class _Reconciliation(_Service):
        async def startup_reconcile(self) -> int:
            startup_order.append("reconcile")
            await asyncio.Event().wait()
            return 0

    async def blocked_reconciliation(*_args, **_kwargs) -> int:
        ownership_started.set()
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
        startup_order.append("recover-deletions")

    async def prune(_db):
        startup_order.append("prune")
        maintenance_started.set()
        await asyncio.Event().wait()
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
            await maintenance_started.wait()
            task = app.state.catalog_ownership_reconciliation_task
            library_task = app.state.library_reconciliation_startup_task
            maintenance_task = app.state.startup_database_maintenance_task
            cleanup_task = app.state.startup_imported_source_cleanup_task
            assert task is library_task is maintenance_task
            assert task.done() is False
            assert ownership_started.is_set() is False
            assert cleanup_task is None
            assert cleanup_started.is_set() is False

    assert task.cancelled()
    assert startup_order == ["recover-deletions", "prune"]


async def test_startup_recovery_pipeline_serializes_database_writers(monkeypatch) -> None:
    entered: list[str] = []
    release = {name: asyncio.Event() for name in ("maintenance", "ownership", "library", "jobs")}

    async def stage(name: str) -> None:
        entered.append(name)
        await release[name].wait()

    monkeypatch.setattr(
        main, "_run_startup_database_maintenance", lambda _settings: stage("maintenance")
    )
    monkeypatch.setattr(
        main, "_reconcile_catalog_ownership_at_startup", lambda _settings: stage("ownership")
    )
    monkeypatch.setattr(
        main,
        "_run_library_reconciliation_at_startup",
        lambda _service: stage("library"),
    )
    monkeypatch.setattr(main, "_run_job_recovery_at_startup", lambda: stage("jobs"))

    background_started = asyncio.Event()

    async def start_background_services() -> None:
        entered.append("background")
        background_started.set()

    task = asyncio.create_task(
        main._run_startup_recovery_pipeline(
            Settings(secret_key="test-secret"),
            _Service(),
            start_background_services,
        )
    )
    await asyncio.sleep(0)
    assert entered == ["maintenance"]

    for current, expected in (
        ("maintenance", ["maintenance", "ownership"]),
        ("ownership", ["maintenance", "ownership", "library"]),
    ):
        release[current].set()
        await asyncio.sleep(0)
        assert entered == expected
        assert background_started.is_set() is False

    release["library"].set()
    await asyncio.sleep(0)
    assert entered == ["maintenance", "ownership", "library", "background", "jobs"]
    assert background_started.is_set()

    release["jobs"].set()
    await task
    assert entered == ["maintenance", "ownership", "library", "background", "jobs"]


async def test_background_service_startup_isolates_individual_failures(caplog) -> None:
    started: list[str] = []

    async def broken() -> None:
        started.append("broken")
        raise RuntimeError("start failed")

    async def healthy() -> None:
        started.append("healthy")

    await main._start_background_services((("broken service", broken), ("healthy", healthy)))

    assert started == ["broken", "healthy"]
    assert "Failed to start background service broken service" in caplog.text


async def test_startup_recovery_pipeline_isolates_unexpected_stage_failure(
    monkeypatch, caplog
) -> None:
    entered: list[str] = []

    async def broken_maintenance(_settings) -> None:
        entered.append("maintenance")
        raise RuntimeError("maintenance failed")

    async def ownership(_settings) -> None:
        entered.append("ownership")

    async def library(_service) -> None:
        entered.append("library")

    async def jobs() -> None:
        entered.append("jobs")

    async def background() -> None:
        entered.append("background")

    monkeypatch.setattr(main, "_run_startup_database_maintenance", broken_maintenance)
    monkeypatch.setattr(main, "_reconcile_catalog_ownership_at_startup", ownership)
    monkeypatch.setattr(main, "_run_library_reconciliation_at_startup", library)
    monkeypatch.setattr(main, "_run_job_recovery_at_startup", jobs)

    await main._run_startup_recovery_pipeline(
        Settings(secret_key="test-secret"), _Service(), background
    )

    assert entered == ["maintenance", "ownership", "library", "background", "jobs"]
    assert "Startup recovery stage failed: database maintenance" in caplog.text
