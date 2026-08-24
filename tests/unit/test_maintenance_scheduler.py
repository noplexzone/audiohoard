from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services.maintenance_scheduler import MaintenanceScheduler
from app.services.maintenance_state import empty_maintenance_state
from app.settings_service import QualityProfile


class _SessionContext:
    def __init__(self, db) -> None:
        self.db = db

    async def __aenter__(self):
        return self.db

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _Factory:
    def __init__(self, db) -> None:
        self.db = db

    def __call__(self):
        return _SessionContext(self.db)


def _runtime(**overrides):
    values = {
        "library_scan_hours": 0,
        "duplicate_scan_hours": 0,
        "duplicate_auto_clean": False,
        "quality_profile": QualityProfile(["flac", "mp3", "m4a/aac", "ogg", "opus"], 320, True),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.asyncio
async def test_disabled_cycle_performs_no_scan_or_delete(
    db_session, test_settings, monkeypatch
) -> None:
    scheduler = MaintenanceScheduler(empty_maintenance_state())
    monkeypatch.setattr(
        "app.services.maintenance_scheduler.get_session_factory",
        lambda: _Factory(db_session),
    )

    async def fake_effective(db, settings):
        return test_settings

    async def fake_runtime(db):
        return _runtime()

    monkeypatch.setattr(
        "app.services.maintenance_scheduler.build_effective_settings", fake_effective
    )
    monkeypatch.setattr("app.services.maintenance_scheduler.get_runtime_settings", fake_runtime)

    async def fail_scan(*args, **kwargs):
        raise AssertionError("scan should not run")

    monkeypatch.setattr("app.services.maintenance_scheduler.scan_library_filesystem", fail_scan)
    monkeypatch.setattr("app.services.maintenance_scheduler.scan_library_duplicates", fail_scan)
    monkeypatch.setattr(
        "app.services.maintenance_scheduler.clean_safe_library_duplicates", fail_scan
    )

    delay = await scheduler._refresh_cycle()

    assert delay == 3600.0


@pytest.mark.asyncio
async def test_duplicate_scan_without_auto_clean_runs_dry_run_only(
    db_session, test_settings, monkeypatch
) -> None:
    scheduler = MaintenanceScheduler(empty_maintenance_state())
    monkeypatch.setattr(
        "app.services.maintenance_scheduler.get_session_factory",
        lambda: _Factory(db_session),
    )

    async def fake_effective(db, settings):
        return test_settings

    async def fake_runtime(db):
        return _runtime(duplicate_scan_hours=1)

    monkeypatch.setattr(
        "app.services.maintenance_scheduler.build_effective_settings", fake_effective
    )
    monkeypatch.setattr("app.services.maintenance_scheduler.get_runtime_settings", fake_runtime)
    calls: list[dict[str, object]] = []

    async def fake_candidates(db):
        return [7]

    async def fake_reconcile(db, album_id, **kwargs):
        calls.append({"album_id": album_id, **kwargs})
        from app.services.quality_upgrade import QualityDuplicateResult

        return QualityDuplicateResult(deleted_files=1)

    async def fail_clean(*args, **kwargs):
        raise AssertionError("real clean should not run")

    monkeypatch.setattr(
        "app.services.maintenance_workflows.duplicate_candidate_album_ids", fake_candidates
    )
    monkeypatch.setattr(
        "app.services.maintenance_workflows.reconcile_album_quality_duplicates", fake_reconcile
    )
    monkeypatch.setattr(
        "app.services.maintenance_scheduler.clean_safe_library_duplicates", fail_clean
    )

    delay = await scheduler._refresh_cycle()

    assert delay == 3600.0
    assert calls and calls[0]["dry_run"] is True


@pytest.mark.asyncio
async def test_start_can_wait_for_initial_cycle(monkeypatch) -> None:
    scheduler = MaintenanceScheduler(empty_maintenance_state())
    cycles: list[str] = []

    async def cycle() -> float:
        cycles.append("initial")
        return 3600.0

    monkeypatch.setattr(scheduler, "_refresh_cycle", cycle)
    await scheduler.start(wait_for_initial_cycle=True)
    try:
        assert cycles == ["initial"]
    finally:
        await scheduler.stop()
