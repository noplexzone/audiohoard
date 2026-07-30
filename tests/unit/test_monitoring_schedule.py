from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.models.job import Job, JobStatus
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release import Release
from app.services.monitoring import QualityUpgradeCycleScheduler
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


def _runtime(hours: int):
    return SimpleNamespace(
        upgrade_check_hours=hours,
        quality_profile=QualityProfile(["flac", "mp3", "m4a/aac", "ogg", "opus"], 320, True),
    )


@pytest.mark.asyncio
async def test_upgrade_check_hours_zero_runs_no_checks(db_session, monkeypatch) -> None:
    scheduler = QualityUpgradeCycleScheduler()
    monkeypatch.setattr(
        "app.services.monitoring.get_session_factory", lambda: _Factory(db_session)
    )

    async def fake_runtime(db):
        return _runtime(0)

    async def fail_check(*args, **kwargs):
        raise AssertionError("monitoring check should not run")

    monkeypatch.setattr("app.services.monitoring.get_runtime_settings", fake_runtime)
    monkeypatch.setattr("app.services.monitoring.run_monitoring_check", fail_check)

    assert await scheduler._refresh_cycle() == 3600.0


@pytest.mark.asyncio
async def test_upgrade_check_hours_runs_active_record_and_persists_result(
    db_session, test_settings, monkeypatch
) -> None:
    job = Job(source="slskd", query="Artist Album", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    record = MonitoringRecord(release=release, status=MonitoringStatus.active)
    db_session.add_all([job, release, record])
    await db_session.flush()
    scheduler = QualityUpgradeCycleScheduler()
    monkeypatch.setattr(
        "app.services.monitoring.get_session_factory", lambda: _Factory(db_session)
    )

    async def fake_runtime(db):
        return _runtime(1)

    async def fake_effective(db, settings):
        return test_settings

    calls = []

    def fake_discovery(db, cfg, record_arg, *, checkpoint=None):
        assert checkpoint is not None

        async def discover():
            return []

        return discover

    async def fake_check(db, record_arg, current_quality, discover, *, checkpoint=None):
        assert checkpoint is not None
        calls.append((record_arg.id, current_quality))
        record_arg.status = MonitoringStatus.failed
        await db.flush()
        return None

    monkeypatch.setattr("app.services.monitoring.get_runtime_settings", fake_runtime)
    monkeypatch.setattr("app.services.monitoring.build_effective_settings", fake_effective)
    monkeypatch.setattr("app.services.quality_discovery.build_upgrade_discovery", fake_discovery)
    monkeypatch.setattr("app.services.monitoring.run_monitoring_check", fake_check)

    assert await scheduler._refresh_cycle() == 3600.0
    refreshed = await db_session.scalar(
        select(MonitoringRecord).where(MonitoringRecord.id == record.id)
    )

    assert calls == [(record.id, {"codec": "", "lossless": False, "reliability": 1.0})]
    assert refreshed.status == MonitoringStatus.failed
