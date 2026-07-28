from __future__ import annotations

import time
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.services.health_status import HealthStatusService
from app.sources.base import CapabilityState


@pytest.mark.asyncio
async def test_health_status_refresh_runs_provider_probes_concurrently(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.prowlarr import ProwlarrAdapter
    from app.sources.sabnzbd import SabnzbdAdapter
    from app.sources.slskd import SlskdAdapter
    from app.sources.tidal import TidalAdapter
    from app.sources.youtube import YouTubeAdapter

    async def slow_ok(self: object) -> CapabilityState:
        await __import__("asyncio").sleep(0.05)
        return CapabilityState(True)

    monkeypatch.setattr(SlskdAdapter, "health", slow_ok)
    monkeypatch.setattr(ProwlarrAdapter, "health", slow_ok)
    monkeypatch.setattr(SabnzbdAdapter, "health", slow_ok)
    monkeypatch.setattr(YouTubeAdapter, "local_health", slow_ok)
    monkeypatch.setattr(TidalAdapter, "local_health", slow_ok)

    service = HealthStatusService(probe_timeout_seconds=0.5)
    settings = Settings(
        secret_key="test-secret",
        database_url="sqlite+aiosqlite:///:memory:",
        auth_cookie_secure=False,
        slskd_url="http://slskd",
        slskd_api_key="k",
        prowlarr_url="http://prowlarr",
        prowlarr_api_key="k",
        sabnzbd_url="http://sabnzbd",
        sabnzbd_api_key="k",
    )

    started = time.perf_counter()
    statuses = await service.refresh_all(db_session, settings)
    elapsed = time.perf_counter() - started

    assert elapsed < 0.14
    assert statuses["slskd"].available is True
    assert statuses["prowlarr"].elapsed_ms is not None


@pytest.mark.asyncio
async def test_settings_get_uses_cached_health_without_live_probe(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.sources.slskd import SlskdAdapter

    async def fail_if_called(self: object) -> CapabilityState:
        raise AssertionError("settings GET must not run live health probe")

    monkeypatch.setattr(SlskdAdapter, "health", fail_if_called)
    response = await client.get("/settings/download-clients")

    assert response.status_code == 200
    assert "Not checked" in response.text


def test_database_url_uses_legacy_file_without_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.config import Settings

    monkeypatch.chdir(tmp_path)
    legacy = tmp_path / "data" / "music_manager.db"
    legacy.parent.mkdir()
    legacy.write_text("legacy")

    settings = Settings(secret_key="test-secret", auth_cookie_secure=False)

    assert settings.database_url.endswith("data/music_manager.db")
    assert legacy.exists()
    assert not (tmp_path / "data" / "audiohoard.db").exists()
