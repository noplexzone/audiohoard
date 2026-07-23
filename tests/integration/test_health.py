from __future__ import annotations

from httpx import AsyncClient

from app.routers import health as health_router
from app.sources.slskd import SlskdAdapter


async def test_liveness_is_cheap_and_public(unauthenticated_client: AsyncClient) -> None:
    response = await unauthenticated_client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_readiness_returns_200_when_database_is_available(client: AsyncClient) -> None:
    response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "sources": {}, "db_writable": True}


async def test_readiness_returns_503_when_database_is_unavailable(
    client: AsyncClient, monkeypatch
) -> None:
    async def unavailable(_db) -> bool:
        return False

    monkeypatch.setattr(health_router, "_check_db", unavailable)

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "down"
    assert response.json()["db_writable"] is False


async def test_legacy_health_does_not_run_live_provider_probes(
    client: AsyncClient, monkeypatch
) -> None:
    async def forbidden_probe(self):
        raise AssertionError("public health endpoint invoked a provider probe")

    monkeypatch.setattr(SlskdAdapter, "health", forbidden_probe)

    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json()["sources"] == {}


async def test_source_diagnostics_require_authentication(
    unauthenticated_client: AsyncClient,
) -> None:
    response = await unauthenticated_client.get("/health/sources")

    assert response.status_code == 401


async def test_source_diagnostics_use_cached_status(client: AsyncClient) -> None:
    response = await client.get("/health/sources")

    assert response.status_code == 200
    assert set(response.json()) == {"slskd", "prowlarr", "sabnzbd", "youtube", "tidal"}
