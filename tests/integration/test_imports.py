from __future__ import annotations

from httpx import AsyncClient


async def test_import_plans_endpoint_starts_empty(client: AsyncClient) -> None:
    response = await client.get("/imports/plans")
    assert response.status_code == 200
    assert response.json() == []


async def test_manual_import_ui_is_removed(client: AsyncClient) -> None:
    assert (await client.get("/imports/ui/review")).status_code == 404
    assert (await client.get("/imports/ui/releases/1/plan")).status_code == 404
    assert (await client.get("/imports/ui/releases/1/execute")).status_code == 404
