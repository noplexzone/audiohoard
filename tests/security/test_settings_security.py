from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_settings_api_requires_auth(unauthenticated_client: AsyncClient) -> None:
    response = await unauthenticated_client.get("/api/settings")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_settings_ui_requires_auth(unauthenticated_client: AsyncClient) -> None:
    response = await unauthenticated_client.get("/settings")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_save_settings_requires_auth(unauthenticated_client: AsyncClient) -> None:
    response = await unauthenticated_client.post("/api/settings/save", json={})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_test_settings_requires_auth(unauthenticated_client: AsyncClient) -> None:
    response = await unauthenticated_client.post("/api/settings/test", json={"provider": "slskd"})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_save_settings_requires_csrf(client: AsyncClient) -> None:
    client.headers.pop("X-CSRF-Token", None)
    response = await client.post("/api/settings/save", json={})
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_test_settings_requires_csrf(client: AsyncClient) -> None:
    client.headers.pop("X-CSRF-Token", None)
    response = await client.post("/api/settings/test", json={"provider": "slskd"})
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_save_and_test_requires_csrf(client: AsyncClient) -> None:
    client.headers.pop("X-CSRF-Token", None)
    response = await client.post("/settings/save-and-test", data={"provider": "slskd"})
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_path_test_requires_csrf(client: AsyncClient) -> None:
    client.headers.pop("X-CSRF-Token", None)
    response = await client.post("/settings/test-paths")
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_settings_does_not_expose_raw_secrets(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.config import Settings, override_settings

    override_settings(
        Settings(
            database_url="sqlite+aiosqlite:///:memory:",
            secret_key="test-secret",
            auth_cookie_secure=False,
            slskd_api_key="raw-secret-value-1234",
            prowlarr_api_key="another-raw-secret",
        )
    )
    response = await client.get("/api/settings")
    assert response.status_code == 200
    body = response.text
    assert "raw-secret-value-1234" not in body
    assert "another-raw-secret" not in body


@pytest.mark.asyncio
async def test_settings_response_uses_masked_placeholder(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.config import Settings, override_settings

    override_settings(
        Settings(
            database_url="sqlite+aiosqlite:///:memory:",
            secret_key="test-secret",
            auth_cookie_secure=False,
            slskd_api_key="secret-key-value",
        )
    )
    response = await client.get("/api/settings")
    assert response.status_code == 200
    data = response.json()
    assert data["slskd_api_key"]["value"] == "***"
    assert data["slskd_api_key"]["configured"] is True
    assert data["slskd_api_key"]["locked_by_environment"] is True


@pytest.mark.asyncio
async def test_member_cannot_save_settings(
    unauthenticated_client: AsyncClient,
) -> None:
    client = unauthenticated_client
    await client.post(
        "/api/auth/setup",
        json={"username": "owner", "password": "Owner-Password-Secure-42"},
    )
    owner_csrf = client.cookies["csrf"]
    await client.post(
        "/api/auth/users",
        json={"username": "member", "password": "Member-Password-42!", "role": "member"},
        headers={"X-CSRF-Token": owner_csrf},
    )
    await client.post("/api/auth/logout", headers={"X-CSRF-Token": owner_csrf})
    login = await client.post(
        "/api/auth/login",
        json={"username": "member", "password": "Member-Password-42!"},
    )
    member_csrf = login.json()["csrf_token"]
    response = await client.post(
        "/api/settings/save",
        json={"slskd_url": "http://test"},
        headers={"X-CSRF-Token": member_csrf},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_owner_can_open_settings_page_without_csrf_header(client: AsyncClient) -> None:
    client.headers.pop("X-CSRF-Token", None)
    response = await client.get("/settings/download-sources")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_member_cannot_read_settings(unauthenticated_client: AsyncClient) -> None:
    client = unauthenticated_client
    await client.post(
        "/api/auth/setup",
        json={"username": "owner", "password": "Owner-Password-Secure-42"},
    )
    owner_csrf = client.cookies["csrf"]
    await client.post(
        "/api/auth/users",
        json={"username": "member", "password": "Member-Password-42!", "role": "member"},
        headers={"X-CSRF-Token": owner_csrf},
    )
    await client.post("/api/auth/logout", headers={"X-CSRF-Token": owner_csrf})
    await client.post(
        "/api/auth/login",
        json={"username": "member", "password": "Member-Password-42!"},
    )
    assert (await client.get("/settings/download-sources")).status_code == 403
    assert (await client.get("/api/settings")).status_code == 403
