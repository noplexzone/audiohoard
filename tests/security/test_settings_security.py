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
    for path, data in (
        ("/settings/save", {"section": "acquisition"}),
        ("/settings/save-and-test", {"provider": "slskd"}),
        ("/settings/test-paths", {}),
        ("/settings/refresh", {"provider": "slskd"}),
        ("/settings/test", {"provider": "slskd"}),
    ):
        response = await client.post(
            path, data=data, headers={"X-CSRF-Token": client.cookies["csrf"]}
        )
        assert response.status_code == 403, path


@pytest.mark.parametrize(
    "host",
    ["2852039166", "0xa9fea9fe", "0251.0376.0251.0376"],
)
def test_provider_url_rejects_alternative_metadata_ipv4_forms(host: str) -> None:
    from pydantic import ValidationError

    from app.schemas.settings import SettingsTestRequest

    with pytest.raises(ValidationError, match="provider URL address is not allowed"):
        SettingsTestRequest(provider="slskd", slskd_url=f"http://{host}:5030")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "host",
    ["2852039166", "0xa9fea9fe", "0251.0376.0251.0376"],
)
async def test_html_provider_probe_rejects_alternative_metadata_ipv4_forms(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch, host: str
) -> None:
    async def forbidden_health(self):
        raise AssertionError("provider adapter must not receive a prohibited URL")

    monkeypatch.setattr("app.sources.slskd.SlskdAdapter.health", forbidden_health)
    response = await client.post(
        "/settings/test",
        data={"provider": "slskd", "slskd_url": f"http://{host}:5030"},
    )
    assert response.status_code == 303
    assert "Provider%20URL%20is%20not%20allowed" in response.headers["location"]


@pytest.mark.asyncio
async def test_save_and_test_revalidates_environment_locked_provider_url(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.config import Settings, override_settings

    async def forbidden_health(self):
        raise AssertionError("provider adapter must not receive a prohibited stored URL")

    override_settings(
        Settings(
            database_url="sqlite+aiosqlite:///:memory:",
            secret_key="test-secret",
            auth_cookie_secure=False,
            slskd_url="http://2852039166:5030",
            slskd_api_key="legacy-environment-secret",
        )
    )
    monkeypatch.setattr("app.sources.slskd.SlskdAdapter.health", forbidden_health)
    response = await client.post(
        "/settings/save-and-test",
        data={"provider": "slskd", "section": "acquisition"},
        headers={"X-Requested-With": "fetch"},
    )
    assert response.status_code == 200
    result = response.json()
    assert result["available"] is False
    assert result["reason"] == "Provider URL is not allowed"
