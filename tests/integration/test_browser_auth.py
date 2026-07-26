from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from app.database import get_session_factory
from app.models.auth import AuthSession


async def test_browser_get_redirects_invalid_session_to_login(
    unauthenticated_client: AsyncClient,
) -> None:
    unauthenticated_client.cookies.set("session", "invalid")

    response = await unauthenticated_client.get(
        "/settings/download-sources?tab=clients",
        headers={"Accept": "text/html"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == (
        "/login?next=%2Fsettings%2Fdownload-sources%3Ftab%3Dclients"
    )
    assert "session=" in response.headers.get("set-cookie", "")


async def test_expired_browser_session_is_deleted_and_redirected(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        session = (await db.scalars(select(AuthSession))).one()
        session.expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()

    response = await client.get(
        "/downloads",
        headers={"Accept": "text/html"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/login?next=%2Fdownloads"
    async with factory() as db:
        assert await db.scalar(select(func.count(AuthSession.id))) == 0


async def test_api_auth_failure_remains_json_401(
    unauthenticated_client: AsyncClient,
) -> None:
    response = await unauthenticated_client.get(
        "/api/settings", headers={"Accept": "application/json"}
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Authentication required"


async def test_browser_logout_clears_session_and_redirects(client: AsyncClient) -> None:
    response = await client.post("/logout", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    assert "session=" in response.headers.get("set-cookie", "")
    factory = get_session_factory()
    async with factory() as db:
        assert await db.scalar(select(func.count(AuthSession.id))) == 0


async def test_login_returns_to_valid_local_deep_link(client: AsyncClient) -> None:
    await client.post("/logout")
    page = await client.get("/login?next=/imports/ui/review")
    assert 'name="next" value="/imports/ui/review"' in page.text

    response = await client.post(
        "/login",
        data={
            "username": "test-owner",
            "password": "Test-Owner-Password-42",
            "next": "/imports/ui/review",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/imports/ui/review"


@pytest.mark.parametrize("target", ["//evil.example", "https://evil.example", "\\evil"])
async def test_login_rejects_external_return_targets(client: AsyncClient, target: str) -> None:
    await client.post("/logout")
    response = await client.post(
        "/login",
        data={
            "username": "test-owner",
            "password": "Test-Owner-Password-42",
            "next": target,
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/"
