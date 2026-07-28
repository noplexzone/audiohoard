from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.config as config
import app.routers.artwork as artwork_router
from app.auth import get_current_user
from app.config import Settings
from app.main import create_app

_ALLOWED_URL = "https://cdn-images.dzcdn.net/images/artist/test.jpg"


def _create_test_app(monkeypatch: Any, cache_root: Path, *, authenticated: bool) -> FastAPI:
    settings = Settings(secret_key="test-secret", artwork_cache_root=cache_root)
    monkeypatch.setattr(config, "_settings", settings)
    application = create_app()
    if authenticated:
        application.dependency_overrides[get_current_user] = lambda: object()
    return application


def test_artwork_requires_authentication(monkeypatch: Any, tmp_path: Path) -> None:
    application = _create_test_app(monkeypatch, tmp_path, authenticated=False)

    response = TestClient(application).get(f"/artwork?url={_ALLOWED_URL}", follow_redirects=False)

    assert response.status_code == 401


def test_artwork_rejects_non_allowlisted_host(monkeypatch: Any, tmp_path: Path) -> None:
    application = _create_test_app(monkeypatch, tmp_path, authenticated=True)

    response = TestClient(application).get(
        "/artwork?url=https://example.com/image.jpg", follow_redirects=False
    )

    assert response.status_code == 400


def test_artwork_caches_upstream_response(monkeypatch: Any, tmp_path: Path) -> None:
    class FakeAsyncClient:
        calls = 0

        def __init__(self, **kwargs: object) -> None:
            pass

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

        def build_request(self, method: str, url: str, **kwargs: object) -> httpx.Request:
            return httpx.Request(method, url)

        async def send(self, request: httpx.Request, *, stream: bool = False) -> httpx.Response:
            type(self).calls += 1
            return httpx.Response(
                200,
                headers={"Content-Type": "image/png"},
                content=b"\x89PNG\r\n\x1a\nimage-bytes",
                request=request,
            )

    monkeypatch.setattr(artwork_router.httpx, "AsyncClient", FakeAsyncClient)
    application = _create_test_app(monkeypatch, tmp_path, authenticated=True)
    client = TestClient(application)

    first = client.get(f"/artwork?url={_ALLOWED_URL}")
    second = client.get(f"/artwork?url={_ALLOWED_URL}")

    assert first.status_code == 200
    assert first.headers["content-type"] == "image/png"
    assert second.status_code == 200
    assert second.headers["content-type"] == "image/png"
    assert FakeAsyncClient.calls == 1
    cache_files = [path for path in tmp_path.rglob("*") if path.is_file()]
    assert len(cache_files) == 1
    assert cache_files[0].read_bytes() == b"\x89PNG\r\n\x1a\nimage-bytes"


def test_artwork_upstream_failure_returns_local_fallback(monkeypatch: Any, tmp_path: Path) -> None:
    class FailingAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            pass

        async def __aenter__(self) -> FailingAsyncClient:
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

        def build_request(self, method: str, url: str, **kwargs: object) -> httpx.Request:
            return httpx.Request(method, url)

        async def send(self, request: httpx.Request, *, stream: bool = False) -> httpx.Response:
            raise httpx.ConnectError("unavailable", request=request)

    monkeypatch.setattr(artwork_router.httpx, "AsyncClient", FailingAsyncClient)
    application = _create_test_app(monkeypatch, tmp_path, authenticated=True)

    response = TestClient(application).get(f"/artwork?url={_ALLOWED_URL}", follow_redirects=False)

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/gif"
    assert response.headers["cache-control"] == "no-store"


def test_artwork_rejects_oversized_content_length(monkeypatch: Any, tmp_path: Path) -> None:
    class OversizedAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            pass

        async def __aenter__(self) -> OversizedAsyncClient:
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

        def build_request(self, method: str, url: str, **kwargs: object) -> httpx.Request:
            return httpx.Request(method, url)

        async def send(self, request: httpx.Request, *, stream: bool = False) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"Content-Type": "image/jpeg", "Content-Length": "6000000"},
                content=b"",
                request=request,
            )

    monkeypatch.setattr(artwork_router.httpx, "AsyncClient", OversizedAsyncClient)
    application = _create_test_app(monkeypatch, tmp_path, authenticated=True)

    response = TestClient(application).get(f"/artwork?url={_ALLOWED_URL}")

    assert response.status_code == 502
    assert not any(path.is_file() for path in tmp_path.rglob("*"))
