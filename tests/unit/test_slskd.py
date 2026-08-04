from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from pytest_httpx import HTTPXMock

import app.sources.slskd as slskd_module
from app.schemas.search import SearchRequest
from app.sources.slskd import SlskdAdapter, _search_state_is_failed, _search_state_is_terminal
from app.sources.youtube import ProviderError


@pytest.fixture(autouse=True)
def clear_download_snapshot_cache() -> None:
    getattr(slskd_module, "_download_snapshots", {}).clear()


class TestSlskdHealth:
    async def test_health_ok(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/application",
            json={"version": "0.21.0", "state": "Connected"},
        )
        adapter = SlskdAdapter("http://slskd.local", "key123")
        state = await adapter.health()
        assert state.available is True

    async def test_health_unreachable(self, httpx_mock: HTTPXMock) -> None:
        import httpx as httpx_lib

        for _ in range(3):
            httpx_mock.add_exception(
                httpx_lib.ConnectError("Connection refused"),
                url="http://slskd.local/api/v0/application",
            )
        adapter = SlskdAdapter("http://slskd.local", "key123")
        state = await adapter.health()
        assert state.available is False
        assert state.reason is not None

    async def test_health_unconfigured(self) -> None:
        adapter = SlskdAdapter("", "")
        state = await adapter.health()
        assert state.available is False
        assert "not configured" in (state.reason or "")


class TestSlskdSearch:
    def test_search_state_accepts_compound_terminal_variants(self) -> None:
        assert _search_state_is_terminal("Completed, FileLimitReached")
        assert _search_state_is_terminal("Completed, TimedOut")
        assert _search_state_is_terminal("Completed, ResponseLimitReached")
        assert _search_state_is_terminal("Timed Out")
        assert _search_state_is_terminal("Completed, Errored")
        assert _search_state_is_failed("Completed, Errored")
        assert _search_state_is_failed("Completed, Cancelled")
        assert not _search_state_is_failed("Completed, TimedOut")
        assert not _search_state_is_terminal("InProgress")

    async def test_search_returns_results(self, httpx_mock: HTTPXMock) -> None:
        search_id = "abc123"
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/searches",
            method="POST",
            json={"id": search_id},
        )
        httpx_mock.add_response(
            url=f"http://slskd.local/api/v0/searches/{search_id}",
            json={"state": "Completed", "id": search_id},
        )
        httpx_mock.add_response(
            url=f"http://slskd.local/api/v0/searches/{search_id}/responses",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {
                            "filename": "music/Artist/Album/01 Song.flac",
                            "size": 30000000,
                            "bitRate": 1411,
                            "sampleRate": 44100,
                            "duration": 186,
                        }
                    ],
                }
            ],
        )
        adapter = SlskdAdapter("http://slskd.local", "key123")
        req = SearchRequest(query="Artist Album Song")
        results = await adapter.search(req)
        assert len(results) == 1
        assert results[0].source == "slskd"
        assert results[0].format == "flac"
        assert results[0].size_bytes == 30000000
        assert results[0].duration_sec == 186

    async def test_search_accepts_compound_completed_state(self, httpx_mock: HTTPXMock) -> None:
        search_id = "compound123"
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/searches",
            method="POST",
            json={"id": search_id},
        )
        httpx_mock.add_response(
            url=f"http://slskd.local/api/v0/searches/{search_id}",
            json={"state": "Completed, FileLimitReached", "id": search_id},
        )
        httpx_mock.add_response(
            url=f"http://slskd.local/api/v0/searches/{search_id}/responses",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {
                            "filename": "music/Artist/Album/01 Song.flac",
                            "size": 30_000_000,
                        }
                    ],
                }
            ],
        )

        results = await SlskdAdapter("http://slskd.local", "key123").search(
            SearchRequest(query="Artist Album Song")
        )

        assert len(results) == 1
        assert [request.url.path for request in httpx_mock.get_requests()] == [
            "/api/v0/searches",
            f"/api/v0/searches/{search_id}",
            f"/api/v0/searches/{search_id}/responses",
        ]

    async def test_search_failed_compound_state_raises_provider_error(
        self, httpx_mock: HTTPXMock
    ) -> None:
        search_id = "failed123"
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/searches",
            method="POST",
            json={"id": search_id},
        )
        httpx_mock.add_response(
            url=f"http://slskd.local/api/v0/searches/{search_id}",
            json={"state": "Completed, Errored", "id": search_id},
        )

        with pytest.raises(ProviderError) as exc_info:
            await SlskdAdapter("http://slskd.local", "key123").search(
                SearchRequest(query="Artist Album Song")
            )

        assert exc_info.value.code == "search_failed"
        assert [request.url.path for request in httpx_mock.get_requests()] == [
            "/api/v0/searches",
            f"/api/v0/searches/{search_id}",
        ]

    async def test_search_ignores_lrc_lyrics_files(self, httpx_mock: HTTPXMock) -> None:
        search_id = "lyrics123"
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/searches",
            method="POST",
            json={"id": search_id},
        )
        httpx_mock.add_response(
            url=f"http://slskd.local/api/v0/searches/{search_id}",
            json={"state": "Completed", "id": search_id},
        )
        httpx_mock.add_response(
            url=f"http://slskd.local/api/v0/searches/{search_id}/responses",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {"filename": "music/Artist/Album/01 Song.lrc", "size": 3000},
                        {"filename": "music/Artist/Album/01 Song.flac", "size": 30000000},
                    ],
                }
            ],
        )

        results = await SlskdAdapter("http://slskd.local", "key123").search(
            SearchRequest(query="Artist Album Song")
        )

        assert [result.format for result in results] == ["flac"]
        assert results[0].metadata["filename"].endswith(".flac")


class TestSlskdTransfers:
    async def test_enqueue_posts_queue_download_request_array(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads/peer1",
            method="POST",
            status_code=201,
            json={},
        )
        transfer_id = await SlskdAdapter("http://slskd.local", "key123").enqueue(
            "peer1", "music/01 Song.flac", 30_000_000
        )
        request = httpx_mock.get_request()
        assert request is not None
        assert json.loads(request.content) == [
            {"filename": "music/01 Song.flac", "size": 30_000_000}
        ]
        assert transfer_id == "peer1:music/01 Song.flac"

    async def test_enqueue_preserves_slskd_error_detail(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads/peer1",
            method="POST",
            status_code=400,
            json={"title": "Validation failed", "errors": {"files": ["Files must be an array"]}},
        )
        with pytest.raises(ProviderError) as exc_info:
            await SlskdAdapter("http://slskd.local", "key123").enqueue(
                "peer1", "01 Song.flac", 123
            )
        assert exc_info.value.code == "slskd_http_400"
        assert "Validation failed" in exc_info.value.message
        assert "Files must be an array" in exc_info.value.message

    async def test_cancel_resolves_transfer_id_and_removes_download(
        self, httpx_mock: HTTPXMock
    ) -> None:
        transfer_id = "4dd4add9-96ce-4ab2-80d4-5b171b324e3e"
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {
                            "id": transfer_id,
                            "filename": "Music\\Artist\\Album\\01 Song.flac",
                            "state": "Completed, Succeeded",
                        }
                    ],
                }
            ],
        )
        httpx_mock.add_response(
            url=(f"http://slskd.local/api/v0/transfers/downloads/peer1/{transfer_id}?remove=true"),
            method="DELETE",
            status_code=200,
        )

        await SlskdAdapter("http://slskd.local", "key123").cancel(
            "peer1", "Music\\Artist\\Album\\01 Song.flac"
        )

        requests = httpx_mock.get_requests()
        assert [request.method for request in requests] == ["GET", "DELETE"]
        assert requests[-1].url.params["remove"] == "true"

    async def test_cleanup_cancel_does_not_remove_replacement_with_same_identity(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {
                            "id": "new-transfer",
                            "filename": "Music\\same.flac",
                            "state": "InProgress",
                        }
                    ],
                }
            ],
        )

        result = await SlskdAdapter("http://slskd.local", "key123").cancel(
            "peer1", "Music\\same.flac", "old-transfer"
        )

        assert result is True
        assert [request.method for request in httpx_mock.get_requests()] == ["GET"]

    async def test_cleanup_cancel_keeps_fallback_identity_pending_when_ambiguous(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {
                            "id": "provider-transfer",
                            "filename": "Music\\same.flac",
                            "state": "Completed, Succeeded",
                        }
                    ],
                }
            ],
        )

        result = await SlskdAdapter("http://slskd.local", "key123").cancel(
            "peer1", "Music\\same.flac", "peer1:Music\\same.flac"
        )

        assert result is False
        assert [request.method for request in httpx_mock.get_requests()] == ["GET"]

    async def test_cancel_is_idempotent_when_transfer_is_already_absent(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(url="http://slskd.local/api/v0/transfers/downloads", json=[])

        await SlskdAdapter("http://slskd.local", "key123").cancel("peer1", "Music\\missing.flac")

        assert len(httpx_mock.get_requests()) == 1

    async def test_downloads_flattens_current_nested_response(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "directories": [
                        {
                            "directory": "music",
                            "files": [
                                {
                                    "filename": "music/01 Song.flac",
                                    "size": 30_000_000,
                                    "state": "Completed, Succeeded",
                                }
                            ],
                        }
                    ],
                }
            ],
        )
        downloads = await SlskdAdapter("http://slskd.local", "key123").downloads()
        assert downloads == [
            {
                "filename": "music/01 Song.flac",
                "size": 30_000_000,
                "state": "Completed, Succeeded",
                "username": "peer1",
                "directory": "music",
            }
        ]

    async def test_status_finds_file_in_downloads_object_envelope(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json={
                "downloads": [
                    {
                        "username": "peer1",
                        "directories": [
                            {"files": [{"filename": "01 Song.flac", "state": "InProgress"}]}
                        ],
                    }
                ]
            },
        )
        status = await SlskdAdapter("http://slskd.local", "key123").status("peer1:01 Song.flac")
        assert status.available is True
        assert status.reason == "inprogress"
        assert status.extra["username"] == "peer1"

    async def test_status_matches_fallback_identity_when_item_has_uuid(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "directories": [
                        {
                            "files": [
                                {
                                    "id": "4dd4add9-96ce-4ab2-80d4-5b171b324e3e",
                                    "filename": "Music\\Artist\\Album\\01 Song.flac",
                                    "state": "Completed, Succeeded",
                                }
                            ]
                        }
                    ],
                }
            ],
        )

        status = await SlskdAdapter("http://slskd.local", "key123").status(
            "peer1:Music\\Artist\\Album\\01 Song.flac"
        )

        assert status.available is True
        assert status.reason == "completed, succeeded"
        assert status.extra["id"] == "4dd4add9-96ce-4ab2-80d4-5b171b324e3e"

    async def test_concurrent_status_calls_share_one_download_snapshot(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {"id": "transfer-1", "filename": "one.flac", "state": "Queued"},
                        {"id": "transfer-2", "filename": "two.flac", "state": "InProgress"},
                    ],
                }
            ],
        )
        first, second = await asyncio.gather(
            SlskdAdapter("http://slskd.local", "key123").status("transfer-1"),
            SlskdAdapter("http://slskd.local", "key123").status("transfer-2"),
        )

        assert first.reason == "queued"
        assert second.reason == "inprogress"
        assert len(httpx_mock.get_requests()) == 1

    async def test_download_snapshot_reuses_ttl_then_refreshes(
        self, httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = 100.0
        monkeypatch.setattr(slskd_module, "_monotonic", lambda: now)
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer",
                    "files": [{"id": "t1", "filename": "one.flac", "state": "Queued"}],
                }
            ],
        )
        adapter = SlskdAdapter("http://slskd.local", "key123")

        assert (await adapter.status("t1")).reason == "queued"
        now += slskd_module._DOWNLOAD_SNAPSHOT_TTL_SEC / 2
        assert (await adapter.status("t1")).reason == "queued"
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer",
                    "files": [{"id": "t1", "filename": "one.flac", "state": "Completed"}],
                }
            ],
        )
        now += slskd_module._DOWNLOAD_SNAPSHOT_TTL_SEC
        assert (await adapter.status("t1")).reason == "completed"
        assert len(httpx_mock.get_requests()) == 2

    async def test_download_snapshots_are_isolated_by_endpoint_and_credential(
        self, httpx_mock: HTTPXMock
    ) -> None:
        for url, state in (
            ("http://slskd.local/api/v0/transfers/downloads", "Queued"),
            ("http://slskd.local/api/v0/transfers/downloads", "Completed"),
            ("http://other-slskd.local/api/v0/transfers/downloads", "InProgress"),
        ):
            httpx_mock.add_response(
                url=url,
                json=[
                    {
                        "username": "peer",
                        "files": [{"id": "t1", "filename": "one.flac", "state": state}],
                    }
                ],
            )

        first = await SlskdAdapter("http://slskd.local", "first-key").status("t1")
        second = await SlskdAdapter("http://slskd.local", "second-key").status("t1")
        other_endpoint = await SlskdAdapter("http://other-slskd.local", "first-key").status("t1")

        assert (first.reason, second.reason, other_endpoint.reason) == (
            "queued",
            "completed",
            "inprogress",
        )
        assert len(httpx_mock.get_requests()) == 3

    async def test_failed_snapshot_does_not_poison_future_fetch(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads", status_code=400
        )
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer",
                    "files": [{"id": "t1", "filename": "one.flac", "state": "Completed"}],
                }
            ],
        )
        adapter = SlskdAdapter("http://slskd.local", "key123")

        with pytest.raises(httpx.HTTPStatusError):
            await adapter.status("t1")
        assert (await adapter.status("t1")).reason == "completed"
        assert len(httpx_mock.get_requests()) == 2

    async def test_download_poll_429_uses_bounded_backoff_without_real_sleep(
        self, httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sleeps: list[float] = []

        async def capture_sleep(delay: float) -> None:
            sleeps.append(delay)

        monkeypatch.setattr(slskd_module, "_transfer_sleep", capture_sleep)
        monkeypatch.setattr(slskd_module, "_transfer_jitter", lambda _low, _high: 0.0)
        for _ in range(2):
            httpx_mock.add_response(
                url="http://slskd.local/api/v0/transfers/downloads",
                status_code=429,
                text="api_key=do-not-expose",
            )
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer",
                    "files": [{"id": "t1", "filename": "one.flac", "state": "Completed"}],
                }
            ],
        )

        status = await SlskdAdapter("http://slskd.local", "key123").status("t1")

        assert status.reason == "completed"
        assert sleeps == [0.25, 0.5]
        assert len(httpx_mock.get_requests()) == 3

    async def test_download_poll_429_exhaustion_is_sanitized_and_retryable(
        self, httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def no_sleep(_delay: float) -> None:
            return None

        monkeypatch.setattr(slskd_module, "_transfer_sleep", no_sleep)
        monkeypatch.setattr(slskd_module, "_transfer_jitter", lambda _low, _high: 0.0)
        for _ in range(slskd_module._TRANSFER_429_MAX_ATTEMPTS):
            httpx_mock.add_response(
                url="http://slskd.local/api/v0/transfers/downloads",
                status_code=429,
                text="secret response api_key=do-not-expose",
            )

        with pytest.raises(ProviderError) as exc_info:
            await SlskdAdapter("http://slskd.local", "key123").downloads()

        assert exc_info.value.code == "slskd_http_429"
        assert exc_info.value.retryable is True
        assert "key123" not in exc_info.value.message
        assert "do-not-expose" not in exc_info.value.message
        assert "?" not in exc_info.value.message
