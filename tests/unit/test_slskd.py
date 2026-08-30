from __future__ import annotations

import asyncio
import concurrent.futures
import json
import threading

import httpx
import pytest
from pytest_httpx import HTTPXMock

import app.sources.slskd as slskd_module
from app.schemas.search import SearchRequest
from app.sources.slskd import SlskdAdapter, _search_state_is_failed, _search_state_is_terminal
from app.sources.youtube import ProviderError


@pytest.fixture(autouse=True)
async def clear_snapshot_caches() -> None:
    getattr(slskd_module, "_download_snapshots", {}).clear()
    clear_search_snapshots = getattr(slskd_module, "_clear_search_snapshot_cache", None)
    if clear_search_snapshots is not None:
        await clear_search_snapshots()
    yield
    if clear_search_snapshots is not None:
        await clear_search_snapshots()


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
    @staticmethod
    def _completed_search_request(
        *,
        responses: list[dict[str, object]] | None = None,
        wait_before_responses: asyncio.Event | None = None,
        responses_started: asyncio.Event | None = None,
    ):
        counts = {"POST": 0, "poll": 0, "responses": 0}
        bodies: list[dict[str, object]] = []

        async def fake_request(
            client: httpx.AsyncClient, method: str, path: str, **kwargs: object
        ) -> httpx.Response:
            request = httpx.Request(method, f"{str(client.base_url).rstrip('/')}{path}")
            if method == "POST":
                counts["POST"] += 1
                body = kwargs["json"]
                assert isinstance(body, dict)
                bodies.append(body)
                return httpx.Response(
                    200, json={"id": f"search-{counts['POST']}"}, request=request
                )
            if path.endswith("/responses"):
                counts["responses"] += 1
                if responses_started is not None:
                    responses_started.set()
                if wait_before_responses is not None:
                    await wait_before_responses.wait()
                return httpx.Response(200, json=responses or [], request=request)
            counts["poll"] += 1
            return httpx.Response(200, json={"state": "Completed"}, request=request)

        async def poll_once(
            adapter: SlskdAdapter, client: httpx.AsyncClient, search_id: str
        ) -> None:
            del adapter
            await slskd_module.request_with_retry(client, "GET", f"/api/v0/searches/{search_id}")

        return fake_request, poll_once, counts, bodies

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

    async def test_simultaneous_identical_searches_share_one_provider_sequence(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        release = asyncio.Event()
        responses_started = asyncio.Event()
        fake_request, poll_once, counts, _ = self._completed_search_request(
            wait_before_responses=release, responses_started=responses_started
        )
        monkeypatch.setattr(slskd_module, "request_with_retry", fake_request)
        monkeypatch.setattr(SlskdAdapter, "_wait_for_search", poll_once)
        adapter = SlskdAdapter("http://slskd.local/", "key123")
        request = SearchRequest(query="same query")

        first = asyncio.create_task(adapter.search(request))
        second = asyncio.create_task(adapter.search(request))
        await responses_started.wait()
        release.set()

        assert await asyncio.gather(first, second) == [[], []]
        assert counts == {"POST": 1, "poll": 1, "responses": 1}

    async def test_identical_searches_are_isolated_between_real_event_loops(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first_started = threading.Event()
        both_started = threading.Event()
        release = threading.Event()
        starts = 0
        starts_lock = threading.Lock()

        async def fake_fetch(adapter: SlskdAdapter, search_text: str, file_limit: int) -> bytes:
            nonlocal starts
            del adapter, search_text, file_limit
            with starts_lock:
                starts += 1
                first_started.set()
                if starts == 2:
                    both_started.set()
            await asyncio.to_thread(release.wait)
            return b"[]"

        async def run_search() -> list[dict[str, object]]:
            adapter = SlskdAdapter("http://slskd.local", "key123")
            return await adapter._raw_search("same query", mode="ordinary", file_limit=100)

        monkeypatch.setattr(SlskdAdapter, "_fetch_raw_search", fake_fetch)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(asyncio.run, run_search())
            assert await asyncio.to_thread(first_started.wait, 1)
            second = executor.submit(asyncio.run, run_search())
            started_in_both_loops = await asyncio.to_thread(both_started.wait, 1)
            release.set()
            results = await asyncio.gather(
                asyncio.to_thread(first.result, 2),
                asyncio.to_thread(second.result, 2),
                return_exceptions=True,
            )

        assert started_in_both_loops
        assert starts == 2
        assert results == [[], []]

    async def test_search_coalescing_key_isolates_configuration_query_and_mode(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_request, poll_once, counts, bodies = self._completed_search_request()
        monkeypatch.setattr(slskd_module, "request_with_retry", fake_request)
        monkeypatch.setattr(SlskdAdapter, "_wait_for_search", poll_once)
        ordinary = SearchRequest(query="same query")

        await SlskdAdapter("http://slskd.local", "first-key").search(ordinary)
        await SlskdAdapter("http://other.local", "first-key").search(ordinary)
        await SlskdAdapter("http://slskd.local", "second-key").search(ordinary)
        await SlskdAdapter("http://slskd.local", "first-key").search(
            SearchRequest(query="different query")
        )
        await SlskdAdapter("http://slskd.local", "first-key").search_album_folders(ordinary)
        await SlskdAdapter("http://slskd.local", "first-key").search(
            SearchRequest(query="same query", expected_duration_sec=123, preferred_format="flac")
        )

        assert counts == {"POST": 5, "poll": 5, "responses": 5}
        assert [body["fileLimit"] for body in bodies] == [100, 100, 100, 100, 500]

    async def test_cancelled_search_waiter_does_not_cancel_shared_producer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        release = asyncio.Event()
        responses_started = asyncio.Event()
        fake_request, poll_once, counts, _ = self._completed_search_request(
            wait_before_responses=release, responses_started=responses_started
        )
        monkeypatch.setattr(slskd_module, "request_with_retry", fake_request)
        monkeypatch.setattr(SlskdAdapter, "_wait_for_search", poll_once)
        adapter = SlskdAdapter("http://slskd.local", "key123")
        request = SearchRequest(query="same query")
        cancelled = asyncio.create_task(adapter.search(request))
        survivor = asyncio.create_task(adapter.search(request))
        await responses_started.wait()

        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        release.set()

        assert await survivor == []
        assert counts == {"POST": 1, "poll": 1, "responses": 1}

    async def test_failed_shared_search_is_evicted_and_next_call_refetches(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first_started = asyncio.Event()
        release_failure = asyncio.Event()
        posts = 0

        async def fake_request(
            client: httpx.AsyncClient, method: str, path: str, **kwargs: object
        ) -> httpx.Response:
            nonlocal posts
            request = httpx.Request(method, f"{str(client.base_url).rstrip('/')}{path}")
            if method == "POST":
                posts += 1
                if posts == 1:
                    first_started.set()
                    await release_failure.wait()
                    return httpx.Response(400, request=request)
                return httpx.Response(200, json={"id": "recovered"}, request=request)
            if path.endswith("/responses"):
                return httpx.Response(200, json=[], request=request)
            return httpx.Response(200, json={"state": "Completed"}, request=request)

        async def poll_once(
            adapter: SlskdAdapter, client: httpx.AsyncClient, search_id: str
        ) -> None:
            del adapter
            await slskd_module.request_with_retry(client, "GET", f"/api/v0/searches/{search_id}")

        monkeypatch.setattr(slskd_module, "request_with_retry", fake_request)
        monkeypatch.setattr(SlskdAdapter, "_wait_for_search", poll_once)
        adapter = SlskdAdapter("http://slskd.local", "key123")
        request = SearchRequest(query="retry me")
        first = asyncio.create_task(adapter.search(request))
        second = asyncio.create_task(adapter.search(request))
        await first_started.wait()
        await asyncio.sleep(0)
        release_failure.set()

        failures = await asyncio.gather(first, second, return_exceptions=True)
        assert all(isinstance(failure, httpx.HTTPStatusError) for failure in failures)
        assert posts == 1
        assert await adapter.search(request) == []
        assert posts == 2

    async def test_search_snapshot_reuses_ttl_then_refreshes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = 100.0
        fake_request, poll_once, counts, _ = self._completed_search_request()
        monkeypatch.setattr(slskd_module, "_monotonic", lambda: now)
        monkeypatch.setattr(slskd_module, "request_with_retry", fake_request)
        monkeypatch.setattr(SlskdAdapter, "_wait_for_search", poll_once)
        adapter = SlskdAdapter("http://slskd.local", "key123")
        request = SearchRequest(query="ttl query")

        await adapter.search(request)
        now += slskd_module._SEARCH_SNAPSHOT_TTL_SEC / 2
        await adapter.search(request)
        now += slskd_module._SEARCH_SNAPSHOT_TTL_SEC
        await adapter.search(request)

        assert counts == {"POST": 2, "poll": 2, "responses": 2}

    async def test_album_search_consumers_receive_independent_raw_copies(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        provider_responses: list[dict[str, object]] = [
            {
                "username": "peer1",
                "files": [{"filename": "Artist/Album/01 Song.flac", "size": 123}],
            }
        ]
        fake_request, poll_once, counts, _ = self._completed_search_request(
            responses=provider_responses
        )
        monkeypatch.setattr(slskd_module, "request_with_retry", fake_request)
        monkeypatch.setattr(SlskdAdapter, "_wait_for_search", poll_once)
        adapter = SlskdAdapter("http://slskd.local", "key123")
        request = SearchRequest(query="copy query")

        _, first_raw = await adapter.search_album_folders(request)
        first_raw[0]["username"] = "mutated"
        first_files = first_raw[0]["files"]
        assert isinstance(first_files, list)
        first_file = first_files[0]
        assert isinstance(first_file, dict)
        first_file["filename"] = "mutated.mp3"
        _, second_raw = await adapter.search_album_folders(request)

        assert second_raw == provider_responses
        assert first_raw is not second_raw
        assert counts == {"POST": 1, "poll": 1, "responses": 1}

    async def test_full_search_cache_backpressures_and_coalesces_overflow(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(slskd_module, "_SEARCH_SNAPSHOT_MAX_ENTRIES", 1)
        release_first = asyncio.Event()
        first_started = asyncio.Event()
        posts: list[str] = []
        observed_cache_sizes: list[tuple[int, int]] = []

        async def fake_request(
            client: httpx.AsyncClient, method: str, path: str, **kwargs: object
        ) -> httpx.Response:
            request = httpx.Request(method, f"{str(client.base_url).rstrip('/')}{path}")
            if method == "POST":
                body = kwargs["json"]
                assert isinstance(body, dict)
                search_text = str(body["searchText"])
                posts.append(search_text)
                state = slskd_module._get_search_cache_state()
                observed_cache_sizes.append(
                    (
                        len(state.snapshots),
                        sum(
                            not snapshot.in_flight.done() for snapshot in state.snapshots.values()
                        ),
                    )
                )
                if search_text == "first":
                    first_started.set()
                    await release_first.wait()
                return httpx.Response(200, json={"id": search_text}, request=request)
            if path.endswith("/responses"):
                return httpx.Response(200, json=[], request=request)
            return httpx.Response(200, json={"state": "Completed"}, request=request)

        async def poll_once(
            adapter: SlskdAdapter, client: httpx.AsyncClient, search_id: str
        ) -> None:
            del adapter
            await slskd_module.request_with_retry(client, "GET", f"/api/v0/searches/{search_id}")

        monkeypatch.setattr(slskd_module, "request_with_retry", fake_request)
        monkeypatch.setattr(SlskdAdapter, "_wait_for_search", poll_once)
        adapter = SlskdAdapter("http://slskd.local", "key123")
        first = asyncio.create_task(adapter.search(SearchRequest(query="first")))
        await first_started.wait()
        overflow = [
            asyncio.create_task(adapter.search(SearchRequest(query="overflow"))) for _ in range(2)
        ]
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        state = slskd_module._get_search_cache_state()
        assert posts == ["first"]
        assert len(state.snapshots) == 1
        assert sum(not snapshot.in_flight.done() for snapshot in state.snapshots.values()) == 1
        assert not any(task.done() for task in overflow)

        release_first.set()
        assert await first == []
        assert await asyncio.gather(*overflow) == [[], []]
        assert posts == ["first", "overflow"]
        assert all(
            cache_size <= 1 and live_producers <= 1
            for cache_size, live_producers in observed_cache_sizes
        )
        assert len(state.snapshots) <= 1
        assert sum(not snapshot.in_flight.done() for snapshot in state.snapshots.values()) <= 1

    async def test_cancelling_capacity_waiter_creates_no_snapshot_or_producer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(slskd_module, "_SEARCH_SNAPSHOT_MAX_ENTRIES", 1)
        release_first = asyncio.Event()
        first_started = asyncio.Event()
        posts: list[str] = []

        async def fake_fetch(adapter: SlskdAdapter, search_text: str, file_limit: int) -> bytes:
            del adapter, file_limit
            posts.append(search_text)
            if search_text == "first":
                first_started.set()
                await release_first.wait()
            return b"[]"

        monkeypatch.setattr(SlskdAdapter, "_fetch_raw_search", fake_fetch)
        adapter = SlskdAdapter("http://slskd.local", "key123")
        first = asyncio.create_task(adapter._raw_search("first", mode="ordinary", file_limit=100))
        await first_started.wait()
        cancelled = asyncio.create_task(
            adapter._raw_search("later", mode="ordinary", file_limit=100)
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        state = slskd_module._get_search_cache_state()
        assert posts == ["first"]
        assert len(state.snapshots) == 1
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        assert posts == ["first"]
        assert len(state.snapshots) == 1

        release_first.set()
        assert await first == []
        assert await adapter._raw_search("later", mode="ordinary", file_limit=100) == []
        assert posts == ["first", "later"]
        assert len(state.snapshots) <= 1

    async def test_search_request_bodies_preserve_mode_file_limits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_request, poll_once, _, bodies = self._completed_search_request()
        monkeypatch.setattr(slskd_module, "request_with_retry", fake_request)
        monkeypatch.setattr(SlskdAdapter, "_wait_for_search", poll_once)
        adapter = SlskdAdapter("http://slskd.local", "key123")
        request = SearchRequest(query="limits")

        await adapter.search(request)
        await adapter.search_album_folders(request)

        assert bodies == [
            {"searchText": "limits", "fileLimit": 100},
            {"searchText": "limits", "fileLimit": 500},
        ]


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

    async def test_enqueue_invalidates_pre_enqueue_download_snapshot(
        self, httpx_mock: HTTPXMock
    ) -> None:
        adapter = SlskdAdapter("http://slskd.local", "key123")
        filename = "music/01 Song.flac"
        fallback_id = f"peer1:{filename}"
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[],
        )
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads/peer1",
            method="POST",
            status_code=201,
            json={},
        )
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {
                            "id": "4dd4add9-96ce-4ab2-80d4-5b171b324e3e",
                            "filename": filename,
                            "state": "Queued",
                        }
                    ],
                }
            ],
        )

        assert await adapter.downloads() == []
        assert await adapter.enqueue("peer1", filename, 30_000_000) == fallback_id
        state = await adapter.status(fallback_id)

        assert state.available is True
        assert state.reason == "queued"
        assert [request.method for request in httpx_mock.get_requests()] == ["GET", "POST", "GET"]

    @pytest.mark.parametrize("stale_get_fails", [False, True])
    @pytest.mark.parametrize("replacement_completes_first", [False, True])
    async def test_inflight_pre_enqueue_snapshot_retries_current_generation(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stale_get_fails: bool,
        replacement_completes_first: bool,
    ) -> None:
        adapter = SlskdAdapter("http://slskd.local", "key123")
        filename = "music/01 Song.flac"
        first_get_started = asyncio.Event()
        release_stale_get = asyncio.Event()
        get_count = 0

        async def fake_request(
            client: httpx.AsyncClient, method: str, path: str, **kwargs: object
        ) -> httpx.Response:
            nonlocal get_count
            request = httpx.Request(method, f"http://slskd.local{path}")
            if method == "POST":
                return httpx.Response(201, json={}, request=request)
            assert method == "GET"
            get_count += 1
            if get_count == 1:
                first_get_started.set()
                await release_stale_get.wait()
                if stale_get_fails:
                    raise httpx.ReadTimeout("stale generation", request=request)
                return httpx.Response(200, json=[], request=request)
            return httpx.Response(
                200,
                json=[
                    {
                        "username": "peer1",
                        "files": [
                            {
                                "id": "4dd4add9-96ce-4ab2-80d4-5b171b324e3e",
                                "filename": filename,
                                "state": "Queued",
                            }
                        ],
                    }
                ],
                request=request,
            )

        monkeypatch.setattr(slskd_module, "request_with_retry", fake_request)
        stale_waiter = asyncio.create_task(adapter.downloads())
        await first_get_started.wait()

        assert await adapter.enqueue("peer1", filename, 30_000_000) == f"peer1:{filename}"
        if replacement_completes_first:
            fresh_downloads = await adapter.downloads()
            assert fresh_downloads[0]["id"] == "4dd4add9-96ce-4ab2-80d4-5b171b324e3e"
        release_stale_get.set()
        downloads = await stale_waiter

        assert downloads[0]["id"] == "4dd4add9-96ce-4ab2-80d4-5b171b324e3e"
        assert get_count == 2

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

    async def test_cleanup_cancel_refreshes_stale_snapshot_before_matching_transfer(
        self, httpx_mock: HTTPXMock
    ) -> None:
        adapter = SlskdAdapter("http://slskd.local", "key123")
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {
                            "id": "old-transfer",
                            "filename": "Music\\same.flac",
                            "state": "Completed, Succeeded",
                        }
                    ],
                }
            ],
        )
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {
                            "id": "replacement-transfer",
                            "filename": "Music\\same.flac",
                            "state": "InProgress",
                        }
                    ],
                }
            ],
        )

        assert (await adapter.status("old-transfer")).available is True
        result = await adapter.cancel("peer1", "Music\\same.flac", "old-transfer")

        assert result is True
        requests = httpx_mock.get_requests()
        assert [request.method for request in requests] == ["GET", "GET"]

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

    async def test_provisional_transfer_match_reports_unique_exact_peer_path_evidence(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {"id": "first", "filename": r"Music\Album\01 Song.flac"},
                        {"id": "other", "filename": "Music/Album/02 Song.flac"},
                    ],
                }
            ],
        )

        evidence = await SlskdAdapter("http://slskd.local", "key123").match_provisional_transfer(
            "peer1", "Music/Album/01 Song.flac", force_refresh=True
        )

        assert evidence.match_count == 1
        assert evidence.transfer is not None
        assert evidence.transfer["id"] == "first"

    async def test_provisional_transfer_match_with_multiple_matches_returns_no_transfer(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="http://slskd.local/api/v0/transfers/downloads",
            json=[
                {
                    "username": "peer1",
                    "files": [
                        {"id": "first", "filename": "Music/Album/01 Song.flac"},
                        {"id": "replacement", "filename": "Music/Album/01 Song.flac"},
                    ],
                }
            ],
        )

        evidence = await SlskdAdapter("http://slskd.local", "key123").match_provisional_transfer(
            "peer1", "Music/Album/01 Song.flac", force_refresh=True
        )

        assert evidence.match_count == 2
        assert evidence.transfer is None

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


async def test_search_coalescing_key_isolates_timeout_policy() -> None:
    interactive = SlskdAdapter("http://slskd.local", "key", search_timeout_sec=60)
    durable = SlskdAdapter("http://slskd.local", "key", search_timeout_sec=900)

    interactive_key = interactive._search_snapshot_key("same query", "ordinary", 100)
    durable_key = durable._search_snapshot_key("same query", "ordinary", 100)

    assert interactive_key != durable_key
