from __future__ import annotations

import json

import pytest
from pytest_httpx import HTTPXMock

from app.schemas.search import SearchRequest
from app.sources.slskd import SlskdAdapter
from app.sources.youtube import ProviderError


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
