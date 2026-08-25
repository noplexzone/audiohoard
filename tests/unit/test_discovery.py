from __future__ import annotations

import httpx
import pytest

from app.metadata.base import ArtistDetail, ArtistHit, DiscoveryRelease
from app.metadata.deezer import DeezerClient
from app.services.discovery import DiscoveryService


class FakeDiscoveryProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []
        self.fail = False

    async def discovery_feed(
        self, feed: str, *, page: int = 1, limit: int = 12, genre_id: str | None = None
    ) -> list[ArtistHit | DiscoveryRelease]:
        self.calls.append((feed, page))
        if self.fail:
            raise OSError("provider details must not leak")
        result: list[ArtistHit | DiscoveryRelease] = [
            ArtistHit("deezer", f"{feed}-{page}", feed.title())
        ]
        return result[:limit]

    async def get_artist(self, provider_id: str) -> ArtistDetail:
        return ArtistDetail(
            provider="deezer",
            provider_id=provider_id,
            deezer_id=provider_id,
            name="Validated artist",
        )


async def test_cache_is_partitioned_by_region_page_and_can_invalidate() -> None:
    provider = FakeDiscoveryProvider()
    service = DiscoveryService(provider, ttl_seconds=60)  # type: ignore[arg-type]

    us = await service.get("popular", "US")
    assert await service.get("popular", "US") is us
    await service.get("popular", "CA")
    await service.get("popular", "US", page=2)
    assert provider.calls == [("popular", 1), ("popular", 1), ("popular", 2)]

    service.invalidate_region("US")
    await service.get("popular", "US")
    assert provider.calls[-1] == ("popular", 1)


async def test_expired_cache_is_served_stale_on_provider_error() -> None:
    provider = FakeDiscoveryProvider()
    service = DiscoveryService(provider, ttl_seconds=0)  # type: ignore[arg-type]
    fresh = await service.get("new", "GB")
    provider.fail = True

    stale = await service.get("new", "GB")

    assert stale.items == fresh.items
    assert stale.stale is True
    assert stale.message == "Showing cached results; refresh failed"
    assert "provider details" not in stale.message


async def test_landing_sections_fail_independently_and_report_global_fallback() -> None:
    class PartialProvider(FakeDiscoveryProvider):
        async def discovery_feed(
            self, feed: str, *, page: int = 1, limit: int = 12, genre_id: str | None = None
        ) -> list[ArtistHit | DiscoveryRelease]:
            if feed == "genres":
                raise OSError("down")
            return await super().discovery_feed(feed, page=page, limit=limit, genre_id=genre_id)

    sections = await DiscoveryService(PartialProvider()).landing("JP")  # type: ignore[arg-type]

    assert [section.feed for section in sections] == ["popular", "genres", "new", "trending"]
    assert sections[1].state == "error"
    assert sections[0].state == sections[2].state == sections[3].state == "ready"
    assert all(section.requested_region == "JP" for section in sections)
    assert all(section.effective_region == "GLOBAL" for section in sections)
    assert all(section.fallback_global for section in sections)


async def test_deezer_discovery_maps_rank_release_identity_and_genre_pages(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/genre":
            return httpx.Response(
                200,
                json={"data": [{"id": index, "name": f"Genre {index}"} for index in range(1, 31)]},
            )
        if request.url.path == "/chart/0/artists":
            return httpx.Response(
                200,
                json={"data": [{"id": 7, "name": "Ranked Artist", "position": 3}]},
            )
        if request.url.path == "/editorial/0/releases":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": 8,
                            "title": "New Release",
                            "release_date": "2026-08-01",
                            "artist": {"id": 9, "name": "Release Artist"},
                        }
                    ]
                },
            )
        raise AssertionError(f"unexpected discovery URL: {request.url}")

    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com", transport=httpx.MockTransport(handler)
        ),
    )

    genres = await provider.discovery_feed("genres", page=2, limit=12)
    popular = await provider.discovery_feed("popular")
    releases = await provider.discovery_feed("new")

    assert [item.provider_id for item in genres] == [str(index) for index in range(13, 25)]
    assert isinstance(popular[0], ArtistHit)
    assert popular[0].rank == 3
    assert isinstance(releases[0], DiscoveryRelease)
    assert releases[0].artist_provider_id == "9"
    assert releases[0].release_date == "2026-08-01"


async def test_deezer_discovery_rejects_http_200_error_envelope(monkeypatch) -> None:
    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com",
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, json={"error": {"code": 800}})
            ),
        ),
    )

    with pytest.raises(ValueError, match="error envelope"):
        await provider.discovery_feed("popular")


async def test_deezer_genre_discovery_uses_exact_radio_tracks_and_deduplicates(
    monkeypatch,
) -> None:
    requests: list[str] = []
    radio_tracks = {
        "pop-a": [
            {"artist": {"id": 10, "name": "Pop One"}},
            {"artist": {"id": 11, "name": "Pop Two"}},
        ],
        "pop-b": [
            {"artist": {"id": 11, "name": "Pop Two duplicate"}},
            {"artist": {"id": 12, "name": "Pop Three"}},
        ],
        "rap-a": [
            {"artist": {"id": 20, "name": "Rap One"}},
            {"artist": {"id": 21, "name": "Rap Two"}},
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path in {"/genre/132", "/genre/116"}:
            genre_id = int(request.url.path.rsplit("/", 1)[1])
            return httpx.Response(200, json={"id": genre_id, "name": "Exact genre"})
        if request.url.path == "/genre/132/radios":
            return httpx.Response(200, json={"data": [{"id": "pop-a"}, {"id": "pop-b"}]})
        if request.url.path == "/genre/116/radios":
            return httpx.Response(200, json={"data": [{"id": "rap-a"}]})
        if request.url.path.startswith("/radio/") and request.url.path.endswith("/tracks"):
            radio_id = request.url.path.split("/")[2]
            return httpx.Response(200, json={"data": radio_tracks[radio_id]})
        raise AssertionError(f"genre discovery used forbidden URL: {request.url}")

    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com", transport=httpx.MockTransport(handler)
        ),
    )

    pop = await provider.discovery_feed("genre", genre_id="132", limit=10)
    rap = await provider.discovery_feed("genre", genre_id="116", limit=10)

    assert [artist.provider_id for artist in pop] == ["10", "11", "12"]
    assert [artist.name for artist in pop] == ["Pop One", "Pop Two", "Pop Three"]
    assert [artist.provider_id for artist in rap] == ["20", "21"]
    assert set(artist.provider_id for artist in pop).isdisjoint(
        artist.provider_id for artist in rap
    )
    assert "/genre/132" in requests
    assert "/genre/132/radios" in requests
    assert "/radio/pop-a/tracks" in requests
    assert not any(path.endswith("/artists") for path in requests)
    assert not any(path.startswith("/chart/") for path in requests)


async def test_deezer_genre_discovery_pagination_is_deterministic(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/genre/152":
            return httpx.Response(200, json={"id": 152, "name": "Rock"})
        if request.url.path == "/genre/152/radios":
            return httpx.Response(200, json={"data": [{"id": 1}, {"id": 2}]})
        if request.url.path == "/radio/1/tracks":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"artist": {"id": 1, "name": "One"}},
                        {"artist": {"id": 2, "name": "Two"}},
                        {"artist": {"id": 1, "name": "One duplicate"}},
                    ]
                },
            )
        if request.url.path == "/radio/2/tracks":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"artist": {"id": 3, "name": "Three"}},
                        {"artist": {"id": 4, "name": "Four"}},
                    ]
                },
            )
        raise AssertionError(f"unexpected URL: {request.url}")

    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com", transport=httpx.MockTransport(handler)
        ),
    )

    first = await provider.discovery_feed("genre", genre_id="152", page=1, limit=2)
    second = await provider.discovery_feed("genre", genre_id="152", page=2, limit=2)
    repeated = await provider.discovery_feed("genre", genre_id="152", page=2, limit=2)

    assert [artist.provider_id for artist in first] == ["1", "2"]
    assert [artist.provider_id for artist in second] == ["3", "4"]
    assert repeated == second


async def test_deezer_genre_discovery_caps_radio_fanout_and_track_depth(monkeypatch) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/genre/132":
            return httpx.Response(200, json={"id": 132, "name": "Pop"})
        if request.url.path == "/genre/132/radios":
            return httpx.Response(
                200, json={"data": [{"id": radio_id} for radio_id in range(1, 9)]}
            )
        if request.url.path.startswith("/radio/"):
            return httpx.Response(200, json={"data": []})
        raise AssertionError(f"unexpected URL: {request.url}")

    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com", transport=httpx.MockTransport(handler)
        ),
    )

    assert await provider.discovery_feed("genre", genre_id="132", page=20, limit=25) == []

    radio_requests = [request for request in requests if request.url.path.startswith("/radio/")]
    assert [request.url.path for request in radio_requests] == [
        "/radio/1/tracks",
        "/radio/2/tracks",
        "/radio/3/tracks",
        "/radio/4/tracks",
        "/radio/5/tracks",
    ]
    assert all(request.url.params["limit"] == "100" for request in radio_requests)
    radios_request = next(request for request in requests if request.url.path.endswith("/radios"))
    assert radios_request.url.params["limit"] == "5"
    assert len(requests) == 7


@pytest.mark.parametrize(
    "failing_path,payload",
    [
        ("/genre/999999", {"error": {"code": 800}}),
        ("/genre/132", {"id": 116, "name": "Wrong genre"}),
        ("/genre/132/radios", {"data": "not-a-list"}),
        ("/radio/1/tracks", {"error": {"code": 800}}),
    ],
)
async def test_deezer_genre_discovery_fails_closed_on_invalid_or_malformed_envelopes(
    monkeypatch, failing_path: str, payload: object
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == failing_path:
            return httpx.Response(200, json=payload)
        if request.url.path == "/genre/132":
            return httpx.Response(200, json={"id": 132, "name": "Pop"})
        if request.url.path == "/genre/132/radios":
            return httpx.Response(200, json={"data": [{"id": 1}]})
        if request.url.path == "/radio/1/tracks":
            return httpx.Response(200, json={"data": [{"artist": {"id": 10, "name": "Artist"}}]})
        raise AssertionError(f"unexpected URL: {request.url}")

    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com", transport=httpx.MockTransport(handler)
        ),
    )

    genre_id = "999999" if failing_path == "/genre/999999" else "132"
    with pytest.raises(ValueError, match="genre"):
        await provider.discovery_feed("genre", genre_id=genre_id)


async def test_discovery_filters_definitively_invalid_artist_identity() -> None:
    class InvalidProvider(FakeDiscoveryProvider):
        async def discovery_feed(
            self,
            feed: str,
            *,
            page: int = 1,
            limit: int = 12,
            genre_id: str | None = None,
        ) -> list[ArtistHit | DiscoveryRelease]:
            if feed == "new":
                return [
                    DiscoveryRelease(
                        "deezer", "release-bad", "Bad release", "Bad artist", "10002824"
                    )
                ]
            return await super().discovery_feed(feed, page=page, limit=limit, genre_id=genre_id)

        async def get_artist(self, provider_id: str) -> ArtistDetail:
            raise ValueError("Deezer returned an error envelope")

    service = DiscoveryService(InvalidProvider())  # type: ignore[arg-type]

    section = await service.get("popular", "US")
    release_section = await service.get("new", "US")

    assert section.items == ()
    assert release_section.items == ()


async def test_discovery_cache_is_bounded_and_expires_stale_entries() -> None:
    provider = FakeDiscoveryProvider()
    service = DiscoveryService(
        provider,
        ttl_seconds=0,
        stale_seconds=0,
        max_entries=2,  # type: ignore[arg-type]
    )

    await service.get("trending", "US", page=1)
    await service.get("trending", "US", page=2)
    await service.get("new", "US", page=1)

    assert len(service._cache) <= 2
