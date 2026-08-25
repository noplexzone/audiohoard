from __future__ import annotations

import asyncio

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
        self,
        feed: str,
        *,
        page: int = 1,
        limit: int = 12,
        genre_id: str | None = None,
        offset: int | None = None,
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


class GenreCandidateProvider(FakeDiscoveryProvider):
    def __init__(self, candidate_ids: list[str], invalid_ids: set[str] | None = None) -> None:
        super().__init__()
        self.candidate_ids = candidate_ids
        self.invalid_ids = invalid_ids or set()

    async def genre_artist_candidates(self, genre_id: str) -> list[ArtistHit]:
        assert genre_id == "132"
        return [
            ArtistHit("deezer", artist_id, f"Artist {artist_id}", deezer_id=artist_id)
            for artist_id in self.candidate_ids
        ]

    async def get_artist(self, provider_id: str) -> ArtistDetail:
        if provider_id in self.invalid_ids:
            raise ValueError("invalid exact artist")
        return await super().get_artist(provider_id)


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


def test_landing_snapshot_is_local_only_and_distinguishes_ready_stale_and_pending() -> None:
    provider = FakeDiscoveryProvider()
    service = DiscoveryService(provider, ttl_seconds=60, stale_seconds=3600)  # type: ignore[arg-type]

    pending = service.landing_snapshot("US")

    assert [section.feed for section in pending] == ["popular", "genres", "new", "trending"]
    assert all(section.state == "pending" for section in pending)
    assert provider.calls == []


async def test_landing_snapshot_reuses_ready_and_usable_stale_cache_without_io() -> None:
    provider = FakeDiscoveryProvider()
    service = DiscoveryService(provider, ttl_seconds=60, stale_seconds=3600)  # type: ignore[arg-type]
    ready = await service.get("popular", "US", limit=12)
    key = "deezer:popular:US:1:12:"
    expires, cached = service._cache[key]

    snapshot = service.landing_snapshot("US")
    assert snapshot[0] is ready
    assert snapshot[0].state == "ready"
    assert provider.calls == [("popular", 1)]

    service._cache[key] = (expires - 120, cached)
    stale_snapshot = service.landing_snapshot("US")
    assert stale_snapshot[0].state == "stale"
    assert stale_snapshot[0].stale is True
    assert stale_snapshot[0].items == ready.items
    assert provider.calls == [("popular", 1)]


def test_progressive_discovery_script_has_bounded_navigation_safe_fragment_contract() -> None:
    from pathlib import Path

    script = Path("app/static/js/discovery.js").read_text(encoding="utf-8")

    for contract in (
        "data-discover-fragment-url",
        'dataset.discoverState !== "pending"',
        'credentials: "same-origin"',
        "container.replaceWith",
        'document.addEventListener("visibilitychange"',
        'window.addEventListener("pagehide"',
        "audiohoard:page-dispose",
        "AbortController",
    ):
        assert contract in script
    assert "setInterval" not in script


async def test_landing_sections_fail_independently_and_report_global_fallback() -> None:
    class PartialProvider(FakeDiscoveryProvider):
        async def discovery_feed(
            self,
            feed: str,
            *,
            page: int = 1,
            limit: int = 12,
            genre_id: str | None = None,
            offset: int | None = None,
        ) -> list[ArtistHit | DiscoveryRelease]:
            if feed == "genres":
                raise OSError("down")
            return await super().discovery_feed(
                feed, page=page, limit=limit, genre_id=genre_id, offset=offset
            )

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
        "1": [
            {"artist": {"id": 10, "name": "Pop One"}},
            {"artist": {"id": 11, "name": "Pop Two"}},
        ],
        "2": [
            {"artist": {"id": 11, "name": "Pop Two duplicate"}},
            {"artist": {"id": 12, "name": "Pop Three"}},
        ],
        "3": [
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
            return httpx.Response(200, json={"data": [{"id": 1}, {"id": "2"}]})
        if request.url.path == "/genre/116/radios":
            return httpx.Response(200, json={"data": [{"id": 3}]})
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
    assert "/radio/1/tracks" in requests
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


async def test_genre_pagination_validates_before_slicing_and_reports_truthful_next() -> None:
    provider = GenreCandidateProvider(
        [str(artist_id) for artist_id in range(1, 17)], invalid_ids={"2", "4", "6"}
    )
    service = DiscoveryService(provider)  # type: ignore[arg-type]

    first = await service.get("genre", "US", genre_id="132", limit=12)
    second = await service.get("genre", "US", genre_id="132", page=2, limit=12)

    assert [artist.provider_id for artist in first.items] == [
        "1",
        "3",
        "5",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
    ]
    assert first.has_next is True
    assert [artist.provider_id for artist in second.items] == ["16"]
    assert second.has_next is False
    assert set(first.items).isdisjoint(second.items)


@pytest.mark.parametrize("count,expected_next", [(12, False), (13, True)])
async def test_genre_pagination_probes_one_valid_candidate_for_next(
    count: int, expected_next: bool
) -> None:
    provider = GenreCandidateProvider([str(artist_id) for artist_id in range(1, count + 1)])

    section = await DiscoveryService(provider).get(  # type: ignore[arg-type]
        "genre", "US", genre_id="132", limit=12
    )

    assert len(section.items) == 12
    assert section.has_next is expected_next


async def test_duplicate_heavy_genre_tracks_backfill_from_full_bounded_pool(monkeypatch) -> None:
    tracks = [{"artist": {"id": 1, "name": "Artist 1"}} for _ in range(12)]
    tracks.extend(
        {"artist": {"id": artist_id, "name": f"Artist {artist_id}"}} for artist_id in range(2, 15)
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/genre/132":
            return httpx.Response(200, json={"id": 132, "name": "Pop"})
        if request.url.path == "/genre/132/radios":
            return httpx.Response(200, json={"data": [{"id": 1}]})
        if request.url.path == "/radio/1/tracks":
            return httpx.Response(200, json={"data": tracks})
        if request.url.path.startswith("/artist/"):
            artist_id = request.url.path.rsplit("/", 1)[1]
            return httpx.Response(200, json={"id": int(artist_id), "name": f"Artist {artist_id}"})
        raise AssertionError(f"unexpected URL: {request.url}")

    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com", transport=httpx.MockTransport(handler)
        ),
    )

    section = await DiscoveryService(provider).get("genre", "US", genre_id="132", limit=12)

    assert [artist.provider_id for artist in section.items] == [
        str(index) for index in range(1, 13)
    ]
    assert section.has_next is True


async def test_genre_page_twenty_stays_bounded_and_never_advertises_page_twenty_one() -> None:
    provider = GenreCandidateProvider([str(artist_id) for artist_id in range(1, 501)])

    section = await DiscoveryService(provider).get(  # type: ignore[arg-type]
        "genre", "US", genre_id="132", page=20, limit=12
    )

    assert [artist.provider_id for artist in section.items] == [
        str(artist_id) for artist_id in range(229, 241)
    ]
    assert section.has_next is False


async def test_deezer_genre_candidates_keep_radio_order_when_later_response_finishes_first(
    monkeypatch,
) -> None:
    completed: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/genre/132":
            return httpx.Response(200, json={"id": 132, "name": "Pop"})
        if request.url.path == "/genre/132/radios":
            return httpx.Response(200, json={"data": [{"id": 1}, {"id": 2}]})
        if request.url.path == "/radio/1/tracks":
            await asyncio.sleep(0.02)
            completed.append("1")
            return httpx.Response(
                200, json={"data": [{"artist": {"id": 10, "name": "First radio"}}]}
            )
        if request.url.path == "/radio/2/tracks":
            completed.append("2")
            return httpx.Response(
                200, json={"data": [{"artist": {"id": 20, "name": "Second radio"}}]}
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

    artists = await provider.genre_artist_candidates("132")

    assert artists.genre_name == "Pop"
    assert completed == ["2", "1"]
    assert [artist.provider_id for artist in artists] == ["10", "20"]


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


@pytest.mark.parametrize(
    "failing_path,payload",
    [
        ("/genre/132", {"id": 132, "name": "Pop", "error": {}}),
        ("/genre/132/radios", {"data": [{"id": 1}], "error": {}}),
        ("/radio/1/tracks", {"data": [], "error": {}}),
    ],
)
async def test_deezer_genre_discovery_rejects_error_key_regardless_of_truthiness(
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

    with pytest.raises(ValueError, match="genre"):
        await provider.genre_artist_candidates("132")


@pytest.mark.parametrize("bad_id", [{"unsafe": 1}, [1], True, 1.5, "", "0", 0])
@pytest.mark.parametrize("location", ["radio", "artist"])
async def test_deezer_genre_discovery_rejects_non_positive_scalar_ids_before_use(
    monkeypatch, bad_id: object, location: str
) -> None:
    requested_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_paths.append(request.url.path)
        if request.url.path == "/genre/132":
            return httpx.Response(200, json={"id": 132, "name": "Pop"})
        if request.url.path == "/genre/132/radios":
            radio_id: object = bad_id if location == "radio" else 1
            return httpx.Response(200, json={"data": [{"id": radio_id}]})
        if request.url.path == "/radio/1/tracks":
            return httpx.Response(
                200, json={"data": [{"artist": {"id": bad_id, "name": "Artist"}}]}
            )
        raise AssertionError(f"unsafe or unexpected URL: {request.url}")

    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com", transport=httpx.MockTransport(handler)
        ),
    )

    with pytest.raises(ValueError, match="genre"):
        await provider.genre_artist_candidates("132")
    if location == "radio":
        assert not any(path.startswith("/radio/") for path in requested_paths)


@pytest.mark.parametrize("bad_id", [{"unsafe": 1}, [132], True, 132.0, "", "0", 0])
async def test_deezer_genre_discovery_rejects_invalid_exact_genre_ids(
    monkeypatch, bad_id: object
) -> None:
    requested_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_paths.append(request.url.path)
        if request.url.path == "/genre/132":
            return httpx.Response(200, json={"id": bad_id, "name": "Pop"})
        raise AssertionError(f"unsafe or unexpected URL: {request.url}")

    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com", transport=httpx.MockTransport(handler)
        ),
    )

    with pytest.raises(ValueError, match="genre"):
        await provider.genre_artist_candidates("132")
    assert requested_paths == ["/genre/132"]


async def test_discovery_filters_definitively_invalid_artist_identity() -> None:
    class InvalidProvider(FakeDiscoveryProvider):
        async def discovery_feed(
            self,
            feed: str,
            *,
            page: int = 1,
            limit: int = 12,
            genre_id: str | None = None,
            offset: int | None = None,
        ) -> list[ArtistHit | DiscoveryRelease]:
            if feed == "new":
                return [
                    DiscoveryRelease(
                        "deezer", "release-bad", "Bad release", "Bad artist", "10002824"
                    )
                ]
            return await super().discovery_feed(
                feed, page=page, limit=limit, genre_id=genre_id, offset=offset
            )

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


async def test_deezer_exact_artist_rejects_error_key_even_when_null(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/artist/1"
        return httpx.Response(
            200,
            json={"id": 1, "name": "Rejected envelope", "error": None},
        )

    provider = DeezerClient()
    monkeypatch.setattr(
        provider,
        "_client",
        lambda: httpx.AsyncClient(
            base_url="https://api.deezer.com", transport=httpx.MockTransport(handler)
        ),
    )

    with pytest.raises(ValueError, match="valid matching artist identity"):
        await provider.get_artist("1")


async def test_card_state_projection_is_exact_batched_and_requires_present_artifact(
    db_session, tmp_path
) -> None:
    from sqlalchemy import event

    from app.models.catalog_entities import CatalogAlbum, CatalogArtist, CatalogArtistIdentity
    from app.models.import_plan import ImportPlan, LibraryFileState
    from app.models.job import Job, JobStatus
    from app.models.release import Release
    from app.models.track import Track
    from app.models.workflow import AcquisitionState, ImportWorkflowState
    from app.services.discovery import project_discovery_card_states

    present = tmp_path / "present.flac"
    present.write_bytes(b"audio")
    monitored = CatalogArtist(name="Same Name", monitored=True)
    monitored.identities.append(
        CatalogArtistIdentity(provider="deezer", provider_artist_id="one", name="Same Name")
    )
    album = CatalogAlbum(artist=monitored, title="Known Album")
    job = Job(source="legacy", query="known", status=JobStatus.done, catalog_album=album)
    release = Release(job=job, source="legacy", title="Known Album", album_artist="Same Name")
    track = Track(
        job=job,
        release=release,
        catalog_album=album,
        source="legacy",
        title="Song",
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.imported,
        file_size_bytes=5,
    )
    plan = ImportPlan(
        release=release,
        track=track,
        source_path=str(present),
        destination_path=str(present),
        status=ImportWorkflowState.imported,
        file_state=LibraryFileState.present,
    )
    unmonitored = CatalogArtist(name="Same Name", monitored=False)
    unmonitored.identities.append(
        CatalogArtistIdentity(provider="deezer", provider_artist_id="two", name="Same Name")
    )
    db_session.add_all([monitored, unmonitored, plan])
    await db_session.commit()

    statements = 0

    def count(_conn, _cursor, statement, _parameters, _context, _many):
        nonlocal statements
        if statement.lstrip().upper().startswith("SELECT"):
            statements += 1

    event.listen(db_session.bind.sync_engine, "before_cursor_execute", count)
    try:
        states = await project_discovery_card_states(
            db_session,
            {("deezer", "one"), ("deezer", "two"), ("deezer", "missing")},
        )
    finally:
        event.remove(db_session.bind.sync_engine, "before_cursor_execute", count)

    assert statements == 1
    assert states[("deezer", "one")].monitored is True
    assert states[("deezer", "one")].local_library is True
    assert states[("deezer", "two")].monitored is False
    assert states[("deezer", "two")].local_library is False
    assert ("deezer", "missing") not in states


async def test_discovery_library_probe_is_bounded_and_truthful_when_truncated(
    monkeypatch,
) -> None:
    from app.services import discovery as discovery_service

    calls: list[str] = []

    def missing(path):
        calls.append(str(path))
        return False

    monkeypatch.setattr(discovery_service.Path, "is_file", missing)
    paths = [f"/library/{index}.flac" for index in range(100)]
    result = await discovery_service._probe_discovery_library_paths(paths, total_count=100)
    assert result is None
    assert len(calls) == discovery_service._DISCOVERY_FILE_PROBE_LIMIT


async def test_non_genre_pagination_uses_display_offset_and_validated_continuation() -> None:
    class Provider(FakeDiscoveryProvider):
        def __init__(self) -> None:
            super().__init__()
            self.requests: list[tuple[int, int | None]] = []

        async def discovery_feed(
            self,
            feed: str,
            *,
            page: int = 1,
            limit: int = 12,
            genre_id: str | None = None,
            offset: int | None = None,
        ) -> list[ArtistHit | DiscoveryRelease]:
            self.requests.append((limit, offset))
            start = offset if offset is not None else (page - 1) * limit
            return [
                ArtistHit(
                    "deezer",
                    str(index),
                    f"Artist {index}",
                    deezer_id=str(index),
                )
                for index in range(start, start + limit)
            ]

        async def get_artist(self, provider_id: str) -> ArtistDetail:
            if int(provider_id) < 22:
                raise ValueError("invalid early artist")
            return ArtistDetail(
                "deezer", provider_id, f"Artist {provider_id}", deezer_id=provider_id
            )

    provider = Provider()
    service = DiscoveryService(provider)  # type: ignore[arg-type]
    first = await service.get("popular", "US", page=1, limit=12)
    second = await service.get("popular", "US", page=2, limit=12)

    assert provider.requests == [(25, 0), (25, 25), (25, 0), (25, 25)]
    assert [item.provider_id for item in first.items] == [str(index) for index in range(22, 34)]
    assert [item.provider_id for item in second.items] == [str(index) for index in range(34, 46)]
    assert set(item.provider_id for item in first.items).isdisjoint(
        item.provider_id for item in second.items
    )
    assert first.has_next is second.has_next is True
