from __future__ import annotations

import httpx

from app.metadata.base import ArtistHit, DiscoveryRelease
from app.metadata.deezer import DeezerClient
from app.services.discovery import DiscoveryService


class FakeDiscoveryProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []
        self.fail = False

    async def discovery_feed(
        self, feed: str, *, page: int = 1, limit: int = 12, genre_id: str | None = None
    ) -> list[ArtistHit]:
        self.calls.append((feed, page))
        if self.fail:
            raise OSError("provider details must not leak")
        return [ArtistHit("deezer", f"{feed}-{page}", feed.title())][:limit]


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
        ) -> list[ArtistHit]:
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
