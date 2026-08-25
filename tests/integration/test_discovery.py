from __future__ import annotations

import asyncio

from sqlalchemy import select

from app.metadata.base import (
    ArtistDetail,
    ArtistHit,
    DiscoveryGenre,
    DiscoveryRelease,
    DiscoverySection,
)
from app.models.catalog_entities import CatalogArtist


async def test_empty_search_renders_pending_shell_without_calling_provider(
    client, monkeypatch
) -> None:
    from app.routers import search as search_router

    provider_called = asyncio.Event()

    async def get(*_args, **_kwargs):
        provider_called.set()
        await asyncio.Event().wait()

    def landing_snapshot(region: str):
        return [
            DiscoverySection(feed, title, region, "GLOBAL", True, state="pending")
            for feed, title in (
                ("popular", "Popular artists"),
                ("genres", "Genres"),
                ("new", "New releases"),
                ("trending", "Trending releases"),
            )
        ]

    monkeypatch.setattr(search_router.discovery_service, "get", get)
    monkeypatch.setattr(search_router.discovery_service, "landing_snapshot", landing_snapshot)

    response = await asyncio.wait_for(client.get("/search"), timeout=0.5)

    assert response.status_code == 200
    for text in ("Popular artists", "Genres", "New releases", "Trending releases"):
        assert text in response.text
    assert response.text.count('data-discover-state="pending"') == 4
    assert response.text.count("Loading discovery feed") == 4
    assert "Nothing to show" not in response.text
    assert provider_called.is_set() is False

    from app.database import get_session_factory

    async with get_session_factory()() as db:
        assert list(await db.scalars(select(CatalogArtist))) == []


async def test_empty_search_renders_cached_ready_and_stale_sections_without_provider(
    client, monkeypatch
) -> None:
    from app.routers import search as search_router

    sections = [
        DiscoverySection(
            "popular",
            "Popular artists",
            "US",
            "GLOBAL",
            True,
            (ArtistHit("deezer", "artist-1", "Cached Artist"),),
        ),
        DiscoverySection("genres", "Genres", "US", "GLOBAL", True, state="stale", stale=True),
        DiscoverySection("new", "New releases", "US", "GLOBAL", True, state="pending"),
        DiscoverySection("trending", "Trending releases", "US", "GLOBAL", True, state="pending"),
    ]

    def landing_snapshot(_region: str):
        return sections

    async def unexpected_get(*_args, **_kwargs):
        raise AssertionError("landing shell must not call provider")

    monkeypatch.setattr(search_router.discovery_service, "landing_snapshot", landing_snapshot)
    monkeypatch.setattr(search_router.discovery_service, "get", unexpected_get)

    response = await client.get("/search")

    assert response.status_code == 200
    assert "Cached Artist" in response.text
    assert 'data-discover-state="ready"' in response.text
    assert 'data-discover-state="stale"' in response.text
    assert "Cached" in response.text
    assert "This feed is currently empty" in response.text


async def test_discovery_fragment_requires_authentication(unauthenticated_client) -> None:
    response = await unauthenticated_client.get(
        "/discover/fragments/popular", follow_redirects=False
    )
    assert response.status_code == 401


async def test_discovery_fragment_allowlist_errors_empty_and_exact_card_state(
    client, monkeypatch
) -> None:
    from app.metadata.base import DiscoveryCardState
    from app.routers import search as search_router

    calls: list[str] = []
    projections: list[set[tuple[str, str]]] = []

    async def get(feed, region, *, page=1, limit=12, genre_id=None):
        calls.append(feed)
        if feed == "new":
            return DiscoverySection(
                feed,
                "New releases",
                region,
                "GLOBAL",
                True,
                state="error",
                message="Discovery provider is temporarily unavailable",
            )
        items = ()
        if feed == "popular":
            items = (ArtistHit("deezer", "exact-artist", "Exact Artist"),)
        elif feed == "genres":
            items = (DiscoveryGenre("deezer", "132", "Pop"),)
        elif feed == "trending":
            items = (
                DiscoveryRelease(
                    "deezer",
                    "release-1",
                    "Exact Release",
                    "Release Artist",
                    "release-artist",
                ),
            )
        return DiscoverySection(feed, feed.title(), region, "GLOBAL", True, items)

    async def project(_db, identities):
        projections.append(identities)
        if not identities:
            return {}
        return {("deezer", "exact-artist"): DiscoveryCardState(42, False, False)}

    monkeypatch.setattr(search_router.discovery_service, "get", get)
    monkeypatch.setattr(search_router, "project_discovery_card_states", project)

    assert (await client.get("/discover/fragments/unknown")).status_code == 404

    failed = await client.get("/discover/fragments/new")
    ready = await client.get("/discover/fragments/popular")
    genre = await client.get("/discover/fragments/genres")
    release = await client.get("/discover/fragments/trending")

    assert (
        failed.status_code == ready.status_code == genre.status_code == release.status_code == 200
    )
    assert "temporarily unavailable" in failed.text
    assert 'href="/search#discovery-new"' in failed.text
    assert "data-discover-retry" in failed.text
    assert "Exact Artist" in ready.text
    assert 'name="csrf_token"' in ready.text
    assert 'name="provider_id" value="exact-artist"' in ready.text
    assert 'name="return_to" value="/search#discovery-popular"' in ready.text
    assert "/discover/genres/132" in genre.text
    assert "Exact Release" in release.text
    assert "Release Artist" in release.text
    assert "/artists/provider-preview?" in release.text
    assert "/artists/catalog/open?" not in release.text
    assert "Loading discovery feed" not in genre.text
    assert calls == ["new", "popular", "genres", "trending"]
    assert projections == [
        set(),
        {("deezer", "exact-artist")},
        {("deezer", "132")},
        {("deezer", "release-artist")},
    ]


async def test_dedicated_discovery_routes_bound_page_and_genre(client, monkeypatch) -> None:
    from app.routers import search as search_router

    calls: list[tuple[str, str, int, int, str | None]] = []

    async def get(feed, region, *, page=1, limit=12, genre_id=None):
        calls.append((feed, region, page, limit, genre_id))
        item = (
            DiscoveryGenre("deezer", "132", "Pop")
            if feed == "genres"
            else ArtistHit("deezer", "artist-page", "Paged Artist")
        )
        return DiscoverySection(feed, feed.title(), region, "GLOBAL", True, (item,))

    monkeypatch.setattr(search_router.discovery_service, "get", get)

    assert (await client.get("/discover/popular?page=2")).status_code == 200
    assert calls[-1][0:5] == ("popular", "US", 2, 12, None)
    genre = await client.get("/discover/genres/132?page=3")
    assert genre.status_code == 200
    assert calls[-1][0:5] == ("genre", "US", 3, 12, "132")
    assert (await client.get("/discover/genres/not-a-number")).status_code == 404
    assert (await client.get("/discover/popular?page=21")).status_code == 422


async def test_genre_next_link_uses_explicit_continuation_contract(client, monkeypatch) -> None:
    from app.routers import search as search_router

    async def get(feed, region, *, page=1, limit=12, genre_id=None):
        assert feed == "genre"
        return DiscoverySection(
            feed,
            "Genre artists",
            region,
            "GLOBAL",
            True,
            tuple(ArtistHit("deezer", str(index), f"Artist {index}") for index in range(1, 13)),
            has_next=False,
        )

    monkeypatch.setattr(search_router.discovery_service, "get", get)

    response = await client.get("/discover/genres/132")

    assert response.status_code == 200
    assert 'href="?page=2"' not in response.text


async def test_poster_cards_and_dedicated_genre_use_operate_contract(client, monkeypatch) -> None:
    from app.routers import search as search_router

    async def get(feed, region, *, page=1, limit=12, genre_id=None):
        if feed == "genre":
            return DiscoverySection(
                feed,
                "Jazz",
                region,
                "GLOBAL",
                True,
                (
                    ArtistHit(
                        "deezer",
                        "same-1",
                        "A very long artist name that must wrap safely",
                        artwork_url="https://images.example/one.jpg",
                    ),
                    ArtistHit("deezer", "same-2", "A very long artist name that must wrap safely"),
                ),
                has_next=True,
            )
        if feed == "genres":
            return DiscoverySection(
                feed,
                "Genres",
                region,
                "GLOBAL",
                True,
                (DiscoveryGenre("deezer", "132", "Pop"),),
                has_next=False,
            )
        return DiscoverySection(feed, feed.title(), region, "GLOBAL", True, (), has_next=False)

    monkeypatch.setattr(search_router.discovery_service, "get", get)
    genre = await client.get("/discover/genres/132")
    genres = await client.get("/discover/genres")
    assert "<h1>Jazz</h1>" in genre.text
    assert 'aria-label="Breadcrumb"' in genre.text
    assert 'data-provider-id="same-1"' in genre.text
    assert 'data-provider-id="same-2"' in genre.text
    assert 'loading="lazy"' in genre.text and 'decoding="async"' in genre.text
    assert 'width="480" height="480"' in genre.text
    assert "Audiohoard" in genre.text
    assert 'method="post" action="/artists/catalog/open"' in genre.text
    assert 'href="?page=2"' in genre.text
    assert 'href="/discover/genres/132"' in genres.text
    assert 'aria-label="Explore Pop genre"' in genres.text
    assert "Explore Pop genre" in genres.text
    assert 'class="discover-poster-grid"' in genres.text
    assert "horizontal-scroller" not in genres.text
    assert "/artists/catalog/open?" not in genre.text


async def test_advanced_search_skips_discovery_network(client, monkeypatch) -> None:
    from app.routers import search as search_router

    async def unexpected_landing(_region: str):
        raise AssertionError("advanced search must not load discovery")

    monkeypatch.setattr(search_router.discovery_service, "landing", unexpected_landing)

    response = await client.get("/search?tab=advanced")

    assert response.status_code == 200
    assert "Manual search" in response.text


async def test_provider_preview_is_read_only_and_has_csrf_watch_form(client, monkeypatch) -> None:
    from app.routers import catalog as catalog_router

    async def detail(_settings, provider: str, provider_id: str):
        return ArtistDetail(
            provider=provider,
            provider_id=provider_id,
            name="Exact Preview Artist",
            country="US",
            disambiguation="not the namesake",
        )

    def unexpected_task(*_args):
        raise AssertionError("read-only preview must not start discography work")

    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", detail)
    monkeypatch.setattr(catalog_router, "_start_discography_task", unexpected_task)
    response = await client.get(
        "/artists/provider-preview?provider=deezer&provider_id=artist-preview"
        "&return_to=/discover/popular?page=2%23discover-results"
    )
    assert response.status_code == 200
    assert "Exact Preview Artist" in response.text
    assert "artist-preview" in response.text
    assert "not the namesake" in response.text
    assert 'method="post" action="/artists/catalog/open"' in response.text
    assert 'name="csrf_token"' in response.text

    from app.database import get_session_factory

    async with get_session_factory()() as db:
        assert list(await db.scalars(select(CatalogArtist))) == []


def test_discover_return_path_allowlist() -> None:
    from app.routers.catalog import _safe_discover_return_path

    assert _safe_discover_return_path("/search?q=a#card") == "/search?q=a#card"
    assert (
        _safe_discover_return_path("/discover/popular?page=2#card")
        == "/discover/popular?page=2#card"
    )
    for unsafe in (
        "https://evil.example/discover/popular",
        "//evil.example/discover/popular",
        "/library",
        "/discover\\popular",
        "/discover/%0apopular",
        "/discover/../settings",
        "/discover/%2e%2e/settings",
        "/discover/%252e%252e/settings",
        "/discover/%255csettings",
        "/discover/%250asettings",
    ):
        assert _safe_discover_return_path(unsafe) is None


async def test_native_watch_returns_exact_safe_path_and_fetch_json_is_unchanged(
    client, monkeypatch
) -> None:
    from app.routers import catalog as catalog_router

    async def detail(_settings, provider: str, provider_id: str):
        return ArtistDetail(provider=provider, provider_id=provider_id, name="Return Artist")

    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", detail)
    monkeypatch.setattr(catalog_router, "_start_discography_task", lambda *_args: True)
    payload = {
        "csrf_token": client.cookies.get("csrf", ""),
        "provider": "deezer",
        "provider_id": "return-artist",
        "monitor": "true",
        "return_to": "/discover/popular?page=2#discover-results",
    }
    native = await client.post("/artists/catalog/open", data=payload, follow_redirects=False)
    assert native.status_code == 303
    assert native.headers["location"] == "/discover/popular?page=2#discover-results"

    payload["return_to"] = "https://evil.example/discover/popular"
    unsafe = await client.post("/artists/catalog/open", data=payload, follow_redirects=False)
    assert unsafe.status_code == 303
    assert unsafe.headers["location"].startswith("/artists/catalog/")

    fetch = await client.post(
        "/artists/catalog/open",
        data=payload,
        headers={"Accept": "application/json", "X-Requested-With": "fetch"},
    )
    assert fetch.status_code == 200
    assert fetch.json()["watched"] is True
    assert "artist_id" in fetch.json()


async def test_native_watch_preserves_provider_error_instead_of_redirecting(
    client, monkeypatch
) -> None:
    from app.routers import catalog as catalog_router

    async def invalid_detail(*_args, **_kwargs):
        raise ValueError("stale provider identity")

    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", invalid_detail)
    response = await client.post(
        "/artists/catalog/open",
        data={
            "csrf_token": client.cookies.get("csrf", ""),
            "provider": "deezer",
            "provider_id": "stale",
            "monitor": "true",
            "return_to": "/discover/popular?page=2#discover-results",
        },
        follow_redirects=False,
    )
    assert response.status_code == 422
    assert "Invalid artist identity" in response.text
    assert "location" not in response.headers
