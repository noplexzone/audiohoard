from __future__ import annotations

from sqlalchemy import select

from app.metadata.base import (
    ArtistDetail,
    ArtistHit,
    DiscoveryGenre,
    DiscoveryRelease,
    DiscoverySection,
)
from app.models.catalog_entities import CatalogArtist


async def test_empty_search_renders_neutral_discovery_without_persisting_rows(
    client, monkeypatch
) -> None:
    from app.routers import search as search_router

    async def landing(region: str):
        return [
            DiscoverySection(
                "popular",
                "Popular artists",
                region,
                "GLOBAL",
                True,
                (ArtistHit("deezer", "artist-1", "Popular Artist", rank=1),),
            ),
            DiscoverySection(
                "genres",
                "Genres",
                region,
                "GLOBAL",
                True,
                (DiscoveryGenre("deezer", "132", "Pop"),),
            ),
            DiscoverySection(
                "new",
                "New releases",
                region,
                "GLOBAL",
                True,
                (
                    DiscoveryRelease(
                        "deezer",
                        "release-1",
                        "New Album",
                        "New Artist",
                        "artist-2",
                        release_date="2026-08-01",
                    ),
                ),
            ),
            DiscoverySection(
                "trending",
                "Trending releases",
                region,
                "GLOBAL",
                True,
                (
                    DiscoveryRelease(
                        "deezer",
                        "release-2",
                        "Trending Album",
                        "Trending Artist",
                        "artist-3",
                        rank=2,
                    ),
                ),
            ),
        ]

    monkeypatch.setattr(search_router.discovery_service, "landing", landing)

    response = await client.get("/search")

    assert response.status_code == 200
    for text in ("Popular artists", "Genres", "New releases", "Trending releases"):
        assert text in response.text
    assert "Global fallback" in response.text
    assert 'action="/artists/catalog/open"' in response.text
    assert 'name="provider_id" value="artist-2"' in response.text
    assert "/artists/provider-preview?" in response.text
    assert "/artists/catalog/open?" not in response.text
    assert 'action="/downloads/create"' not in response.text
    assert "data-download-form" not in response.text
    assert "/discover/genres/132" in response.text

    from app.database import get_session_factory

    async with get_session_factory()() as db:
        assert list(await db.scalars(select(CatalogArtist))) == []


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
