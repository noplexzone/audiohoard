from __future__ import annotations

from sqlalchemy import select

from app.metadata.base import ArtistHit, DiscoveryGenre, DiscoveryRelease, DiscoverySection
from app.models.catalog_entities import CatalogArtist


async def test_empty_search_renders_neutral_discovery_without_persisting_rows(
    client, monkeypatch
) -> None:
    from app.routers import search as search_router

    captured_session = None
    original_watched = search_router._watched_catalog_artists

    async def capture_watched(db):
        nonlocal captured_session
        captured_session = db
        return await original_watched(db)

    async def landing(region: str):
        assert captured_session is not None
        assert captured_session.in_transaction() is False
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

    monkeypatch.setattr(search_router, "_watched_catalog_artists", capture_watched)
    monkeypatch.setattr(search_router.discovery_service, "landing", landing)

    response = await client.get("/search")

    assert response.status_code == 200
    for text in ("Popular artists", "Genres", "New releases", "Trending releases"):
        assert text in response.text
    assert "Global fallback" in response.text
    assert 'action="/artists/catalog/open"' in response.text
    assert 'name="provider_id" value="artist-2"' in response.text
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
