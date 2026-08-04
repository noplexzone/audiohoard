from __future__ import annotations

import asyncio
import time
from dataclasses import replace

from app.metadata.base import ArtistHit, DiscoveryRelease, DiscoverySection
from app.metadata.deezer import DeezerClient
from app.services.catalog_metadata import validated_artist_hits

_TITLES = {
    "popular": "Popular artists",
    "genres": "Genres",
    "genre": "Genre artists",
    "new": "New releases",
    "trending": "Trending releases",
}


class DiscoveryService:
    def __init__(
        self,
        provider: DeezerClient | None = None,
        *,
        ttl_seconds: int = 300,
        stale_seconds: int = 3600,
        max_entries: int = 256,
    ) -> None:
        self.provider = provider or DeezerClient()
        self.ttl_seconds = ttl_seconds
        self.stale_seconds = stale_seconds
        self.max_entries = max_entries
        self._cache: dict[str, tuple[float, DiscoverySection]] = {}

    def invalidate_region(self, region: str) -> None:
        marker = f":{region}:"
        self._cache = {key: value for key, value in self._cache.items() if marker not in key}

    async def get(
        self,
        feed: str,
        region: str,
        *,
        page: int = 1,
        limit: int = 12,
        genre_id: str | None = None,
    ) -> DiscoverySection:
        page = max(1, min(page, 20))
        limit = max(1, min(limit, 25))
        key = f"deezer:{feed}:{region}:{page}:{genre_id or ''}"
        now = time.monotonic()
        self._cache = {
            cache_key: value
            for cache_key, value in self._cache.items()
            if value[0] + self.stale_seconds > now
        }
        cached = self._cache.get(key)
        if cached and cached[0] > now:
            self._cache.pop(key)
            self._cache[key] = cached
            return cached[1]
        try:
            items = await self.provider.discovery_feed(
                feed, page=page, limit=limit, genre_id=genre_id
            )
            if feed in {"popular", "genre"}:
                artists = [item for item in items if isinstance(item, ArtistHit)]
                validated = await validated_artist_hits(self.provider, "deezer", artists)
                items.clear()
                items.extend(validated)
            elif feed in {"new", "trending"}:
                releases = [item for item in items if isinstance(item, DiscoveryRelease)]
                identities = {
                    release.artist_provider_id: ArtistHit(
                        provider="deezer",
                        provider_id=release.artist_provider_id,
                        deezer_id=release.artist_provider_id,
                        name=release.artist_name,
                    )
                    for release in releases
                }
                validated = await validated_artist_hits(
                    self.provider, "deezer", list(identities.values())
                )
                allowed = {artist.provider_id for artist in validated}
                items.clear()
                items.extend(
                    release for release in releases if release.artist_provider_id in allowed
                )
        except Exception:
            if cached and cached[0] + self.stale_seconds > now:
                return replace(
                    cached[1], stale=True, message="Showing cached results; refresh failed"
                )
            return DiscoverySection(
                feed=feed,
                title=_TITLES[feed],
                requested_region=region,
                effective_region="GLOBAL",
                fallback_global=True,
                state="error",
                message="Discovery provider is temporarily unavailable",
            )
        section = DiscoverySection(
            feed=feed,
            title=_TITLES[feed],
            requested_region=region,
            effective_region="GLOBAL",
            fallback_global=True,
            items=tuple(items),
        )
        self._cache[key] = (now + self.ttl_seconds, section)
        while len(self._cache) > self.max_entries:
            self._cache.pop(next(iter(self._cache)))
        return section

    async def landing(self, region: str) -> list[DiscoverySection]:
        feeds = ("popular", "genres", "new", "trending")
        outcomes = await asyncio.gather(
            *(self.get(feed, region) for feed in feeds), return_exceptions=True
        )
        sections: list[DiscoverySection] = []
        for feed, outcome in zip(feeds, outcomes, strict=True):
            if isinstance(outcome, BaseException):
                sections.append(
                    DiscoverySection(
                        feed,
                        _TITLES[feed],
                        region,
                        "GLOBAL",
                        True,
                        state="error",
                        message="Discovery provider is temporarily unavailable",
                    )
                )
            else:
                sections.append(outcome)
        return sections


discovery_service = DiscoveryService()
