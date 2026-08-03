from __future__ import annotations

import asyncio
import time
from dataclasses import replace

from app.metadata.base import DiscoverySection
from app.metadata.deezer import DeezerClient

_TITLES = {
    "popular": "Popular artists",
    "genres": "Genres",
    "genre": "Genre artists",
    "new": "New releases",
    "trending": "Trending releases",
}


class DiscoveryService:
    def __init__(self, provider: DeezerClient | None = None, *, ttl_seconds: int = 300) -> None:
        self.provider = provider or DeezerClient()
        self.ttl_seconds = ttl_seconds
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
        cached = self._cache.get(key)
        now = time.monotonic()
        if cached and cached[0] > now:
            return cached[1]
        try:
            items = await self.provider.discovery_feed(
                feed, page=page, limit=limit, genre_id=genre_id
            )
        except Exception:
            if cached:
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
