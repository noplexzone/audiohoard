from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from sqlalchemy import and_, func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.metadata.base import (
    ArtistHit,
    DiscoveryCardState,
    DiscoveryGenre,
    DiscoveryRelease,
    DiscoverySection,
)
from app.metadata.deezer import DeezerClient
from app.models.catalog_entities import CatalogAlbum, CatalogArtist, CatalogArtistIdentity
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.catalog_metadata import validated_artist_hits

_DISCOVERY_FILE_PROBE_LIMIT = 64
_DISCOVERY_FILE_PROBE_CONCURRENCY = 8

_LANDING_FEEDS = ("popular", "genres", "new", "trending")

_TITLES = {
    "popular": "Popular artists",
    "genres": "Genres",
    "genre": "Genre artists",
    "new": "New releases",
    "trending": "Trending releases",
}


async def _probe_discovery_library_paths(paths: list[str], *, total_count: int) -> bool | None:
    candidates = sorted(set(paths))[:_DISCOVERY_FILE_PROBE_LIMIT]
    for start in range(0, len(candidates), _DISCOVERY_FILE_PROBE_CONCURRENCY):
        present = await asyncio.gather(
            *(
                asyncio.to_thread(Path(path).is_file)
                for path in candidates[start : start + _DISCOVERY_FILE_PROBE_CONCURRENCY]
            )
        )
        if any(present):
            return True
    return None if total_count > len(candidates) else False


async def project_discovery_card_states(
    db: AsyncSession, identities: set[tuple[str, str]]
) -> dict[tuple[str, str], DiscoveryCardState]:
    """Load exact visible identities and bounded verified artifact candidates."""
    if not identities:
        return {}
    path_candidates = (
        select(
            CatalogAlbum.artist_id.label("artist_id"),
            ImportPlan.destination_path.label("destination_path"),
        )
        .select_from(CatalogAlbum)
        .join(Track, Track.catalog_album_id == CatalogAlbum.id)
        .join(ImportPlan, ImportPlan.track_id == Track.id)
        .where(
            Track.acquisition_state == AcquisitionState.downloaded,
            Track.import_state == ImportWorkflowState.imported,
            Track.file_size_bytes.is_not(None),
            Track.file_size_bytes > 0,
            ImportPlan.status == ImportWorkflowState.imported,
            ImportPlan.file_state == LibraryFileState.present,
            ImportPlan.destination_path != "",
        )
        .distinct()
        .subquery()
    )
    ranked_paths = select(
        path_candidates.c.artist_id,
        path_candidates.c.destination_path,
        func.row_number()
        .over(
            partition_by=path_candidates.c.artist_id,
            order_by=path_candidates.c.destination_path,
        )
        .label("path_rank"),
        func.count().over(partition_by=path_candidates.c.artist_id).label("path_count"),
    ).subquery()
    rows = (
        await db.execute(
            select(
                CatalogArtistIdentity.provider,
                CatalogArtistIdentity.provider_artist_id,
                CatalogArtist.id.label("catalog_artist_id"),
                CatalogArtist.monitored,
                CatalogArtist.watchlist_release_albums,
                CatalogArtist.watchlist_release_singles,
                CatalogArtist.watchlist_release_eps,
                CatalogArtist.watchlist_monitor_upgrades,
                ranked_paths.c.destination_path,
                ranked_paths.c.path_count,
            )
            .join(CatalogArtist, CatalogArtist.id == CatalogArtistIdentity.artist_id)
            .outerjoin(
                ranked_paths,
                and_(
                    ranked_paths.c.artist_id == CatalogArtist.id,
                    ranked_paths.c.path_rank <= _DISCOVERY_FILE_PROBE_LIMIT,
                ),
            )
            .where(
                tuple_(
                    CatalogArtistIdentity.provider, CatalogArtistIdentity.provider_artist_id
                ).in_(identities)
            )
        )
    ).all()
    grouped: dict[tuple[str, str], tuple[Any, list[str], int]] = {}
    for row in rows:
        key = (str(row.provider), str(row.provider_artist_id))
        state, paths, total_count = grouped.setdefault(key, (row, [], int(row.path_count or 0)))
        if row.destination_path:
            paths.append(str(row.destination_path))
        grouped[key] = (state, paths, max(total_count, int(row.path_count or 0)))
    result: dict[tuple[str, str], DiscoveryCardState] = {}
    for key, (row, paths, total_count) in grouped.items():
        result[key] = DiscoveryCardState(
            catalog_artist_id=int(row.catalog_artist_id),
            monitored=bool(row.monitored),
            local_library=await _probe_discovery_library_paths(paths, total_count=total_count),
            watchlist_release_albums=bool(row.watchlist_release_albums),
            watchlist_release_singles=bool(row.watchlist_release_singles),
            watchlist_release_eps=bool(row.watchlist_release_eps),
            watchlist_monitor_upgrades=bool(row.watchlist_monitor_upgrades),
        )
    return result


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

    @staticmethod
    def _cache_key(feed: str, region: str, *, page: int, limit: int, genre_id: str | None) -> str:
        return f"deezer:{feed}:{region}:{page}:{limit}:{genre_id or ''}"

    def peek(
        self,
        feed: str,
        region: str,
        *,
        page: int = 1,
        limit: int = 12,
        genre_id: str | None = None,
    ) -> DiscoverySection:
        """Return a usable cached section or a pending placeholder without doing I/O."""
        page = max(1, min(page, 20))
        limit = max(1, min(limit, 25))
        key = self._cache_key(feed, region, page=page, limit=limit, genre_id=genre_id)
        now = time.monotonic()
        cached = self._cache.get(key)
        if cached is None or cached[0] + self.stale_seconds <= now:
            self._cache.pop(key, None)
            return DiscoverySection(
                feed=feed,
                title=_TITLES[feed],
                requested_region=region,
                effective_region=region,
                fallback_global=False,
                state="pending",
            )
        if cached[0] > now:
            return cached[1]
        return replace(
            cached[1],
            state="stale",
            stale=True,
            message="Showing cached results",
        )

    def landing_snapshot(self, region: str, *, limit: int = 12) -> list[DiscoverySection]:
        """Build the landing feed entirely from local cache state."""
        return [self.peek(feed, region, limit=limit) for feed in _LANDING_FEEDS]

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
        key = self._cache_key(feed, region, page=page, limit=limit, genre_id=genre_id)
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
            has_next: bool | None = None
            items: list[ArtistHit | DiscoveryGenre | DiscoveryRelease] = []
            if feed == "genre":
                if genre_id is None:
                    raise ValueError("Genre discovery requires an ID")
                candidates = await self.provider.genre_artist_candidates(genre_id)
                index = (page - 1) * limit
                target = index + limit + 1
                validated: list[ArtistHit] = []
                batch_size = max(25, limit + 1)
                for start in range(0, len(candidates), batch_size):
                    validated.extend(
                        await validated_artist_hits(
                            self.provider,
                            "deezer",
                            candidates[start : start + batch_size],
                            preserve_order=True,
                        )
                    )
                    if len(validated) >= target:
                        break
                items.extend(validated[index : index + limit])
                has_next = page < 20 and len(validated) > index + limit
            else:
                items = await self.provider.discovery_feed(
                    feed, page=page, limit=limit, genre_id=genre_id
                )
            if feed == "popular":
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
                    cached[1],
                    state="stale",
                    stale=True,
                    message="Showing cached results; refresh failed",
                )
            return DiscoverySection(
                feed=feed,
                title=_TITLES[feed],
                requested_region=region,
                effective_region="GLOBAL",
                fallback_global=True,
                state="error",
                message="Discovery provider is temporarily unavailable",
                has_next=False if feed == "genre" else None,
            )
        section = DiscoverySection(
            feed=feed,
            title=_TITLES[feed],
            requested_region=region,
            effective_region="GLOBAL",
            fallback_global=True,
            items=tuple(items),
            has_next=has_next,
        )
        self._cache[key] = (now + self.ttl_seconds, section)
        while len(self._cache) > self.max_entries:
            self._cache.pop(next(iter(self._cache)))
        return section

    async def landing(self, region: str) -> list[DiscoverySection]:
        outcomes = await asyncio.gather(
            *(self.get(feed, region) for feed in _LANDING_FEEDS), return_exceptions=True
        )
        sections: list[DiscoverySection] = []
        for feed, outcome in zip(_LANDING_FEEDS, outcomes, strict=True):
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
