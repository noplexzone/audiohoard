from __future__ import annotations

import asyncio
from typing import cast

import httpx

from app.http import request_with_retry
from app.metadata.base import AlbumDetail, AlbumHit, AlbumTrack, ArtistDetail, ArtistHit, TTLCache
from app.metadata.content_rating import itunes_content_rating
from app.sources.base import CapabilityState

_BASE_URL = "https://itunes.apple.com"
_HTTP_TIMEOUT = httpx.Timeout(10.0)
_THROTTLE_DELAY = 3.1  # keyless public API, about 20/minute
_THROTTLE_LOCK = asyncio.Lock()


class ITunesClient:
    """Keyless iTunes Search API provider.

    Apple Music API support can be added later behind MetadataProvider with a tokened client.
    """

    name = "itunes"

    def __init__(self, base_url: str = _BASE_URL) -> None:
        self._base_url = base_url.rstrip("/")
        self._cache = TTLCache()

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base_url, timeout=_HTTP_TIMEOUT)

    async def _get(self, path: str, params: dict[str, object]) -> httpx.Response:
        async with _THROTTLE_LOCK:
            async with self._client() as client:
                resp = await request_with_retry(client, "GET", path, params=params)
            await asyncio.sleep(_THROTTLE_DELAY)
        return resp

    async def health(self) -> CapabilityState:
        return CapabilityState(available=True)

    async def search_artists(self, query: str) -> list[ArtistHit]:
        cache_key = f"artist-search:{query}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return list(cast(list[ArtistHit], cached))
        resp = await self._get("/search", {"term": query, "entity": "musicArtist", "limit": 10})
        resp.raise_for_status()
        hits = [
            _parse_artist(item)
            for item in resp.json().get("results", [])
            if isinstance(item, dict)
        ]
        self._cache.set(cache_key, hits, 15 * 60)
        return hits

    async def get_artist(self, id: str) -> ArtistDetail:
        cache_key = f"artist:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cast(ArtistDetail, cached)
        resp = await self._get("/lookup", {"id": id})
        resp.raise_for_status()
        results = resp.json().get("results", [])
        data = results[0] if isinstance(results, list) and results else {"artistId": id}
        detail = (
            ArtistDetail(**_parse_artist(data).__dict__)
            if isinstance(data, dict)
            else ArtistDetail(provider="itunes", provider_id=id, name="", itunes_id=id)
        )
        self._cache.set(cache_key, detail, 24 * 60 * 60)
        return detail

    async def get_discography(self, id: str) -> list[AlbumHit]:
        cache_key = f"discography:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return list(cast(list[AlbumHit], cached))
        resp = await self._get("/lookup", {"id": id, "entity": "album", "limit": 200})
        resp.raise_for_status()
        albums = [
            _parse_album_hit(item, artist_id=id)
            for item in resp.json().get("results", [])
            if isinstance(item, dict) and item.get("wrapperType") == "collection"
        ]
        albums.sort(key=lambda a: (a.year or "0000", a.title), reverse=True)
        self._cache.set(cache_key, albums, 24 * 60 * 60)
        return albums

    async def get_album(self, id: str) -> AlbumDetail:
        cache_key = f"album:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cast(AlbumDetail, cached)
        resp = await self._get("/lookup", {"id": id, "entity": "song", "limit": 200})
        resp.raise_for_status()
        results = [item for item in resp.json().get("results", []) if isinstance(item, dict)]
        collection: dict[str, object] = next(
            (item for item in results if item.get("wrapperType") == "collection"),
            {"collectionId": id},
        )
        hit = _parse_album_hit(collection, artist_id=str(collection.get("artistId") or "") or None)
        tracks = [_parse_track(item) for item in results if item.get("wrapperType") == "track"]
        values = hit.__dict__.copy()
        values["track_count"] = len(tracks) or hit.track_count
        detail = AlbumDetail(**values, tracks=tracks)
        self._cache.set(cache_key, detail, 24 * 60 * 60)
        return detail

    async def search_track(self, title: str, artist: str | None = None) -> list[AlbumTrack]:
        term = " ".join(part for part in (artist, title) if part).strip()
        resp = await self._get("/search", {"term": term, "entity": "song", "limit": 10})
        resp.raise_for_status()
        return [
            _parse_track(item)
            for item in resp.json().get("results", [])
            if isinstance(item, dict) and item.get("wrapperType", "track") == "track"
        ]


def _year(value: object) -> str | None:
    text = str(value or "")
    return text[:4] if len(text) >= 4 and text[:4].isdigit() else None


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _artwork(value: object) -> str | None:
    text = str(value or "")
    return text.replace("100x100bb", "300x300bb") if text else None


def _parse_artist(data: dict[str, object]) -> ArtistHit:
    iid = str(data.get("artistId") or "")
    return ArtistHit(
        provider="itunes",
        provider_id=iid,
        itunes_id=iid or None,
        name=str(data.get("artistName") or ""),
        disambiguation=str(data.get("primaryGenreName") or "") or None,
    )


def _parse_album_hit(data: dict[str, object], artist_id: str | None) -> AlbumHit:
    iid = str(data.get("collectionId") or "")
    title = str(data.get("collectionName") or "")
    release_type = str(data.get("collectionType") or "Album") or "Album"
    normalized_title = title.casefold().replace("–", "-").replace("—", "-").rstrip()
    if normalized_title.endswith("- single"):
        release_type = "Single"
    elif normalized_title.endswith("- ep") or normalized_title.endswith("- e.p."):
        release_type = "EP"
    release_kind = {"single": "single", "ep": "ep", "album": "album"}.get(
        release_type.casefold(), "unknown"
    )
    return AlbumHit(
        provider="itunes",
        provider_id=iid,
        itunes_id=iid or None,
        title=title,
        artist_name=str(data.get("artistName") or "") or None,
        artist_provider_id=artist_id,
        year=_year(data.get("releaseDate")),
        release_type=release_type,
        release_kind=release_kind,
        release_type_raw=str(data.get("collectionType") or "") or None,
        artwork_url=_artwork(data.get("artworkUrl100")),
        track_count=_to_int(data.get("trackCount")),
        content_rating=itunes_content_rating(data),
    )


def _parse_track(data: dict[str, object]) -> AlbumTrack:
    duration_ms = _to_int(data.get("trackTimeMillis"))
    return AlbumTrack(
        position=_to_int(data.get("trackNumber")) or 1,
        disc=_to_int(data.get("discNumber")) or 1,
        title=str(data.get("trackName") or ""),
        duration_sec=duration_ms // 1000 if duration_ms else None,
        provider_track_id=str(data.get("trackId") or "") or None,
        preview_url=str(data.get("previewUrl") or "") or None,
        content_rating=itunes_content_rating(data),
    )
