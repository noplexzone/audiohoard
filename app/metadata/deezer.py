from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import cast

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.http import request_with_retry
from app.metadata.base import AlbumDetail, AlbumHit, AlbumTrack, ArtistDetail, ArtistHit, TTLCache
from app.sources.base import CapabilityState

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(10.0)
_DISCOGRAPHY_COUNT_HTTP_TIMEOUT = httpx.Timeout(2.0)
_DISCOGRAPHY_COUNT_BUDGET_SECONDS = 4.0


@dataclass
class DeezerTrack:
    deezer_id: str
    title: str
    artist: str | None = None
    album: str | None = None
    bpm: float | None = None
    gain: float | None = None
    preview_url: str | None = None
    explicit: bool = False
    rank: int | None = None
    duration_sec: int | None = None


class DeezerClient:
    name = "deezer"

    def __init__(self, base_url: str = "https://api.deezer.com") -> None:
        self._base_url = base_url.rstrip("/")
        self._cache = TTLCache()

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base_url, timeout=_HTTP_TIMEOUT)

    def _count_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base_url, timeout=_DISCOGRAPHY_COUNT_HTTP_TIMEOUT)

    async def health(self) -> CapabilityState:
        return CapabilityState(available=True)

    async def search_artists(self, query: str) -> list[ArtistHit]:
        cache_key = f"artist-search:{query}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return list(cast(list[ArtistHit], cached))
        async with self._client() as client:
            resp = await request_with_retry(
                client, "GET", "/search/artist", params={"q": query, "limit": 10}
            )
            resp.raise_for_status()
        hits = [
            _parse_artist(item) for item in resp.json().get("data", []) if isinstance(item, dict)
        ]
        self._cache.set(cache_key, hits, 15 * 60)
        return hits

    async def get_artist(self, id: str) -> ArtistDetail:
        cache_key = f"artist:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cast(ArtistDetail, cached)
        async with self._client() as client:
            resp = await request_with_retry(client, "GET", f"/artist/{id}")
            resp.raise_for_status()
        detail = _parse_artist_detail(resp.json())
        self._cache.set(cache_key, detail, 24 * 60 * 60)
        return detail

    async def get_discography(self, id: str) -> list[AlbumHit]:
        cache_key = f"discography:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return list(cast(list[AlbumHit], cached))
        async with self._client() as client:
            resp = await request_with_retry(
                client, "GET", f"/artist/{id}/albums", params={"limit": 100}
            )
            resp.raise_for_status()
        album_rows = [item for item in resp.json().get("data", []) if isinstance(item, dict)]
        await _backfill_discography_track_counts(self, album_rows)
        albums = [_parse_album_hit(item, artist_id=id) for item in album_rows]
        albums.sort(key=lambda a: (a.year or "0000", a.title), reverse=True)
        self._cache.set(cache_key, albums, 24 * 60 * 60)
        return albums

    async def _get_album_track_count(self, http: httpx.AsyncClient, album_id: str) -> int | None:
        # Count enrichment is optional page metadata. Do not amplify 429s or
        # hold rendering through the general three-attempt retry policy.
        resp = await http.get(f"/album/{album_id}/tracks", params={"limit": 1})
        resp.raise_for_status()
        payload = resp.json()
        return _to_int(payload.get("total")) if isinstance(payload, dict) else None

    async def get_album(self, id: str) -> AlbumDetail:
        cache_key = f"album:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cast(AlbumDetail, cached)
        async with self._client() as client:
            resp = await request_with_retry(client, "GET", f"/album/{id}")
            resp.raise_for_status()
        data = resp.json()
        hit = _parse_album_hit(data, artist_id=None)
        tracks_obj = data.get("tracks", {})
        tracks_raw = tracks_obj.get("data", []) if isinstance(tracks_obj, dict) else []
        tracks = [_parse_album_track(item) for item in tracks_raw if isinstance(item, dict)]
        values = hit.__dict__.copy()
        values["track_count"] = len(tracks) or hit.track_count
        detail = AlbumDetail(**values, tracks=tracks)
        self._cache.set(cache_key, detail, 24 * 60 * 60)
        return detail

    @retry(
        retry=retry_if_exception_type(httpx.TransportError),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(3),
    )
    async def search_track(self, title: str, artist: str | None = None) -> list[DeezerTrack]:
        q = title
        if artist:
            q = f'track:"{title}" artist:"{artist}"'
        async with self._client() as client:
            resp = await request_with_retry(client, "GET", "/search", params={"q": q, "limit": 10})
            resp.raise_for_status()
        return [_parse_track(item) for item in resp.json().get("data", [])]

    @retry(
        retry=retry_if_exception_type(httpx.TransportError),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(3),
    )
    async def get_track(self, deezer_id: str) -> DeezerTrack | None:
        async with self._client() as client:
            resp = await request_with_retry(client, "GET", f"/track/{deezer_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return _parse_track(resp.json())


async def _backfill_discography_track_counts(
    client: DeezerClient, album_rows: list[dict[str, object]]
) -> None:
    """Fill counts omitted by Deezer's artist-albums endpoint with bounded I/O."""
    semaphore = asyncio.Semaphore(8)

    async with client._count_client() as http:

        async def fill(row: dict[str, object]) -> None:
            if _to_int(row.get("nb_tracks")) is not None:
                return
            album_id = str(row.get("id") or "")
            if not album_id:
                return
            try:
                async with semaphore:
                    count = await client._get_album_track_count(http, album_id)
            except httpx.HTTPError:
                logger.warning("Could not load Deezer track count for album %s", album_id)
                return
            if count is not None:
                row["nb_tracks"] = count

        try:
            async with asyncio.timeout(_DISCOGRAPHY_COUNT_BUDGET_SECONDS):
                await asyncio.gather(*(fill(row) for row in album_rows))
        except TimeoutError:
            logger.warning(
                "Deezer discography track-count backfill exceeded %.1f seconds",
                _DISCOGRAPHY_COUNT_BUDGET_SECONDS,
            )


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (ValueError, TypeError):
        return None


def _parse_track(data: dict[str, object]) -> DeezerTrack:
    track_id = str(data.get("id", ""))
    title = str(data.get("title") or data.get("title_short") or "")
    artist = None
    album = None

    artist_data = data.get("artist")
    if isinstance(artist_data, dict):
        artist = str(artist_data.get("name") or "") or None

    album_data = data.get("album")
    if isinstance(album_data, dict):
        album = str(album_data.get("title") or "") or None

    bpm = data.get("bpm")
    gain = data.get("gain")
    preview = data.get("preview")
    duration = data.get("duration")
    explicit_flag = data.get("explicit_lyrics")
    rank = data.get("rank")

    return DeezerTrack(
        deezer_id=track_id,
        title=title,
        artist=artist,
        album=album,
        bpm=_to_float(bpm),
        gain=_to_float(gain),
        preview_url=str(preview) if preview else None,
        explicit=bool(explicit_flag),
        rank=_to_int(rank),
        duration_sec=_to_int(duration),
    )


def _year(value: object) -> str | None:
    text = str(value or "")
    return text[:4] if len(text) >= 4 and text[:4].isdigit() else None


def _parse_artist(data: dict[str, object]) -> ArtistHit:
    did = str(data.get("id") or "")
    return ArtistHit(
        provider="deezer",
        provider_id=did,
        deezer_id=did or None,
        name=str(data.get("name") or ""),
        artwork_url=str(
            data.get("picture_xl")
            or data.get("picture_big")
            or data.get("picture_medium")
            or data.get("picture")
            or ""
        )
        or None,
    )


def _parse_artist_detail(data: dict[str, object]) -> ArtistDetail:
    hit = _parse_artist(data)
    return ArtistDetail(**hit.__dict__)


def _parse_album_hit(data: dict[str, object], artist_id: str | None) -> AlbumHit:
    did = str(data.get("id") or "")
    artist_name = None
    artist_obj = data.get("artist")
    if isinstance(artist_obj, dict):
        artist_name = str(artist_obj.get("name") or "") or None
        artist_id = artist_id or str(artist_obj.get("id") or "") or None
    raw_type = str(data.get("record_type") or "") or None
    release_kind = {
        "album": "album",
        "single": "single",
        "ep": "ep",
        "compile": "compilation",
        "compilation": "compilation",
    }.get((raw_type or "").casefold(), "unknown")
    return AlbumHit(
        provider="deezer",
        provider_id=did,
        deezer_id=did or None,
        title=str(data.get("title") or ""),
        artist_name=artist_name,
        artist_provider_id=artist_id,
        year=_year(data.get("release_date")),
        release_type=raw_type,
        release_kind=release_kind,
        release_type_raw=raw_type,
        artwork_url=str(
            data.get("cover_xl")
            or data.get("cover_big")
            or data.get("cover_medium")
            or data.get("cover")
            or ""
        )
        or None,
        track_count=_to_int(data.get("nb_tracks")),
    )


def _parse_album_track(data: dict[str, object]) -> AlbumTrack:
    tid = str(data.get("id") or "") or None
    return AlbumTrack(
        position=_to_int(data.get("track_position") or data.get("position")) or 1,
        disc=_to_int(data.get("disk_number")) or 1,
        title=str(data.get("title") or data.get("title_short") or ""),
        duration_sec=_to_int(data.get("duration")),
        provider_track_id=tid,
    )
