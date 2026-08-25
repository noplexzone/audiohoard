from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace
from typing import cast
from urllib.parse import urlsplit

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.http import request_with_retry
from app.metadata.base import (
    AlbumDetail,
    AlbumHit,
    AlbumTrack,
    ArtistDetail,
    ArtistHit,
    DiscoveryGenre,
    DiscoveryRelease,
    TTLCache,
)
from app.metadata.content_rating import deezer_content_rating
from app.sources.base import CapabilityState

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(10.0)
_ARTIST_EVIDENCE_HTTP_TIMEOUT = httpx.Timeout(2.0)
_ARTIST_EVIDENCE_BUDGET_SECONDS = 3.0
_DEEZER_PLACEHOLDER_IMAGE_HASH = "d41d8cd98f00b204e9800998ecf8427e"
_GENRE_RADIO_LIMIT = 5
_GENRE_TRACK_LIMIT = 100


@dataclass
class DeezerTrack:
    deezer_id: str
    title: str
    artist: str | None = None
    album: str | None = None
    album_id: str | None = None
    content_rating: str = "unknown"
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

    def _evidence_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base_url, timeout=_ARTIST_EVIDENCE_HTTP_TIMEOUT)

    async def health(self) -> CapabilityState:
        return CapabilityState(available=True)

    async def discovery_feed(
        self, feed: str, *, page: int = 1, limit: int = 12, genre_id: str | None = None
    ) -> list[ArtistHit | DiscoveryGenre | DiscoveryRelease]:
        """Return a bounded provider-neutral Deezer discovery feed."""
        limit = max(1, min(limit, 25))
        index = (max(1, min(page, 20)) - 1) * limit
        local_start = 0
        if feed == "popular":
            path = "/chart/0/artists"
        elif feed == "genres":
            path, local_start = "/genre", index
        elif feed == "genre":
            normalized_genre_id = _positive_scalar_id(genre_id)
            if normalized_genre_id is None:
                raise ValueError("Invalid Deezer genre")
            return cast(
                list[ArtistHit | DiscoveryGenre | DiscoveryRelease],
                (await self.genre_artist_candidates(normalized_genre_id))[index : index + limit],
            )
        elif feed == "new":
            path = "/editorial/0/releases"
        elif feed == "trending":
            path = "/chart/0/albums"
        else:
            raise ValueError("Unknown discovery feed")
        params = {} if feed == "genres" else {"limit": limit, "index": index}
        async with asyncio.timeout(12), self._client() as client:
            response = await request_with_retry(client, "GET", path, params=params)
            response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and "error" in payload:
            raise ValueError("Deezer returned an error envelope")
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            raise ValueError("Deezer returned an invalid discovery feed")
        valid = [row for row in rows[local_start : local_start + limit] if isinstance(row, dict)]
        if feed == "popular":
            return [
                artist
                for row in valid
                if (artist := _parse_artist(row)).provider_id and artist.name
            ]
        if feed == "genres":
            return [
                genre for row in valid if (genre := _parse_genre(row)).provider_id and genre.name
            ]
        return [
            release
            for row in valid
            if (release := _parse_discovery_release(row)).provider_id
            and release.title
            and release.artist_provider_id
        ]

    async def genre_artist_candidates(self, genre_id: str) -> list[ArtistHit]:
        """Return the complete bounded, ordered candidate pool for an exact genre."""
        normalized_genre_id = _positive_scalar_id(genre_id)
        if normalized_genre_id is None:
            raise ValueError("Invalid Deezer genre")
        async with asyncio.timeout(12), self._client() as client:
            genre_response = await request_with_retry(
                client, "GET", f"/genre/{normalized_genre_id}"
            )
            genre_response.raise_for_status()
            genre = genre_response.json()
            if (
                not isinstance(genre, dict)
                or "error" in genre
                or _positive_scalar_id(genre.get("id")) != normalized_genre_id
                or not isinstance(genre.get("name"), str)
                or not genre["name"].strip()
            ):
                raise ValueError("Deezer returned an invalid exact genre")

            radios_response = await request_with_retry(
                client,
                "GET",
                f"/genre/{normalized_genre_id}/radios",
                params={"limit": _GENRE_RADIO_LIMIT},
            )
            radios_response.raise_for_status()
            radio_rows = _genre_collection(radios_response.json(), "genre radios")
            radio_ids: list[str] = []
            for row in radio_rows[:_GENRE_RADIO_LIMIT]:
                radio_id = _positive_scalar_id(row.get("id"))
                if radio_id is None:
                    raise ValueError("Deezer returned malformed genre radios")
                radio_ids.append(radio_id)

            async def get_radio_tracks(radio_id: str) -> list[dict[str, object]]:
                response = await request_with_retry(
                    client,
                    "GET",
                    f"/radio/{radio_id}/tracks",
                    params={"limit": _GENRE_TRACK_LIMIT},
                )
                response.raise_for_status()
                return _genre_collection(response.json(), "genre radio tracks")

            track_groups = await asyncio.gather(
                *(get_radio_tracks(radio_id) for radio_id in radio_ids)
            )

        artists: list[ArtistHit] = []
        seen_artist_ids: set[str] = set()
        for tracks in track_groups:
            for track in tracks:
                artist_row = track.get("artist")
                if not isinstance(artist_row, dict):
                    raise ValueError("Deezer returned malformed genre radio tracks")
                artist = _parse_genre_radio_artist(artist_row)
                if artist.provider_id in seen_artist_ids:
                    continue
                seen_artist_ids.add(artist.provider_id)
                artists.append(artist)

        return artists

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
        await _backfill_artist_search_evidence(self, hits)
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
        payload = resp.json()
        if not isinstance(payload, dict) or payload.get("error") is not None:
            raise ValueError(f"Deezer artist {id} did not return a valid matching artist identity")
        detail = _parse_artist_detail(payload)
        if detail.provider_id != id or detail.deezer_id != id or not detail.name.strip():
            raise ValueError(f"Deezer artist {id} did not return a valid matching artist identity")
        self._cache.set(cache_key, detail, 24 * 60 * 60)
        return detail

    async def get_discography(self, id: str) -> list[AlbumHit]:
        cache_key = f"discography:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return list(cast(list[AlbumHit], cached))
        next_url = f"/artist/{id}/albums"
        params: dict[str, int] | None = {"limit": 100}
        album_rows: list[dict[str, object]] = []
        visited: set[str] = set()
        async with asyncio.timeout(30), self._client() as client:
            for _ in range(20):
                resp = await request_with_retry(client, "GET", next_url, params=params)
                resp.raise_for_status()
                payload = resp.json()
                if not isinstance(payload, dict) or not isinstance(payload.get("data", []), list):
                    raise ValueError(f"Deezer artist {id} returned an invalid album page")
                album_rows.extend(item for item in payload["data"] if isinstance(item, dict))
                raw_next = payload.get("next")
                if not raw_next:
                    break
                next_url = str(raw_next)
                if (
                    not _is_deezer_artist_albums_page(self._base_url, next_url, id)
                    or next_url in visited
                ):
                    raise ValueError(f"Deezer artist {id} returned an unsafe album page")
                visited.add(next_url)
                params = None
            else:
                raise ValueError(f"Deezer artist {id} exceeded the album page limit")
        albums = [_parse_album_hit(item, artist_id=id) for item in album_rows]
        albums.sort(key=lambda a: (a.year or "0000", a.title), reverse=True)
        self._cache.set(cache_key, albums, 24 * 60 * 60)
        return albums

    async def get_album(self, id: str) -> AlbumDetail:
        cache_key = f"album:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cast(AlbumDetail, cached)
        async with self._client() as client:
            album_resp = await request_with_retry(client, "GET", f"/album/{id}")
            album_resp.raise_for_status()
            data = album_resp.json()
            embedded_tracks = _embedded_album_tracks(data)
            try:
                tracks_raw = await self._get_album_tracks(client, id)
            except httpx.HTTPError:
                if not _tracks_have_authoritative_positions(embedded_tracks):
                    raise
                logger.warning(
                    "Deezer album tracklist lookup failed for %s; "
                    "using positioned embedded tracks",
                    id,
                )
                tracks_raw = embedded_tracks

        if not _tracks_have_authoritative_positions(tracks_raw):
            raise ValueError(f"Deezer album {id} returned tracks without authoritative positions")
        hit = _parse_album_hit(data, artist_id=None)
        tracks = [_parse_album_track(item) for item in tracks_raw if isinstance(item, dict)]
        values = hit.__dict__.copy()
        values["track_count"] = max(hit.track_count or 0, len(tracks)) or None
        detail = AlbumDetail(**values, tracks=tracks)
        self._cache.set(cache_key, detail, 24 * 60 * 60)
        return detail

    async def _get_album_tracks(self, client: httpx.AsyncClient, album_id: str) -> list[object]:
        next_url = f"/album/{album_id}/tracks"
        params: dict[str, int] | None = {"limit": 100}
        tracks: list[object] = []
        visited: set[str] = set()
        for _ in range(100):
            response = await request_with_retry(client, "GET", next_url, params=params)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Deezer album {album_id} returned an invalid tracklist")
            page = payload.get("data", [])
            if not isinstance(page, list):
                raise ValueError(f"Deezer album {album_id} returned an invalid track page")
            tracks.extend(page)
            raw_next = payload.get("next")
            if not raw_next:
                return tracks
            next_url = str(raw_next)
            if not _same_deezer_origin(self._base_url, next_url) or next_url in visited:
                raise ValueError(f"Deezer album {album_id} returned an unsafe track page")
            visited.add(next_url)
            params = None
        raise ValueError(f"Deezer album {album_id} exceeded the track page limit")

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


def _embedded_album_tracks(data: dict[str, object]) -> list[object]:
    tracks = data.get("tracks", {})
    if not isinstance(tracks, dict):
        return []
    rows = tracks.get("data", [])
    return rows if isinstance(rows, list) else []


def _tracks_have_authoritative_positions(rows: list[object]) -> bool:
    if not rows:
        return False
    positions: set[tuple[int, int]] = set()
    for row in rows:
        if not isinstance(row, dict):
            return False
        position = _to_int(row.get("track_position"))
        disc = _to_int(row.get("disk_number")) or 1
        key = (disc, position or 0)
        if position is None or position < 1 or key in positions:
            return False
        positions.add(key)
    return True


def _same_deezer_origin(base_url: str, candidate: str) -> bool:
    base = urlsplit(base_url)
    next_page = urlsplit(candidate)
    return (
        next_page.scheme in {"http", "https"}
        and next_page.scheme == base.scheme
        and next_page.netloc == base.netloc
    )


def _is_deezer_artist_albums_page(base_url: str, candidate: str, artist_id: str) -> bool:
    next_page = urlsplit(candidate)
    return (
        _same_deezer_origin(base_url, candidate)
        and next_page.path.rstrip("/") == f"/artist/{artist_id}/albums"
        and not next_page.fragment
    )


async def _backfill_artist_search_evidence(client: DeezerClient, hits: list[ArtistHit]) -> None:
    if not hits:
        return
    sem = asyncio.Semaphore(5)
    async with client._evidence_client() as http:

        async def fill(idx: int, hit: ArtistHit) -> None:
            if not hit.provider_id or (hit.fan_count is None and hit.album_count is None):
                return
            try:
                async with sem:
                    r = await http.get(f"/artist/{hit.provider_id}/top", params={"limit": 5})
                    r.raise_for_status()
            except httpx.HTTPError:
                logger.warning("Could not load Deezer artist evidence for %s", hit.provider_id)
                return
            payload = r.json()
            rows = payload.get("data", []) if isinstance(payload, dict) else []
            if not isinstance(rows, list):
                return
            titles: list[str] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title_short") or row.get("title") or "").strip()
                if title and title not in titles:
                    titles.append(title)
                if len(titles) >= 3:
                    break
            if titles:
                hits[idx] = replace(hit, top_tracks=tuple(titles))

        try:
            async with asyncio.timeout(_ARTIST_EVIDENCE_BUDGET_SECONDS):
                await asyncio.gather(*(fill(idx, hit) for idx, hit in enumerate(hits)))
        except TimeoutError:
            logger.warning(
                "Deezer artist search evidence backfill exceeded %.1f seconds",
                _ARTIST_EVIDENCE_BUDGET_SECONDS,
            )


def _genre_collection(payload: object, context: str) -> list[dict[str, object]]:
    if not isinstance(payload, dict) or "error" in payload:
        raise ValueError(f"Deezer returned an invalid {context} envelope")
    rows = payload.get("data")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"Deezer returned malformed {context}")
    return cast(list[dict[str, object]], rows)


def _positive_scalar_id(value: object) -> str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value) if value > 0 else None
    if isinstance(value, str) and value.isdigit():
        normalized = value.lstrip("0")
        return normalized or None
    return None


def _parse_genre_radio_artist(data: dict[str, object]) -> ArtistHit:
    artist_id = _positive_scalar_id(data.get("id"))
    name = data.get("name")
    if artist_id is None or not isinstance(name, str) or not name.strip():
        raise ValueError("Deezer returned malformed genre radio artists")
    return replace(_parse_artist(data), provider_id=artist_id, deezer_id=artist_id, name=name)


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
    album_id = None

    artist_data = data.get("artist")
    if isinstance(artist_data, dict):
        artist = str(artist_data.get("name") or "") or None

    album_data = data.get("album")
    if isinstance(album_data, dict):
        album = str(album_data.get("title") or "") or None
        album_id = str(album_data.get("id") or "") or None

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
        album_id=album_id,
        content_rating=deezer_content_rating(data),
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
    art = (
        str(
            data.get("picture_big")
            or data.get("picture_xl")
            or data.get("picture_medium")
            or data.get("picture")
            or ""
        )
        or None
    )
    if art and _DEEZER_PLACEHOLDER_IMAGE_HASH in art:
        art = None
    return ArtistHit(
        provider="deezer",
        provider_id=did,
        deezer_id=did or None,
        name=str(data.get("name") or ""),
        artwork_url=art,
        external_url=str(data.get("link") or "") or None,
        album_count=_to_int(data.get("nb_album")),
        fan_count=_to_int(data.get("nb_fan")),
        rank=_to_int(data.get("position") or data.get("rank")),
    )


def _parse_artist_detail(data: dict[str, object]) -> ArtistDetail:
    hit = _parse_artist(data)
    return ArtistDetail(**hit.__dict__)


def _parse_genre(data: dict[str, object]) -> DiscoveryGenre:
    genre_id = _positive_scalar_id(data.get("id"))
    if genre_id is None:
        raise ValueError("Deezer returned an invalid genre ID")
    return DiscoveryGenre(
        provider="deezer",
        provider_id=genre_id,
        name=str(data.get("name") or ""),
        artwork_url=str(data.get("picture_big") or data.get("picture_medium") or "") or None,
    )


def _parse_discovery_release(data: dict[str, object]) -> DiscoveryRelease:
    artist = data.get("artist")
    artist_name = str(artist.get("name") or "") if isinstance(artist, dict) else ""
    artist_id = str(artist.get("id") or "") if isinstance(artist, dict) else ""
    return DiscoveryRelease(
        provider="deezer",
        provider_id=str(data.get("id") or ""),
        title=str(data.get("title") or ""),
        artist_name=artist_name,
        artist_provider_id=artist_id,
        artwork_url=str(
            data.get("cover_big") or data.get("cover_medium") or data.get("cover") or ""
        )
        or None,
        release_date=str(data.get("release_date") or "") or None,
        rank=_to_int(data.get("rank")),
    )


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
        content_rating=deezer_content_rating(data),
        upc=str(data.get("upc") or "") or None,
    )


def _parse_album_track(data: dict[str, object]) -> AlbumTrack:
    tid = str(data.get("id") or "") or None
    artist = data.get("artist")
    artist_name = (
        str(artist.get("name") or "").strip() or None if isinstance(artist, dict) else None
    )
    artist_provider_id = (
        str(artist.get("id") or "").strip() or None if isinstance(artist, dict) else None
    )
    return AlbumTrack(
        position=_to_int(data.get("track_position") or data.get("position")) or 1,
        disc=_to_int(data.get("disk_number")) or 1,
        title=str(data.get("title") or data.get("title_short") or ""),
        duration_sec=_to_int(data.get("duration")),
        provider_track_id=tid,
        preview_url=str(data.get("preview") or "") or None,
        artist_name=artist_name,
        artist_provider_id=artist_provider_id,
        content_rating=deezer_content_rating(data),
    )
