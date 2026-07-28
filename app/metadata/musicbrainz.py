from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import cast

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from app.metadata.base import AlbumDetail, AlbumHit, AlbumTrack, ArtistDetail, ArtistHit, TTLCache
from app.sources.base import CapabilityState

logger = logging.getLogger(__name__)

_LUCENE_SPECIALS = (
    "\\",
    "+",
    "-",
    "&&",
    "||",
    "!",
    "(",
    ")",
    "{",
    "}",
    "[",
    "]",
    "^",
    '"',
    "~",
    "*",
    "?",
    ":",
    "/",
)


def escape_lucene(value: str) -> str:
    escaped = value
    escaped = escaped.replace("\\", "\\\\")
    for token in _LUCENE_SPECIALS[1:]:
        escaped = escaped.replace(token, "\\" + token)
    return escaped


_TRAILING_QUALIFIER_RE = re.compile(r"\s*(?:\((?P<paren>[^)]*)\)|\[(?P<bracket>[^]]*)\])\s*$")
_EQUIVALENT_EDITION_RE = re.compile(
    r"(?:"
    r"bonus(?:\s+track)?\s+version"
    r"|bonus\s+tracks?"
    r"|deluxe(?:\s+(?:edition|version))?"
    r"|expanded(?:\s+(?:edition|version))?"
    r"|(?:\d+(?:st|nd|rd|th)\s+)?anniversary(?:\s+edition)?"
    r"|special\s+edition"
    r"|(?:\d{4}\s+)?remaster(?:ed)?(?:\s+(?:edition|version))?"
    r")",
    re.IGNORECASE,
)


def _album_title_candidates(album: str | None) -> list[str | None]:
    if not album:
        return [None]
    candidates: list[str | None] = [album]
    suffix = _TRAILING_QUALIFIER_RE.search(album)
    if suffix is None:
        return candidates
    qualifier = suffix.group("paren") or suffix.group("bracket") or ""
    if _EQUIVALENT_EDITION_RE.fullmatch(qualifier.strip()) is None:
        return candidates
    base_title = album[: suffix.start()].strip()
    if base_title:
        candidates.append(base_title)
    return candidates


_MB_BASE = "https://musicbrainz.org/ws/2"
_RATE_SEMAPHORE = asyncio.Semaphore(1)
_RATE_DELAY = 1.0
_LAST_REQUEST_AT = 0.0


@dataclass
class TrackMeta:
    mbid: str
    title: str
    artist: str | None = None
    album: str | None = None
    album_artist: str | None = None
    year: str | None = None
    disc: int | None = None
    disc_total: int | None = None
    track_no: int | None = None
    duration_ms: int | None = None


def _is_503(exc: BaseException) -> bool:
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 503


class MusicBrainzClient:
    name = "musicbrainz"

    def __init__(self, user_agent: str) -> None:
        self._user_agent = user_agent
        self._cache = TTLCache()

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=_MB_BASE,
            headers={"User-Agent": self._user_agent, "Accept": "application/json"},
            timeout=httpx.Timeout(15.0),
        )

    async def _get(self, path: str, params: dict[str, str]) -> httpx.Response:
        global _LAST_REQUEST_AT
        async with _RATE_SEMAPHORE:
            now = asyncio.get_running_loop().time()
            delay = max(0.0, _RATE_DELAY - (now - _LAST_REQUEST_AT))
            if delay:
                await asyncio.sleep(delay)
            async with self._client() as client:
                resp = await client.get(path, params=params)
            _LAST_REQUEST_AT = asyncio.get_running_loop().time()
        if resp.status_code == 503:
            resp.raise_for_status()
        return resp

    @retry(
        retry=retry_if_exception(_is_503),
        wait=wait_exponential(multiplier=2, min=2, max=8),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    async def _get_with_retry(self, path: str, params: dict[str, str]) -> httpx.Response:
        return await self._get(path, params)

    async def health(self) -> CapabilityState:
        return CapabilityState(
            available=bool(self._user_agent),
            reason=None if self._user_agent else "MusicBrainz contact is required",
        )

    async def search_artists(self, query: str) -> list[ArtistHit]:
        cache_key = f"artist-search:{query}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return list(cast(list[ArtistHit], cached))
        lucene = f'artist:"{escape_lucene(query)}"'
        resp = await self._get_with_retry(
            "/artist", {"query": lucene, "limit": "10", "fmt": "json"}
        )
        resp.raise_for_status()
        hits = [
            _parse_artist_hit(item)
            for item in resp.json().get("artists", [])
            if isinstance(item, dict)
        ]
        self._cache.set(cache_key, hits, 15 * 60)
        return hits

    async def get_artist(self, id: str) -> ArtistDetail:
        cache_key = f"artist:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cast(ArtistDetail, cached)
        resp = await self._get_with_retry(f"/artist/{id}", {"fmt": "json"})
        resp.raise_for_status()
        detail = _parse_artist_detail(resp.json())
        self._cache.set(cache_key, detail, 24 * 60 * 60)
        return detail

    async def get_discography(self, id: str) -> list[AlbumHit]:
        cache_key = f"discography:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return list(cast(list[AlbumHit], cached))
        resp = await self._get_with_retry(
            "/release-group",
            {"artist": id, "type": "album|ep|single", "limit": "100", "fmt": "json"},
        )
        resp.raise_for_status()
        allowed = {"album", "ep", "single"}
        seen: set[str] = set()
        albums: list[AlbumHit] = []
        for item in resp.json().get("release-groups", []):
            if not isinstance(item, dict):
                continue
            primary = str(item.get("primary-type") or "").casefold()
            if primary not in allowed:
                continue
            mbid = str(item.get("id") or "")
            if not mbid or mbid in seen:
                continue
            seen.add(mbid)
            albums.append(_parse_release_group_hit(item, artist_id=id))
        albums.sort(key=lambda a: (a.year or "0000", a.title), reverse=True)
        self._cache.set(cache_key, albums, 24 * 60 * 60)
        return albums

    async def get_album(self, id: str) -> AlbumDetail:
        cache_key = f"album:{id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cast(AlbumDetail, cached)
        rg_resp = await self._get_with_retry(
            f"/release-group/{id}", {"inc": "releases", "fmt": "json"}
        )
        rg_resp.raise_for_status()
        rg = rg_resp.json()
        releases = rg.get("releases", [])
        release_id = None
        if isinstance(releases, list) and releases:
            first = releases[0]
            if isinstance(first, dict):
                release_id = str(first.get("id") or "") or None
        tracks: list[AlbumTrack] = []
        if release_id:
            rel_resp = await self._get_with_retry(
                f"/release/{release_id}", {"inc": "recordings", "fmt": "json"}
            )
            if rel_resp.status_code != 404:
                rel_resp.raise_for_status()
                tracks = _parse_release_tracks(rel_resp.json())
        artwork_url = await self._artwork_url(id)
        hit = _parse_release_group_hit(rg, artist_id=None)
        values = hit.__dict__.copy()
        values["artwork_url"] = artwork_url
        values["track_count"] = len(tracks) or hit.track_count
        detail = AlbumDetail(**values, tracks=tracks)
        self._cache.set(cache_key, detail, 24 * 60 * 60)
        return detail

    async def _artwork_url(self, release_group_mbid: str) -> str | None:
        url = f"https://coverartarchive.org/release-group/{release_group_mbid}/front-250"
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(10.0), follow_redirects=False
            ) as client:
                resp = await client.head(url)
        except httpx.HTTPError:
            return None
        if resp.status_code == 200:
            return url
        return None

    async def lookup_recording(self, mbid: str) -> TrackMeta | None:
        resp = await self._get_with_retry(
            f"/recording/{mbid}",
            {"inc": "artists releases artist-credits", "fmt": "json"},
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return _parse_recording(resp.json())

    async def search_recording(
        self, title: str, artist: str | None = None, album: str | None = None
    ) -> list[TrackMeta]:
        for album_candidate in _album_title_candidates(album):
            parts = [f'recording:"{escape_lucene(title)}"']
            if artist:
                parts.append(f'artist:"{escape_lucene(artist)}"')
            if album_candidate:
                parts.append(f'release:"{escape_lucene(album_candidate)}"')
            lucene = " AND ".join(parts)

            resp = await self._get_with_retry(
                "/recording",
                {"query": lucene, "limit": "10", "fmt": "json"},
            )
            resp.raise_for_status()
            recordings = resp.json().get("recordings", [])
            results = [
                result
                for result in (_parse_recording(recording) for recording in recordings)
                if result is not None
            ]
            if results:
                return results
        return []


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (ValueError, TypeError):
        return None


def _parse_recording(data: dict[str, object]) -> TrackMeta | None:
    mbid = data.get("id")
    if not mbid:
        return None

    title: str = str(data.get("title") or "")
    artist: str | None = None
    album_artist: str | None = None
    album: str | None = None
    year: str | None = None
    disc: int | None = None
    disc_total: int | None = None
    track_no: int | None = None
    duration_ms: int | None = None

    credits = data.get("artist-credit", [])
    if credits and isinstance(credits, list):
        artist_parts = []
        for credit in credits:
            if isinstance(credit, dict):
                a = credit.get("artist", {})
                if isinstance(a, dict):
                    name = a.get("name")
                    if name:
                        artist_parts.append(str(name))
                joinphrase = credit.get("joinphrase", "")
                if joinphrase:
                    artist_parts.append(str(joinphrase))
        artist = "".join(artist_parts).strip() or None

    releases = data.get("releases", [])
    if releases and isinstance(releases, list):
        rel = releases[0]
        if isinstance(rel, dict):
            album = str(rel.get("title") or "") or None
            date = str(rel.get("date") or "")
            year = date[:4] if date else None

            media_list = rel.get("media", [])
            if media_list and isinstance(media_list, list):
                media = media_list[0]
                if isinstance(media, dict):
                    disc = media.get("position")
                    disc_total = rel.get("media-count")
                    track_list = media.get("track", [])
                    if track_list and isinstance(track_list, list):
                        track_no = track_list[0].get("number")
                        try:
                            track_no = _to_int(track_no)
                        except (ValueError, TypeError):
                            track_no = None

    raw_duration = data.get("length")
    if raw_duration is not None:
        try:
            duration_ms = _to_int(raw_duration)
        except (ValueError, TypeError):
            duration_ms = None

    album_artist = artist

    return TrackMeta(
        mbid=str(mbid),
        title=title,
        artist=artist,
        album_artist=album_artist,
        album=album,
        year=year,
        disc=_to_int(disc),
        disc_total=_to_int(disc_total),
        track_no=track_no,
        duration_ms=duration_ms,
    )


def _year_from_date(value: object) -> str | None:
    text = str(value or "")
    return text[:4] if len(text) >= 4 and text[:4].isdigit() else None


def _parse_artist_hit(data: dict[str, object]) -> ArtistHit:
    mbid = str(data.get("id") or "")
    return ArtistHit(
        provider="musicbrainz",
        provider_id=mbid,
        mbid=mbid or None,
        name=str(data.get("name") or ""),
        disambiguation=str(data.get("disambiguation") or "") or None,
        sort_name=str(data.get("sort-name") or "") or None,
    )


def _parse_artist_detail(data: dict[str, object]) -> ArtistDetail:
    hit = _parse_artist_hit(data)
    return ArtistDetail(
        **hit.__dict__,
        country=str(data.get("country") or "") or None,
        type=str(data.get("type") or "") or None,
    )


def _parse_release_group_hit(data: dict[str, object], artist_id: str | None) -> AlbumHit:
    mbid = str(data.get("id") or "")
    primary = str(data.get("primary-type") or "")
    secondary_raw = data.get("secondary-types") or []
    secondary = [str(value) for value in secondary_raw] if isinstance(secondary_raw, list) else []
    if any(value.casefold() == "compilation" for value in secondary):
        release_kind = "compilation"
    else:
        release_kind = {"album": "album", "single": "single", "ep": "ep"}.get(
            primary.casefold(), "unknown"
        )
    raw_type = " + ".join([primary, *secondary]) or None
    return AlbumHit(
        provider="musicbrainz",
        provider_id=mbid,
        title=str(data.get("title") or ""),
        artist_provider_id=artist_id,
        year=_year_from_date(data.get("first-release-date")),
        release_type=raw_type,
        release_kind=release_kind,
        release_type_raw=raw_type,
        mbid=mbid or None,
        artwork_url=f"https://coverartarchive.org/release-group/{mbid}/front-250"
        if mbid
        else None,
        track_count=_to_int(data.get("count")),
    )


def _parse_release_tracks(data: dict[str, object]) -> list[AlbumTrack]:
    tracks: list[AlbumTrack] = []
    media_list = data.get("media", [])
    if not isinstance(media_list, list):
        return tracks
    for media in media_list:
        if not isinstance(media, dict):
            continue
        disc = _to_int(media.get("position")) or 1
        raw_tracks = media.get("tracks", []) or media.get("track", [])
        if not isinstance(raw_tracks, list):
            continue
        for raw in raw_tracks:
            if not isinstance(raw, dict):
                continue
            rec = raw.get("recording")
            recording_mbid = str(rec.get("id") or "") if isinstance(rec, dict) else None
            duration_ms = _to_int(raw.get("length"))
            tracks.append(
                AlbumTrack(
                    position=_to_int(raw.get("position") or raw.get("number")) or len(tracks) + 1,
                    disc=disc,
                    title=str(
                        raw.get("title")
                        or (rec.get("title") if isinstance(rec, dict) else "")
                        or ""
                    ),
                    duration_sec=duration_ms // 1000 if duration_ms is not None else None,
                    recording_mbid=recording_mbid or None,
                )
            )
    return tracks
