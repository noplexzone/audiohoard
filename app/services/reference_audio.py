from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from urllib.parse import parse_qs, urlsplit

from app.metadata.deezer import DeezerClient

if TYPE_CHECKING:
    from app.config import Settings
    from app.models.catalog_entities import CatalogAlbumTrack
    from app.models.track import Track

ReferenceMatchMethod = Literal["exact_track_id", "exact_album_position"]


@dataclass(frozen=True)
class ReferenceAudio:
    """Import-review audio whose provenance proves an exact recording identity path."""

    url: str
    provider: Literal["deezer"]
    provider_track_id: str
    match_method: ReferenceMatchMethod
    cached: bool
    expires_at: int | None = None


@dataclass(frozen=True)
class ExactDeezerReference:
    provider_track_id: str
    title: str
    duration_sec: int | None
    preview_url: str
    album_title: str
    artist_name: str
    track_artist_name: str = ""
    track_content_rating: str = "unknown"
    album_content_rating: str = "unknown"


@dataclass(frozen=True)
class _CachedExactReference:
    url: str
    provider_track_id: str
    match_method: ReferenceMatchMethod
    provider_album_id: str | None
    disc: int | None
    position: int | None


_DEEZER_PREVIEW_MINIMUM_VALIDITY_SECONDS = 120


def _deezer_preview_expiry(url: str) -> int | None:
    try:
        signature = parse_qs(urlsplit(url).query).get("hdnea", [])
        return next(
            int(part.removeprefix("exp="))
            for part in signature[0].split("~")
            if part.startswith("exp=")
        )
    except (IndexError, StopIteration, TypeError, ValueError):
        return None


def _deezer_preview_is_usable(url: str, *, now: float | None = None) -> bool:
    """Reject signed Deezer previews that expire before review analysis can finish."""
    expiry = _deezer_preview_expiry(url)
    if expiry is None:
        return True
    current_time = time.time() if now is None else now
    return expiry > current_time + _DEEZER_PREVIEW_MINIMUM_VALIDITY_SECONDS


async def resolve_exact_deezer_position_reference(
    *,
    album_deezer_id: str | None,
    disc: int,
    position: int,
    settings: Settings,
    deezer_client: DeezerClient | None = None,
) -> ExactDeezerReference | None:
    """Resolve a Deezer preview by exact album identity, disc, and position only."""
    exact_album_id = str(album_deezer_id or "").strip()
    if not exact_album_id or disc < 1 or position < 1:
        return None
    client = deezer_client or DeezerClient(settings.deezer_api_url)
    async with asyncio.timeout(12):
        album = await client.get_album(exact_album_id)
    if str(album.deezer_id or album.provider_id or "").strip() != exact_album_id:
        return None
    matches = [
        track for track in album.tracks if track.disc == disc and track.position == position
    ]
    if len(matches) != 1:
        return None
    selected = matches[0]
    provider_track_id = str(selected.provider_track_id or "").strip()
    title = str(selected.title or "").strip()
    preview_url = str(selected.preview_url or "").strip()
    album_title = str(album.title or "").strip()
    artist_name = str(album.artist_name or "").strip()
    track_artist_name = str(selected.artist_name or "").strip()
    if (
        not provider_track_id
        or not title
        or not preview_url
        or not album_title
        or not artist_name
        or not track_artist_name
        or not _deezer_preview_is_usable(preview_url)
    ):
        return None
    return ExactDeezerReference(
        provider_track_id=provider_track_id,
        title=title,
        duration_sec=selected.duration_sec,
        preview_url=preview_url,
        album_title=album_title,
        artist_name=artist_name,
        track_artist_name=track_artist_name,
        track_content_rating=selected.content_rating,
        album_content_rating=album.content_rating,
    )


async def resolve_exact_deezer_catalog_reference(
    catalog_track: CatalogAlbumTrack | object,
    *,
    settings: Settings,
    deezer_client: DeezerClient | None = None,
) -> ExactDeezerReference | None:
    """Resolve exact preview evidence using only catalog album identity/position."""
    album = getattr(catalog_track, "album", None)
    return await resolve_exact_deezer_position_reference(
        album_deezer_id=getattr(album, "deezer_id", None),
        disc=int(getattr(catalog_track, "disc", 0) or 0),
        position=int(getattr(catalog_track, "position", 0) or 0),
        settings=settings,
        deezer_client=deezer_client,
    )


def _positive_int(value: object) -> int | None:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _stored_exact_deezer_reference(
    catalog_track: CatalogAlbumTrack | object | None,
) -> _CachedExactReference | None:
    """Read only cached previews carrying complete exact-match provenance."""
    if catalog_track is None:
        return None
    album = getattr(catalog_track, "album", None)
    try:
        provenance = json.loads(getattr(album, "provenance_json", None) or "{}")
    except (json.JSONDecodeError, TypeError):
        return None
    previews = provenance.get("track_previews") if isinstance(provenance, dict) else None
    deezer = previews.get("deezer") if isinstance(previews, dict) else None
    key = f"{getattr(catalog_track, 'disc', 1)}:{getattr(catalog_track, 'position', 0)}"
    raw = deezer.get(key) if isinstance(deezer, dict) else None
    if not isinstance(raw, dict):
        return None
    url = str(raw.get("url") or "").strip()
    provider_track_id = str(raw.get("provider_track_id") or "").strip()
    method = str(raw.get("match_method") or "")
    if (
        not url
        or not provider_track_id
        or method
        not in {
            "exact_track_id",
            "exact_album_position",
        }
    ):
        return None
    if method == "exact_track_id":
        return _CachedExactReference(
            url=url,
            provider_track_id=provider_track_id,
            match_method="exact_track_id",
            provider_album_id=None,
            disc=None,
            position=None,
        )
    provider_album_id = str(raw.get("provider_album_id") or "").strip()
    disc = _positive_int(raw.get("disc"))
    position = _positive_int(raw.get("position"))
    if (
        not provider_album_id
        or provider_album_id != str(getattr(album, "deezer_id", None) or "").strip()
        or disc != _positive_int(getattr(catalog_track, "disc", None))
        or position != _positive_int(getattr(catalog_track, "position", None))
    ):
        return None
    return _CachedExactReference(
        url=url,
        provider_track_id=provider_track_id,
        match_method="exact_album_position",
        provider_album_id=provider_album_id,
        disc=disc,
        position=position,
    )


def _reference(
    *, url: str, provider_track_id: str, match_method: ReferenceMatchMethod, cached: bool
) -> ReferenceAudio:
    return ReferenceAudio(
        url=url,
        provider="deezer",
        provider_track_id=provider_track_id,
        match_method=match_method,
        cached=cached,
        expires_at=_deezer_preview_expiry(url),
    )


async def resolve_exact_deezer_track_reference(
    track_id: str, *, settings: Settings, deezer_client: DeezerClient | None = None
) -> ReferenceAudio | None:
    """Resolve a track ID supplied by an already exact identity path."""
    client = deezer_client or DeezerClient(settings.deezer_api_url)
    async with asyncio.timeout(5):
        candidate = await client.get_track(track_id)
    if candidate is None or str(candidate.deezer_id or "").strip() != track_id:
        return None
    url = str(candidate.preview_url or "").strip()
    if not url or not _deezer_preview_is_usable(url):
        return None
    return _reference(
        url=url, provider_track_id=track_id, match_method="exact_track_id", cached=False
    )


async def _resolve_exact_album_position(
    *,
    album_id: str,
    disc: int,
    position: int,
    settings: Settings,
    deezer_client: DeezerClient | None,
) -> ReferenceAudio | None:
    exact = await resolve_exact_deezer_position_reference(
        album_deezer_id=album_id,
        disc=disc,
        position=position,
        settings=settings,
        deezer_client=deezer_client,
    )
    if exact is None:
        return None
    return _reference(
        url=exact.preview_url,
        provider_track_id=exact.provider_track_id,
        match_method="exact_album_position",
        cached=False,
    )


async def resolve_reference_audio(
    track: Track,
    catalog_track: CatalogAlbumTrack | object | None,
    *,
    artist_name: str | None,
    settings: Settings,
    deezer_client: DeezerClient | None = None,
) -> ReferenceAudio | None:
    """Resolve import-review evidence through exact Deezer identities only.

    ``artist_name`` remains in the interface for callers, but is intentionally not
    searched: title/artist results are catalog previews, not verification evidence.
    """
    del artist_name
    cached = _stored_exact_deezer_reference(catalog_track)
    try:
        if cached is not None:
            if _deezer_preview_is_usable(cached.url):
                return _reference(
                    url=cached.url,
                    provider_track_id=cached.provider_track_id,
                    match_method=cached.match_method,
                    cached=True,
                )
            if cached.match_method == "exact_track_id":
                return await resolve_exact_deezer_track_reference(
                    cached.provider_track_id,
                    settings=settings,
                    deezer_client=deezer_client,
                )
            if cached.provider_album_id and cached.disc and cached.position:
                return await _resolve_exact_album_position(
                    album_id=cached.provider_album_id,
                    disc=cached.disc,
                    position=cached.position,
                    settings=settings,
                    deezer_client=deezer_client,
                )
            return None

        # Track.deezer_id is populated by fuzzy enrichment in the acquisition
        # pipeline and therefore cannot authorize verification evidence.
        album = getattr(catalog_track, "album", None) if catalog_track is not None else None
        album_id = str(getattr(album, "deezer_id", None) or "").strip()
        disc = _positive_int(getattr(catalog_track, "disc", None))
        position = _positive_int(getattr(catalog_track, "position", None))
        if album_id and disc and position:
            return await _resolve_exact_album_position(
                album_id=album_id,
                disc=disc,
                position=position,
                settings=settings,
                deezer_client=deezer_client,
            )
    except Exception:
        return None
    return None
