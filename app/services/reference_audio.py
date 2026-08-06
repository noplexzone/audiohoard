from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict
from urllib.parse import parse_qs, urlsplit

from app.metadata.deezer import DeezerClient
from app.metadata.itunes import ITunesClient

if TYPE_CHECKING:
    from app.config import Settings
    from app.models.catalog_entities import CatalogAlbumTrack
    from app.models.track import Track


class ReferenceAudio(TypedDict):
    url: str
    source: Literal["deezer", "itunes"]


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


_DEEZER_PREVIEW_MINIMUM_VALIDITY_SECONDS = 120


async def resolve_exact_deezer_position_reference(
    *,
    album_deezer_id: str | None,
    disc: int,
    position: int,
    settings: Settings,
    deezer_client: DeezerClient | None = None,
) -> ExactDeezerReference | None:
    """Resolve one current Deezer preview from exact album and manifest position.

    The album endpoint is the authority for disc/position.  This deliberately has
    no title-search or track-enrichment fallback.
    """
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
    ):
        return None
    if not _deezer_preview_is_usable(preview_url):
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


def _deezer_preview_is_usable(url: str, *, now: float | None = None) -> bool:
    """Reject signed Deezer previews that will expire before review analysis can finish."""
    try:
        signature = parse_qs(urlsplit(url).query).get("hdnea", [])
        expiry = next(
            int(part.removeprefix("exp="))
            for part in signature[0].split("~")
            if part.startswith("exp=")
        )
    except (IndexError, StopIteration, TypeError, ValueError):
        return True
    current_time = time.time() if now is None else now
    return expiry > current_time + _DEEZER_PREVIEW_MINIMUM_VALIDITY_SECONDS


def _stored_preview(
    catalog_track: object | None, source: Literal["deezer", "itunes"]
) -> str | None:
    if catalog_track is None:
        return None
    source_specific = getattr(catalog_track, f"{source}_preview_url", None)
    if isinstance(source_specific, str) and source_specific.strip():
        return source_specific.strip()
    preview_source = str(getattr(catalog_track, "preview_source", "") or "").casefold()
    preview_url = getattr(catalog_track, "preview_url", None)
    if preview_source == source and isinstance(preview_url, str) and preview_url.strip():
        return preview_url.strip()

    album = getattr(catalog_track, "album", None)
    try:
        provenance = json.loads(getattr(album, "provenance_json", None) or "{}")
    except (json.JSONDecodeError, TypeError):
        return None
    previews = provenance.get("track_previews") if isinstance(provenance, dict) else None
    source_previews = previews.get(source) if isinstance(previews, dict) else None
    key = f"{getattr(catalog_track, 'disc', 1)}:{getattr(catalog_track, 'position', 0)}"
    stored_url = source_previews.get(key) if isinstance(source_previews, dict) else None
    return stored_url.strip() if isinstance(stored_url, str) and stored_url.strip() else None


async def resolve_reference_audio(
    track: Track,
    catalog_track: CatalogAlbumTrack | object | None,
    *,
    artist_name: str | None,
    settings: Settings,
    deezer_client: DeezerClient | None = None,
    itunes_client: ITunesClient | None = None,
) -> ReferenceAudio | None:
    """Resolve a comparison preview, keeping provider identity separate from acquisition."""
    stored_deezer = _stored_preview(catalog_track, "deezer")
    expired_deezer = bool(stored_deezer and not _deezer_preview_is_usable(stored_deezer))
    if stored_deezer and not expired_deezer:
        return {"url": stored_deezer, "source": "deezer"}
    stored_itunes = _stored_preview(catalog_track, "itunes")
    exact_deezer_refresh = expired_deezer and bool(track.deezer_id)
    if stored_itunes and not exact_deezer_refresh:
        return {"url": stored_itunes, "source": "itunes"}

    title = str(getattr(catalog_track, "title", None) or track.title or "").strip()
    deezer = deezer_client or DeezerClient(settings.deezer_api_url)
    try:
        async with asyncio.timeout(5):
            candidates = []
            if track.deezer_id:
                candidate = await deezer.get_track(track.deezer_id)
                if candidate is not None:
                    candidates = [candidate]
            elif title:
                candidates = await deezer.search_track(title, artist_name)
        for candidate in candidates:
            if candidate.preview_url and _deezer_preview_is_usable(candidate.preview_url):
                return {"url": candidate.preview_url, "source": "deezer"}
    except Exception:
        pass

    if stored_itunes:
        return {"url": stored_itunes, "source": "itunes"}
    if not title:
        return None
    itunes = itunes_client or ITunesClient()
    try:
        async with asyncio.timeout(5):
            itunes_candidates = await itunes.search_track(title, artist_name)
        for itunes_candidate in itunes_candidates:
            if itunes_candidate.preview_url:
                return {"url": itunes_candidate.preview_url, "source": "itunes"}
    except Exception:
        pass
    return None
