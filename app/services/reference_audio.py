from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Literal, TypedDict

from app.metadata.deezer import DeezerClient
from app.metadata.itunes import ITunesClient

if TYPE_CHECKING:
    from app.config import Settings
    from app.models.catalog_entities import CatalogAlbumTrack
    from app.models.track import Track


class ReferenceAudio(TypedDict):
    url: str
    source: Literal["deezer", "itunes"]


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
    if stored_deezer:
        return {"url": stored_deezer, "source": "deezer"}
    stored_itunes = _stored_preview(catalog_track, "itunes")
    if stored_itunes:
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
            if candidate.preview_url:
                return {"url": candidate.preview_url, "source": "deezer"}
    except Exception:
        pass

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
