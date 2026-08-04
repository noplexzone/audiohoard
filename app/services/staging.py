from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from app.config import Settings
from app.metadata.audio_file import AudioFileMetadata, read_audio_file_metadata
from app.models.staging_review import StagingReviewItem
from app.naming.convention import _sanitize_segment
from app.services.reference_audio import resolve_reference_audio

REVIEW_TAG_FIELDS = (
    "title",
    "artist",
    "album",
    "album_artist",
    "track_number",
    "disc_number",
    "year",
    "genre",
)


class StagingPathError(ValueError):
    pass


def build_staging_release_path(settings: Settings, *, source: str, release_id: int) -> Path:
    safe_source = _sanitize_segment(source)
    if safe_source != source or safe_source in {".", ".."}:
        raise StagingPathError("source cannot escape the staging root")

    root = settings.staging_root.resolve()
    candidate = (root / safe_source / f"release-{release_id}").resolve()
    if root != candidate and root not in candidate.parents:
        raise StagingPathError("staging path escapes the staging root")
    return candidate


def _empty_tags() -> dict[str, object | None]:
    return dict.fromkeys(REVIEW_TAG_FIELDS)


def _normalized(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().casefold()
    return text or None


def build_tag_diff(
    as_tagged: dict[str, object | None], should_be: dict[str, object | None]
) -> dict[str, bool]:
    return {
        field: _normalized(as_tagged.get(field)) != _normalized(should_be.get(field))
        for field in REVIEW_TAG_FIELDS
    }


def _project_file_tags(metadata: AudioFileMetadata) -> dict[str, object | None]:
    return {
        "title": metadata.title,
        "artist": metadata.artist,
        "album": metadata.album,
        "album_artist": metadata.album_artist,
        "track_number": metadata.track,
        "disc_number": metadata.disc,
        "year": metadata.year,
        "genre": metadata.genre,
    }


async def _read_staged_tags(
    item: StagingReviewItem, settings: Settings
) -> dict[str, object | None]:
    staging_path = item.track.staging_path
    if not staging_path:
        return _empty_tags()
    try:
        # Imported lazily to reuse the serving route's single path-safety implementation
        # without creating a router import cycle at application startup.
        from app.routers.staging import _validate_audio_path

        path = await asyncio.to_thread(_validate_audio_path, staging_path, settings.staging_root)
        metadata = await asyncio.to_thread(read_audio_file_metadata, path, suffix_hint=path.suffix)
    except Exception:
        return _empty_tags()
    return _project_file_tags(metadata)


def _expected_tags(item: StagingReviewItem) -> dict[str, object | None]:
    track = item.track
    catalog_track = track.catalog_track
    album = track.catalog_album or (catalog_track.album if catalog_track is not None else None)
    artist = album.artist if album is not None else None
    artist_name = getattr(artist, "name", None) or track.artist
    album_title = getattr(album, "title", None) or track.album or item.release.title
    album_artist = artist_name or track.album_artist or item.release.album_artist
    return {
        "title": getattr(catalog_track, "title", None) or track.title or item.expected_title,
        "artist": artist_name,
        "album": album_title,
        "album_artist": album_artist,
        "track_number": getattr(catalog_track, "position", None) or track.track_no,
        "disc_number": getattr(catalog_track, "disc", None) or track.disc,
        "year": getattr(album, "year", None) or track.year or item.release.year,
        "genre": getattr(catalog_track, "genre", None) or getattr(album, "genre", None),
    }


async def build_review_item(
    item: StagingReviewItem,
    settings: Settings,
    *,
    resolve_reference: bool = True,
) -> dict[str, Any]:
    track = item.track
    catalog_track = track.catalog_track
    album = track.catalog_album or (catalog_track.album if catalog_track is not None else None)
    artist = album.artist if album is not None else None
    artist_name = getattr(artist, "name", None) or track.artist or item.release.album_artist
    as_tagged = await _read_staged_tags(item, settings)
    should_be = _expected_tags(item)
    reference = None
    if resolve_reference:
        reference = await resolve_reference_audio(
            track,
            catalog_track,
            artist_name=artist_name,
            settings=settings,
        )
    expected_duration = getattr(catalog_track, "duration_sec", None) or track.duration_sec
    duration_delta = None
    if expected_duration is not None and item.fingerprint_duration_sec is not None:
        duration_delta = item.fingerprint_duration_sec - expected_duration
    observed_mbids = item.observed_acoustid_mbids
    expected_mbid = item.expected_recording_mbid
    return {
        "item": item,
        "track": track,
        "release": item.release,
        "catalog_track": catalog_track,
        "catalog_album": album,
        "as_tagged": as_tagged,
        "should_be": should_be,
        "diff": build_tag_diff(as_tagged, should_be),
        "reference": reference,
        "source_label": item.source_label,
        "original_filename": item.original_filename,
        "acoustid_score": item.acoustid_score,
        "expected_recording_mbid": expected_mbid,
        "observed_acoustid_mbids": observed_mbids,
        "mbid_match": bool(expected_mbid and expected_mbid in observed_mbids),
        "fingerprint_duration_sec": item.fingerprint_duration_sec,
        "expected_duration_sec": expected_duration,
        "duration_delta_sec": duration_delta,
        "artwork_url": getattr(album, "artwork_url", None),
        "artist_name": artist_name,
        "album_title": getattr(album, "title", None) or item.release.title,
        "album_track_count": getattr(album, "track_count", None),
    }
