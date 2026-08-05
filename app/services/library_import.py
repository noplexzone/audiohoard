from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import os
import re
import shutil
import stat
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, no_type_check
from urllib.parse import urljoin, urlparse

import httpx
from mutagen.flac import FLAC, Picture
from mutagen.id3 import (
    APIC,
    ID3,
    TALB,
    TCON,
    TDRC,
    TDRL,
    TIT2,
    TPE1,
    TPE2,
    TPOS,
    TPUB,
    TRCK,
    TSRC,
    TXXX,
    ID3NoHeaderError,
)
from mutagen.mp4 import MP4, MP4Cover
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.database import register_transaction_callbacks, run_with_sqlite_lock_retry
from app.http import stream_with_retry
from app.media_formats import is_importable_audio, supported_audio_formats_display
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.models.import_plan import (
    CollisionState,
    ImportPlan,
    LibraryFileState,
    TagVerificationState,
)
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.naming.convention import NamingError, render_path
from app.services.acquisition_cleanup import (
    ImportedSourceCleanup,
    schedule_imported_source_cleanup,
)
from app.services.pinned_destination import PinnedDestination
from app.services.quality_upgrade import reconcile_album_quality_duplicates
from app.settings_service import QualityProfile, get_runtime_settings

logger = logging.getLogger(__name__)

_DESTINATION_TEMPLATE = "{album_artist}/{album} ({year})/{disc_track} - {title}.{ext}"

_MANAGED_TAG_KEYS = frozenset(
    {
        "title",
        "artist",
        "album",
        "album_artist",
        "album artist",
        "album_artists",
        "albumartist",
        "albumartists",
        "albumartist_credit",
        "albumartists_credit",
        "albumartists_sort",
        "albumartistsort",
        "albumversion",
        "musicbrainz_albumcomment",
        "disc",
        "discc",
        "track",
        "trackc",
        "date",
        "year",
        "releasedate",
        "release_date",
        "originaldate",
        "original_date",
        "originalyear",
        "tracknumber",
        "discnumber",
        "genre",
        "organization",
        "label",
        "recordlabel",
        "copyright",
        "barcode",
        "isrc",
        "media",
        "releasecountry",
        "releasestatus",
        "releasetype",
        "tracktotal",
        "disctotal",
        "totaltracks",
        "totaldiscs",
        "musicbrainz_trackid",
        "musicbrainz_albumid",
        "musicbrainz_albumartistid",
        "musicbrainz_releasegroupid",
        "musicbrainz_albumstatus",
        "musicbrainz_albumtype",
        "musicbrainz_artistid",
        "musicbrainz_releasetrackid",
    }
)

_MANAGED_ID3_TXXX_DESCRIPTIONS = frozenset(
    {
        "barcode",
        "album version",
        "albumversion",
        "musicbrainz album comment",
        "disc",
        "discc",
        "track",
        "trackc",
        "media",
        "release country",
        "release status",
        "release type",
        "track total",
        "disc total",
        "musicbrainz album release country",
        "musicbrainz album status",
        "musicbrainz album type",
        "musicbrainz album media",
        "musicbrainz track total",
        "musicbrainz disc total",
        "musicbrainz track id",
        "musicbrainz album id",
        "musicbrainz album artist id",
        "musicbrainz release group id",
        "musicbrainz artist id",
        "musicbrainz release track id",
    }
)


def _normalized_id3_txxx_description(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


_MANAGED_ID3_TXXX_NORMALIZED = frozenset(
    _normalized_id3_txxx_description(description)
    for description in _MANAGED_TAG_KEYS | _MANAGED_ID3_TXXX_DESCRIPTIONS
)


@dataclass(frozen=True)
class CanonicalArtwork:
    data: bytes
    mime: str


_ARTWORK_TIMEOUT_SECONDS = 10.0
_MAX_ARTWORK_BYTES = 5 * 1024 * 1024
_ALLOWED_ARTWORK_HOSTS = frozenset(
    {
        "e-cdns-images.dzcdn.net",
        "cdn-images.dzcdn.net",
        "is1-ssl.mzstatic.com",
        "is2-ssl.mzstatic.com",
        "is3-ssl.mzstatic.com",
        "is4-ssl.mzstatic.com",
        "is5-ssl.mzstatic.com",
        "coverartarchive.org",
        "archive.org",
        "ia801504.us.archive.org",
    }
)


class ImportPlanningError(ValueError):
    pass


class ImportExecutionError(RuntimeError):
    pass


def _resolved_path(path: Path) -> Path:
    return path.resolve()


def _path_exists(path: Path) -> bool:
    return path.exists()


def _is_regular_non_symlink(path: Path) -> bool:
    return not path.is_symlink() and path.is_file()


def _unlink_missing_ok(path: Path) -> None:
    path.unlink(missing_ok=True)


def _unlink_backup_after_commit(path: Path) -> None:
    path.unlink(missing_ok=True)


def _write_tags_compatible(
    tag_writer: MutagenTagWriter,
    path: Path,
    tags: dict[str, str],
    artwork: CanonicalArtwork | None,
) -> bool:
    try:
        return bool(tag_writer.write_and_verify(path, tags, artwork))
    except TypeError as exc:
        if "positional" not in str(exc) and "argument" not in str(exc):
            raise
        return bool(tag_writer.write_and_verify(path, tags))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_fileobj(handle: BinaryIO) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def _open_regular_source_no_follow(path: Path) -> int:
    absolute = path.absolute()
    parts = absolute.parts
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_fd: int | None = None
    try:
        directory_fd = os.open(absolute.anchor, directory_flags)
        for part in parts[1:-1]:
            next_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        fd = os.open(parts[-1], file_flags, dir_fd=directory_fd)
    except OSError as exc:
        raise ImportExecutionError("source path is not a regular non-symlink file") from exc
    finally:
        if directory_fd is not None:
            os.close(directory_fd)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ImportExecutionError("source path is not a regular non-symlink file")
    except Exception:
        os.close(fd)
        raise
    return fd


def _sha256_regular_source_no_follow(path: Path) -> str:
    with os.fdopen(_open_regular_source_no_follow(path), "rb") as handle:
        return _sha256_fileobj(handle)


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _ensure_regular_source(path: Path) -> None:
    if path.is_symlink():
        raise ImportPlanningError("source path is a symlink")
    if not path.is_file():
        raise ImportPlanningError("source path is not a regular file")


def _destination_inside_root(library_root: Path, destination: Path) -> None:
    root = library_root.resolve()
    resolved = destination.resolve(strict=False)
    if resolved != root and root not in resolved.parents:
        raise ImportPlanningError("destination escapes library root")


def _existing_parent_symlink(library_root: Path, destination: Path) -> Path | None:
    root = library_root.resolve()
    current = root
    for part in destination.relative_to(root).parts[:-1]:
        current = current / part
        if current.exists() and current.is_symlink():
            return current
    return None


def _track_source_path(track: Track) -> Path:
    raw = track.staging_path or track.source_path
    if not raw:
        raise ImportPlanningError(f"track {track.id} has no staged source path")
    return Path(raw)


def _artwork_url_allowed(url: str) -> bool:
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    if parsed.scheme != "https":
        return False
    if hostname in _ALLOWED_ARTWORK_HOSTS:
        return True
    return hostname.endswith(".ca.archive.org") and hostname.startswith("dn")


def _redirect_location_allowed(current_url: str, location: str | None) -> str | None:
    if not location:
        return None
    redirected = urljoin(current_url, location)
    return redirected if _artwork_url_allowed(redirected) else None


async def _fetch_canonical_artwork(url: str | None) -> CanonicalArtwork | None:
    if not url or not _artwork_url_allowed(url):
        return None
    data = bytearray()
    content_type = ""
    current_url = url
    redirects_remaining = 5
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_ARTWORK_TIMEOUT_SECONDS)) as client:
            while True:
                response = await stream_with_retry(client, "GET", current_url)
                try:
                    if response.status_code in {301, 302, 303, 307, 308}:
                        if redirects_remaining <= 0:
                            return None
                        redirected = _redirect_location_allowed(
                            current_url, response.headers.get("location")
                        )
                        if redirected is None:
                            return None
                        current_url = redirected
                        redirects_remaining -= 1
                        continue
                    if response.status_code != 200:
                        return None
                    content_type = (
                        response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                    )
                    content_length = response.headers.get("content-length")
                    if content_length:
                        try:
                            declared_size = int(content_length)
                        except ValueError:
                            declared_size = 0
                        if declared_size > _MAX_ARTWORK_BYTES:
                            return None
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > _MAX_ARTWORK_BYTES:
                            return None
                    break
                finally:
                    await response.aclose()
    except httpx.HTTPError:
        logger.warning("Could not fetch canonical artwork for metadata repair", exc_info=True)
        return None
    if content_type not in {"image/jpeg", "image/jpg", "image/png"}:
        if data.startswith(b"\xff\xd8"):
            content_type = "image/jpeg"
        elif data.startswith(b"\x89PNG\r\n\x1a\n"):
            content_type = "image/png"
        else:
            logger.warning("Canonical artwork URL did not return JPEG/PNG content")
            return None
    if content_type == "image/jpg":
        content_type = "image/jpeg"
    return CanonicalArtwork(data=bytes(data), mime=content_type)


def _mp4_cover_format(mime: str) -> int:
    return MP4Cover.FORMAT_PNG if mime == "image/png" else MP4Cover.FORMAT_JPEG


def _slash_number_pair(value: str) -> tuple[int, int]:
    parts = str(value).split("/", 1)
    current = int(parts[0])
    total = int(parts[1]) if len(parts) > 1 and parts[1] else 0
    return current, total


_TRACK_NUMBER_PREFIX = re.compile(r"^(?:(?P<disc>\d{1,2})[-_.])?(?P<track>\d{1,3})(?:\D|$)")
_DISC_FOLDER = re.compile(r"^(?:cd|disc)[ _.-]?(\d{1,2})$", re.IGNORECASE)


def _track_key_from_filename(path: Path, album_folder: Path) -> tuple[int, int] | None:
    match = _TRACK_NUMBER_PREFIX.match(path.stem.strip())
    if not match:
        return None
    disc = int(match.group("disc") or 1)
    track = int(match.group("track"))
    if match.group("disc") is None:
        for parent_part in path.relative_to(album_folder).parts[:-1]:
            disc_match = _DISC_FOLDER.match(parent_part)
            if disc_match:
                disc = int(disc_match.group(1))
        if 100 <= track <= 999:
            disc, track = divmod(track, 100)
    return disc, track


def _normalized_title(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").replace("-", " ").split())


def _ensure_library_file_readable(path: Path) -> None:
    """Keep imported/retagged media readable by scanners such as Navidrome."""
    with contextlib.suppress(OSError):
        mode = stat.S_IMODE(path.stat().st_mode)
        path.chmod(mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def _tags_for(release: Release, track: Track) -> dict[str, str]:
    tags = {
        "title": track.title or "",
        "artist": track.artist or "",
        "album": track.album or release.title or "",
        "album_artist": track.album_artist or release.album_artist or track.artist or "",
        "date": track.year or release.year or "",
        "release_date": track.year or release.year or "",
        "releasedate": track.year or release.year or "",
        "tracknumber": str(track.track_no or ""),
        "discnumber": str(track.disc or ""),
        "musicbrainz_trackid": track.mbid or "",
        "musicbrainz_albumid": release.release_mbid or "",
    }
    if track.disc_total and track.disc_total > 1:
        tags["disctotal"] = str(track.disc_total)
        tags["totaldiscs"] = str(track.disc_total)
    return {key: value for key, value in tags.items() if value}


class MutagenTagWriter:
    @no_type_check
    def write_and_verify(
        self, path: Path, tags: dict[str, str], artwork: CanonicalArtwork | None = None
    ) -> bool:
        suffix = path.suffix.casefold()
        if suffix == ".mp3":
            try:
                id3 = ID3(path)
            except ID3NoHeaderError:
                id3 = ID3()
            for frame_id in (
                "TIT2",
                "TPE1",
                "TALB",
                "TPE2",
                "TDRC",
                "TDRL",
                "TDOR",
                "TORY",
                "TRCK",
                "TPOS",
                "TCON",
                "TPUB",
                "TCOP",
                "TSRC",
            ):
                id3.delall(frame_id)
            if artwork is not None:
                id3.delall("APIC")
            for frame in list(id3.getall("TXXX")):
                if _normalized_id3_txxx_description(frame.desc) in _MANAGED_ID3_TXXX_NORMALIZED:
                    id3.delall(f"TXXX:{frame.desc}")
            if title := tags.get("title"):
                id3.add(TIT2(encoding=3, text=title))
            if artist := tags.get("artist"):
                id3.add(TPE1(encoding=3, text=artist))
            if album := tags.get("album"):
                id3.add(TALB(encoding=3, text=album))
            if album_artist := tags.get("album_artist"):
                id3.add(TPE2(encoding=3, text=album_artist))
            if date := tags.get("date"):
                id3.add(TDRC(encoding=3, text=date))
            if release_date := tags.get("release_date"):
                id3.add(TDRL(encoding=3, text=release_date))
            if tracknumber := tags.get("tracknumber"):
                id3.add(TRCK(encoding=3, text=tracknumber))
            if discnumber := tags.get("discnumber"):
                id3.add(TPOS(encoding=3, text=discnumber))
            if genre := tags.get("genre"):
                id3.add(TCON(encoding=3, text=genre))
            if label := tags.get("label"):
                id3.add(TPUB(encoding=3, text=label))
            if isrc := tags.get("isrc"):
                id3.add(TSRC(encoding=3, text=isrc))
            if artwork is not None:
                id3.add(
                    APIC(encoding=3, mime=artwork.mime, type=3, desc="Cover", data=artwork.data)
                )
            for key, description in {
                "tracktotal": "Track Total",
                "disctotal": "Disc Total",
                "totaltracks": "MusicBrainz Track Total",
                "totaldiscs": "MusicBrainz Disc Total",
                "musicbrainz_trackid": "MusicBrainz Track Id",
                "musicbrainz_albumid": "MusicBrainz Album Id",
                "musicbrainz_albumartistid": "MusicBrainz Album Artist Id",
                "musicbrainz_releasegroupid": "MusicBrainz Release Group Id",
            }.items():
                if value := tags.get(key):
                    id3.add(TXXX(encoding=3, desc=description, text=value))
            id3.save(path, v2_version=3)
        elif suffix == ".flac":
            flac = FLAC(path)
            for key in _MANAGED_TAG_KEYS:
                if key in flac:
                    del flac[key]
            for key, value in tags.items():
                flac[key] = value
            if album_artist := tags.get("album_artist"):
                flac["albumartist"] = album_artist
                flac["albumartists"] = album_artist
            if artwork is not None:
                flac.clear_pictures()
                picture = Picture()
                picture.type = 3
                picture.mime = artwork.mime
                picture.desc = "Cover"
                picture.data = artwork.data
                flac.add_picture(picture)
            flac.save()
        elif suffix in {".ogg", ".oga", ".opus"}:
            ogg = OggOpus(path) if suffix == ".opus" else OggVorbis(path)
            for key in _MANAGED_TAG_KEYS:
                if key in ogg:
                    del ogg[key]
            if artwork is not None:
                for key in ("metadata_block_picture", "coverart"):
                    if key in ogg:
                        del ogg[key]
            for key, value in tags.items():
                ogg[key] = value
            if album_artist := tags.get("album_artist"):
                ogg["album_artist"] = album_artist
                ogg["albumartist"] = album_artist
                ogg["albumartists"] = album_artist
            if artwork is not None:
                picture = Picture()
                picture.type = 3
                picture.mime = artwork.mime
                picture.desc = "Cover"
                picture.data = artwork.data
                ogg["metadata_block_picture"] = base64.b64encode(picture.write()).decode("ascii")
            ogg.save()
        elif suffix in {".m4a", ".mp4"}:
            mp4 = MP4(path)
            text_atoms = {
                "title": "\xa9nam",
                "artist": "\xa9ART",
                "album": "\xa9alb",
                "album_artist": "aART",
                "date": "\xa9day",
                "release_date": "\xa9day",
                "releasedate": "\xa9day",
                "genre": "\xa9gen",
            }
            freeform_atoms = {
                "disctotal": "----:com.apple.iTunes:Disc Total",
                "tracktotal": "----:com.apple.iTunes:Track Total",
                "totaldiscs": "----:com.apple.iTunes:MusicBrainz Disc Total",
                "totaltracks": "----:com.apple.iTunes:MusicBrainz Track Total",
                "musicbrainz_trackid": "----:com.apple.iTunes:MusicBrainz Track Id",
                "musicbrainz_albumid": "----:com.apple.iTunes:MusicBrainz Album Id",
                "musicbrainz_albumartistid": "----:com.apple.iTunes:MusicBrainz Album Artist Id",
                "musicbrainz_releasegroupid": "----:com.apple.iTunes:MusicBrainz Release Group Id",
            }
            cleanup_atoms = {
                *text_atoms.values(),
                "trkn",
                "disk",
                "covr",
                "cprt",
                "----:com.apple.iTunes:LABEL",
                "----:com.apple.iTunes:BARCODE",
                "----:com.apple.iTunes:ISRC",
                "----:com.apple.iTunes:MEDIA",
                "----:com.apple.iTunes:MusicBrainz Album Id",
                *freeform_atoms.values(),
            }
            for atom in cleanup_atoms:
                if atom in mp4:
                    del mp4[atom]
            for key, atom in text_atoms.items():
                if value := tags.get(key):
                    mp4[atom] = [value]
            if value := tags.get("tracknumber"):
                current, total = _slash_number_pair(value)
                if track_total := tags.get("tracktotal"):
                    total = _slash_number_pair(track_total)[0]
                mp4["trkn"] = [(current, total)]
            if value := tags.get("discnumber"):
                current, total = _slash_number_pair(value)
                if disc_total := tags.get("disctotal"):
                    total = _slash_number_pair(disc_total)[0]
                mp4["disk"] = [(current, total)]
            for key, atom in freeform_atoms.items():
                if value := tags.get(key):
                    mp4[atom] = [value.encode()]
            if label := tags.get("label"):
                mp4["----:com.apple.iTunes:LABEL"] = [label.encode()]
            if artwork is not None:
                mp4["covr"] = [MP4Cover(artwork.data, imageformat=_mp4_cover_format(artwork.mime))]
            mp4.save()
        else:
            return False
        _ensure_library_file_readable(path)
        readback = self.read_tags(path)
        return all(readback.get(key) == value for key, value in tags.items())

    @no_type_check
    def read_tags(self, path: Path, *, suffix_hint: str | None = None) -> dict[str, str]:
        suffix = (suffix_hint or path.suffix).casefold()
        comment_keys = (
            "title",
            "artist",
            "album",
            "album_artist",
            "date",
            "releasedate",
            "release_date",
            "genre",
            "label",
            "isrc",
            "tracknumber",
            "discnumber",
            "tracktotal",
            "disctotal",
            "totaltracks",
            "totaldiscs",
            "musicbrainz_trackid",
            "musicbrainz_albumid",
            "musicbrainz_albumartistid",
            "musicbrainz_releasegroupid",
            "musicbrainz_albumstatus",
            "musicbrainz_albumtype",
            "musicbrainz_artistid",
            "musicbrainz_releasetrackid",
        )
        if suffix == ".flac":
            flac = FLAC(path)
            return {
                key: str(tag_values[0]) for key in comment_keys if (tag_values := flac.get(key))
            }
        if suffix in {".ogg", ".oga", ".opus"}:
            ogg = OggOpus(path) if suffix == ".opus" else OggVorbis(path)
            return {
                key: str(ogg_values[0]) for key in comment_keys if (ogg_values := ogg.get(key))
            }
        if suffix in {".m4a", ".mp4"}:
            mp4 = MP4(path)
            mp4_values: dict[str, str] = {}
            text_atoms = {
                "title": "\xa9nam",
                "artist": "\xa9ART",
                "album": "\xa9alb",
                "album_artist": "aART",
                "date": "\xa9day",
                "release_date": "\xa9day",
                "releasedate": "\xa9day",
                "genre": "\xa9gen",
            }
            for key, atom in text_atoms.items():
                if atom_values := mp4.get(atom):
                    mp4_values[key] = str(atom_values[0])
            if track_values := mp4.get("trkn"):
                current, total = track_values[0]
                mp4_values["tracknumber"] = str(current)
                if total:
                    mp4_values["tracktotal"] = str(total)
            if disc_values := mp4.get("disk"):
                current, total = disc_values[0]
                mp4_values["discnumber"] = str(current)
                if total:
                    mp4_values["disctotal"] = str(total)
            if atom_values := mp4.get("----:com.apple.iTunes:LABEL"):
                raw = atom_values[0]
                mp4_values["label"] = raw.decode() if isinstance(raw, bytes) else str(raw)
            for key, atom in {
                "disctotal": "----:com.apple.iTunes:Disc Total",
                "tracktotal": "----:com.apple.iTunes:Track Total",
                "totaldiscs": "----:com.apple.iTunes:MusicBrainz Disc Total",
                "totaltracks": "----:com.apple.iTunes:MusicBrainz Track Total",
                "musicbrainz_trackid": "----:com.apple.iTunes:MusicBrainz Track Id",
                "musicbrainz_albumid": "----:com.apple.iTunes:MusicBrainz Album Id",
                "musicbrainz_albumartistid": "----:com.apple.iTunes:MusicBrainz Album Artist Id",
                "musicbrainz_releasegroupid": "----:com.apple.iTunes:MusicBrainz Release Group Id",
            }.items():
                if atom_values := mp4.get(atom):
                    raw = atom_values[0]
                    mp4_values[key] = raw.decode() if isinstance(raw, bytes) else str(raw)
            return mp4_values
        if suffix != ".mp3":
            return {}
        id3 = ID3(path)
        values: dict[str, str] = {}
        frame_map = {
            "title": "TIT2",
            "artist": "TPE1",
            "album": "TALB",
            "album_artist": "TPE2",
            "date": "TDRC",
            "releasedate": "TDRL",
            "release_date": "TDRL",
            "genre": "TCON",
            "label": "TPUB",
            "isrc": "TSRC",
            "tracknumber": "TRCK",
            "discnumber": "TPOS",
        }
        for key, frame_id in frame_map.items():
            frame = id3.get(frame_id)
            if frame is not None and getattr(frame, "text", None):
                values[key] = str(frame.text[0])
        descriptions = {
            "track total": "tracktotal",
            "disc total": "disctotal",
            "musicbrainz track total": "totaltracks",
            "musicbrainz disc total": "totaldiscs",
            "musicbrainz track id": "musicbrainz_trackid",
            "musicbrainz album id": "musicbrainz_albumid",
            "musicbrainz album artist id": "musicbrainz_albumartistid",
            "musicbrainz release group id": "musicbrainz_releasegroupid",
        }
        for frame in id3.getall("TXXX"):
            if frame.text and (key := descriptions.get(frame.desc.casefold())):
                values[key] = str(frame.text[0])
        return values


@dataclass(frozen=True)
class AlbumRetagResult:
    files_retagged: int
    folder: Path
    files_renamed: int = 0


def _catalog_disc_total(album: CatalogAlbum) -> int | None:
    discs = [track.disc for track in album.tracks if track.disc and track.disc > 0]
    total = max(discs, default=1)
    return total if total > 1 else None


def _catalog_disc_total_values(album: CatalogAlbum) -> dict[str, str]:
    disc_total = _catalog_disc_total(album)
    if not disc_total:
        return {}
    value = str(disc_total)
    return {"disctotal": value, "totaldiscs": value}


def _catalog_track_total_values(album: CatalogAlbum, disc: int) -> dict[str, str]:
    total = sum(1 for track in album.tracks if track.disc == disc)
    if total <= 0:
        return {}
    value = str(total)
    return {"tracktotal": value, "totaltracks": value}


def _catalog_tags(
    album: CatalogAlbum, catalog_track: CatalogAlbumTrack, track: Track | None
) -> dict[str, str]:
    values = {
        "title": catalog_track.title,
        "artist": (track.artist if track is not None else None) or album.artist.name,
        "album": album.title,
        "album_artist": album.artist.name,
        "date": album.year or "",
        "releasedate": album.year or "",
        "release_date": album.year or "",
        "tracknumber": str(catalog_track.position),
        "discnumber": str(catalog_track.disc),
        **_catalog_disc_total_values(album),
        **_catalog_track_total_values(album, catalog_track.disc),
        "musicbrainz_trackid": catalog_track.recording_mbid
        or (track.mbid if track is not None else "")
        or "",
        "musicbrainz_releasegroupid": album.mbid or "",
        "musicbrainz_albumartistid": album.artist.mbid or "",
    }
    if not values["title"] or not values["artist"] or not values["album_artist"]:
        raise ImportExecutionError("stored track metadata is incomplete")
    if catalog_track.position < 1 or catalog_track.disc < 1:
        raise ImportExecutionError("stored track numbering is invalid")
    return {key: value for key, value in values.items() if value}


def _sync_track_from_catalog(
    track: Track, album: CatalogAlbum, catalog_track: CatalogAlbumTrack
) -> None:
    disc_total = _catalog_disc_total(album)
    track.catalog_album_id = album.id
    track.catalog_track_id = catalog_track.id
    track.title = catalog_track.title
    track.artist = album.artist.name
    track.album_artist = album.artist.name
    track.album = album.title
    track.year = album.year
    track.disc = catalog_track.disc
    track.disc_total = disc_total
    track.track_no = catalog_track.position
    track.mbid = catalog_track.recording_mbid or track.mbid


def _sync_track_numbering_from_catalog(
    track: Track, album: CatalogAlbum, catalog_track: CatalogAlbumTrack
) -> None:
    track.catalog_album_id = album.id
    track.catalog_track_id = catalog_track.id
    track.disc = catalog_track.disc
    track.disc_total = _catalog_disc_total(album)
    track.track_no = catalog_track.position


def _canonical_destination_for_catalog_track(
    root: Path, album: CatalogAlbum, catalog_track: CatalogAlbumTrack, current_path: Path
) -> Path:
    rendered = render_path(
        title=catalog_track.title,
        artist=album.artist.name,
        album_artist=album.artist.name,
        album=album.title,
        year=album.year,
        disc=catalog_track.disc,
        disc_total=_catalog_disc_total(album),
        track_no=catalog_track.position,
        ext=current_path.suffix.lower().lstrip("."),
        template=_DESTINATION_TEMPLATE,
        library_root=root,
    )
    return current_path.parent / Path(rendered).name


def _discover_legacy_album_files(
    album: CatalogAlbum, library_root: Path
) -> list[tuple[Path, None, CatalogAlbumTrack]]:
    folder = library_root.resolve() / album.artist.name / f"{album.title} ({album.year or ''})"
    if not folder.is_dir() or folder.is_symlink():
        return []
    catalog_by_position = {(track.disc, track.position): track for track in album.tracks}
    catalog_by_title = {_normalized_title(track.title): track for track in album.tracks}
    targets: list[tuple[Path, None, CatalogAlbumTrack]] = []
    used_catalog_ids: set[int] = set()
    for path in sorted(folder.rglob("*")):
        if path.is_symlink() or not path.is_file() or not is_importable_audio(path):
            continue
        track_key = _track_key_from_filename(path, folder)
        stripped = path.stem
        prefix = _TRACK_NUMBER_PREFIX.match(path.stem.strip())
        if prefix is not None:
            stripped = stripped[prefix.end() :].lstrip(" .-_")
        title_match = catalog_by_title.get(_normalized_title(stripped))
        catalog_track = catalog_by_position.get(track_key or (0, 0))
        if (
            title_match is not None
            and track_key is not None
            and title_match.position == track_key[1]
            and title_match is not catalog_track
        ):
            catalog_track = title_match
        if catalog_track is None:
            catalog_track = title_match
        if catalog_track is None or catalog_track.id in used_catalog_ids:
            raise ImportExecutionError(
                "album folder contains audio not linked to stored track metadata"
            )
        used_catalog_ids.add(catalog_track.id)
        targets.append((path, None, catalog_track))
    return targets


async def retag_catalog_album(
    db: AsyncSession,
    album_id: int,
    *,
    library_root: Path,
    tag_writer: MutagenTagWriter | None = None,
) -> AlbumRetagResult:
    album = (
        await db.execute(
            select(CatalogAlbum)
            .where(CatalogAlbum.id == album_id)
            .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
        )
    ).scalar_one_or_none()
    if album is None:
        raise ImportExecutionError("catalog album not found")

    rows = (
        await db.execute(
            select(Track, ImportPlan)
            .join(ImportPlan, ImportPlan.track_id == Track.id)
            .where(
                or_(
                    Track.catalog_album_id == album_id,
                    Track.catalog_album_id.is_(None),
                ),
                Track.import_state == ImportWorkflowState.imported,
                ImportPlan.status == ImportWorkflowState.imported,
                ImportPlan.destination_path != "",
            )
            .order_by(ImportPlan.id)
        )
    ).all()
    latest: dict[int, tuple[Track, ImportPlan]] = {}
    for track, plan in rows:
        if track.catalog_album_id is None:
            legacy_artist = track.album_artist or track.artist or ""
            if (track.album or "").casefold() != album.title.casefold() or (
                legacy_artist.casefold() != album.artist.name.casefold()
            ):
                continue
        latest[track.id] = (track, plan)
    artwork = await _fetch_canonical_artwork(album.artwork_url)
    catalog_tracks = {item.id: item for item in album.tracks}
    catalog_tracks_by_position = {(item.disc, item.position): item for item in album.tracks}
    for track, _plan in latest.values():
        catalog_track = catalog_tracks.get(track.catalog_track_id or 0)
        if catalog_track is None:
            catalog_track = catalog_tracks_by_position.get((track.disc or 1, track.track_no or 0))
        if catalog_track is not None:
            _sync_track_numbering_from_catalog(track, album, catalog_track)
    legacy_targets = _discover_legacy_album_files(album, library_root)
    if latest:
        imported_destinations = {plan.destination_path for _track, plan in latest.values()}
        legacy_targets = [
            (path, track, catalog_track)
            for path, track, catalog_track in legacy_targets
            if str(path) not in imported_destinations
        ]
    if not latest and not legacy_targets:
        raise ImportExecutionError("album has no imported files to retag")

    return await asyncio.to_thread(
        _retag_catalog_album_files,
        album,
        list(latest.values()),
        library_root=library_root,
        tag_writer=tag_writer,
        artwork=artwork,
        legacy_targets=legacy_targets,
    )


def _retag_catalog_album_files(
    album: CatalogAlbum,
    imported: list[tuple[Track, ImportPlan]],
    *,
    library_root: Path,
    tag_writer: MutagenTagWriter | None,
    artwork: CanonicalArtwork | None = None,
    legacy_targets: list[tuple[Path, None, CatalogAlbumTrack]] | None = None,
) -> AlbumRetagResult:
    catalog_tracks = {item.id: item for item in album.tracks}
    catalog_tracks_by_position = {(item.disc, item.position): item for item in album.tracks}
    root = library_root.resolve()
    targets: list[tuple[Path, Path, Track | None, ImportPlan | None, CatalogAlbumTrack]] = []
    for path, track, catalog_track in legacy_targets or []:
        targets.append(
            (
                path,
                _canonical_destination_for_catalog_track(root, album, catalog_track, path),
                track,
                None,
                catalog_track,
            )
        )
    folders: set[Path] = set()
    mapped_destinations: set[Path] = set()
    current_destinations: set[Path] = set()
    for track, plan in imported:
        imported_catalog_track = catalog_tracks.get(track.catalog_track_id or 0)
        if imported_catalog_track is None:
            imported_catalog_track = catalog_tracks_by_position.get(
                (track.disc or 1, track.track_no or 0)
            )
        if imported_catalog_track is None:
            raise ImportExecutionError("imported file is not linked to stored track metadata")
        destination = Path(plan.destination_path)
        try:
            _destination_inside_root(root, destination)
        except ImportPlanningError as exc:
            raise ImportExecutionError(str(exc)) from exc
        if _existing_parent_symlink(root, destination) is not None:
            raise ImportExecutionError("album folder contains a symlinked path")
        if not _is_regular_non_symlink(destination):
            raise ImportExecutionError(f"imported file is missing or unsafe: {destination.name}")
        if not is_importable_audio(destination):
            raise ImportExecutionError(f"unsupported audio format: {destination.suffix}")
        resolved_destination = destination.resolve()
        if resolved_destination in current_destinations:
            raise ImportExecutionError("duplicate destination mapping in stored import metadata")
        current_destinations.add(resolved_destination)
        canonical_destination = _canonical_destination_for_catalog_track(
            root, album, imported_catalog_track, destination
        )
        if canonical_destination.resolve() in mapped_destinations:
            raise ImportExecutionError("duplicate destination mapping in stored import metadata")
        mapped_destinations.add(canonical_destination.resolve())
        targets.append((destination, canonical_destination, track, plan, imported_catalog_track))
        folders.add(destination.parent.resolve())
    for destination, canonical_destination, _track, _plan, _catalog_track in targets:
        try:
            _destination_inside_root(root, destination)
        except ImportPlanningError as exc:
            raise ImportExecutionError(str(exc)) from exc
        if _existing_parent_symlink(root, destination) is not None:
            raise ImportExecutionError("album folder contains a symlinked path")
        if not _is_regular_non_symlink(destination):
            raise ImportExecutionError(f"imported file is missing or unsafe: {destination.name}")
        _destination_inside_root(root, canonical_destination)
        if _existing_parent_symlink(root, canonical_destination) is not None:
            raise ImportExecutionError("album folder contains a symlinked path")
        folders.add(destination.parent.resolve())
    if len(folders) != 1:
        raise ImportExecutionError("imported album files do not share one album folder")
    folder = next(iter(folders))
    actual_audio = {
        item.resolve()
        for item in folder.iterdir()
        if item.is_file() and not item.is_symlink() and is_importable_audio(item)
    }
    tracked_audio = {path.resolve() for path, _canonical, _track, _plan, _catalog_track in targets}
    if actual_audio != tracked_audio:
        raise ImportExecutionError(
            "album folder contains audio not linked to stored track metadata"
        )

    writer = tag_writer or MutagenTagWriter()
    pinned_destinations: list[PinnedDestination] = []
    temp_paths: list[tuple[PinnedDestination, str]] = []
    created_destinations: list[tuple[PinnedDestination, str]] = []
    backup_paths: list[tuple[PinnedDestination, str, str]] = []
    prepared: list[tuple[PinnedDestination, str, str]] = []
    try:
        renamed = 0
        for destination, _canonical_destination, target_track, _plan, catalog_track in targets:
            pinned = PinnedDestination.open(root, destination)
            pinned_destinations.append(pinned)
            if not pinned.is_regular_non_symlink():
                raise ImportExecutionError("album file changed before retag preparation")
            expected_hash = _sha256_regular_source_no_follow(destination)
            temp_name, temp_path = _copy_to_temp(destination, pinned, expected_hash)
            temp_paths.append((pinned, temp_name))
            if not writer.write_and_verify(
                temp_path, _catalog_tags(album, catalog_track, target_track), artwork=artwork
            ):
                raise ImportExecutionError("tag readback failed")
            with pinned.open_read(temp_name) as tagged_temp:
                os.fsync(tagged_temp.fileno())
            prepared.append((pinned, temp_name, expected_hash))

        temporary_names = {temp_name for _pinned, temp_name in temp_paths}
        current_audio = {
            item.resolve()
            for item in folder.iterdir()
            if item.is_file()
            and not item.is_symlink()
            and item.name not in temporary_names
            and is_importable_audio(item)
        }
        if current_audio != actual_audio:
            raise ImportExecutionError("album folder changed before retag commit")

        prepared_by_temp = {
            temp_name: (pinned, expected_hash) for pinned, temp_name, expected_hash in prepared
        }
        for (
            destination,
            canonical_destination,
            _target_track,
            target_plan,
            _catalog_track,
        ) in targets:
            pinned = next(item for item in pinned_destinations if item.destination == destination)
            temp_name = next(name for item, name in temp_paths if item is pinned)
            expected_hash = prepared_by_temp[temp_name][1]
            pinned.verify_attached()
            if not pinned.is_regular_non_symlink():
                raise ImportExecutionError("album file changed before retag commit")
            with pinned.open_read(pinned.name) as current_file:
                if _sha256_fileobj(current_file) != expected_hash:
                    raise ImportExecutionError("album file changed before retag commit")
            final_pinned = pinned
            if canonical_destination != destination:
                final_pinned = PinnedDestination.open(root, canonical_destination)
                pinned_destinations.append(final_pinned)
                if final_pinned.exists():
                    raise ImportExecutionError(
                        f"canonical retag destination already exists: {canonical_destination.name}"
                    )
                renamed += 1
            backup_name = (
                final_pinned.backup_existing(suffix=".retag-backup")
                if final_pinned.exists()
                else ""
            )
            if backup_name:
                backup_paths.append((final_pinned, final_pinned.name, backup_name))
            if final_pinned is not pinned:
                backup_paths.append(
                    (pinned, pinned.name, pinned.backup_existing(suffix=".retag-backup"))
                )
            final_pinned.replace(temp_name, final_pinned.name)
            final_pinned.fsync()
            temp_paths.remove((pinned, temp_name))
            created_destinations.append((final_pinned, final_pinned.name))
            if final_pinned is not pinned:
                pinned.fsync()
                if target_plan is not None:
                    target_plan.destination_path = str(canonical_destination)
        for pinned, _destination_name, backup_name in backup_paths:
            try:
                pinned.unlink(backup_name)
                pinned.fsync()
            except OSError:
                logger.warning("retag succeeded but a temporary backup could not be removed")
        if created_destinations:
            scanner_pinned, scanner_name = created_destinations[0]
            try:
                scanner_pinned.notify_changed(scanner_name)
                scanner_pinned.fsync()
            except OSError:
                logger.warning("retag succeeded but the library scanner notification failed")
        _close_pinned_destinations_safely(pinned_destinations)
        return AlbumRetagResult(files_retagged=len(targets), folder=folder, files_renamed=renamed)
    except Exception as exc:
        _rollback_pinned_filesystem(temp_paths, created_destinations, backup_paths)
        _close_pinned_destinations_safely(pinned_destinations)
        if isinstance(exc, ImportExecutionError):
            raise
        raise ImportExecutionError(f"album retag failed: {exc}") from exc


async def plan_release_import(
    db: AsyncSession,
    release: Release,
    *,
    library_root: Path,
    naming_template: str = _DESTINATION_TEMPLATE,
    source_artifacts: dict[int, tuple[Path, str]] | None = None,
    track_ids: set[int] | None = None,
    replace_existing_imports: bool = False,
) -> list[ImportPlan]:
    delete_query = delete(ImportPlan).where(ImportPlan.release_id == release.id)
    if not replace_existing_imports:
        delete_query = delete_query.where(ImportPlan.status != ImportWorkflowState.imported)
    if track_ids is not None:
        if not track_ids:
            return []
        delete_query = delete_query.where(ImportPlan.track_id.in_(track_ids))
    await db.execute(delete_query)
    await db.flush()
    track_query = select(Track).where(Track.release_id == release.id)
    if not replace_existing_imports:
        track_query = track_query.where(Track.import_state != ImportWorkflowState.imported)
    if track_ids is not None:
        track_query = track_query.where(Track.id.in_(track_ids))
    tracks_result = await db.execute(track_query.order_by(Track.track_no, Track.id))
    tracks = list(tracks_result.scalars().all())
    catalog_album_ids = {track.catalog_album_id for track in tracks if track.catalog_album_id}
    if catalog_album_ids:
        albums = {
            album.id: album
            for album in (
                await db.scalars(
                    select(CatalogAlbum)
                    .where(CatalogAlbum.id.in_(catalog_album_ids))
                    .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
                )
            ).all()
        }
        for track in tracks:
            album = albums.get(track.catalog_album_id or 0)
            if album is None:
                continue
            if track.catalog_track_id is None:
                continue
            catalog_track = next(
                (item for item in album.tracks if item.id == track.catalog_track_id), None
            )
            if catalog_track is not None:
                _sync_track_from_catalog(track, album, catalog_track)

    library_root = _resolved_path(library_root)
    plans: list[ImportPlan] = []
    for track in tracks:
        artifact = (
            source_artifacts.get(track.id) if source_artifacts and track.id is not None else None
        )
        source = artifact[0] if artifact else _track_source_path(track)
        status = ImportWorkflowState.ready
        collision = CollisionState.clear
        error: str | None = None
        source_hash: str | None = None
        operations: list[str] = [
            "hash-source",
            "copy-to-destination-filesystem-temp",
            "fsync-temp",
            "tag-and-readback",
            "recheck-destination",
            "atomic-rename",
            "fsync-destination-directory",
            "remove-staged-source-after-commit",
            "remove-completed-provider-entry-after-commit",
        ]

        try:
            _ensure_regular_source(source)
            if not is_importable_audio(source):
                raise ImportPlanningError(
                    f"unsupported audio format '{source.suffix.casefold() or '(none)'}'; "
                    f"supported formats: {supported_audio_formats_display()}"
                )
            source_hash = _sha256(source)
            if artifact is not None and source_hash != artifact[1]:
                raise ImportPlanningError(
                    f"candidate artifact hash does not match track {track.id}"
                )
            relative = render_path(track, template=naming_template)
            destination = library_root / relative
            symlink_parent = _existing_parent_symlink(library_root, destination)
            if symlink_parent is not None:
                raise ImportPlanningError(f"destination parent is a symlink: {symlink_parent}")
            _destination_inside_root(library_root, destination)
            if destination.exists():
                destination_hash = _sha256(destination) if destination.is_file() else None
                status = ImportWorkflowState.needs_review
                if destination_hash == source_hash:
                    collision = CollisionState.duplicate
                    error = "destination already contains same bytes"
                else:
                    collision = CollisionState.conflict
                    error = "destination already exists with different bytes"
            else:
                existing = await db.execute(
                    select(Track).where(Track.content_sha256 == source_hash, Track.id != track.id)
                )
                if existing.scalars().first() is not None:
                    status = ImportWorkflowState.needs_review
                    collision = CollisionState.duplicate
                    error = "same content hash already belongs to another track"
        except (ImportPlanningError, NamingError, OSError) as exc:
            try:
                relative = render_path(track)
            except NamingError:
                relative = f"track-{track.id or 'unknown'}"
            destination = library_root / relative
            status = ImportWorkflowState.needs_review
            collision = CollisionState.needs_review
            error = str(exc)

        track.content_sha256 = source_hash
        track.import_state = status
        plan = ImportPlan(
            release_id=release.id,
            track_id=track.id,
            source_path=str(source),
            staging_path=track.staging_path,
            destination_path=str(destination),
            planned_operations_json=json.dumps(operations),
            collision_state=collision,
            tag_verification_state=TagVerificationState.pending,
            status=status,
            error_detail=error,
        )
        db.add(plan)
        plans.append(plan)

    release.import_state = (
        ImportWorkflowState.ready
        if plans and all(plan.status == ImportWorkflowState.ready for plan in plans)
        else ImportWorkflowState.needs_review
    )
    await db.flush()
    return plans


def _copy_to_temp(source: Path, pinned: PinnedDestination, expected_hash: str) -> tuple[str, Path]:
    source_fd = _open_regular_source_no_follow(source)
    fd, temp_name = pinned.create_temp(suffix=pinned.destination.suffix)
    temp_path = pinned.proc_path(temp_name)
    try:
        with os.fdopen(source_fd, "rb") as src, os.fdopen(fd, "wb") as temp:
            shutil.copyfileobj(src, temp, length=1024 * 1024)
            temp.flush()
            os.fsync(temp.fileno())
        with pinned.open_read(temp_name) as temp_read:
            copied_hash = _sha256_fileobj(temp_read)
        if copied_hash != expected_hash:
            raise ImportExecutionError(
                "short copy or checksum mismatch while staging destination temp"
            )
    except Exception:
        pinned.unlink(temp_name)
        raise
    return temp_name, temp_path


def _close_pinned_destinations(destinations: list[PinnedDestination]) -> None:
    for pinned in reversed(destinations):
        pinned.close()


def _close_pinned_destinations_safely(destinations: list[PinnedDestination]) -> None:
    for pinned in reversed(destinations):
        try:
            pinned.close()
        except OSError:
            logger.warning("failed to close pinned album destination after retag")


async def _reconcile_catalog_ownership(db: AsyncSession, release: Release) -> None:
    album_ids = set(
        (
            await db.scalars(
                select(Track.catalog_album_id).where(
                    Track.release_id == release.id,
                    Track.catalog_album_id.is_not(None),
                )
            )
        ).all()
    )
    for album_id in album_ids:
        expected_ids = set(
            (
                await db.scalars(
                    select(CatalogAlbumTrack.id).where(CatalogAlbumTrack.album_id == album_id)
                )
            ).all()
        )
        imported_rows = (
            await db.execute(
                select(Track.catalog_track_id, ImportPlan.destination_path)
                .join(ImportPlan, ImportPlan.track_id == Track.id)
                .where(
                    Track.catalog_album_id == album_id,
                    Track.import_state == ImportWorkflowState.imported,
                    ImportPlan.status == ImportWorkflowState.imported,
                )
            )
        ).all()
        present_ids = {
            catalog_track_id
            for catalog_track_id, destination_path in imported_rows
            if catalog_track_id is not None
            and destination_path
            and _is_regular_non_symlink(Path(destination_path))
        }
        album = await db.get(CatalogAlbum, album_id)
        if album is not None:
            album.in_library = bool(expected_ids) and expected_ids <= present_ids


def _rollback_pinned_filesystem(
    temp_paths: list[tuple[PinnedDestination, str]],
    created_destinations: list[tuple[PinnedDestination, str]],
    backup_paths: list[tuple[PinnedDestination, str, str]],
) -> None:
    for pinned, temp_name in temp_paths:
        try:
            pinned.unlink(temp_name)
        except OSError:
            logger.exception("failed to remove import temporary file during rollback")
    for pinned, destination_name in reversed(created_destinations):
        try:
            pinned.unlink(destination_name)
        except OSError:
            logger.exception("failed to remove imported destination during rollback")
    for pinned, destination_name, backup_name in reversed(backup_paths):
        if pinned.exists(backup_name):
            try:
                # os.replace overwrites a surviving new destination atomically.
                pinned.replace(backup_name, destination_name)
                pinned.fsync()
            except OSError:
                logger.exception("failed to restore library backup during rollback")


async def execute_release_import(
    db: AsyncSession,
    release: Release,
    *,
    library_root: Path,
    tag_writer: MutagenTagWriter | None = None,
    before_commit: Callable[[Path], None] | None = None,
    replace_existing_verified: bool = False,
    plan_ids: set[int] | None = None,
    quality_profile: QualityProfile | None = None,
) -> list[ImportPlan]:
    tag_writer = tag_writer or MutagenTagWriter()
    if quality_profile is None:
        runtime = await get_runtime_settings(db)
        quality_profile = runtime.quality_profile
    plan_query = (
        select(ImportPlan)
        .join(Track, ImportPlan.track_id == Track.id)
        .where(
            ImportPlan.release_id == release.id,
            ImportPlan.status == ImportWorkflowState.ready,
        )
    )
    if plan_ids is not None:
        if not plan_ids:
            raise ImportExecutionError("release import plans are not ready")
        plan_query = plan_query.where(ImportPlan.id.in_(plan_ids))
    plans_result = await db.execute(plan_query.order_by(ImportPlan.id))
    plans = list(plans_result.scalars().all())
    if not plans:
        raise ImportExecutionError("release import plans are not ready")
    release_id = release.id
    if release_id is None or any(plan.id is None for plan in plans):
        raise ImportExecutionError("release import plans are not persisted")
    execution_statuses = {
        plan.id: ImportWorkflowState.importing for plan in plans if plan.id is not None
    }

    catalog_album_id = next(
        (
            plan.track.catalog_album_id
            for plan in plans
            if plan.track and plan.track.catalog_album_id
        ),
        None,
    )
    artwork = None
    if catalog_album_id is not None:
        catalog_album = await db.get(CatalogAlbum, catalog_album_id)
        if catalog_album is not None:
            artwork = await _fetch_canonical_artwork(catalog_album.artwork_url)

    pinned_destinations: list[PinnedDestination] = []
    created_destinations: list[tuple[PinnedDestination, str]] = []
    temp_paths: list[tuple[PinnedDestination, str]] = []
    backup_paths: list[tuple[PinnedDestination, str, str]] = []
    # Persist any ready-plan work before copying, hashing, and retagging files.
    # Those operations can take seconds per release and must not hold SQLite's
    # single writer lock against interactive actions. The importing states remain
    # part of the final transaction so a failed commit restores the ready state.
    await db.commit()
    release.import_state = ImportWorkflowState.importing
    for plan in plans:
        plan.status = ImportWorkflowState.importing

    try:
        for plan in plans:
            track = plan.track
            if track is None:
                raise ImportExecutionError("import plan is missing a track")
            source = Path(plan.source_path)
            destination = Path(plan.destination_path)
            _destination_inside_root(library_root, destination)
            pinned = PinnedDestination.open(library_root, destination)
            pinned_destinations.append(pinned)
            if pinned.exists() and not replace_existing_verified:
                raise ImportExecutionError("destination exists before import commit")
            expected_hash = track.content_sha256 or _sha256_regular_source_no_follow(source)
            temp_name, temp_path = _copy_to_temp(source, pinned, expected_hash)
            temp_paths.append((pinned, temp_name))
            plan.destination_temp_path = str(pinned.display_path(temp_name))
            if not _write_tags_compatible(
                tag_writer, temp_path, _tags_for(release, track), artwork
            ):
                plan.tag_verification_state = TagVerificationState.failed
                raise ImportExecutionError("tag readback failed")
            plan.tag_verification_state = TagVerificationState.verified
            if before_commit is not None:
                before_commit(destination)
            pinned.verify_attached()
            if replace_existing_verified:
                if not pinned.is_regular_non_symlink():
                    raise ImportExecutionError("upgrade destination changed before atomic rename")
                backup_name = pinned.backup_existing(suffix=".backup")
                backup_paths.append((pinned, pinned.name, backup_name))
            elif pinned.exists():
                raise ImportExecutionError("destination appeared before atomic rename")
            pinned.replace(temp_name, pinned.name)
            pinned.fsync()
            temp_paths.remove((pinned, temp_name))
            created_destinations.append((pinned, pinned.name))
            track.import_state = ImportWorkflowState.imported
            with pinned.open_read(pinned.name) as imported_file:
                track.content_sha256 = _sha256_fileobj(imported_file)
                with contextlib.suppress(OSError):
                    track.file_size_bytes = os.fstat(imported_file.fileno()).st_size
            ext = destination.suffix.lower().lstrip(".")
            if ext and len(ext) <= 16 and ext.isalnum():
                track.file_format = ext
            plan.status = ImportWorkflowState.imported
            if plan.id is not None:
                execution_statuses[plan.id] = ImportWorkflowState.imported
            plan.file_state = LibraryFileState.present
            plan.file_checked_at = datetime.now(UTC)
            plan.file_removed_at = None
            plan.file_removal_reason = None
            plan.collision_state = CollisionState.clear
        imported_tracks = list(
            (
                await db.scalars(
                    select(Track).where(
                        Track.release_id == release.id,
                        Track.import_state == ImportWorkflowState.imported,
                    )
                )
            ).all()
        )
        imported_catalog_ids = {
            track.catalog_track_id
            for track in imported_tracks
            if track.catalog_track_id is not None
        }
        catalog_scoped_import = any(
            track.catalog_album_id is not None for track in imported_tracks
        )
        remaining_tracks = list(
            (
                await db.scalars(
                    select(Track).where(
                        Track.release_id == release.id,
                        Track.import_state != ImportWorkflowState.imported,
                    )
                )
            ).all()
        )
        expected_count = release.track_count or 0
        imported_identity_count = (
            len(imported_catalog_ids) if catalog_scoped_import else len(imported_tracks)
        )
        release_complete = (
            imported_identity_count >= expected_count if expected_count else not remaining_tracks
        )
        if release_complete:
            release.import_state = ImportWorkflowState.imported
            release.error_detail = None
        elif catalog_scoped_import:
            release.import_state = ImportWorkflowState.needs_review
            missing_count = max(expected_count - imported_identity_count, 1)
            release.error_detail = f"{missing_count} catalog track(s) still require review"
        elif any(track.source_path or track.staging_path for track in remaining_tracks):
            release.import_state = ImportWorkflowState.needs_review
        else:
            release.import_state = ImportWorkflowState.discovered
        album_ids_for_quality = {
            track.catalog_album_id
            for track in imported_tracks
            if track.catalog_album_id is not None
        }
        for album_id in album_ids_for_quality:
            await reconcile_album_quality_duplicates(
                db,
                album_id,
                library_root=library_root,
                quality_profile=quality_profile,
                defer_filesystem_delete=True,
            )
        await _reconcile_catalog_ownership(db, release)
        await db.flush()

        cleanup_items = tuple(
            ImportedSourceCleanup(
                plan.id,
                Path(plan.staging_path or plan.source_path),
                plan.track.acquisition_provenance_json if plan.track else None,
                plan.track.source_job_id if plan.track else None,
                plan.track_id,
                session_factory=async_sessionmaker(db.bind, expire_on_commit=False)
                if db.bind is not None
                else None,
            )
            for plan in plans
        )
        committed_destinations = tuple(created_destinations)
        committed_backups = tuple(backup_paths)
        pending_temps = tuple(temp_paths)
        committed_handles = tuple(pinned_destinations)

        def finalize_filesystem_commit() -> None:
            try:
                for pinned, _destination_name, backup_name in committed_backups:
                    try:
                        _unlink_backup_after_commit(pinned.proc_path(backup_name))
                    except OSError:
                        continue
            finally:
                _close_pinned_destinations(list(committed_handles))
            schedule_imported_source_cleanup(cleanup_items)

        def rollback_filesystem_commit() -> None:
            try:
                _rollback_pinned_filesystem(
                    list(pending_temps),
                    list(committed_destinations),
                    list(committed_backups),
                )
            finally:
                _close_pinned_destinations(list(committed_handles))

        register_transaction_callbacks(
            db,
            after_commit=finalize_filesystem_commit,
            after_rollback=rollback_filesystem_commit,
        )
        return plans
    except Exception as exc:
        detail = str(exc)
        try:
            _rollback_pinned_filesystem(temp_paths, created_destinations, backup_paths)
        finally:
            _close_pinned_destinations(pinned_destinations)

        # A failed flush leaves SQLAlchemy's transaction rollback-only. Never
        # inspect expired ORM attributes until that failed transaction is cleared;
        # doing so raises PendingRollbackError and masks the original failure.
        await db.rollback()

        async def record_failure_state() -> None:
            failed_release = await db.get(Release, release_id)
            failed_plans = list(
                (
                    await db.scalars(
                        select(ImportPlan)
                        .where(ImportPlan.id.in_(execution_statuses))
                        .options(selectinload(ImportPlan.track))
                    )
                ).all()
            )
            if failed_release is not None:
                failed_release.import_state = ImportWorkflowState.rolled_back
                failed_release.rollback_detail = detail
            for failed_plan in failed_plans:
                prior_status = execution_statuses.get(failed_plan.id)
                if prior_status == ImportWorkflowState.imported:
                    failed_plan.status = ImportWorkflowState.rolled_back
                    failed_plan.rollback_detail = detail
                    if failed_plan.track is not None:
                        failed_plan.track.import_state = ImportWorkflowState.rolled_back
                elif prior_status == ImportWorkflowState.importing:
                    failed_plan.status = ImportWorkflowState.failed
                    failed_plan.error_detail = detail
                failed_plan.destination_temp_path = None
            await db.flush()

        await run_with_sqlite_lock_retry(db, record_failure_state)
        if isinstance(exc, ImportExecutionError):
            raise
        raise ImportExecutionError(detail) from exc
