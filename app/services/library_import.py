from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import shutil
import stat
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, no_type_check

from mutagen.flac import FLAC
from mutagen.id3 import ID3, TALB, TDRC, TIT2, TPE1, TPE2, TPOS, TRCK, TXXX, ID3NoHeaderError
from mutagen.mp4 import MP4
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import register_transaction_callbacks
from app.media_formats import is_importable_audio, supported_audio_formats_display
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.models.import_plan import CollisionState, ImportPlan, TagVerificationState
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.naming.convention import NamingError, render_path
from app.services.acquisition_cleanup import (
    ImportedSourceCleanup,
    schedule_imported_source_cleanup,
)
from app.services.pinned_destination import PinnedDestination

logger = logging.getLogger(__name__)

_DESTINATION_TEMPLATE = "{album_artist}/{album} ({year})/{disc_track} - {title}.{ext}"

_MANAGED_TAG_KEYS = frozenset(
    {
        "title",
        "artist",
        "album",
        "album_artist",
        "albumartist",
        "albumartists",
        "date",
        "tracknumber",
        "discnumber",
        "musicbrainz_trackid",
        "musicbrainz_albumid",
        "musicbrainz_albumartistid",
        "musicbrainz_releasegroupid",
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


def _tags_for(release: Release, track: Track) -> dict[str, str]:
    tags = {
        "title": track.title or "",
        "artist": track.artist or "",
        "album": track.album or release.title or "",
        "album_artist": track.album_artist or release.album_artist or track.artist or "",
        "date": track.year or release.year or "",
        "tracknumber": str(track.track_no or ""),
        "discnumber": str(track.disc or ""),
        "musicbrainz_trackid": track.mbid or "",
        "musicbrainz_albumid": release.release_mbid or "",
    }
    return {key: value for key, value in tags.items() if value}


class MutagenTagWriter:
    @no_type_check
    def write_and_verify(self, path: Path, tags: dict[str, str]) -> bool:
        suffix = path.suffix.casefold()
        if suffix == ".mp3":
            try:
                id3 = ID3(path)
            except ID3NoHeaderError:
                id3 = ID3()
            for frame_id in ("TIT2", "TPE1", "TALB", "TPE2", "TDRC", "TRCK", "TPOS"):
                id3.delall(frame_id)
            for frame in list(id3.getall("TXXX")):
                if frame.desc.casefold() in {
                    "musicbrainz track id",
                    "musicbrainz album id",
                    "musicbrainz album artist id",
                    "musicbrainz release group id",
                }:
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
            if tracknumber := tags.get("tracknumber"):
                id3.add(TRCK(encoding=3, text=tracknumber))
            if discnumber := tags.get("discnumber"):
                id3.add(TPOS(encoding=3, text=discnumber))
            for key, description in {
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
            flac.save()
        elif suffix in {".ogg", ".oga", ".opus"}:
            ogg = OggOpus(path) if suffix == ".opus" else OggVorbis(path)
            for key in _MANAGED_TAG_KEYS:
                if key in ogg:
                    del ogg[key]
            for key, value in tags.items():
                ogg[key] = value
            if album_artist := tags.get("album_artist"):
                ogg["albumartist"] = album_artist
                ogg["albumartists"] = album_artist
            ogg.save()
        elif suffix in {".m4a", ".mp4"}:
            mp4 = MP4(path)
            text_atoms = {
                "title": "\xa9nam",
                "artist": "\xa9ART",
                "album": "\xa9alb",
                "album_artist": "aART",
                "date": "\xa9day",
            }
            freeform_atoms = {
                "musicbrainz_trackid": "----:com.apple.iTunes:MusicBrainz Track Id",
                "musicbrainz_albumid": "----:com.apple.iTunes:MusicBrainz Album Id",
                "musicbrainz_albumartistid": "----:com.apple.iTunes:MusicBrainz Album Artist Id",
                "musicbrainz_releasegroupid": "----:com.apple.iTunes:MusicBrainz Release Group Id",
            }
            for atom in (*text_atoms.values(), "trkn", "disk", *freeform_atoms.values()):
                if atom in mp4:
                    del mp4[atom]
            for key, atom in text_atoms.items():
                if value := tags.get(key):
                    mp4[atom] = [value]
            if value := tags.get("tracknumber"):
                mp4["trkn"] = [(int(value), 0)]
            if value := tags.get("discnumber"):
                mp4["disk"] = [(int(value), 0)]
            for key, atom in freeform_atoms.items():
                if value := tags.get(key):
                    mp4[atom] = [value.encode()]
            mp4.save()
        else:
            return False
        readback = self.read_tags(path)
        return all(readback.get(key) == value for key, value in tags.items())

    @no_type_check
    def read_tags(self, path: Path) -> dict[str, str]:
        suffix = path.suffix.casefold()
        comment_keys = (
            "title",
            "artist",
            "album",
            "album_artist",
            "date",
            "tracknumber",
            "discnumber",
            "musicbrainz_trackid",
            "musicbrainz_albumid",
            "musicbrainz_albumartistid",
            "musicbrainz_releasegroupid",
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
            }
            for key, atom in text_atoms.items():
                if atom_values := mp4.get(atom):
                    mp4_values[key] = str(atom_values[0])
            if track_values := mp4.get("trkn"):
                mp4_values["tracknumber"] = str(track_values[0][0])
            if disc_values := mp4.get("disk"):
                mp4_values["discnumber"] = str(disc_values[0][0])
            for key, atom in {
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
            "tracknumber": "TRCK",
            "discnumber": "TPOS",
        }
        for key, frame_id in frame_map.items():
            frame = id3.get(frame_id)
            if frame is not None and getattr(frame, "text", None):
                values[key] = str(frame.text[0])
        descriptions = {
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


def _catalog_tags(
    album: CatalogAlbum, catalog_track: CatalogAlbumTrack, track: Track
) -> dict[str, str]:
    values = {
        "title": catalog_track.title,
        "artist": track.artist or album.artist.name,
        "album": album.title,
        "album_artist": album.artist.name,
        "date": album.year or "",
        "tracknumber": str(catalog_track.position),
        "discnumber": str(catalog_track.disc),
        "musicbrainz_trackid": catalog_track.recording_mbid or track.mbid or "",
        "musicbrainz_releasegroupid": album.mbid or "",
        "musicbrainz_albumartistid": album.artist.mbid or "",
    }
    if not values["title"] or not values["artist"] or not values["album_artist"]:
        raise ImportExecutionError("stored track metadata is incomplete")
    if catalog_track.position < 1 or catalog_track.disc < 1:
        raise ImportExecutionError("stored track numbering is invalid")
    return {key: value for key, value in values.items() if value}


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
                    and_(
                        Track.catalog_album_id.is_(None),
                        func.lower(Track.album) == album.title.casefold(),
                        func.lower(
                            func.coalesce(func.nullif(Track.album_artist, ""), Track.artist)
                        )
                        == album.artist.name.casefold(),
                    ),
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
        latest[track.id] = (track, plan)
    if not latest:
        raise ImportExecutionError("album has no imported files to retag")

    return await asyncio.to_thread(
        _retag_catalog_album_files,
        album,
        list(latest.values()),
        library_root=library_root,
        tag_writer=tag_writer,
    )


def _retag_catalog_album_files(
    album: CatalogAlbum,
    imported: list[tuple[Track, ImportPlan]],
    *,
    library_root: Path,
    tag_writer: MutagenTagWriter | None,
) -> AlbumRetagResult:
    catalog_tracks = {item.id: item for item in album.tracks}
    catalog_tracks_by_position = {(item.disc, item.position): item for item in album.tracks}
    root = library_root.resolve()
    targets: list[tuple[Path, Track, CatalogAlbumTrack]] = []
    folders: set[Path] = set()
    mapped_destinations: set[Path] = set()
    for track, plan in imported:
        catalog_track = catalog_tracks.get(track.catalog_track_id or 0)
        if catalog_track is None:
            catalog_track = catalog_tracks_by_position.get((track.disc or 1, track.track_no or 0))
        if catalog_track is None:
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
        if resolved_destination in mapped_destinations:
            raise ImportExecutionError("duplicate destination mapping in stored import metadata")
        mapped_destinations.add(resolved_destination)
        targets.append((destination, track, catalog_track))
        folders.add(destination.parent.resolve())
    if len(folders) != 1:
        raise ImportExecutionError("imported album files do not share one album folder")
    folder = next(iter(folders))
    actual_audio = {
        item.resolve()
        for item in folder.iterdir()
        if item.is_file() and not item.is_symlink() and is_importable_audio(item)
    }
    tracked_audio = {path.resolve() for path, _track, _catalog_track in targets}
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
        for destination, track, catalog_track in targets:
            pinned = PinnedDestination.open(root, destination)
            pinned_destinations.append(pinned)
            if not pinned.is_regular_non_symlink():
                raise ImportExecutionError("album file changed before retag preparation")
            expected_hash = _sha256_regular_source_no_follow(destination)
            temp_name, temp_path = _copy_to_temp(destination, pinned, expected_hash)
            temp_paths.append((pinned, temp_name))
            if not writer.write_and_verify(temp_path, _catalog_tags(album, catalog_track, track)):
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

        for pinned, temp_name, expected_hash in prepared:
            pinned.verify_attached()
            if not pinned.is_regular_non_symlink():
                raise ImportExecutionError("album file changed before retag commit")
            with pinned.open_read(pinned.name) as current_file:
                if _sha256_fileobj(current_file) != expected_hash:
                    raise ImportExecutionError("album file changed before retag commit")
            backup_name = pinned.backup_existing(suffix=".retag-backup")
            backup_paths.append((pinned, pinned.name, backup_name))
            pinned.replace(temp_name, pinned.name)
            pinned.fsync()
            temp_paths.remove((pinned, temp_name))
            created_destinations.append((pinned, pinned.name))
        for pinned, _destination_name, backup_name in backup_paths:
            try:
                pinned.unlink(backup_name)
                pinned.fsync()
            except OSError:
                logger.warning("retag succeeded but a temporary backup could not be removed")
        _close_pinned_destinations_safely(pinned_destinations)
        return AlbumRetagResult(files_retagged=len(targets), folder=folder)
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
) -> list[ImportPlan]:
    tag_writer = tag_writer or MutagenTagWriter()
    plan_query = select(ImportPlan).where(
        ImportPlan.release_id == release.id,
        ImportPlan.status == ImportWorkflowState.ready,
    )
    if plan_ids is not None:
        if not plan_ids:
            raise ImportExecutionError("release import plans are not ready")
        plan_query = plan_query.where(ImportPlan.id.in_(plan_ids))
    plans_result = await db.execute(plan_query.order_by(ImportPlan.id))
    plans = list(plans_result.scalars().all())
    if not plans:
        raise ImportExecutionError("release import plans are not ready")

    pinned_destinations: list[PinnedDestination] = []
    created_destinations: list[tuple[PinnedDestination, str]] = []
    temp_paths: list[tuple[PinnedDestination, str]] = []
    backup_paths: list[tuple[PinnedDestination, str, str]] = []
    release.import_state = ImportWorkflowState.importing
    for plan in plans:
        plan.status = ImportWorkflowState.importing
    await db.flush()

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
            if not tag_writer.write_and_verify(temp_path, _tags_for(release, track)):
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
        imported_identity_count = len(imported_catalog_ids) or len(imported_tracks)
        release_complete = (
            imported_identity_count >= expected_count if expected_count else not remaining_tracks
        )
        if release_complete:
            release.import_state = ImportWorkflowState.imported
        elif any(track.source_path or track.staging_path for track in remaining_tracks):
            release.import_state = ImportWorkflowState.needs_review
        else:
            release.import_state = ImportWorkflowState.discovered
        await _reconcile_catalog_ownership(db, release)
        await db.flush()

        cleanup_items = tuple(
            ImportedSourceCleanup(
                plan.id,
                Path(plan.staging_path or plan.source_path),
                plan.track.acquisition_provenance_json if plan.track else None,
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
        try:
            _rollback_pinned_filesystem(temp_paths, created_destinations, backup_paths)
        finally:
            _close_pinned_destinations(pinned_destinations)
        detail = str(exc)
        release.import_state = ImportWorkflowState.rolled_back
        release.rollback_detail = detail
        for plan in plans:
            if plan.status == ImportWorkflowState.imported:
                plan.status = ImportWorkflowState.rolled_back
                plan.rollback_detail = detail
            elif plan.status == ImportWorkflowState.importing:
                plan.status = ImportWorkflowState.failed
                plan.error_detail = detail
            plan.destination_temp_path = None
        for plan in plans:
            if plan.track is not None and plan.track.import_state == ImportWorkflowState.imported:
                plan.track.import_state = ImportWorkflowState.rolled_back
        await db.flush()
        if isinstance(exc, ImportExecutionError):
            raise
        raise ImportExecutionError(detail) from exc
