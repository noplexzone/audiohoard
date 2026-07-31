from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import stat
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from uuid import uuid4
from weakref import WeakValueDictionary

from fastapi import Request, Response
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.track import Track
from app.models.workflow import ImportWorkflowState

_MIME_TYPES: Final[dict[str, str]] = {
    ".aac": "audio/aac",
    ".flac": "audio/flac",
    ".m4a": "audio/mp4",
    ".mp3": "audio/mpeg",
    ".mp4": "audio/mp4",
    ".oga": "audio/ogg",
    ".ogg": "audio/ogg",
    ".opus": "audio/ogg; codecs=opus",
    ".wav": "audio/wav",
}
_CACHE_PROFILE = "mp3-libmp3lame-q4-stereo-v1"
_CACHE_MAX_ITEMS = 128
_CACHE_MAX_BYTES = 512 * 1024 * 1024
_TRANSCODE_MAX_OUTPUT_BYTES = 64 * 1024 * 1024
_TRANSCODE_MAX_DURATION_SECONDS = 30 * 60
_TRANSCODE_TIMEOUT_SECONDS = 180.0
_TRANSCODE_SEMAPHORE = asyncio.Semaphore(2)
_TRANSCODE_LOCKS: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()


class MediaAssetError(Exception):
    """An imported media asset could not be opened safely."""


class TranscodeError(Exception):
    """A bounded browser transcode could not be produced."""


class RangeNotSatisfiable(Exception):
    """The request did not contain one satisfiable byte range."""


@dataclass(frozen=True)
class ByteRange:
    start: int
    end: int

    def __eq__(self, other: object) -> bool:
        if isinstance(other, tuple):
            return (self.start, self.end) == other
        if isinstance(other, ByteRange):
            return (self.start, self.end) == (other.start, other.end)
        return NotImplemented


@dataclass
class OpenMediaAsset:
    fd: int
    size: int
    mtime_ns: int
    device: int
    inode: int
    content_type: str
    etag: str
    _closed: bool = False

    def close(self) -> None:
        if not self._closed:
            os.close(self.fd)
            self._closed = True

    async def iter_bytes(
        self, start: int, end: int, *, chunk_size: int = 64 * 1024
    ) -> AsyncIterator[bytes]:
        offset = start
        try:
            while offset <= end:
                data = await asyncio.to_thread(
                    os.pread, self.fd, min(chunk_size, end - offset + 1), offset
                )
                if not data:
                    break
                offset += len(data)
                yield data
        finally:
            self.close()


def _opaque_etag(metadata: os.stat_result, profile: str = "source") -> str:
    value = (
        f"{metadata.st_dev}:{metadata.st_ino}:{metadata.st_size}:{metadata.st_mtime_ns}:{profile}"
    )
    return f'"{hashlib.sha256(value.encode()).hexdigest()}"'


def _asset_from_fd(fd: int, content_type: str) -> OpenMediaAsset:
    metadata = os.fstat(fd)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size <= 0:
        raise MediaAssetError("media asset is unavailable")
    return OpenMediaAsset(
        fd=fd,
        size=metadata.st_size,
        mtime_ns=metadata.st_mtime_ns,
        device=metadata.st_dev,
        inode=metadata.st_ino,
        content_type=content_type,
        etag=_opaque_etag(metadata),
    )


def open_path_beneath_root(path: Path, root: Path) -> OpenMediaAsset:
    """Open an audio file beneath root without following any symlink.

    Every component is opened relative to the already-open parent descriptor. The
    returned descriptor is the same descriptor later used for fstat and reads.
    """
    if not path.is_absolute() or not root.is_absolute() or ".." in path.parts:
        raise MediaAssetError("media asset is unavailable")
    try:
        relative = path.relative_to(root)
    except ValueError:
        raise MediaAssetError("media asset is unavailable") from None
    if not relative.parts:
        raise MediaAssetError("media asset is unavailable")
    suffix = path.suffix.casefold()
    content_type = _MIME_TYPES.get(suffix)
    if content_type is None:
        raise MediaAssetError("media asset is unavailable")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    current_fd = -1
    try:
        current_fd = os.open(root, directory_flags)
        for component in relative.parts[:-1]:
            next_fd = os.open(component, directory_flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        file_fd = os.open(relative.parts[-1], file_flags, dir_fd=current_fd)
        try:
            return _asset_from_fd(file_fd, content_type)
        except BaseException:
            os.close(file_fd)
            raise
    except (OSError, ValueError) as exc:
        raise MediaAssetError("media asset is unavailable") from exc
    finally:
        if current_fd >= 0:
            os.close(current_fd)


def _open_cached_mp3(path: Path) -> OpenMediaAsset:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            asset = _asset_from_fd(fd, "audio/mpeg")
            asset.etag = _opaque_etag(os.fstat(fd), _CACHE_PROFILE)
            return asset
        except BaseException:
            os.close(fd)
            raise
    except (OSError, MediaAssetError) as exc:
        raise MediaAssetError("media asset is unavailable") from exc


async def open_imported_track(
    db: AsyncSession, track_id: int, *, library_root: Path
) -> OpenMediaAsset:
    destinations = await db.scalars(
        select(ImportPlan.destination_path)
        .join(Track, ImportPlan.track_id == Track.id)
        .where(
            Track.id == track_id,
            Track.import_state == ImportWorkflowState.imported,
            ImportPlan.status == ImportWorkflowState.imported,
            ImportPlan.file_state == LibraryFileState.present,
        )
        .order_by(ImportPlan.id.desc())
    )
    for destination in destinations:
        try:
            return await asyncio.to_thread(open_path_beneath_root, Path(destination), library_root)
        except MediaAssetError:
            continue
    raise MediaAssetError("media asset is unavailable")


def parse_single_byte_range(value: str | None, size: int) -> ByteRange | None:
    if value is None:
        return None
    if not value.startswith("bytes=") or "," in value:
        raise RangeNotSatisfiable
    spec = value[6:]
    if spec.count("-") != 1:
        raise RangeNotSatisfiable
    start_text, end_text = spec.split("-", 1)
    if not start_text and not end_text:
        raise RangeNotSatisfiable
    if (start_text and not start_text.isdigit()) or (end_text and not end_text.isdigit()):
        raise RangeNotSatisfiable
    if size <= 0:
        raise RangeNotSatisfiable

    if not start_text:
        suffix = int(end_text)
        if suffix <= 0:
            raise RangeNotSatisfiable
        return ByteRange(max(0, size - suffix), size - 1)

    start = int(start_text)
    if start >= size:
        raise RangeNotSatisfiable
    if not end_text:
        return ByteRange(start, size - 1)
    end = int(end_text)
    if end < start:
        raise RangeNotSatisfiable
    return ByteRange(start, min(end, size - 1))


def _response_headers(asset: OpenMediaAsset, content_length: int) -> dict[str, str]:
    return {
        "Accept-Ranges": "bytes",
        "Cache-Control": "private, no-cache",
        "Content-Length": str(content_length),
        "Content-Type": asset.content_type,
        "ETag": asset.etag,
        "X-Content-Type-Options": "nosniff",
    }


def media_response(request: Request, asset: OpenMediaAsset) -> Response:
    try:
        requested = parse_single_byte_range(request.headers.get("range"), asset.size)
    except RangeNotSatisfiable:
        asset.close()
        return Response(
            status_code=416,
            headers={
                "Accept-Ranges": "bytes",
                "Cache-Control": "private, no-cache",
                "Content-Length": "0",
                "Content-Range": f"bytes */{asset.size}",
                "X-Content-Type-Options": "nosniff",
            },
        )

    byte_range = requested or ByteRange(0, asset.size - 1)
    length = byte_range.end - byte_range.start + 1
    headers = _response_headers(asset, length)
    status_code = 200
    if requested is not None:
        status_code = 206
        headers["Content-Range"] = f"bytes {byte_range.start}-{byte_range.end}/{asset.size}"
    if request.method == "HEAD":
        asset.close()
        return Response(status_code=status_code, headers=headers)
    return StreamingResponse(
        asset.iter_bytes(byte_range.start, byte_range.end),
        status_code=status_code,
        headers=headers,
    )


def _cache_key(track_id: int, asset: OpenMediaAsset) -> str:
    identity = (
        f"{track_id}:{asset.device}:{asset.inode}:{asset.size}:{asset.mtime_ns}:{_CACHE_PROFILE}"
    )
    return hashlib.sha256(identity.encode()).hexdigest()


async def _run_ffmpeg(source_fd: int, destination: Path) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        raise TranscodeError("browser transcode is unavailable")
    process = await asyncio.create_subprocess_exec(
        ffmpeg,
        "-nostdin",
        "-v",
        "error",
        "-y",
        "-i",
        f"/proc/self/fd/{source_fd}",
        "-map",
        "0:a:0",
        "-vn",
        "-t",
        str(_TRANSCODE_MAX_DURATION_SECONDS),
        "-ac",
        "2",
        "-c:a",
        "libmp3lame",
        "-q:a",
        "4",
        str(destination),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        pass_fds=(source_fd,),
    )
    deadline = time.monotonic() + _TRANSCODE_TIMEOUT_SECONDS
    try:
        while process.returncode is None:
            if time.monotonic() >= deadline:
                raise TranscodeError("browser transcode timed out")
            try:
                output_size = await asyncio.to_thread(lambda: destination.stat().st_size)
                if output_size > _TRANSCODE_MAX_OUTPUT_BYTES:
                    raise TranscodeError("browser transcode exceeded its output limit")
            except FileNotFoundError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=0.05)
            except TimeoutError:
                continue
        if process.returncode != 0:
            raise TranscodeError("browser transcode failed")
    except BaseException:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise


def _cleanup_cache(cache_root: Path) -> None:
    entries: list[tuple[int, int, Path]] = []
    for path in cache_root.glob("*.mp3"):
        try:
            metadata = path.stat(follow_symlinks=False)
        except OSError:
            continue
        if stat.S_ISREG(metadata.st_mode):
            entries.append((metadata.st_mtime_ns, metadata.st_size, path))
    entries.sort(reverse=True)
    retained_bytes = 0
    for index, (_, size, path) in enumerate(entries):
        if index >= _CACHE_MAX_ITEMS or retained_bytes + size > _CACHE_MAX_BYTES:
            path.unlink(missing_ok=True)
        else:
            retained_bytes += size


def _track_cache_entries(cache_root: Path, track_id: int) -> list[Path]:
    return list(cache_root.glob(f"{track_id}-*.mp3"))


async def open_or_create_mp3_preview(
    asset: OpenMediaAsset, *, track_id: int, cache_root: Path
) -> OpenMediaAsset:
    key = _cache_key(track_id, asset)
    destination = cache_root / f"{track_id}-{key}.mp3"
    lock = _TRANSCODE_LOCKS.setdefault(key, asyncio.Lock())
    try:
        async with lock:
            await asyncio.to_thread(cache_root.mkdir, parents=True, exist_ok=True)
            try:
                cached = await asyncio.to_thread(_open_cached_mp3, destination)
            except MediaAssetError:
                temporary = cache_root / f".{destination.name}.{uuid4().hex}.tmp.mp3"
                try:
                    async with _TRANSCODE_SEMAPHORE:
                        await _run_ffmpeg(asset.fd, temporary)
                    try:
                        output_size = (await asyncio.to_thread(temporary.stat)).st_size
                    except OSError as exc:
                        raise TranscodeError("browser transcode failed") from exc
                    if not 0 < output_size <= _TRANSCODE_MAX_OUTPUT_BYTES:
                        raise TranscodeError("browser transcode output was invalid")
                    await asyncio.to_thread(os.replace, temporary, destination)
                    cached = await asyncio.to_thread(_open_cached_mp3, destination)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    raise TranscodeError("browser transcode is unavailable") from exc
                finally:
                    cleanup = asyncio.create_task(
                        asyncio.to_thread(temporary.unlink, missing_ok=True)
                    )
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        await cleanup
                        raise
            stale_entries = await asyncio.to_thread(_track_cache_entries, cache_root, track_id)
            for stale in stale_entries:
                if stale != destination:
                    await asyncio.to_thread(stale.unlink, missing_ok=True)
            await asyncio.to_thread(_cleanup_cache, cache_root)
            return cached
    finally:
        asset.close()
