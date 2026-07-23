from __future__ import annotations

import asyncio
import base64
import json
import math
import os
import shutil
import signal
import stat
import sys
import time
import uuid
from contextlib import suppress
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from urllib.parse import urlparse

import mutagen

from app.media_formats import IMPORTABLE_AUDIO_EXTENSIONS
from app.schemas.search import SearchRequest, SearchResult
from app.sources.base import CapabilityState
from app.sources.youtube import AcquiredMedia, ProviderError, _open_pinned_directory

_DEFAULT_TIMEOUT_SEC = 300.0
_PROCESS_STOP_GRACE_SEC = 2.0
_MAX_OUTPUT_BYTES = 64 * 1024
_SUPPORTED_AUDIO_EXTENSIONS = IMPORTABLE_AUDIO_EXTENSIONS
_QUALITIES = frozenset({"Normal", "High", "HiFi", "Master"})


def _tidal_dl_version() -> str:
    try:
        return version("tidal-dl")
    except PackageNotFoundError:
        return ""


def _tidal_dl_executable() -> str:
    return shutil.which("tidal-dl") or ""


def _is_supported_audio(path: Path) -> bool:
    try:
        media = mutagen.File(path)
    except (mutagen.MutagenError, OSError):
        return False
    return media is not None and media.info is not None


def _profile_status(config_value: str, session_value: str) -> tuple[bool, str, str, Path | None]:
    if not config_value or not session_value:
        return False, "profile_unconfigured", "TIDAL profile and session paths are required", None
    config = Path(config_value).absolute()
    session = Path(session_value).absolute()
    if (
        config.name != ".tidal-dl.json"
        or session.name != ".tidal-dl.token.json"
        or config.parent != session.parent
    ):
        return (
            False,
            "unsafe_profile_layout",
            "TIDAL profile paths must use the required layout",
            None,
        )
    for path in (config, session):
        try:
            current = Path(path.anchor)
            for component in path.parts[1:]:
                current /= component
                mode = current.lstat().st_mode
                if stat.S_ISLNK(mode):
                    raise OSError
            if not stat.S_ISREG(path.stat().st_mode) or not os.access(path, os.R_OK):
                raise OSError
        except OSError:
            return (
                False,
                "unsafe_profile_layout",
                "TIDAL profile files are missing or unsafe",
                None,
            )
    try:
        profile = json.loads(config.read_text(encoding="utf-8"))
        if not isinstance(profile, dict):
            raise ValueError
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
        return False, "profile_invalid", "TIDAL profile file is invalid", None
    try:
        raw_token = session.read_bytes()
        decoded = base64.b64decode(raw_token, validate=True).decode("utf-8")
        token = json.loads(decoded)
        required = ("userid", "countryCode", "accessToken", "refreshToken", "expiresAfter")
        if not isinstance(token, dict) or any(not token.get(key) for key in required):
            raise ValueError
        expires_after = float(token["expiresAfter"])
        if not math.isfinite(expires_after):
            raise ValueError
        if expires_after <= time.time():
            return False, "session_expired", "TIDAL session has expired; authenticate again", None
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
        return False, "session_invalid", "TIDAL session file is invalid", None
    return True, "ok", "TIDAL local profile is ready; authentication is unprobed", config.parent


def _validated_track_url(value: str, operation: str) -> tuple[str, str]:
    parsed = urlparse(value)
    parts = parsed.path.split("/")
    valid_path = (
        parsed.hostname == "tidal.com"
        and len(parts) == 5
        and parts[1:4] == ["browse", "track", parts[3]]
    ) or (parsed.hostname == "listen.tidal.com" and len(parts) == 3 and parts[1] == "track")
    # The first branch above is expressed explicitly below to avoid accepting extra segments.
    if parsed.hostname == "tidal.com":
        valid_path = len(parts) == 4 and parts[1:3] == ["browse", "track"]
    elif parsed.hostname == "listen.tidal.com":
        valid_path = len(parts) == 3 and parts[1] == "track"
    else:
        valid_path = False
    track_id = parts[-1] if valid_path else ""
    if (
        parsed.scheme != "https"
        or not valid_path
        or not track_id.isascii()
        or not track_id.isdigit()
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or bool(parsed.query)
        or bool(parsed.fragment)
        or bool(parsed.params)
    ):
        raise ProviderError("invalid_url", "A direct HTTPS TIDAL track URL is required", operation)
    return value, track_id


def _classify_failure(output: bytes) -> ProviderError:
    text = output[:_MAX_OUTPUT_BYTES].decode("utf-8", errors="replace").casefold()
    if (
        "login" in text
        or "log in" in text
        or "authentication" in text
        or "token" in text
        and "expired" in text
    ):
        return ProviderError("auth_required", "TIDAL authentication is required", "acquire")
    if "429" in text or "rate limit" in text or "too many requests" in text:
        return ProviderError(
            "rate_limited", "TIDAL temporarily rate limited the request", "acquire", True
        )
    if "quality" in text and ("not available" in text or "unavailable" in text):
        return ProviderError(
            "format_unavailable", "Requested TIDAL quality is unavailable", "acquire"
        )
    return ProviderError("downloader_error", "tidal-dl failed", "acquire", True)


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.communicate(), _PROCESS_STOP_GRACE_SEC)
    except TimeoutError:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.communicate()


async def _drain_bounded(reader: asyncio.StreamReader) -> bytes:
    kept = bytearray()
    while chunk := await reader.read(8192):
        if len(kept) < _MAX_OUTPUT_BYTES:
            kept.extend(chunk[: _MAX_OUTPUT_BYTES - len(kept)])
    return bytes(kept)


async def _communicate_bounded(process: asyncio.subprocess.Process) -> tuple[bytes, bytes]:
    if not isinstance(process.stdout, asyncio.StreamReader) or not isinstance(
        process.stderr, asyncio.StreamReader
    ):
        return await process.communicate()
    stdout_task = asyncio.create_task(_drain_bounded(process.stdout))
    stderr_task = asyncio.create_task(_drain_bounded(process.stderr))
    tasks = (stdout_task, stderr_task)
    try:
        await process.wait()
        stdout, stderr = await asyncio.gather(*tasks)
        return stdout, stderr
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


def _snapshot(root: Path) -> set[Path]:
    found: set[Path] = set()
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        base = Path(directory)
        for name in [*dirnames, *filenames]:
            item = base / name
            if item.is_symlink():
                raise ProviderError(
                    "artifact_invalid", "tidal-dl produced an unsafe artifact", "acquire"
                )
            resolved = item.resolve()
            if not resolved.is_relative_to(root.resolve()):
                raise ProviderError(
                    "artifact_invalid", "tidal-dl produced an unsafe artifact", "acquire"
                )
            found.add(item.relative_to(root))
    return found


def _copy_profile_files(source_home: Path, isolated_home: Path) -> None:
    source_fd = os.open(source_home, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for name in (".tidal-dl.json", ".tidal-dl.token.json"):
            source_file = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=source_fd)
            try:
                destination = isolated_home / name
                destination_fd = os.open(
                    destination,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                )
                try:
                    while chunk := os.read(source_file, 64 * 1024):
                        os.write(destination_fd, chunk)
                finally:
                    os.close(destination_fd)
            finally:
                os.close(source_file)
    finally:
        os.close(source_fd)


class TidalAdapter:
    name = "tidal"

    def __init__(
        self,
        config_path: str = "",
        session_path: str = "",
        quality: str = "",
        timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
    ) -> None:
        self._config_path = config_path
        self._session_path = session_path
        self._quality = quality or "Normal"
        self._timeout_sec = timeout_sec

    async def local_health(self) -> CapabilityState:
        """Return local binary/profile/session readiness without network access."""
        installed = _tidal_dl_version()
        if not installed or not _tidal_dl_executable():
            return CapabilityState(
                False,
                "tidal-dl is not installed",
                {"code": "tidal_dl_missing", "profile": "not_probed", "auth": "not_probed"},
            )
        valid, code, reason, _home = _profile_status(self._config_path, self._session_path)
        details: dict[str, object] = {
            "code": code,
            "version": installed,
            "profile": "configured" if valid else "invalid",
            "auth": "unprobed",
            "quality": self._quality,
        }
        return CapabilityState(valid, None if valid else reason, details)

    async def health(self) -> CapabilityState:
        return await self.local_health()

    async def search(self, query: SearchRequest) -> list[SearchResult]:
        url, track_id = _validated_track_url(query.query, "search")
        return [
            SearchResult(
                source="tidal",
                title=f"TIDAL track {track_id}",
                url=url,
                metadata={"track_id": track_id, "direct_url": True},
            )
        ]

    async def acquire(self, url: str, staging_root: Path) -> AcquiredMedia:
        canonical_url, track_id = _validated_track_url(url, "acquire")
        installed = _tidal_dl_version()
        executable = _tidal_dl_executable()
        if not installed or not executable:
            raise ProviderError("tidal_dl_missing", "tidal-dl is not installed", "acquire")
        valid, code, reason, home = _profile_status(self._config_path, self._session_path)
        if not valid or home is None:
            raise ProviderError(code, reason, "acquire")
        if self._quality not in _QUALITIES:
            raise ProviderError(
                "format_unavailable", "Requested TIDAL quality is unavailable", "acquire"
            )

        parent = staging_root / "tidal"
        final_dir = parent / track_id
        nonce = uuid.uuid4().hex
        temp_name = f".{track_id}.{nonce}.partial"
        profile_name = f".profile.{nonce}.partial"
        root_fd: int | None = None
        try:
            root_fd = _open_pinned_directory(staging_root)
            with suppress(FileExistsError):
                os.mkdir("tidal", mode=0o700, dir_fd=root_fd)
            parent_fd = os.open(
                "tidal", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd
            )
        except OSError as exc:
            raise ProviderError(
                "unsafe_staging", "TIDAL staging root is unsafe", "acquire"
            ) from exc
        finally:
            if root_fd is not None:
                os.close(root_fd)
        try:
            os.stat(track_id, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            os.close(parent_fd)
            raise ProviderError("staging_collision", "TIDAL staging destination exists", "acquire")
        pinned_temp = Path(f"/proc/self/fd/{parent_fd}/{temp_name}")
        pinned_profile = Path(f"/proc/self/fd/{parent_fd}/{profile_name}")
        try:
            os.mkdir(temp_name, mode=0o700, dir_fd=parent_fd)
            os.mkdir(profile_name, mode=0o700, dir_fd=parent_fd)
            pinned_temp = Path(f"/proc/self/fd/{parent_fd}/{temp_name}")
            pinned_profile = Path(f"/proc/self/fd/{parent_fd}/{profile_name}")
            _copy_profile_files(home, pinned_profile)
            before = _snapshot(pinned_temp)
            env = os.environ.copy()
            env.pop("XDG_CONFIG_HOME", None)
            env["HOME"] = str(pinned_profile)
            command = [
                sys.executable,
                "-m",
                "app.sources.tidal_runner",
                "--url",
                canonical_url,
                "--output",
                str(pinned_temp),
                "--quality",
                self._quality,
            ]
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
                pass_fds=(parent_fd,),
                env=env,
                limit=8192,
            )
            try:
                _stdout, stderr = await asyncio.wait_for(
                    _communicate_bounded(process), self._timeout_sec
                )
            except TimeoutError as exc:
                await _stop_process(process)
                raise ProviderError(
                    "timeout", "TIDAL acquisition timed out", "acquire", True
                ) from exc
            except asyncio.CancelledError:
                await _stop_process(process)
                raise
            if process.returncode != 0:
                raise _classify_failure(_stdout + b"\n" + stderr)
            after = _snapshot(pinned_temp)
            new_paths = after - before
            audio = [
                pinned_temp / relative
                for relative in new_paths
                if (pinned_temp / relative).is_file()
                and (pinned_temp / relative).suffix.casefold().lstrip(".")
                in _SUPPORTED_AUDIO_EXTENSIONS
                and (pinned_temp / relative).stat().st_size > 0
                and _is_supported_audio(pinned_temp / relative)
            ]
            if len(audio) != 1:
                raise ProviderError(
                    "artifact_invalid",
                    "TIDAL acquisition produced no single verified audio artifact",
                    "acquire",
                )
            relative_audio = audio[0].relative_to(pinned_temp)
            os.rename(temp_name, track_id, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            extension = audio[0].suffix.casefold().lstrip(".")
            return AcquiredMedia(
                final_dir / relative_audio,
                {
                    "provider": "tidal",
                    "track_id": track_id,
                    "quality": self._quality,
                    "tidal_dl_version": installed,
                    "extension": extension,
                },
            )
        finally:
            shutil.rmtree(pinned_temp, ignore_errors=True)
            shutil.rmtree(pinned_profile, ignore_errors=True)
            os.close(parent_fd)
