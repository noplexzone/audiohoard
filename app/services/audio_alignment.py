from __future__ import annotations

import asyncio
import ipaddress
import json
import math
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx

_MAX_PREVIEW_BYTES = 5 * 1024 * 1024
_MAX_REDIRECTS = 4
_FPCALC_TIMEOUT_SECONDS = 45.0
_MAX_TRACK_SECONDS = 3600
_MAX_REFERENCE_SECONDS = 90
# Chromaprint emits one raw fingerprint item per 4096/3 samples at 11025 Hz.
_FINGERPRINT_FRAME_RATE = 11025.0 * 3.0 / 4096.0
_ALIGNMENT_LIMIT = asyncio.Semaphore(2)
_ALLOWED_REFERENCE_SUFFIXES = ("deezer.com", "dzcdn.net")


@dataclass(frozen=True)
class AlignmentResult:
    offset_seconds: float
    score: float
    confidence: str


@dataclass(frozen=True)
class FingerprintData:
    values: tuple[int, ...]
    duration_seconds: float


def estimate_centered_offset(
    downloaded_duration: float | int | None, reference_duration: float | int | None = 30.0
) -> float | None:
    if downloaded_duration is None or reference_duration is None:
        return None
    try:
        downloaded = float(downloaded_duration)
        reference = float(reference_duration)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(downloaded) or not math.isfinite(reference) or downloaded <= 0:
        return None
    return round(max(0.0, (downloaded - max(0.0, reference)) / 2.0), 3)


def _window_distance(
    full: list[int] | tuple[int, ...], reference: list[int] | tuple[int, ...], start: int
) -> float:
    differing_bits = sum(
        ((int(full[start + index]) & 0xFFFFFFFF) ^ (int(value) & 0xFFFFFFFF)).bit_count()
        for index, value in enumerate(reference)
    )
    return differing_bits / (len(reference) * 32)


def find_fingerprint_alignment(
    full: list[int] | tuple[int, ...],
    reference: list[int] | tuple[int, ...],
    *,
    frame_rate: float,
) -> AlignmentResult | None:
    """Find a distinctive reference fingerprint window inside a full-track fingerprint."""
    if (
        not reference
        or len(full) < len(reference)
        or not math.isfinite(frame_rate)
        or frame_rate <= 0
    ):
        return None
    scores = [
        _window_distance(full, reference, start) for start in range(len(full) - len(reference) + 1)
    ]
    best_start = min(range(len(scores)), key=scores.__getitem__)
    best_score = scores[best_start]
    ambiguity_exclusion = max(1, round(frame_rate * 0.5))
    alternatives = [
        score
        for start, score in enumerate(scores)
        if abs(start - best_start) >= ambiguity_exclusion
    ]
    second_score = min(alternatives, default=1.0)
    separation = second_score - best_score
    if best_score > 0.20 or separation < 0.035:
        return None
    confidence = "high" if best_score <= 0.10 and separation >= 0.08 else "medium"
    return AlignmentResult(
        offset_seconds=round(best_start / frame_rate, 3),
        score=round(best_score, 6),
        confidence=confidence,
    )


def _validate_reference_url(url: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("unsupported reference URL")
    if parsed.port not in {None, 443}:
        raise ValueError("unsupported reference port")
    host = parsed.hostname.rstrip(".").casefold()
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise ValueError("literal reference IP is not permitted")
    if not any(
        host == suffix or host.endswith(f".{suffix}") for suffix in _ALLOWED_REFERENCE_SUFFIXES
    ):
        raise ValueError("unapproved reference host")


async def _download_reference(url: str, destination: Path) -> None:
    timeout = httpx.Timeout(12.0, connect=5.0)
    current = url
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        for redirect_count in range(_MAX_REDIRECTS + 1):
            _validate_reference_url(current)
            async with client.stream("GET", current) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get("location")
                    if not location or redirect_count >= _MAX_REDIRECTS:
                        raise RuntimeError("reference redirect limit exceeded")
                    current = urljoin(current, location)
                    continue
                response.raise_for_status()
                declared = response.headers.get("content-length")
                if declared and int(declared) > _MAX_PREVIEW_BYTES:
                    raise RuntimeError("reference preview is too large")
                size = 0
                with destination.open("wb") as output:
                    async for chunk in response.aiter_bytes(64 * 1024):
                        size += len(chunk)
                        if size > _MAX_PREVIEW_BYTES:
                            raise RuntimeError("reference preview is too large")
                        output.write(chunk)
                if size == 0:
                    raise RuntimeError("reference preview is empty")
                return
    raise RuntimeError("reference preview could not be downloaded")


async def _fingerprint(path: Path, *, max_length_seconds: int) -> FingerprintData:
    fpcalc = shutil.which("fpcalc")
    if fpcalc is None:
        raise RuntimeError("audio fingerprinting is unavailable")
    process = await asyncio.create_subprocess_exec(
        fpcalc,
        "-raw",
        "-json",
        "-length",
        str(max_length_seconds),
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, _stderr = await asyncio.wait_for(
            process.communicate(), timeout=_FPCALC_TIMEOUT_SECONDS
        )
    except TimeoutError:
        process.kill()
        await process.communicate()
        raise RuntimeError("audio fingerprinting timed out") from None
    except asyncio.CancelledError:
        if process.returncode is None:
            process.kill()
            await asyncio.shield(process.communicate())
        raise
    if process.returncode != 0:
        raise RuntimeError("audio fingerprinting failed")
    try:
        payload = json.loads(stdout)
        values = tuple(int(value) for value in payload["fingerprint"])
        duration = float(payload["duration"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        raise RuntimeError("audio fingerprint output was invalid") from None
    if not values or not math.isfinite(duration) or duration <= 0:
        raise RuntimeError("audio fingerprint output was empty")
    return FingerprintData(values=values, duration_seconds=duration)


async def align_deezer_preview(source_path: Path, preview_url: str) -> AlignmentResult | None:
    """Transiently fetch and align a Deezer preview; no provider audio is retained."""
    async with _ALIGNMENT_LIMIT:
        with tempfile.TemporaryDirectory(prefix="audiohoard-align-") as temp_dir:
            preview_path = Path(temp_dir) / "reference.audio"
            await _download_reference(preview_url, preview_path)
            fingerprint_tasks = (
                asyncio.create_task(
                    _fingerprint(source_path, max_length_seconds=_MAX_TRACK_SECONDS)
                ),
                asyncio.create_task(
                    _fingerprint(preview_path, max_length_seconds=_MAX_REFERENCE_SECONDS)
                ),
            )
            try:
                full_data, reference_data = await asyncio.gather(*fingerprint_tasks)
            except BaseException:
                for task in fingerprint_tasks:
                    task.cancel()
                await asyncio.gather(*fingerprint_tasks, return_exceptions=True)
                raise
            if (
                full_data.duration_seconds > _MAX_TRACK_SECONDS
                or reference_data.duration_seconds > _MAX_REFERENCE_SECONDS
            ):
                raise RuntimeError("audio duration exceeds alignment limits")
            return await asyncio.to_thread(
                find_fingerprint_alignment,
                full_data.values,
                reference_data.values,
                frame_rate=_FINGERPRINT_FRAME_RATE,
            )
