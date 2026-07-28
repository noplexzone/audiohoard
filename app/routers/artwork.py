from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, Response

from app.auth import get_current_user
from app.config import Settings, get_settings
from app.http import stream_with_retry

router = APIRouter(dependencies=[Depends(get_current_user)])

_ALLOWED_HOSTS = frozenset(
    {
        "e-cdns-images.dzcdn.net",
        "cdn-images.dzcdn.net",
        "is1-ssl.mzstatic.com",
        "is2-ssl.mzstatic.com",
        "is3-ssl.mzstatic.com",
        "is4-ssl.mzstatic.com",
        "is5-ssl.mzstatic.com",
        "coverartarchive.org",
        "ia801504.us.archive.org",
    }
)
_MAX_ARTWORK_BYTES = 5 * 1024 * 1024
_CACHE_CONTROL = "public, max-age=31536000, immutable"
_FALLBACK_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff"
    b"!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00"
    b"\x00\x02\x02D\x01\x00;"
)


def _fallback_response() -> Response:
    return Response(
        content=_FALLBACK_GIF,
        media_type="image/gif",
        headers={"Cache-Control": "no-store"},
    )


def _cached_media_type(path: Path) -> str:
    with path.open("rb") as cached_file:
        header = cached_file.read(12)
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if header.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "image/webp"
    return "application/octet-stream"


@router.get("/artwork", response_model=None)
async def artwork(
    url: Annotated[str, Query()],
    settings: Annotated[Settings, Depends(get_settings)],
) -> Response:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in _ALLOWED_HOSTS:
        raise HTTPException(status_code=400, detail="Artwork URL is not allowed")

    cache_root = settings.artwork_cache_root.resolve()
    key = hashlib.sha256(url.encode()).hexdigest()
    cache_path = (cache_root / key[:2] / key).resolve()
    if not cache_path.is_relative_to(cache_root):
        raise HTTPException(status_code=400, detail="Invalid artwork cache path")

    if cache_path.is_file():
        return FileResponse(
            cache_path,
            media_type=_cached_media_type(cache_path),
            headers={"Cache-Control": _CACHE_CONTROL},
        )

    temp_path: Path | None = None
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            response = await stream_with_retry(client, "GET", url)
            try:
                if response.status_code != 200:
                    return _fallback_response()
                content_type = response.headers.get("content-type", "")
                if not content_type.casefold().startswith("image/"):
                    raise HTTPException(status_code=502, detail="Upstream did not return an image")
                content_length = response.headers.get("content-length")
                if content_length:
                    try:
                        declared_size = int(content_length)
                    except ValueError:
                        declared_size = 0
                    if declared_size > _MAX_ARTWORK_BYTES:
                        raise HTTPException(
                            status_code=502, detail="Upstream artwork exceeds 5 MB"
                        )

                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with tempfile.NamedTemporaryFile(dir=cache_path.parent, delete=False) as temp_file:
                    temp_path = Path(temp_file.name)
                    stored = 0
                    async for chunk in response.aiter_bytes():
                        stored += len(chunk)
                        if stored > _MAX_ARTWORK_BYTES:
                            raise HTTPException(
                                status_code=502, detail="Upstream artwork exceeds 5 MB"
                            )
                        temp_file.write(chunk)
            finally:
                await response.aclose()
        os.replace(temp_path, cache_path)
        temp_path = None
        return FileResponse(
            cache_path,
            media_type=content_type,
            headers={"Cache-Control": _CACHE_CONTROL},
        )
    except httpx.HTTPError:
        return _fallback_response()
    except OSError:
        return _fallback_response()
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
