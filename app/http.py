from __future__ import annotations

import asyncio
from typing import Any

import httpx

_TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    attempts: int = 3,
    retry_status_codes: frozenset[int] = _TRANSIENT_STATUS_CODES,
    **kwargs: Any,
) -> httpx.Response:
    last_transport_error: httpx.TransportError | None = None
    last_response: httpx.Response | None = None
    for attempt in range(attempts):
        try:
            response = await client.request(method, url, **kwargs)
        except httpx.TransportError as exc:
            last_transport_error = exc
            if attempt == attempts - 1:
                raise
        else:
            last_response = response
            if response.status_code not in retry_status_codes or attempt == attempts - 1:
                return response
        await asyncio.sleep(min(1.0, 0.1 * (2.0**attempt)))
    if last_response is not None:
        return last_response
    if last_transport_error is not None:
        raise last_transport_error
    raise RuntimeError("HTTP retry exhausted without response")


async def stream_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    attempts: int = 3,
    retry_status_codes: frozenset[int] = _TRANSIENT_STATUS_CODES,
    **kwargs: Any,
) -> httpx.Response:
    """Return an unbuffered response; the caller must close it."""
    last_transport_error: httpx.TransportError | None = None
    last_response: httpx.Response | None = None
    for attempt in range(attempts):
        try:
            request = client.build_request(method, url, **kwargs)
            response = await client.send(request, stream=True)
        except httpx.TransportError as exc:
            last_transport_error = exc
            if attempt == attempts - 1:
                raise
        else:
            last_response = response
            if response.status_code not in retry_status_codes or attempt == attempts - 1:
                return response
            await response.aclose()
        await asyncio.sleep(min(1.0, 0.1 * (2.0**attempt)))
    if last_response is not None:
        return last_response
    if last_transport_error is not None:
        raise last_transport_error
    raise RuntimeError("HTTP streaming retry exhausted without response")
