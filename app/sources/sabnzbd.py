from __future__ import annotations

import logging
from typing import Any, cast

import httpx

from app.http import request_with_retry
from app.schemas.search import SearchRequest, SearchResult
from app.sources.base import CapabilityState

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(10.0)


class SabnzbdAdapter:
    name = "sabnzbd"

    def __init__(self, base_url: str, api_key: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base_url, timeout=_HTTP_TIMEOUT)

    def _params(
        self, **extra: str | int | float | bool | None
    ) -> dict[str, str | int | float | bool | None]:
        return {"apikey": self._api_key, "output": "json", **extra}

    async def health(self) -> CapabilityState:
        if not self._base_url or not self._api_key:
            return CapabilityState(available=False, reason="sabnzbd not configured")
        try:
            async with self._client() as client:
                resp = await request_with_retry(
                    client, "GET", "/api", params=self._params(mode="version")
                )
            if resp.status_code == 200 and resp.json().get("version"):
                return CapabilityState(available=True)
            return CapabilityState(available=False, reason=f"HTTP {resp.status_code}")
        except Exception as exc:
            logger.warning("sabnzbd health check failed: %s", exc)
            return CapabilityState(available=False, reason=str(exc))

    async def enqueue(self, nzb_url: str, name: str | None = None) -> str:
        """Add an NZB URL to the SABnzbd queue. Returns the SABnzbd job ID."""
        async with self._client() as client:
            resp = await request_with_retry(
                client,
                "GET",
                "/api",
                params=self._params(mode="addurl", name=nzb_url, nzbname=name or ""),
            )
            resp.raise_for_status()
            data = cast(dict[str, Any], resp.json())
            if not data.get("status"):
                raise RuntimeError(f"SABnzbd enqueue failed: {data}")
            nzo_ids = data.get("nzo_ids", [""])
            if isinstance(nzo_ids, list) and nzo_ids:
                return str(nzo_ids[0])
            return ""

    async def status(self, nzo_id: str) -> CapabilityState:
        """Check the status of an enqueued SABnzbd job."""
        async with self._client() as client:
            resp = await request_with_retry(
                client,
                "GET",
                "/api",
                params=self._params(mode="queue", search=nzo_id),
            )
            resp.raise_for_status()
            data = cast(dict[str, Any], resp.json())
            queue = data.get("queue", {})
            slots = queue.get("slots", []) if isinstance(queue, dict) else []
            for slot in slots:
                if slot.get("nzo_id") == nzo_id:
                    return CapabilityState(available=True, reason=slot.get("status"), extra=slot)
        return CapabilityState(available=False, reason="not found")

    async def job_status(self, nzo_id: str) -> CapabilityState:
        return await self.status(nzo_id)

    async def history_status(self, nzo_id: str) -> CapabilityState:
        """Check the completed/failed SABnzbd job history for nzo_id."""
        async with self._client() as client:
            resp = await request_with_retry(
                client,
                "GET",
                "/api",
                params=self._params(mode="history", search=nzo_id),
            )
            resp.raise_for_status()
            data = cast(dict[str, Any], resp.json())
            history = data.get("history", {})
            slots = history.get("slots", []) if isinstance(history, dict) else []
            for slot in slots:
                if slot.get("nzo_id") == nzo_id:
                    return CapabilityState(available=True, reason=slot.get("status"), extra=slot)
        return CapabilityState(available=False, reason="not found in history")

    async def search(self, query: SearchRequest) -> list[SearchResult]:
        return []
