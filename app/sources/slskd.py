from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING
from urllib.parse import quote

import httpx

from app.http import request_with_retry
from app.metadata.filename_parse import (
    compose_search_query,
    parse_filename,
    parsed_position_evidence,
)
from app.schemas.search import SearchRequest, SearchResult
from app.sources.base import CapabilityState
from app.sources.youtube import ProviderError

if TYPE_CHECKING:
    from app.services.slskd_scoring import AlbumFolder

logger = logging.getLogger(__name__)

_SEARCH_POLL_INTERVAL = 1.5
_SEARCH_TIMEOUT_SEC = 60
_HTTP_TIMEOUT = httpx.Timeout(10.0)


def _error_detail(response: httpx.Response) -> str:
    try:
        data = response.json()
    except ValueError:
        return response.text.strip()[:500]
    if not isinstance(data, dict):
        return str(data)[:500]
    parts: list[str] = []
    for key in ("title", "detail", "message"):
        value = data.get(key)
        if value:
            parts.append(str(value))
    errors = data.get("errors")
    if isinstance(errors, dict):
        for field, messages in errors.items():
            if isinstance(messages, list):
                parts.extend(f"{field}: {message}" for message in messages)
            elif messages:
                parts.append(f"{field}: {messages}")
    return "; ".join(parts)[:500]


def _flatten_downloads(data: object) -> list[dict[str, object]]:
    if isinstance(data, dict) and "downloads" in data:
        data = data["downloads"]
    roots = data if isinstance(data, list) else [data]
    flattened: list[dict[str, object]] = []

    def visit(value: object, inherited: dict[str, object]) -> None:
        if not isinstance(value, dict):
            return
        context = dict(inherited)
        for key in ("username", "directory"):
            if value.get(key) is not None:
                context[key] = value[key]
        if value.get("filename") is not None:
            flattened.append({**value, **context})
            return
        for key in ("directories", "files", "downloads"):
            children = value.get(key)
            if isinstance(children, list):
                for child in children:
                    visit(child, context)

    for root in roots:
        visit(root, {})
    return flattened


class SlskdAdapter:
    name = "slskd"

    def __init__(
        self, base_url: str, api_key: str, search_timeout_sec: float = _SEARCH_TIMEOUT_SEC
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._search_timeout_sec = search_timeout_sec

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._base_url,
            headers={"X-API-Key": self._api_key},
            timeout=_HTTP_TIMEOUT,
        )

    async def health(self) -> CapabilityState:
        if not self._base_url or not self._api_key:
            return CapabilityState(available=False, reason="slskd not configured")
        try:
            async with self._client() as client:
                resp = await request_with_retry(client, "GET", "/api/v0/application")
            if resp.status_code == 200:
                return CapabilityState(available=True)
            return CapabilityState(available=False, reason=f"HTTP {resp.status_code}")
        except Exception as exc:
            logger.warning("slskd health check failed: %s", exc)
            return CapabilityState(available=False, reason=str(exc))

    async def search_album_folders(
        self,
        query: SearchRequest,
    ) -> tuple[list[AlbumFolder], list[dict[str, object]]]:
        """Search slskd and return (album_folders, raw_responses).

        Raw responses are returned so the caller can inspect individual files for
        per-track enqueueing.  Audio-only grouping is performed here; the caller
        applies scoring and selection.
        """
        from app.services.slskd_scoring import group_slskd_files_into_folders

        if not self._base_url or not self._api_key:
            return [], []
        async with self._client() as client:
            resp = await request_with_retry(
                client,
                "POST",
                "/api/v0/searches",
                json={
                    "searchText": compose_search_query(
                        query.query, query.artist, query.album, query.track
                    ),
                    "fileLimit": 500,
                },
            )
            resp.raise_for_status()
            search_id = resp.json().get("id") or resp.json().get("searchId", "")
            if not search_id:
                raise ProviderError(
                    "missing_search_id",
                    "slskd create-search response did not include an id",
                    "search",
                )

            elapsed = 0.0
            while elapsed < self._search_timeout_sec:
                await asyncio.sleep(_SEARCH_POLL_INTERVAL)
                elapsed += _SEARCH_POLL_INTERVAL
                state_resp = await request_with_retry(
                    client, "GET", f"/api/v0/searches/{search_id}"
                )
                if state_resp.status_code == 200:
                    state = state_resp.json()
                    if state.get("state") in ("Completed", "Stopped", "TimedOut"):
                        break

            files_resp = await request_with_retry(
                client, "GET", f"/api/v0/searches/{search_id}/responses"
            )
            if files_resp.status_code != 200:
                return [], []

            raw_responses: list[dict[str, object]] = files_resp.json()
            folders = group_slskd_files_into_folders(raw_responses)
            return folders, raw_responses

    async def search(self, query: SearchRequest) -> list[SearchResult]:
        if not self._base_url or not self._api_key:
            return []
        async with self._client() as client:
            resp = await request_with_retry(
                client,
                "POST",
                "/api/v0/searches",
                json={
                    "searchText": compose_search_query(
                        query.query, query.artist, query.album, query.track
                    ),
                    "fileLimit": 100,
                },
            )
            resp.raise_for_status()
            search_id = resp.json().get("id") or resp.json().get("searchId", "")
            if not search_id:
                raise ProviderError(
                    "missing_search_id",
                    "slskd create-search response did not include an id",
                    "search",
                )

            elapsed = 0.0
            while elapsed < self._search_timeout_sec:
                await asyncio.sleep(_SEARCH_POLL_INTERVAL)
                elapsed += _SEARCH_POLL_INTERVAL
                state_resp = await request_with_retry(
                    client, "GET", f"/api/v0/searches/{search_id}"
                )
                if state_resp.status_code == 200:
                    state = state_resp.json()
                    if state.get("state") in ("Completed", "Stopped", "TimedOut"):
                        break

            files_resp = await request_with_retry(
                client, "GET", f"/api/v0/searches/{search_id}/responses"
            )
            if files_resp.status_code != 200:
                return []

            results: list[SearchResult] = []
            for response in files_resp.json():
                username = response.get("username", "")
                for f in response.get("files", []):
                    filename: str = f.get("filename", "")
                    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
                    guess = parse_filename(filename)
                    results.append(
                        SearchResult(
                            source="slskd",
                            title=guess.title,
                            artist=guess.artist,
                            album=guess.album,
                            size_bytes=f.get("size"),
                            format=ext or None,
                            url=f"slskd://{username}/{filename}",
                            metadata={
                                "username": username,
                                "filename": filename,
                                "parse_confidence": guess.confidence,
                                "parse_hints": list(guess.hints),
                                "bit_rate": f.get("bitRate"),
                                "sample_rate": f.get("sampleRate"),
                                **parsed_position_evidence(filename),
                            },
                        )
                    )
            return results

    async def enqueue(self, username: str, filename: str, size: int | None = None) -> str:
        if not username or not filename:
            raise ProviderError(
                "invalid_result", "slskd result is missing username or filename", "acquire"
            )
        payload: dict[str, object] = {"filename": filename}
        if size is not None:
            payload["size"] = size
        async with self._client() as client:
            resp = await request_with_retry(
                client, "POST", f"/api/v0/transfers/downloads/{username}", json=[payload]
            )
            if resp.is_error:
                detail = _error_detail(resp)
                message = f"slskd rejected download queue request (HTTP {resp.status_code})"
                if detail:
                    message = f"{message}: {detail}"
                raise ProviderError(
                    f"slskd_http_{resp.status_code}", message, "acquire", resp.status_code >= 500
                )
        data = resp.json() if resp.content else {}
        response_id = (
            (data.get("id") or data.get("transferId")) if isinstance(data, dict) else None
        )
        transfer_id = str(response_id or f"{username}:{filename}")
        return transfer_id

    async def downloads(self) -> list[dict[str, object]]:
        async with self._client() as client:
            resp = await request_with_retry(client, "GET", "/api/v0/transfers/downloads")
            resp.raise_for_status()
        return _flatten_downloads(resp.json())

    async def status(self, transfer_id: str) -> CapabilityState:
        for item in await self.downloads():
            provider_id = item.get("id") or item.get("transferId")
            fallback_id = f"{item.get('username')}:{item.get('filename')}"
            if transfer_id in {str(provider_id) if provider_id is not None else "", fallback_id}:
                state = str(item.get("state") or item.get("status") or "queued").casefold()
                return CapabilityState(True, state, dict(item))
        return CapabilityState(False, "transfer not found", {"transfer_id": transfer_id})

    async def cancel(self, username: str, filename: str, transfer_id: str | None = None) -> bool:
        """Remove one tracked download, optionally requiring its exact provider ID."""
        expected_filename = filename.replace("\\", "/")
        resolved_transfer_id: str | None = None
        matched_identity = False
        for item in await self.downloads():
            item_username = str(item.get("username") or "")
            item_filename = str(item.get("filename") or "").replace("\\", "/")
            provider_id = item.get("id") or item.get("transferId")
            if item_username != username or item_filename != expected_filename:
                continue
            matched_identity = True
            # Cleanup must never fall back to peer/path identity: that identity
            # can be reused by a newer acquisition while reconciliation runs.
            if transfer_id is not None and (
                provider_id is None or str(provider_id) != transfer_id
            ):
                continue
            if provider_id is not None:
                resolved_transfer_id = str(provider_id)
            break
        if resolved_transfer_id is None:
            fallback_id = f"{username}:{filename}"
            # A persisted fallback identity cannot distinguish this transfer from a
            # replacement using the same peer/path. Keep the cleanup obligation for
            # later/manual reconciliation rather than report a false success.
            return not (matched_identity and transfer_id == fallback_id)

        safe_username = quote(username, safe="")
        safe_transfer_id = quote(resolved_transfer_id, safe="")
        async with self._client() as client:
            resp = await request_with_retry(
                client,
                "DELETE",
                f"/api/v0/transfers/downloads/{safe_username}/{safe_transfer_id}",
                params={"remove": "true"},
            )
            if resp.status_code != 404:
                resp.raise_for_status()
        return True
