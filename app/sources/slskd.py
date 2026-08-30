from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import quote

import httpx

from app.http import request_with_retry
from app.media_formats import is_importable_audio
from app.metadata.filename_parse import (
    compose_search_query,
    parse_filename,
    parsed_position_evidence,
)
from app.schemas.search import SearchRequest, SearchResult
from app.services.slskd_scoring import slskd_file_duration_seconds
from app.sources.base import CapabilityState
from app.sources.youtube import ProviderError

if TYPE_CHECKING:
    from app.services.slskd_scoring import AlbumFolder

logger = logging.getLogger(__name__)

_SEARCH_POLL_INTERVAL = 1.5
_SEARCH_POLL_INTERVAL_MAX = 10.0
_SEARCH_TIMEOUT_SEC = 300
_HTTP_TIMEOUT = httpx.Timeout(10.0)
_DOWNLOAD_SNAPSHOT_TTL_SEC = 1.0
_SEARCH_SNAPSHOT_TTL_SEC = 1.5
_SEARCH_SNAPSHOT_MAX_ENTRIES = 128
_TRANSFER_429_MAX_ATTEMPTS = 4
_TRANSFER_429_BACKOFF_INITIAL_SEC = 0.25
_TRANSFER_429_BACKOFF_MAX_SEC = 2.0
_TRANSFER_429_JITTER_MAX_SEC = 0.1
_monotonic = time.monotonic
_transfer_sleep = asyncio.sleep
_transfer_jitter = random.uniform
_TERMINAL_SEARCH_STATE_TOKENS = frozenset(
    {"completed", "stopped", "timedout", "filelimitreached", "responselimitreached"}
)
_FAILED_SEARCH_STATE_TOKENS = frozenset({"errored", "cancelled", "canceled"})


@dataclass
class _DownloadSnapshot:
    downloads: list[dict[str, object]] | None = None
    expires_at: float = 0.0
    in_flight: asyncio.Task[list[dict[str, object]]] | None = None


_SearchKey = tuple[str, bytes, str, str, int]


@dataclass
class _SearchSnapshot:
    in_flight: asyncio.Task[bytes]
    order: int
    payload: bytes | None = None
    completed_at: float | None = None
    expires_at: float = 0.0
    eviction_task: asyncio.Task[None] | None = None


@dataclass
class _SearchCacheState:
    snapshots: dict[_SearchKey, _SearchSnapshot] = field(default_factory=dict)
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    order: int = 0


_SEARCH_CACHE_STATE_ATTRIBUTE = "_audiohoard_search_cache_state"


def _get_search_cache_state() -> _SearchCacheState:
    """Return cache state owned exclusively by the current running event loop."""
    loop = asyncio.get_running_loop()
    state = getattr(loop, _SEARCH_CACHE_STATE_ATTRIBUTE, None)
    if state is None:
        state = _SearchCacheState()
        setattr(loop, _SEARCH_CACHE_STATE_ATTRIBUTE, state)
    return state


async def _clear_search_snapshot_cache() -> None:
    """Cancel and clear only the current loop's cache tasks (primarily for tests)."""
    state = _get_search_cache_state()
    current = asyncio.current_task()
    tasks: list[asyncio.Task[object]] = []
    async with state.condition:
        for snapshot in state.snapshots.values():
            for task in (snapshot.in_flight, snapshot.eviction_task):
                if task is not None and task is not current and not task.done():
                    task.cancel()
                    tasks.append(task)
        state.snapshots.clear()
        state.condition.notify_all()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


@dataclass(frozen=True)
class ProvisionalTransferMatch:
    """Evidence for an exact provisional peer/path lookup."""

    match_count: int
    transfer: dict[str, object] | None = None


_download_snapshots: dict[tuple[str, bytes], _DownloadSnapshot] = {}


def _search_state_tokens(value: object) -> set[str]:
    normalized = str(value or "").replace(",", " ").replace("_", " ").replace("-", " ").casefold()
    tokens = set(normalized.split())
    compact = "".join(normalized.split())
    for token in _TERMINAL_SEARCH_STATE_TOKENS | _FAILED_SEARCH_STATE_TOKENS:
        if token in compact:
            tokens.add(token)
    return tokens


def _search_state_is_terminal(value: object) -> bool:
    """Return true for slskd's simple or compound terminal search states."""
    tokens = _search_state_tokens(value)
    return bool(tokens & (_TERMINAL_SEARCH_STATE_TOKENS | _FAILED_SEARCH_STATE_TOKENS))


def _search_state_is_failed(value: object) -> bool:
    tokens = _search_state_tokens(value)
    return bool(tokens & _FAILED_SEARCH_STATE_TOKENS)


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


def slskd_fallback_transfer_id(username: str, filename: str) -> str:
    """Return the deterministic peer/path identity used before a UUID is known."""
    return f"{username}:{filename}"


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

    async def _wait_for_search(self, client: httpx.AsyncClient, search_id: str) -> None:
        elapsed = 0.0
        interval = _SEARCH_POLL_INTERVAL
        while elapsed < self._search_timeout_sec:
            sleep_for = min(interval, self._search_timeout_sec - elapsed)
            await asyncio.sleep(sleep_for)
            elapsed += sleep_for
            state_resp = await request_with_retry(client, "GET", f"/api/v0/searches/{search_id}")
            if state_resp.status_code == 200:
                state = state_resp.json()
                search_state = state.get("state")
                if _search_state_is_failed(search_state):
                    raise ProviderError(
                        "search_failed",
                        "slskd search failed before results were available",
                        "search",
                        True,
                    )
                if _search_state_is_terminal(search_state):
                    break
            interval = min(interval * 1.5, _SEARCH_POLL_INTERVAL_MAX)

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

    def _search_snapshot_key(self, search_text: str, mode: str, file_limit: int) -> _SearchKey:
        normalized_endpoint = str(httpx.URL(self._base_url)).rstrip("/")
        credential_digest = hashlib.sha256(self._api_key.encode()).digest()
        return normalized_endpoint, credential_digest, search_text, mode, file_limit

    async def _fetch_raw_search(self, search_text: str, file_limit: int) -> bytes:
        """Run one provider search and serialize its raw JSON as immutable bytes."""
        async with self._client() as client:
            resp = await request_with_retry(
                client,
                "POST",
                "/api/v0/searches",
                json={"searchText": search_text, "fileLimit": file_limit},
            )
            resp.raise_for_status()
            search_id = resp.json().get("id") or resp.json().get("searchId", "")
            if not search_id:
                raise ProviderError(
                    "missing_search_id",
                    "slskd create-search response did not include an id",
                    "search",
                )

            await self._wait_for_search(client, search_id)
            files_resp = await request_with_retry(
                client, "GET", f"/api/v0/searches/{search_id}/responses"
            )
            if files_resp.status_code != 200:
                return b"[]"
            return json.dumps(files_resp.json(), separators=(",", ":")).encode()

    @staticmethod
    def _remove_search_snapshot(
        state: _SearchCacheState, key: _SearchKey, snapshot: _SearchSnapshot
    ) -> None:
        """Remove an identity-matching entry while holding its loop-local condition."""
        if state.snapshots.get(key) is not snapshot:
            return
        state.snapshots.pop(key)
        eviction_task = snapshot.eviction_task
        if (
            eviction_task is not None
            and eviction_task is not asyncio.current_task()
            and not eviction_task.done()
        ):
            eviction_task.cancel()

    @classmethod
    def _prune_search_snapshots(cls, state: _SearchCacheState, *, reserve: int = 0) -> None:
        """Remove expired/oldest completed entries without touching live producers."""
        now = _monotonic()
        completed = [
            (key, snapshot)
            for key, snapshot in state.snapshots.items()
            if snapshot.payload is not None
        ]
        expired = sorted(
            ((key, snapshot) for key, snapshot in completed if snapshot.expires_at <= now),
            key=lambda item: (item[1].expires_at, item[1].order),
        )
        for key, snapshot in expired:
            cls._remove_search_snapshot(state, key, snapshot)

        target = max(0, max(1, _SEARCH_SNAPSHOT_MAX_ENTRIES) - reserve)
        if len(state.snapshots) <= target:
            return
        oldest_completed = sorted(
            (
                (key, snapshot)
                for key, snapshot in state.snapshots.items()
                if snapshot.completed_at is not None
            ),
            key=lambda item: (item[1].completed_at or 0.0, item[1].order),
        )
        for key, snapshot in oldest_completed:
            if len(state.snapshots) <= target:
                break
            cls._remove_search_snapshot(state, key, snapshot)

    @classmethod
    async def _evict_search_snapshot(
        cls, state: _SearchCacheState, key: _SearchKey, snapshot: _SearchSnapshot
    ) -> None:
        await asyncio.sleep(_SEARCH_SNAPSHOT_TTL_SEC)
        async with state.condition:
            if _monotonic() >= snapshot.expires_at:
                cls._remove_search_snapshot(state, key, snapshot)
                state.condition.notify_all()

    async def _produce_search(
        self,
        state: _SearchCacheState,
        key: _SearchKey,
        search_text: str,
        file_limit: int,
    ) -> bytes:
        producer = asyncio.current_task()
        try:
            payload = await self._fetch_raw_search(search_text, file_limit)
        except BaseException:
            async with state.condition:
                snapshot = state.snapshots.get(key)
                if snapshot is not None and snapshot.in_flight is producer:
                    self._remove_search_snapshot(state, key, snapshot)
                    state.condition.notify_all()
            raise

        async with state.condition:
            snapshot = state.snapshots.get(key)
            if snapshot is not None and snapshot.in_flight is producer:
                snapshot.payload = payload
                snapshot.completed_at = _monotonic()
                snapshot.expires_at = snapshot.completed_at + _SEARCH_SNAPSHOT_TTL_SEC
                snapshot.eviction_task = asyncio.create_task(
                    self._evict_search_snapshot(state, key, snapshot)
                )
                self._prune_search_snapshots(state)
                state.condition.notify_all()
        return payload

    async def _raw_search(
        self, search_text: str, *, mode: str, file_limit: int
    ) -> list[dict[str, object]]:
        """Return an independent copy of a bounded, loop-local coalesced search."""
        state = _get_search_cache_state()
        key = self._search_snapshot_key(search_text, mode, file_limit)
        payload: bytes | None = None
        producer: asyncio.Task[bytes] | None = None

        async with state.condition:
            while payload is None and producer is None:
                snapshot = state.snapshots.get(key)
                if snapshot is not None:
                    if snapshot.payload is not None:
                        if _monotonic() < snapshot.expires_at:
                            payload = snapshot.payload
                            continue
                        self._remove_search_snapshot(state, key, snapshot)
                        state.condition.notify_all()
                        continue
                    producer = snapshot.in_flight
                    continue

                self._prune_search_snapshots(state, reserve=1)
                if len(state.snapshots) < max(1, _SEARCH_SNAPSHOT_MAX_ENTRIES):
                    state.order += 1
                    producer = asyncio.create_task(
                        self._produce_search(state, key, search_text, file_limit)
                    )
                    state.snapshots[key] = _SearchSnapshot(in_flight=producer, order=state.order)
                    continue

                await state.condition.wait()

        if payload is None:
            assert producer is not None
            payload = await asyncio.shield(producer)
        decoded: list[dict[str, object]] = json.loads(payload)
        return decoded

    async def search_album_folders(
        self,
        query: SearchRequest,
    ) -> tuple[list[AlbumFolder], list[dict[str, object]]]:
        """Search slskd and return (album_folders, raw_responses).

        Raw responses are returned so the caller can inspect individual files for
        per-track enqueueing. Audio-only grouping is performed here; the caller
        applies scoring and selection.
        """
        from app.services.slskd_scoring import group_slskd_files_into_folders

        if not self._base_url or not self._api_key:
            return [], []
        search_text = compose_search_query(query.query, query.artist, query.album, query.track)
        raw_responses = await self._raw_search(search_text, mode="album", file_limit=500)
        folders = group_slskd_files_into_folders(raw_responses)
        return folders, raw_responses

    async def search(self, query: SearchRequest) -> list[SearchResult]:
        if not self._base_url or not self._api_key:
            return []
        search_text = compose_search_query(query.query, query.artist, query.album, query.track)
        raw_responses = await self._raw_search(search_text, mode="ordinary", file_limit=100)
        results: list[SearchResult] = []
        for response in raw_responses:
            username = response.get("username", "")
            files = response.get("files", [])
            if not isinstance(files, list):
                continue
            for f in files:
                if not isinstance(f, dict):
                    continue
                filename = str(f.get("filename", ""))
                ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
                if not is_importable_audio(filename):
                    continue
                guess = parse_filename(filename)
                results.append(
                    SearchResult(
                        source="slskd",
                        title=guess.title,
                        artist=guess.artist,
                        album=guess.album,
                        duration_sec=slskd_file_duration_seconds(f),
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
        if not is_importable_audio(filename):
            raise ProviderError(
                "invalid_result", "slskd result is not an importable audio file", "acquire"
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
        # The queue changed after a successful POST. Drop any pre-enqueue snapshot so
        # the first poll cannot falsely report the accepted transfer as missing.
        _download_snapshots.pop(self._download_snapshot_key(), None)
        data = resp.json() if resp.content else {}
        response_id = (
            (data.get("id") or data.get("transferId")) if isinstance(data, dict) else None
        )
        transfer_id = str(response_id or slskd_fallback_transfer_id(username, filename))
        return transfer_id

    async def _fetch_downloads(self) -> list[dict[str, object]]:
        async with self._client() as client:
            for attempt in range(_TRANSFER_429_MAX_ATTEMPTS):
                resp = await request_with_retry(
                    client,
                    "GET",
                    "/api/v0/transfers/downloads",
                    retry_status_codes=frozenset({500, 502, 503, 504}),
                )
                if resp.status_code != 429:
                    resp.raise_for_status()
                    return _flatten_downloads(resp.json())
                if attempt == _TRANSFER_429_MAX_ATTEMPTS - 1:
                    raise ProviderError(
                        "slskd_http_429",
                        "slskd download status polling was rate limited",
                        "acquire",
                        True,
                    )
                exponential_delay = min(
                    _TRANSFER_429_BACKOFF_INITIAL_SEC * (2.0**attempt),
                    _TRANSFER_429_BACKOFF_MAX_SEC,
                )
                jitter = max(0.0, _transfer_jitter(0.0, _TRANSFER_429_JITTER_MAX_SEC))
                await _transfer_sleep(
                    min(exponential_delay + jitter, _TRANSFER_429_BACKOFF_MAX_SEC)
                )
        raise RuntimeError("slskd download polling exhausted without a response")

    def _download_snapshot_key(self) -> tuple[str, bytes]:
        credential_key = hashlib.sha256(self._api_key.encode()).digest()
        return self._base_url, credential_key

    async def downloads(self, *, force_refresh: bool = False) -> list[dict[str, object]]:
        """Return a short-lived, configuration-isolated downloads snapshot."""
        key = self._download_snapshot_key()
        while True:
            snapshot = _download_snapshots.get(key)
            if snapshot is None or (force_refresh and snapshot.in_flight is None):
                snapshot = _DownloadSnapshot()
                _download_snapshots[key] = snapshot
            if snapshot.downloads is not None and _monotonic() < snapshot.expires_at:
                return snapshot.downloads

            task = snapshot.in_flight
            if task is None:
                task = asyncio.create_task(self._fetch_downloads())
                snapshot.in_flight = task
            try:
                downloads = await asyncio.shield(task)
            except asyncio.CancelledError:
                raise
            except Exception:
                if _download_snapshots.get(key) is not snapshot:
                    force_refresh = False
                    continue
                if task.done():
                    _download_snapshots.pop(key)
                raise

            if _download_snapshots.get(key) is not snapshot:
                # An enqueue or removal displaced this generation while the GET was
                # in flight. Join the replacement generation rather than returning
                # pre-mutation data or forcing a redundant third request.
                force_refresh = False
                continue
            snapshot.downloads = downloads
            snapshot.expires_at = _monotonic() + _DOWNLOAD_SNAPSHOT_TTL_SEC
            snapshot.in_flight = None
            return downloads

    async def status(self, transfer_id: str, *, force_refresh: bool = False) -> CapabilityState:
        for item in await self.downloads(force_refresh=force_refresh):
            provider_id = item.get("id") or item.get("transferId")
            fallback_id = slskd_fallback_transfer_id(
                str(item.get("username") or ""), str(item.get("filename") or "")
            )
            if transfer_id in {str(provider_id) if provider_id is not None else "", fallback_id}:
                state = str(item.get("state") or item.get("status") or "queued").casefold()
                return CapabilityState(True, state, dict(item))
        return CapabilityState(False, "transfer not found", {"transfer_id": transfer_id})

    async def match_provisional_transfer(
        self, username: str, filename: str, *, force_refresh: bool = False
    ) -> ProvisionalTransferMatch:
        """Return evidence only when a provisional peer/path has one exact live match."""
        expected_filename = filename.replace("\\", "/")
        matches = [
            dict(item)
            for item in await self.downloads(force_refresh=force_refresh)
            if str(item.get("username") or "") == username
            and str(item.get("filename") or "").replace("\\", "/") == expected_filename
        ]
        return ProvisionalTransferMatch(
            match_count=len(matches),
            transfer=matches[0] if len(matches) == 1 else None,
        )

    async def remove_exact(self, username: str, provider_uuid: str) -> None:
        """Delete one exact canonical UUID; callers must freshly verify identity/absence."""
        from app.services.acquisition_attempts import canonical_provider_uuid

        canonical = canonical_provider_uuid(provider_uuid)
        if canonical is None:
            raise ValueError("slskd cleanup requires a canonical provider UUID")
        safe_username = quote(username, safe="")
        safe_transfer_id = quote(canonical, safe="")
        async with self._client() as client:
            resp = await request_with_retry(
                client,
                "DELETE",
                f"/api/v0/transfers/downloads/{safe_username}/{safe_transfer_id}",
                params={"remove": "true"},
            )
            if resp.status_code != 404:
                resp.raise_for_status()
        _download_snapshots.pop(self._download_snapshot_key(), None)

    async def cancel(self, username: str, filename: str, transfer_id: str | None = None) -> bool:
        """Remove one tracked download, optionally requiring its exact provider ID."""
        expected_filename = filename.replace("\\", "/")
        resolved_transfer_id: str | None = None
        matched_identity = False
        for item in await self.downloads(force_refresh=True):
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
            fallback_id = slskd_fallback_transfer_id(username, filename)
            # A persisted fallback identity cannot distinguish this transfer from a
            # replacement using the same peer/path. Keep the cleanup obligation for
            # later/manual reconciliation rather than report a false success.
            return not (matched_identity and transfer_id == fallback_id)

        from app.services.acquisition_attempts import canonical_provider_uuid

        if canonical_provider_uuid(resolved_transfer_id) is None:
            return False
        await self.remove_exact(username, resolved_transfer_id)
        return True
