from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.schemas.health import SourceStatus
from app.schemas.settings import validate_provider_url_value
from app.settings_service import load_raw_db_values, resolve_for_probe
from app.sources.base import CapabilityState
from app.sources.prowlarr import ProwlarrAdapter
from app.sources.sabnzbd import SabnzbdAdapter
from app.sources.slskd import SlskdAdapter
from app.sources.tidal import TidalAdapter
from app.sources.youtube import YouTubeAdapter

logger = logging.getLogger(__name__)
PROVIDERS = ("slskd", "prowlarr", "sabnzbd", "youtube", "tidal")


@dataclass(frozen=True)
class CachedProviderStatus:
    status: SourceStatus
    checked_at: datetime | None = None
    elapsed_ms: int | None = None

    @property
    def available(self) -> bool:
        return self.status.available

    @property
    def reason(self) -> str | None:
        return self.status.reason

    @property
    def details(self) -> dict[str, object]:
        return self.status.details

    @property
    def checked_age_seconds(self) -> int | None:
        if self.checked_at is None:
            return None
        return max(0, int((datetime.now(UTC) - self.checked_at).total_seconds()))


_NOT_CHECKED = CachedProviderStatus(
    SourceStatus(available=False, reason="Not checked", details={})
)


@dataclass
class HealthStatusService:
    ttl_seconds: int = 60
    probe_timeout_seconds: float = 3.0
    _cache: dict[str, CachedProviderStatus] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _task: asyncio.Task[None] | None = None
    _stop: asyncio.Event = field(default_factory=asyncio.Event)

    def snapshot(self, providers: Iterable[str] = PROVIDERS) -> dict[str, CachedProviderStatus]:
        return {provider: self._cache.get(provider, _NOT_CHECKED) for provider in providers}

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="health-status-refresh")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        from app.database import get_session_factory

        factory = get_session_factory()
        while not self._stop.is_set():
            try:
                async with factory() as db:
                    await self.refresh_all(db, get_settings())
            except Exception:
                logger.warning("health status background refresh failed", exc_info=True)
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self.ttl_seconds)

    async def refresh_all(
        self, db: AsyncSession, env: Settings
    ) -> dict[str, CachedProviderStatus]:
        raw_db = await load_raw_db_values(db, env.secret_key)
        results = await asyncio.gather(
            *(self._refresh_provider(provider, env, raw_db) for provider in PROVIDERS),
            return_exceptions=True,
        )
        async with self._lock:
            for provider, result in zip(PROVIDERS, results, strict=True):
                if isinstance(result, BaseException):
                    logger.warning("health probe failed for %s", provider, exc_info=result)
                    self._cache[provider] = CachedProviderStatus(
                        SourceStatus(available=False, reason="Probe failed", details={}),
                        checked_at=datetime.now(UTC),
                    )
                else:
                    self._cache[provider] = result
            return self.snapshot()

    async def refresh_provider(
        self, provider: str, db: AsyncSession, env: Settings
    ) -> CachedProviderStatus:
        if provider not in PROVIDERS:
            return CachedProviderStatus(
                SourceStatus(available=False, reason="Unknown provider", details={}),
                datetime.now(UTC),
            )
        raw_db = await load_raw_db_values(db, env.secret_key)
        status = await self._refresh_provider(provider, env, raw_db)
        async with self._lock:
            self._cache[provider] = status
        return status

    async def _refresh_provider(
        self, provider: str, env: Settings, raw_db: dict[str, str]
    ) -> CachedProviderStatus:
        def r(key: str) -> str:
            return resolve_for_probe(key, "", env, raw_db)

        started = time.perf_counter()
        try:
            cap = await asyncio.wait_for(
                self._probe(provider, r), timeout=self.probe_timeout_seconds
            )
            status = SourceStatus(available=cap.available, reason=cap.reason, details=cap.extra)
        except TimeoutError:
            status = SourceStatus(
                available=False,
                reason="Probe timed out",
                details={"timeout_seconds": self.probe_timeout_seconds},
            )
        elapsed = int((time.perf_counter() - started) * 1000)
        return CachedProviderStatus(
            status=status, checked_at=datetime.now(UTC), elapsed_ms=elapsed
        )

    async def _probe(self, provider: str, r: Callable[[str], str]) -> CapabilityState:
        if provider in {"slskd", "prowlarr", "sabnzbd"}:
            url = r(f"{provider}_url")
            key = r(f"{provider}_api_key")
            if not (url and key):
                return CapabilityState(False, "Not configured")
            try:
                validate_provider_url_value(url)
            except ValueError:
                return CapabilityState(
                    False,
                    "Provider URL is not allowed",
                    {"code": "invalid_provider_url", "retryable": False},
                )
            if provider == "slskd":
                return await SlskdAdapter(url, key).health()
            if provider == "prowlarr":
                return await ProwlarrAdapter(url, key).health()
            return await SabnzbdAdapter(url, key).health()
        if provider == "youtube":
            return await YouTubeAdapter(r("ytdlp_cookies_file"), 3.0).local_health()
        if provider == "tidal":
            return await TidalAdapter(
                r("tidal_config_path"), r("tidal_session_path"), r("tidal_quality")
            ).local_health()
        return CapabilityState(False, "Unknown provider")


_service = HealthStatusService()


def get_health_status_service() -> HealthStatusService:
    return _service
