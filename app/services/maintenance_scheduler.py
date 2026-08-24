from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from app.config import get_settings
from app.database import get_session_factory
from app.services.library_scan import scan_library_filesystem
from app.services.maintenance_state import MaintenanceState
from app.services.maintenance_workflows import (
    clean_safe_library_duplicates,
    scan_library_duplicates,
)
from app.settings_service import build_effective_settings, get_runtime_settings

logger = logging.getLogger(__name__)


class MaintenanceScheduler:
    def __init__(self, maintenance_state: MaintenanceState) -> None:
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._state = maintenance_state
        self._initial_cycle_complete = asyncio.Event()
        self._last_library_scan: float | None = None
        self._last_duplicate_scan: float | None = None

    async def start(self, *, wait_for_initial_cycle: bool = False) -> None:
        if self._task is None:
            self._stop.clear()
            self._initial_cycle_complete.clear()
            self._task = asyncio.create_task(self._run())
        if wait_for_initial_cycle:
            await self._initial_cycle_complete.wait()

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _refresh_cycle(self) -> float:
        now = time.monotonic()
        factory = get_session_factory()
        async with factory() as db:
            cfg = await build_effective_settings(db, get_settings())
            runtime = await get_runtime_settings(db)
            enabled_intervals = [
                hours * 3600
                for hours in (runtime.library_scan_hours, runtime.duplicate_scan_hours)
                if hours > 0
            ]
            if not enabled_intervals:
                return 3600.0

            if runtime.library_scan_hours > 0 and (
                self._last_library_scan is None
                or now - self._last_library_scan >= runtime.library_scan_hours * 3600
            ):
                result = await scan_library_filesystem(db, library_root=cfg.library_root)
                self._state.store_library_scan(result)
                self._last_library_scan = now

            if runtime.duplicate_scan_hours > 0 and (
                self._last_duplicate_scan is None
                or now - self._last_duplicate_scan >= runtime.duplicate_scan_hours * 3600
            ):
                duplicate_summary = await scan_library_duplicates(
                    db,
                    library_root=cfg.library_root,
                    quality_profile=runtime.quality_profile,
                )
                self._state.store_duplicate_scan(duplicate_summary)
                self._last_duplicate_scan = now
                if runtime.duplicate_auto_clean:
                    await clean_safe_library_duplicates(
                        db,
                        library_root=cfg.library_root,
                        quality_profile=runtime.quality_profile,
                        latest_scan=duplicate_summary,
                    )
                    await db.commit()

        return float(min(enabled_intervals))

    async def _run(self) -> None:
        while not self._stop.is_set():
            delay = 3600.0
            try:
                delay = await self._refresh_cycle()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Maintenance cycle failed")
            finally:
                self._initial_cycle_complete.set()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except TimeoutError:
                continue
