from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.services.library_adoption import (
    recover_library_adoption_scans,
    run_library_adoption_scan,
)
from app.settings_service import build_effective_settings

logger = logging.getLogger(__name__)


class LibraryAdoptionRunner:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        interval_seconds: float = 2.0,
    ) -> None:
        self._session_factory = session_factory
        self._interval_seconds = interval_seconds
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._initial_cycle_complete = asyncio.Event()

    async def start(self, *, wait_for_initial_cycle: bool = False) -> None:
        if self._task is None or self._task.done():
            self._initial_cycle_complete.clear()
            self._task = asyncio.create_task(self._loop(), name="library-adoption-runner")
        if wait_for_initial_cycle:
            await self._initial_cycle_complete.wait()

    def wake(self) -> None:
        self._wake.set()

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                async with self._session_factory() as db:
                    scan_ids = await recover_library_adoption_scans(db)
                for scan_id in scan_ids:
                    try:
                        async with self._session_factory() as db:
                            effective = await build_effective_settings(db, get_settings())
                            await run_library_adoption_scan(
                                db,
                                scan_id=scan_id,
                                library_root=effective.library_root,
                            )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("library adoption scan %s failed; will retry", scan_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("library adoption runner iteration failed; will retry")
            finally:
                self._initial_cycle_complete.set()
            self._wake.clear()
            with suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._interval_seconds)
