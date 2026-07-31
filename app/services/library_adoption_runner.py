from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.services.library_adoption import run_queued_library_adoption_scans


class LibraryAdoptionRunner:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        library_root: Path,
        *,
        interval_seconds: float = 2.0,
    ) -> None:
        self._session_factory = session_factory
        self._library_root = library_root
        self._interval_seconds = interval_seconds
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop(), name="library-adoption-runner")

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
            await run_queued_library_adoption_scans(
                self._session_factory, library_root=self._library_root
            )
            self._wake.clear()
            with suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._interval_seconds)
