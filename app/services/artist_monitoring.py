from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings, get_settings
from app.database import get_session_factory
from app.jobs.dispatcher import job_dispatcher
from app.models.catalog_entities import CatalogAlbum, CatalogArtist
from app.models.job import Job, JobStatus
from app.services.catalog_metadata import fetch_and_store_discography
from app.settings_service import build_effective_settings, get_runtime_settings

logger = logging.getLogger(__name__)
_ACTIVE_JOB_STATES = {JobStatus.pending, JobStatus.running}


def apply_monitor_policy(artist: CatalogArtist, new_albums: list[CatalogAlbum]) -> None:
    if not artist.monitored:
        return
    for album in new_albums:
        if artist.monitor_policy == "none_new":
            album.monitored = False
        elif artist.monitor_policy == "albums_only":
            album.monitored = (album.release_type or "album").casefold() == "album"
        else:
            album.monitored = True


def wanted_albums(artist: CatalogArtist) -> list[CatalogAlbum]:
    return [album for album in artist.albums if album.monitored and not album.in_library]


async def refresh_monitored_artist(
    db: AsyncSession,
    settings: Settings,
    artist: CatalogArtist,
    *,
    auto_download: bool = False,
) -> tuple[list[CatalogAlbum], list[int]]:
    before = {album.id for album in artist.albums}
    await fetch_and_store_discography(db, settings, artist)
    await db.refresh(artist, ["albums"])
    new = [album for album in artist.albums if album.id not in before]
    apply_monitor_policy(artist, new)
    artist.last_refreshed_at = datetime.now(tz=UTC)

    job_ids: list[int] = []
    if auto_download:
        wanted = wanted_albums(artist)
        wanted_ids = [album.id for album in wanted]
        active_album_ids: set[int] = set()
        if wanted_ids:
            result = await db.execute(
                select(Job.catalog_album_id).where(
                    Job.catalog_album_id.in_(wanted_ids),
                    Job.status.in_(_ACTIVE_JOB_STATES),
                )
            )
            active_album_ids = {album_id for album_id in result.scalars() if album_id is not None}
        for album in wanted:
            if album.id in active_album_ids:
                continue
            job = Job(
                source="priority",
                query=f"{artist.name} {album.title}",
                status=JobStatus.pending,
                catalog_album_id=album.id,
            )
            db.add(job)
            await db.flush()
            job_ids.append(job.id)
            active_album_ids.add(album.id)
    await db.flush()
    return new, job_ids


class DiscographyRefreshScheduler:
    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _refresh_cycle(self) -> float:
        factory = get_session_factory()
        async with factory() as db:
            cfg = await build_effective_settings(db, get_settings())
            runtime = await get_runtime_settings(db)
            result = await db.execute(
                select(CatalogArtist.id).where(CatalogArtist.monitored.is_(True))
            )
            artist_ids = list(result.scalars().all())

        for artist_id in artist_ids:
            job_ids: list[int] = []
            try:
                async with factory() as db:
                    artist_result = await db.execute(
                        select(CatalogArtist)
                        .where(CatalogArtist.id == artist_id)
                        .options(selectinload(CatalogArtist.albums))
                    )
                    artist = artist_result.scalar_one_or_none()
                    if artist is None or not artist.monitored:
                        continue
                    _, job_ids = await refresh_monitored_artist(
                        db,
                        cfg,
                        artist,
                        auto_download=runtime.auto_download_wanted,
                    )
                    await db.commit()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Discography refresh failed for artist %s", artist_id)
                continue
            for job_id in job_ids:
                await job_dispatcher.dispatch(job_id)

        return runtime.discography_refresh_hours * 3600

    async def _run(self) -> None:
        while not self._stop.is_set():
            delay = 3600.0
            try:
                delay = await self._refresh_cycle()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Discography refresh cycle failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except TimeoutError:
                continue
