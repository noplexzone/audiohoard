from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings, get_settings
from app.database import get_session_factory
from app.jobs.dispatcher import job_dispatcher
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.job import Job, JobStatus
from app.services.catalog_metadata import (
    album_has_provider,
    fetch_and_store_discography,
    release_bucket,
)
from app.services.catalog_ownership import reconcile_deezer_catalog_ownership
from app.services.release_editions import (
    apply_release_monitoring_policy,
    project_release_families,
    sync_canonical_monitoring,
)
from app.services.upgrade_monitoring import sync_artist_upgrade_monitoring
from app.settings_service import build_effective_settings, get_runtime_settings

logger = logging.getLogger(__name__)
_ACTIVE_JOB_STATES = {JobStatus.pending, JobStatus.running}


def apply_monitor_policy(
    artist: CatalogArtist, releases: list[CatalogAlbumProvider] | list[CatalogAlbum]
) -> None:
    """Apply the artist policy to releases from the selected provider only."""
    if releases and isinstance(releases[0], CatalogAlbumProvider):
        apply_release_monitoring_policy(artist, releases)
        return
    if not artist.monitored:
        for release in releases:
            release.monitored = False
        return
    for release in releases:
        if isinstance(release, CatalogAlbum):
            if (
                not album_has_provider(release, artist.watchlist_provider or "")
                or artist.monitor_policy == "none_new"
            ):
                release.monitored = False
            elif artist.monitor_policy == "albums_only":
                release.monitored = release_bucket(release.release_type) == "album"
            else:
                bucket = release_bucket(release.release_type)
                watch_albums = (
                    True
                    if artist.watchlist_release_albums is None
                    else artist.watchlist_release_albums
                )
                watch_singles = (
                    False
                    if artist.watchlist_release_singles is None
                    else artist.watchlist_release_singles
                )
                watch_eps = (
                    False if artist.watchlist_release_eps is None else artist.watchlist_release_eps
                )
                release.monitored = (
                    (bucket == "album" and watch_albums)
                    or (bucket == "single" and watch_singles)
                    or (bucket == "ep" and watch_eps)
                )
            continue
        if artist.monitor_policy == "none_new":
            release.monitored = False
        elif artist.monitor_policy == "albums_only":
            release.monitored = release.release_kind == "album"
        else:
            watch_albums = (
                True
                if artist.watchlist_release_albums is None
                else artist.watchlist_release_albums
            )
            watch_singles = (
                False
                if artist.watchlist_release_singles is None
                else artist.watchlist_release_singles
            )
            watch_eps = (
                False if artist.watchlist_release_eps is None else artist.watchlist_release_eps
            )
            release.monitored = (
                (release.release_kind == "album" and watch_albums)
                or (release.release_kind == "single" and watch_singles)
                or (release.release_kind == "ep" and watch_eps)
            )


def _sync_canonical_monitoring(
    artist: CatalogArtist, releases: list[CatalogAlbumProvider]
) -> None:
    sync_canonical_monitoring(artist, releases)


def wanted_releases(releases: list[CatalogAlbumProvider]) -> list[CatalogAlbumProvider]:
    return [
        release
        for release in releases
        if release.monitored
        and release.catalog_album is not None
        and not release.catalog_album.in_library
    ]


async def queue_wanted_artist_releases(db: AsyncSession, artist: CatalogArtist) -> list[int]:
    """Queue missing monitored albums from the current, reconciled library projection."""
    identity = None
    if artist.watchlist_provider:
        identity = (
            await db.scalars(
                select(CatalogArtistIdentity)
                .where(
                    CatalogArtistIdentity.artist_id == artist.id,
                    CatalogArtistIdentity.provider == artist.watchlist_provider,
                )
                .options(
                    selectinload(CatalogArtistIdentity.releases).selectinload(
                        CatalogAlbumProvider.catalog_album
                    )
                )
            )
        ).first()
    if identity is None:
        albums = list(
            (
                await db.scalars(
                    select(CatalogAlbum).where(
                        CatalogAlbum.artist_id == artist.id,
                        CatalogAlbum.monitored.is_(True),
                        CatalogAlbum.in_library.is_(False),
                    )
                )
            ).all()
        )
    else:
        releases = list(identity.releases)
        apply_release_monitoring_policy(artist, releases)
        families = project_release_families(releases)
        family_album_ids = {
            release.catalog_album_id
            for family in families
            for release in family.releases
            if release.catalog_album_id is not None
        }
        active_jobs = (
            list(
                (
                    await db.scalars(
                        select(Job).where(
                            Job.catalog_album_id.in_(family_album_ids),
                            Job.status.in_(_ACTIVE_JOB_STATES),
                        )
                    )
                ).all()
            )
            if family_album_ids
            else []
        )
        jobs_by_album: dict[int, list[Job]] = {}
        for job in active_jobs:
            if job.catalog_album_id is not None:
                jobs_by_album.setdefault(job.catalog_album_id, []).append(job)

        albums = []
        for family in families:
            selected = [
                release
                for release in family.releases
                if release.monitored
                and release.catalog_album is not None
                and not release.catalog_album.in_library
            ]
            explicitly_multi_selected = len({release.catalog_album_id for release in selected}) > 1
            for release in selected:
                album = release.catalog_album
                assert album is not None
                if jobs_by_album.get(album.id):
                    continue
                sibling_ids = {
                    sibling.catalog_album_id
                    for sibling in family.releases
                    if sibling.catalog_album_id is not None
                    and sibling.catalog_album_id != album.id
                    and not sibling.monitored
                }
                sibling_jobs = [
                    job for sibling_id in sibling_ids for job in jobs_by_album.get(sibling_id, [])
                ]
                if not explicitly_multi_selected and any(
                    job.status == JobStatus.running for job in sibling_jobs
                ):
                    continue
                if not explicitly_multi_selected:
                    for job in sibling_jobs:
                        if job.status == JobStatus.pending:
                            job.status = JobStatus.cancelled
                            job.result_json = json.dumps(
                                {
                                    "status": "cancelled",
                                    "reason": "superseded_by_preferred_release_edition",
                                    "preferred_catalog_album_id": album.id,
                                },
                                sort_keys=True,
                            )
                albums.append(album)
    unique_albums = {album.id: album for album in albums}
    album_ids = list(unique_albums)
    active_album_ids: set[int] = set()
    if album_ids:
        active_album_ids = {
            album_id
            for album_id in (
                await db.scalars(
                    select(Job.catalog_album_id).where(
                        Job.catalog_album_id.in_(album_ids),
                        Job.status.in_(_ACTIVE_JOB_STATES),
                    )
                )
            ).all()
            if album_id is not None
        }
    job_ids: list[int] = []
    for album in unique_albums.values():
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
    return job_ids


async def refresh_monitored_artist(
    db: AsyncSession,
    settings: Settings,
    artist: CatalogArtist,
    *,
    auto_download: bool = False,
) -> tuple[list[CatalogAlbumProvider], list[int]]:
    provider = artist.watchlist_provider
    identity = None
    if provider:
        identity = (
            await db.scalars(
                select(CatalogArtistIdentity)
                .where(
                    CatalogArtistIdentity.artist_id == artist.id,
                    CatalogArtistIdentity.provider == provider,
                )
                .options(selectinload(CatalogArtistIdentity.releases))
            )
        ).first()
    before = {release.id for release in identity.releases} if identity is not None else set()
    if provider:
        await fetch_and_store_discography(db, settings, artist, provider_name=provider)
        identity = (
            await db.scalars(
                select(CatalogArtistIdentity)
                .where(
                    CatalogArtistIdentity.artist_id == artist.id,
                    CatalogArtistIdentity.provider == provider,
                )
                .options(
                    selectinload(CatalogArtistIdentity.releases).selectinload(
                        CatalogAlbumProvider.catalog_album
                    )
                )
            )
        ).first()
    releases = list(identity.releases) if identity is not None else []
    if identity is None:
        # Compatibility for direct fixtures and pre-0012 rows. Production migration
        # creates provider snapshots before the scheduler starts.
        new_albums = [album for album in artist.albums if album.id not in before]
        if provider:
            apply_monitor_policy(artist, new_albums)
        job_ids = await queue_wanted_artist_releases(db, artist) if auto_download else []
        artist.last_refreshed_at = datetime.now(tz=UTC)
        await db.flush()
        return [], job_ids
    new = [release for release in releases if release.id not in before]
    apply_monitor_policy(artist, releases)
    _sync_canonical_monitoring(artist, releases)
    await sync_artist_upgrade_monitoring(db, artist, releases)
    artist.last_refreshed_at = datetime.now(tz=UTC)

    normalized_job_ids = await queue_wanted_artist_releases(db, artist) if auto_download else []
    await db.flush()
    return new, normalized_job_ids


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
                        .options(
                            selectinload(CatalogArtist.albums),
                            selectinload(CatalogArtist.identities).selectinload(
                                CatalogArtistIdentity.releases
                            ),
                        )
                    )
                    artist = artist_result.scalar_one_or_none()
                    if artist is None or not artist.monitored:
                        continue
                    await refresh_monitored_artist(
                        db,
                        cfg,
                        artist,
                        auto_download=False,
                    )
                    await db.commit()
                try:
                    await reconcile_deezer_catalog_ownership(
                        factory,
                        cfg,
                        artist_id=artist_id,
                        fail_on_provider_error=True,
                    )
                except Exception:
                    logger.exception(
                        "Catalog ownership reconciliation failed for artist %s", artist_id
                    )
                else:
                    if runtime.auto_download_wanted:
                        async with factory() as db:
                            artist = await db.get(CatalogArtist, artist_id)
                            if artist is not None and artist.monitored:
                                job_ids = await queue_wanted_artist_releases(db, artist)
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
