from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models.job import Job, JobStatus
from app.models.track import Track
from app.services.catalog import LibraryStats, TrackRow, get_library_stats, to_track_row
from app.sources.base import CapabilityState
from app.sources.tidal import TidalAdapter
from app.sources.youtube import YouTubeAdapter

logger = logging.getLogger(__name__)
_RECENT_LIMIT = 6


@dataclass(frozen=True)
class ProviderReadiness:
    name: str
    configured: bool
    detail: str


@dataclass
class DashboardData:
    library: LibraryStats
    job_counts: dict[str, int]
    recent_jobs: list[Job]
    recent_tracks: list[TrackRow]
    providers: list[ProviderReadiness]


async def _safe_local_health(name: str, check: Awaitable[CapabilityState]) -> CapabilityState:
    try:
        return await check
    except Exception:
        logger.warning("%s dashboard readiness check failed", name, exc_info=True)
        return CapabilityState(False, f"{name} local readiness check unavailable", {})


async def _provider_readiness(settings: Settings) -> list[ProviderReadiness]:
    youtube_state, tidal_state = await asyncio.gather(
        _safe_local_health("YouTube", YouTubeAdapter(settings.ytdlp_cookies_file).local_health()),
        _safe_local_health(
            "TIDAL",
            TidalAdapter(
                settings.tidal_config_path,
                settings.tidal_session_path,
                settings.tidal_quality,
            ).local_health(),
        ),
    )
    return [
        ProviderReadiness(
            name="slskd",
            configured=settings.slskd_configured,
            detail="URL and API key configured"
            if settings.slskd_configured
            else "Add a URL and API key",
        ),
        ProviderReadiness(
            name="Prowlarr",
            configured=settings.prowlarr_configured and settings.sabnzbd_configured,
            detail=(
                "Indexer and SABnzbd configured"
                if settings.prowlarr_configured and settings.sabnzbd_configured
                else "Add Prowlarr and SABnzbd credentials"
            ),
        ),
        ProviderReadiness(
            name="YouTube",
            configured=youtube_state.available,
            detail=(
                "Local yt-dlp backend is ready"
                if youtube_state.available
                else (youtube_state.reason or "yt-dlp is unavailable")
            ),
        ),
        ProviderReadiness(
            name="TIDAL",
            configured=tidal_state.available,
            detail=(
                "Local profile and session are ready"
                if tidal_state.available
                else (tidal_state.reason or "TIDAL is unavailable")
            ),
        ),
    ]


async def get_dashboard_data(db: AsyncSession, settings: Settings) -> DashboardData:
    """Load dashboard aggregates and bounded recent activity from persisted data."""
    library = await get_library_stats(db)

    status_rows = await db.execute(
        select(Job.status, func.count(Job.id))
        .where(Job.queue_hidden.is_(False))
        .group_by(Job.status)
    )
    job_counts = {status.value: 0 for status in JobStatus}
    for status, count in status_rows:
        job_counts[status.value] = int(count)

    jobs_result = await db.execute(
        select(Job)
        .where(Job.queue_hidden.is_(False))
        .order_by(Job.created_at.desc(), Job.id.desc())
        .limit(_RECENT_LIMIT)
    )
    tracks_result = await db.execute(select(Track).order_by(Track.id.desc()).limit(_RECENT_LIMIT))

    return DashboardData(
        library=library,
        job_counts=job_counts,
        recent_jobs=list(jobs_result.scalars().all()),
        recent_tracks=[to_track_row(track) for track in tracks_result.scalars().all()],
        providers=await _provider_readiness(settings),
    )
