from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models.job import Job, JobStatus
from app.services.catalog import LibraryStats, TrackRow, get_library_stats, list_library_tracks
from app.services.health_status import CachedProviderStatus

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


def _provider_readiness(
    settings: Settings, provider_snapshot: dict[str, CachedProviderStatus]
) -> list[ProviderReadiness]:
    youtube_state = provider_snapshot.get("youtube")
    tidal_state = provider_snapshot.get("tidal")
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
            configured=bool(youtube_state and youtube_state.available),
            detail=(
                "Local yt-dlp backend is ready"
                if youtube_state and youtube_state.available
                else (
                    "YouTube local readiness check unavailable"
                    if youtube_state and youtube_state.reason == "Not checked"
                    else (
                        youtube_state.reason
                        if youtube_state and youtube_state.reason
                        else "yt-dlp is unavailable"
                    )
                )
            ),
        ),
        ProviderReadiness(
            name="TIDAL",
            configured=bool(tidal_state and tidal_state.available),
            detail=(
                "Local profile and session are ready"
                if tidal_state and tidal_state.available
                else (
                    "TIDAL local readiness check unavailable"
                    if tidal_state and tidal_state.reason == "Not checked"
                    else (
                        tidal_state.reason
                        if tidal_state and tidal_state.reason
                        else "TIDAL is unavailable"
                    )
                )
            ),
        ),
    ]


async def get_dashboard_data(
    db: AsyncSession, settings: Settings, provider_snapshot: dict[str, CachedProviderStatus]
) -> DashboardData:
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
    recent_library = await list_library_tracks(db, sort="added", per_page=_RECENT_LIMIT)

    return DashboardData(
        library=library,
        job_counts=job_counts,
        recent_jobs=list(jobs_result.scalars().all()),
        recent_tracks=recent_library.items,
        providers=_provider_readiness(settings, provider_snapshot),
    )
