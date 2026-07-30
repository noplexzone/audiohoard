from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models.job import Job, JobStatus
from app.models.track import Track
from app.services.catalog import (
    LibraryStats,
    TrackRow,
    _library_artifact_filter,
    get_library_stats,
    list_library_tracks,
    track_meets_quality,
)
from app.services.health_status import CachedProviderStatus
from app.settings_service import QualityProfile, get_runtime_settings

logger = logging.getLogger(__name__)
_RECENT_JOB_LIMIT = 4
_RECENT_TRACK_LIMIT = 10


@dataclass(frozen=True)
class ProviderReadiness:
    name: str
    configured: bool
    detail: str


_LOSSLESS_FORMATS = {"flac", "alac", "wav", "aiff", "aif"}
_LOSSY_FORMATS = {"mp3", "aac", "m4a", "mp4", "opus", "ogg", "vorbis"}


@dataclass(frozen=True)
class QualityTierStat:
    name: str
    count: int
    upgrade_eligible: int
    formats: tuple[str, ...]


@dataclass
class _QualityTierBucket:
    count: int = 0
    upgrade_eligible: int = 0
    formats: set[str] | None = None

    def add_format(self, value: str) -> None:
        if self.formats is None:
            self.formats = set()
        self.formats.add(value)

    def format_tuple(self) -> tuple[str, ...]:
        return tuple(sorted(self.formats or set()))


def _format_family(value: str | None) -> str:
    normalized = (value or "").strip().casefold().lstrip(".")
    if normalized in {"m4a", "mp4", "aac"}:
        return "aac"
    if normalized.startswith("mp3"):
        return "mp3"
    return normalized


def _quality_tier_name(value: str | None) -> str:
    family = _format_family(value)
    if family in _LOSSLESS_FORMATS:
        return "Lossless"
    if family in _LOSSY_FORMATS:
        return "Lossy"
    return "Unknown"


def _format_label(value: str | None) -> str:
    family = _format_family(value)
    return family.upper() if family else "Unknown"


def _profile_counts_as_upgrade_eligible(file_format: str | None, profile: QualityProfile) -> bool:
    return bool((file_format or "").strip()) and not track_meets_quality(file_format, profile)


@dataclass
class DashboardData:
    library: LibraryStats
    job_counts: dict[str, int]
    recent_jobs: list[Job]
    recent_tracks: list[TrackRow]
    providers: list[ProviderReadiness]
    quality_tiers: list[QualityTierStat]
    quality_upgrade_eligible: int


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


async def _get_quality_tier_stats(
    db: AsyncSession, profile: QualityProfile
) -> tuple[list[QualityTierStat], int]:
    rows = (
        await db.execute(
            select(Track.file_format, func.count(Track.id).label("cnt"))
            .where(_library_artifact_filter())
            .group_by(Track.file_format)
        )
    ).all()
    buckets: dict[str, _QualityTierBucket] = {
        "Lossless": _QualityTierBucket(),
        "Lossy": _QualityTierBucket(),
        "Unknown": _QualityTierBucket(),
    }
    for file_format, count_raw in rows:
        count = int(count_raw or 0)
        tier = _quality_tier_name(file_format)
        bucket = buckets[tier]
        bucket.count += count
        bucket.add_format(_format_label(file_format))
        if _profile_counts_as_upgrade_eligible(str(file_format) if file_format else None, profile):
            bucket.upgrade_eligible += count
    stats = [
        QualityTierStat(
            name=name,
            count=bucket.count,
            upgrade_eligible=bucket.upgrade_eligible,
            formats=bucket.format_tuple(),
        )
        for name, bucket in buckets.items()
        if bucket.count > 0
    ]
    return stats, sum(item.upgrade_eligible for item in stats)


async def get_dashboard_data(
    db: AsyncSession, settings: Settings, provider_snapshot: dict[str, CachedProviderStatus]
) -> DashboardData:
    """Load dashboard aggregates and bounded recent activity from persisted data."""
    library = await get_library_stats(db)
    runtime = await get_runtime_settings(db)
    quality_tiers, quality_upgrade_eligible = await _get_quality_tier_stats(
        db, runtime.quality_profile
    )

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
        .limit(_RECENT_JOB_LIMIT)
    )
    recent_library = await list_library_tracks(db, sort="added", per_page=_RECENT_TRACK_LIMIT)

    return DashboardData(
        library=library,
        job_counts=job_counts,
        recent_jobs=list(jobs_result.scalars().all()),
        recent_tracks=recent_library.items,
        providers=_provider_readiness(settings, provider_snapshot),
        quality_tiers=quality_tiers,
        quality_upgrade_eligible=quality_upgrade_eligible,
    )
