from __future__ import annotations

import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import CatalogAlbumProvider, CatalogArtist
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.monitoring import _monitoring_profile_from_runtime, current_release_quality
from app.settings_service import get_runtime_settings


async def _latest_imported_release_id_for_album(db: AsyncSession, album_id: int) -> int | None:
    return (
        await db.scalars(
            select(Release.id)
            .join(Track, Track.release_id == Release.id)
            .where(
                Track.catalog_album_id == album_id,
                Track.import_state == ImportWorkflowState.imported,
            )
            .order_by(Release.id.desc())
            .limit(1)
        )
    ).first()


async def _set_release_upgrade_monitoring(
    db: AsyncSession, release_id: int, enabled: bool
) -> None:
    record = (
        await db.scalars(
            select(MonitoringRecord).where(MonitoringRecord.release_id == release_id).limit(1)
        )
    ).first()
    if not enabled:
        if record is not None:
            record.status = MonitoringStatus.paused
            record.candidate_id = None
        return
    runtime = await get_runtime_settings(db)
    profile = _monitoring_profile_from_runtime(runtime)
    baseline_quality = await current_release_quality(db, release_id)
    history = json.dumps([{"outcome": "watch_created", "baseline_quality": baseline_quality}])
    if record is None:
        db.add(
            MonitoringRecord(
                release_id=release_id,
                status=MonitoringStatus.active,
                desired_quality_json=profile.to_json(),
                history_json=history,
            )
        )
    else:
        record.status = MonitoringStatus.active
        desired_quality_json = profile.to_json()
        if record.desired_quality_json != desired_quality_json:
            record.desired_quality_json = desired_quality_json


async def sync_album_upgrade_monitoring(db: AsyncSession, album_id: int, enabled: bool) -> None:
    release_id = await _latest_imported_release_id_for_album(db, album_id)
    if release_id is not None:
        await _set_release_upgrade_monitoring(db, release_id, enabled)


async def sync_artist_upgrade_monitoring(
    db: AsyncSession, artist: CatalogArtist, releases: list[CatalogAlbumProvider]
) -> None:
    """Project provider monitoring to each canonical album with OR semantics."""
    enabled_by_album: dict[int, bool] = {}
    for release in releases:
        if release.catalog_album_id is None:
            continue
        enabled_by_album[release.catalog_album_id] = bool(
            enabled_by_album.get(release.catalog_album_id, False)
            or (artist.monitored and artist.watchlist_monitor_upgrades and release.monitored)
        )
    for album_id, enabled in enabled_by_album.items():
        await sync_album_upgrade_monitoring(db, album_id, enabled)
