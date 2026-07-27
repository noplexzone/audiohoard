from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import case, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.metadata.filename_parse import (
    normalize_for_catalog_match,
    parsed_position_evidence,
    strip_non_identity_descriptors,
)
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.models.release import Release
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import AcoustIDVerificationState, AcquisitionState, ImportWorkflowState
from app.services.auto_import import try_auto_import_release


async def recover_approved_downloads(
    db: AsyncSession, settings: Settings, *, limit: int = 25
) -> int:
    """Bounded, idempotent repair of legacy fully-approved catalog releases."""
    releases = list(
        (
            await db.scalars(
                select(Release)
                .where(
                    Release.import_state != ImportWorkflowState.imported,
                    Release.tracks.any(),
                    ~Release.tracks.any(
                        or_(
                            Track.acquisition_state != AcquisitionState.downloaded,
                            Track.acoustid_verification_state.is_(None),
                            Track.acoustid_verification_state.not_in(
                                {
                                    AcoustIDVerificationState.approved,
                                    AcoustIDVerificationState.verified,
                                }
                            ),
                        )
                    ),
                )
                .options(selectinload(Release.job))
                .order_by(
                    case((Release.recovery_attempted_at.is_(None), 0), else_=1),
                    Release.recovery_attempted_at,
                    Release.id,
                )
                .limit(limit)
            )
        ).all()
    )
    recovered = 0
    for release in releases:
        release.recovery_attempted_at = datetime.now(UTC)
        tracks = list(
            (
                await db.scalars(
                    select(Track).where(Track.release_id == release.id).order_by(Track.id)
                )
            ).all()
        )
        if not tracks or any(
            t.acquisition_state != AcquisitionState.downloaded
            or t.acoustid_verification_state
            not in {AcoustIDVerificationState.approved, AcoustIDVerificationState.verified}
            for t in tracks
        ):
            continue
        album_id = next((t.catalog_album_id for t in tracks if t.catalog_album_id), None)
        if album_id is None and release.job is not None:
            album_id = release.job.catalog_album_id
        album = None
        if album_id is not None:
            album = await db.scalar(
                select(CatalogAlbum)
                .where(CatalogAlbum.id == album_id)
                .options(selectinload(CatalogAlbum.tracks))
            )
        elif release.release_mbid:
            album = await db.scalar(
                select(CatalogAlbum)
                .where(CatalogAlbum.mbid == release.release_mbid)
                .options(selectinload(CatalogAlbum.tracks))
            )
        if album is None or (release.track_count and len(tracks) != release.track_count):
            continue
        unused = {cat.id: cat for cat in album.tracks}
        assignments: list[tuple[Track, CatalogAlbumTrack]] = []
        for track in tracks:
            if track.catalog_track_id in unused:
                assignments.append((track, unused.pop(track.catalog_track_id)))
                continue
            evidence = parsed_position_evidence(
                Path(track.staging_path or track.source_path or track.title or "").name
            )
            candidates = [
                cat
                for cat in unused.values()
                if evidence.get("track_no") == cat.position and evidence.get("disc", 1) == cat.disc
            ]
            if len(candidates) != 1:
                title = normalize_for_catalog_match(
                    strip_non_identity_descriptors(
                        track.title or Path(track.source_path or "").stem
                    )
                )
                candidates = [
                    cat
                    for cat in unused.values()
                    if normalize_for_catalog_match(strip_non_identity_descriptors(cat.title))
                    == title
                ]
            if len(candidates) != 1:
                assignments = []
                break
            cat = candidates[0]
            assignments.append((track, cat))
            unused.pop(cat.id)
        if not assignments or unused:
            continue
        for track, cat in assignments:
            track.catalog_album_id = album.id
            track.catalog_track_id = cat.id
            track.title = cat.title
            track.disc = cat.disc
            track.track_no = cat.position
            track.duration_sec = track.duration_sec or cat.duration_sec
            track.mbid = track.mbid or cat.recording_mbid
            if track.mbid:
                track.identity_state = IdentityResolutionState.resolved
        await db.flush()
        if await try_auto_import_release(
            db,
            release,
            library_root=settings.library_root,
            naming_template=settings.naming_template,
        ):
            recovered += 1
    return recovered
