from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Literal, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.acquisition_claim import AcquisitionDispatchClaim
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyScopeKind,
)
from app.models.job import Job, JobStatus
from app.services.catalog import (
    _missing_releases_query,
    get_missing_release_ids,
    get_release_progress,
    track_meets_quality,
)
from app.services.catalog_manifest import catalog_manifest_issue
from app.settings_service import QualityProfile, get_runtime_settings

_YEAR = re.compile(r"^\d{4}$")
_RELEASE_TYPES = frozenset({"all", "album", "single_ep", "compilation"})
_MONITORING_STATUSES = frozenset({"all", "monitored", "unmonitored"})
_WANTED_SORTS = frozenset({"year", "artist", "title"})
_WANTED_STATUSES = frozenset({"all", "active", "failed", "needs-search"})
_MANIFEST_REASONS = {
    "catalog_tracks_empty": "catalog_manifest_missing",
    "catalog_tracks_incomplete": "catalog_manifest_incomplete",
    "catalog_tracks_overfull": "catalog_manifest_overfull",
    "catalog_tracks_invalid_positions": "catalog_manifest_invalid_positions",
}


class DiscographyScopeError(ValueError):
    """Raised when a requested batch scope is invalid or unsafe."""


@dataclass(frozen=True, slots=True)
class ArtistDiscographyScope:
    artist_id: int
    provider: str
    release_type: Literal["all", "album", "single_ep", "compilation"]
    year_from: str | None
    year_to: str | None
    monitoring_status: Literal["all", "monitored", "unmonitored"]


@dataclass(frozen=True, slots=True)
class WantedIdsDiscographyScope:
    album_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class WantedAllMatchingDiscographyScope:
    q: str
    sort: Literal["year", "artist", "title"]
    status: Literal["all", "active", "failed", "needs-search"]


DiscographyScope = (
    ArtistDiscographyScope | WantedIdsDiscographyScope | WantedAllMatchingDiscographyScope
)


@dataclass(frozen=True, slots=True)
class DiscographyBatchPreview:
    id: int
    scope_kind: DiscographyScopeKind
    scope_json: str
    scope_hash: str
    state: DiscographyBatchState
    matching_count: int
    complete_count: int
    active_count: int
    hydration_required_count: int
    missing_count: int
    skipped_count: int
    estimated_job_count: int


def _year(value: object, field: str) -> str | None:
    if value is None or value == "":
        return None
    normalized = str(value).strip()
    if not _YEAR.fullmatch(normalized):
        raise DiscographyScopeError(f"{field} must be a 4-digit year or null")
    return normalized


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise DiscographyScopeError(f"{field} must be a positive integer")
    try:
        result = int(str(value))
    except (TypeError, ValueError) as exc:
        raise DiscographyScopeError(f"{field} must be a positive integer") from exc
    if result < 1:
        raise DiscographyScopeError(f"{field} must be a positive integer")
    return result


def canonicalize_scope(
    scope_kind: DiscographyScopeKind | str, payload: dict[str, object]
) -> tuple[DiscographyScope, str]:
    """Parse an immutable scope and return deterministic compact JSON."""
    try:
        kind = DiscographyScopeKind(scope_kind)
    except ValueError as exc:
        raise DiscographyScopeError("unsupported scope kind") from exc
    if kind == DiscographyScopeKind.artist:
        provider = str(payload.get("provider", "")).strip().casefold()
        if not provider:
            raise DiscographyScopeError("provider is required")
        release_type = str(payload.get("release_type", "all")).strip().casefold()
        monitoring = str(payload.get("monitoring_status", "all")).strip().casefold()
        if release_type not in _RELEASE_TYPES:
            raise DiscographyScopeError("release_type is invalid")
        if monitoring not in _MONITORING_STATUSES:
            raise DiscographyScopeError("monitoring_status is invalid")
        year_from = _year(payload.get("year_from"), "year_from")
        year_to = _year(payload.get("year_to"), "year_to")
        if year_from is not None and year_to is not None and year_from > year_to:
            raise DiscographyScopeError("year_from must not be after year_to")
        scope: DiscographyScope = ArtistDiscographyScope(
            artist_id=_positive_int(payload.get("artist_id"), "artist_id"),
            provider=provider,
            release_type=cast(Any, release_type),
            year_from=year_from,
            year_to=year_to,
            monitoring_status=cast(Any, monitoring),
        )
    elif kind in {DiscographyScopeKind.wanted_selected, DiscographyScopeKind.wanted_page}:
        raw_ids = payload.get("album_ids")
        if not isinstance(raw_ids, (list, tuple, set, frozenset)):
            raise DiscographyScopeError("album_ids must be a collection")
        scope = WantedIdsDiscographyScope(
            album_ids=tuple(sorted({_positive_int(value, "album_ids") for value in raw_ids}))
        )
    else:
        q = str(payload.get("q", "")).strip()
        sort = str(payload.get("sort", "year")).strip().casefold()
        status = str(payload.get("status", "all")).strip().casefold()
        if sort not in _WANTED_SORTS:
            raise DiscographyScopeError("sort is invalid")
        if status not in _WANTED_STATUSES:
            raise DiscographyScopeError("status is invalid")
        scope = WantedAllMatchingDiscographyScope(
            q=q, sort=cast(Any, sort), status=cast(Any, status)
        )
    raw = asdict(scope)
    if isinstance(scope, WantedIdsDiscographyScope):
        raw["album_ids"] = list(scope.album_ids)
    return scope, json.dumps(raw, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class _SelectedRelease:
    provider_release_id: int | None
    provider_release_identity: str
    album: CatalogAlbum | None
    artist_name: str
    title: str
    year: str | None
    release_kind: str | None
    provider: str | None
    expected_count: int | None


async def _select_artist_releases(
    db: AsyncSession, scope: ArtistDiscographyScope
) -> list[_SelectedRelease]:
    artist = await db.get(CatalogArtist, scope.artist_id)
    if artist is None:
        raise DiscographyScopeError("artist does not exist")
    identity_id = await db.scalar(
        select(CatalogArtistIdentity.id).where(
            CatalogArtistIdentity.artist_id == scope.artist_id,
            CatalogArtistIdentity.provider == scope.provider,
        )
    )
    if identity_id is None:
        raise DiscographyScopeError("provider is not a persisted identity for artist")
    stmt = select(CatalogAlbumProvider).where(
        CatalogAlbumProvider.artist_identity_id == identity_id
    )
    if scope.release_type == "album":
        stmt = stmt.where(CatalogAlbumProvider.release_kind == "album")
    elif scope.release_type == "single_ep":
        stmt = stmt.where(CatalogAlbumProvider.release_kind.in_(("single", "ep")))
    elif scope.release_type == "compilation":
        stmt = stmt.where(CatalogAlbumProvider.release_kind == "compilation")
    if scope.year_from is not None:
        stmt = stmt.where(CatalogAlbumProvider.year >= scope.year_from)
    if scope.year_to is not None:
        stmt = stmt.where(CatalogAlbumProvider.year <= scope.year_to)
    if scope.monitoring_status == "monitored":
        stmt = stmt.where(CatalogAlbumProvider.monitored.is_(True))
    elif scope.monitoring_status == "unmonitored":
        stmt = stmt.where(CatalogAlbumProvider.monitored.is_(False))
    releases = list(
        (
            await db.scalars(
                stmt.options(
                    selectinload(CatalogAlbumProvider.catalog_album).selectinload(
                        CatalogAlbum.tracks
                    )
                ).order_by(CatalogAlbumProvider.provider_album_id, CatalogAlbumProvider.id)
            )
        ).all()
    )
    return [
        _SelectedRelease(
            provider_release_id=release.id,
            provider_release_identity=f"{scope.provider}:{release.provider_album_id}",
            album=release.catalog_album,
            artist_name=artist.name,
            title=release.title,
            year=release.year,
            release_kind=release.release_kind,
            provider=scope.provider,
            expected_count=max(
                release.track_count or 0,
                release.catalog_album.track_count or 0 if release.catalog_album is not None else 0,
            )
            or None,
        )
        for release in releases
    ]


async def _select_wanted_releases(
    db: AsyncSession,
    kind: DiscographyScopeKind,
    scope: WantedIdsDiscographyScope | WantedAllMatchingDiscographyScope,
) -> list[_SelectedRelease]:
    if isinstance(scope, WantedAllMatchingDiscographyScope):
        ids = await get_missing_release_ids(
            db, q=scope.q, sort=scope.sort, status=scope.status, limit=10_001
        )
        if len(ids) > 10_000:
            raise DiscographyScopeError("wanted_all_matching exceeds 10000 releases")
    else:
        if not scope.album_ids:
            ids = []
        else:
            missing = _missing_releases_query().subquery()
            ids = [
                int(value)
                for value in (
                    await db.scalars(
                        select(missing.c.album_id)
                        .where(missing.c.album_id.in_(scope.album_ids))
                        .order_by(missing.c.album_id)
                    )
                ).all()
            ]
    if not ids:
        return []
    albums = list(
        (
            await db.scalars(
                select(CatalogAlbum)
                .where(CatalogAlbum.id.in_(ids))
                .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
            )
        ).all()
    )
    by_id = {album.id: album for album in albums}
    return [
        _SelectedRelease(
            provider_release_id=None,
            provider_release_identity=f"catalog_album:{album_id}",
            album=by_id[album_id],
            artist_name=by_id[album_id].artist.name,
            title=by_id[album_id].title,
            year=by_id[album_id].year,
            release_kind=by_id[album_id].release_type,
            provider=None,
            expected_count=by_id[album_id].track_count,
        )
        for album_id in ids
        if album_id in by_id
    ]


def _hash(scope_json: str, identities: list[str]) -> str:
    encoded = json.dumps(identities, separators=(",", ":"))
    return hashlib.sha256(f"{scope_json}\n{encoded}".encode()).hexdigest()


async def create_discography_batch_preview(
    db: AsyncSession,
    scope_kind: DiscographyScopeKind | str,
    payload: dict[str, object],
    *,
    quality_profile: QualityProfile | None = None,
) -> DiscographyBatchPreview:
    """Select, classify, and durably persist a provider-I/O-free preview."""
    kind = DiscographyScopeKind(scope_kind)
    scope, scope_json = canonicalize_scope(kind, payload)
    if isinstance(scope, ArtistDiscographyScope):
        releases = await _select_artist_releases(db, scope)
    else:
        releases = await _select_wanted_releases(db, kind, scope)
    identities = [release.provider_release_identity for release in releases]
    if quality_profile is None:
        quality_profile = (await get_runtime_settings(db)).quality_profile

    batch = DiscographyBatch(
        scope_kind=kind,
        scope_json=scope_json,
        scope_hash=_hash(scope_json, identities),
        state=DiscographyBatchState.preview,
        matching_count=len(releases),
    )
    db.add(batch)
    await db.flush()

    actionable: list[_SelectedRelease] = []
    seen_albums: set[int] = set()
    skipped_count = 0
    for release in releases:
        if release.album is None:
            skipped_count += 1
            db.add(
                DiscographyBatchItem(
                    batch_id=batch.id,
                    provider_release_id=release.provider_release_id,
                    artist_name=release.artist_name,
                    release_title=release.title,
                    release_year=release.year,
                    release_kind=release.release_kind,
                    provider=release.provider,
                    state=DiscographyBatchItemState.skipped,
                    reason_code="catalog_release_unbound",
                    skipped_count=1,
                )
            )
        elif release.album.id not in seen_albums:
            seen_albums.add(release.album.id)
            actionable.append(release)

    album_ids = [release.album.id for release in actionable if release.album is not None]
    progress_by_album = await get_release_progress(db, album_ids)
    all_track_ids = {
        track.id
        for release in actionable
        if release.album is not None
        for track in release.album.tracks
        if track.id is not None
    }
    active_track_ids = (
        set(
            int(value)
            for value in (
                await db.scalars(
                    select(AcquisitionDispatchClaim.catalog_track_id)
                    .join(Job, Job.id == AcquisitionDispatchClaim.job_id)
                    .where(
                        AcquisitionDispatchClaim.catalog_track_id.in_(all_track_ids),
                        Job.status.in_((JobStatus.pending, JobStatus.running)),
                    )
                )
            ).all()
        )
        if all_track_ids
        else set()
    )

    complete_count = active_count = hydration_count = missing_count = estimated = 0
    for release in actionable:
        album = release.album
        assert album is not None
        progress = progress_by_album[album.id]
        issue = catalog_manifest_issue(album.tracks, release.expected_count)
        state = DiscographyBatchItemState.preview
        reason: str | None = None
        target_count = item_active = item_estimated = 0
        if issue is not None:
            hydration_count += 1
            reason = _MANIFEST_REASONS[issue]
            target_count = max((release.expected_count or 0) - progress.downloaded_track_count, 0)
            missing_count += target_count
            item_estimated = target_count
            estimated += item_estimated
        else:
            imported = set(progress.downloaded_catalog_track_ids)
            files = {item.catalog_track_id: item for item in progress.library_files}
            subquality = {
                track_id
                for track_id, item in files.items()
                if not track_meets_quality(item.file_format, quality_profile)
            }
            targets = {
                track.id
                for track in album.tracks
                if track.id is not None and (track.id not in imported or track.id in subquality)
            }
            target_count = len(targets)
            item_active = len(targets & active_track_ids)
            active_count += item_active
            missing_count += target_count
            item_estimated = target_count - item_active
            estimated += item_estimated
            if target_count == 0:
                state = DiscographyBatchItemState.complete
                reason = "already_complete"
                complete_count += 1
            elif item_active == target_count:
                state = DiscographyBatchItemState.skipped
                reason = "already_active"
                skipped_count += 1
        db.add(
            DiscographyBatchItem(
                batch_id=batch.id,
                provider_release_id=release.provider_release_id,
                catalog_album_id=album.id,
                artist_name=release.artist_name,
                release_title=release.title,
                release_year=release.year,
                release_kind=release.release_kind,
                provider=release.provider,
                state=state,
                reason_code=reason,
                target_count=target_count,
                active_count=item_active,
                skipped_count=1 if state == DiscographyBatchItemState.skipped else 0,
                estimated_job_count=item_estimated,
            )
        )

    batch.complete_count = complete_count
    batch.active_count = active_count
    batch.hydration_required_count = hydration_count
    batch.missing_count = missing_count
    batch.skipped_count = skipped_count
    batch.estimated_job_count = estimated
    await db.commit()
    return DiscographyBatchPreview(
        id=batch.id,
        scope_kind=batch.scope_kind,
        scope_json=batch.scope_json,
        scope_hash=batch.scope_hash,
        state=batch.state,
        matching_count=batch.matching_count,
        complete_count=batch.complete_count,
        active_count=batch.active_count,
        hydration_required_count=batch.hydration_required_count,
        missing_count=batch.missing_count,
        skipped_count=batch.skipped_count,
        estimated_job_count=batch.estimated_job_count,
    )


preview_discography_batch = create_discography_batch_preview
