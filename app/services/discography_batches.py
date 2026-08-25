from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Literal, cast

from sqlalchemy import delete, select, update
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
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyJobOwnership,
    DiscographyScopeKind,
)
from app.models.job import Job, JobStatus
from app.services.catalog import (
    _missing_releases_query,
    get_missing_release_ids,
    project_catalog_album_queue_targets,
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
    release_identity: str
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
            release_identity=f"provider:{scope.provider}:{release.provider_album_id}",
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
            release_identity=f"catalog_album:{album_id}",
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


async def _populate_batch_items(
    db: AsyncSession,
    batch: DiscographyBatch,
    releases: list[_SelectedRelease],
    quality_profile: QualityProfile,
) -> None:
    """Replace one batch's materialized preview from an exact server-side selection."""
    await db.execute(delete(DiscographyBatchItem).where(DiscographyBatchItem.batch_id == batch.id))
    await db.flush()
    complete_count = active_count = hydration_count = missing_count = estimated = 0
    skipped_count = 0
    actionable: list[_SelectedRelease] = []
    seen_albums: set[int] = set()
    for release in releases:
        reason: str | None = None
        if release.album is None:
            reason = "catalog_release_unbound"
        elif release.album.id in seen_albums:
            reason = "duplicate_catalog_album"
        else:
            seen_albums.add(release.album.id)
            actionable.append(release)
        if reason is not None:
            skipped_count += 1
            db.add(
                DiscographyBatchItem(
                    batch_id=batch.id,
                    release_identity=release.release_identity,
                    provider_release_id=release.provider_release_id,
                    catalog_album_id=release.album.id if release.album is not None else None,
                    artist_name=release.artist_name,
                    release_title=release.title,
                    release_year=release.year,
                    release_kind=release.release_kind,
                    provider=release.provider,
                    state=DiscographyBatchItemState.skipped,
                    reason_code=reason,
                    skipped_count=1,
                )
            )

    album_ids = [release.album.id for release in actionable if release.album is not None]
    projections = await project_catalog_album_queue_targets(
        db, album_ids, quality_profile=quality_profile
    )
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

    for release in actionable:
        album = release.album
        assert album is not None
        projection = projections[album.id]
        issue = catalog_manifest_issue(album.tracks, release.expected_count)
        state = DiscographyBatchItemState.preview
        reason = None
        target_count = item_active = item_estimated = 0
        if issue is not None:
            hydration_count += 1
            reason = _MANIFEST_REASONS[issue]
            target_count = max(
                (release.expected_count or 0) - len(projection.imported_track_ids), 0
            )
            missing_count += target_count
            item_estimated = target_count
            estimated += item_estimated
        else:
            targets = set(projection.target_track_ids)
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
                release_identity=release.release_identity,
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

    batch.matching_count = len(releases)
    batch.complete_count = complete_count
    batch.active_count = active_count
    batch.hydration_required_count = hydration_count
    batch.missing_count = missing_count
    batch.skipped_count = skipped_count
    batch.estimated_job_count = estimated
    await db.flush()


def _preview_result(batch: DiscographyBatch) -> DiscographyBatchPreview:
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


async def _select_persisted_scope(
    db: AsyncSession, batch: DiscographyBatch
) -> tuple[list[_SelectedRelease], str]:
    try:
        payload = json.loads(batch.scope_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise DiscographyScopeError("persisted scope_json is invalid") from exc
    if not isinstance(payload, dict):
        raise DiscographyScopeError("persisted scope_json must be an object")
    scope, canonical = canonicalize_scope(batch.scope_kind, payload)
    if canonical != batch.scope_json:
        raise DiscographyScopeError("persisted scope_json is not canonical")
    if isinstance(scope, ArtistDiscographyScope):
        releases = await _select_artist_releases(db, scope)
    else:
        releases = await _select_wanted_releases(db, batch.scope_kind, scope)
    return releases, canonical


async def create_discography_batch_preview(
    db: AsyncSession,
    scope_kind: DiscographyScopeKind | str,
    payload: dict[str, object],
    *,
    quality_profile: QualityProfile | None = None,
) -> DiscographyBatchPreview:
    """Select, classify, and durably persist a provider-I/O-free preview."""
    scope, scope_json = canonicalize_scope(scope_kind, payload)
    kind = DiscographyScopeKind(scope_kind)
    releases = (
        await _select_artist_releases(db, scope)
        if isinstance(scope, ArtistDiscographyScope)
        else await _select_wanted_releases(db, kind, scope)
    )
    if quality_profile is None:
        quality_profile = (await get_runtime_settings(db)).quality_profile
    batch = DiscographyBatch(
        scope_kind=kind,
        scope_json=scope_json,
        scope_hash=_hash(scope_json, [release.release_identity for release in releases]),
        state=DiscographyBatchState.preview,
    )
    try:
        async with db.begin_nested():
            db.add(batch)
            await db.flush()
            await _populate_batch_items(db, batch, releases, quality_profile)
    except Exception:
        await db.rollback()
        raise
    await db.commit()
    return _preview_result(batch)


@dataclass(frozen=True, slots=True)
class DiscographyBatchConfirmation:
    batch: DiscographyBatchPreview
    scope_changed: bool


@dataclass(frozen=True, slots=True)
class DiscographyBatchControlResult:
    batch_id: int
    state: DiscographyBatchState
    cancel_job_ids: tuple[int, ...] = ()
    reset_item_ids: tuple[int, ...] = ()


async def confirm_discography_batch(
    db: AsyncSession,
    batch_id: int,
    *,
    quality_profile: QualityProfile | None = None,
) -> DiscographyBatchConfirmation:
    """Revalidate an immutable preview and queue it only when scope is unchanged."""
    batch = await db.get(DiscographyBatch, batch_id)
    if batch is None:
        raise DiscographyScopeError("discography batch does not exist")
    if batch.state != DiscographyBatchState.preview:
        raise DiscographyScopeError("only a preview batch can be confirmed")
    releases, scope_json = await _select_persisted_scope(db, batch)
    new_hash = _hash(scope_json, [release.release_identity for release in releases])
    scope_changed = new_hash != batch.scope_hash
    if quality_profile is None:
        quality_profile = (await get_runtime_settings(db)).quality_profile
    await _populate_batch_items(db, batch, releases, quality_profile)
    batch.scope_hash = new_hash
    if scope_changed:
        batch.state = DiscographyBatchState.preview
    else:
        await db.execute(
            update(DiscographyBatchItem)
            .where(
                DiscographyBatchItem.batch_id == batch.id,
                DiscographyBatchItem.state.in_(
                    (DiscographyBatchItemState.preview, DiscographyBatchItemState.failed)
                ),
            )
            .values(state=DiscographyBatchItemState.pending)
        )
        batch.state = DiscographyBatchState.queued
    await db.commit()
    return DiscographyBatchConfirmation(batch=_preview_result(batch), scope_changed=scope_changed)


async def pause_discography_batch(
    db: AsyncSession, batch_id: int
) -> DiscographyBatchControlResult:
    batch = await db.get(DiscographyBatch, batch_id)
    if batch is None:
        raise DiscographyScopeError("discography batch does not exist")
    if batch.state not in {DiscographyBatchState.queued, DiscographyBatchState.running}:
        raise DiscographyScopeError("batch is not pausable")
    batch.state = DiscographyBatchState.paused
    await db.commit()
    return DiscographyBatchControlResult(batch.id, batch.state)


async def cancel_discography_batch(
    db: AsyncSession, batch_id: int
) -> DiscographyBatchControlResult:
    batch = await db.get(DiscographyBatch, batch_id)
    if batch is None:
        raise DiscographyScopeError("discography batch does not exist")
    if batch.state not in {
        DiscographyBatchState.queued,
        DiscographyBatchState.running,
        DiscographyBatchState.paused,
    }:
        raise DiscographyScopeError("batch is not cancellable")
    created_ids = (
        select(DiscographyBatchItemJob.job_id)
        .join(
            DiscographyBatchItem,
            DiscographyBatchItem.id == DiscographyBatchItemJob.item_id,
        )
        .where(
            DiscographyBatchItem.batch_id == batch_id,
            DiscographyBatchItemJob.ownership == DiscographyJobOwnership.created,
        )
    )
    cancelled = tuple(
        int(value)
        for value in (
            await db.scalars(
                update(Job)
                .where(Job.id.in_(created_ids), Job.status == JobStatus.pending)
                .values(status=JobStatus.cancelled)
                .returning(Job.id)
            )
        ).all()
    )
    await db.execute(
        update(DiscographyBatchItem)
        .where(
            DiscographyBatchItem.batch_id == batch_id,
            DiscographyBatchItem.state.in_(
                (
                    DiscographyBatchItemState.pending,
                    DiscographyBatchItemState.hydrating,
                    DiscographyBatchItemState.expanding,
                    DiscographyBatchItemState.waiting,
                )
            ),
        )
        .values(
            state=DiscographyBatchItemState.cancelled,
            reason_code="batch_cancelled",
            lease_token=None,
            heartbeat_at=None,
        )
    )
    batch.state = DiscographyBatchState.cancelled
    await db.commit()
    return DiscographyBatchControlResult(batch.id, batch.state, tuple(sorted(cancelled)))


async def resume_discography_batch(
    db: AsyncSession,
    batch_id: int,
    *,
    quality_profile: QualityProfile | None = None,
) -> DiscographyBatchControlResult:
    batch = await db.get(DiscographyBatch, batch_id)
    if batch is None:
        raise DiscographyScopeError("discography batch does not exist")
    if batch.state != DiscographyBatchState.paused:
        raise DiscographyScopeError("batch is not paused")
    if quality_profile is None:
        quality_profile = (await get_runtime_settings(db)).quality_profile
    items = list(
        (
            await db.scalars(
                select(DiscographyBatchItem)
                .where(DiscographyBatchItem.batch_id == batch_id)
                .options(
                    selectinload(DiscographyBatchItem.catalog_album).selectinload(
                        CatalogAlbum.tracks
                    )
                )
            )
        ).all()
    )
    reset: list[int] = []
    for item in items:
        if item.state in {DiscographyBatchItemState.complete, DiscographyBatchItemState.skipped}:
            continue
        album = item.catalog_album
        if album is None:
            item.state = DiscographyBatchItemState.skipped
            item.reason_code = "catalog_release_unbound"
            continue
        expected = album.track_count
        if item.provider_release_id is not None:
            provider_expected = await db.scalar(
                select(CatalogAlbumProvider.track_count).where(
                    CatalogAlbumProvider.id == item.provider_release_id
                )
            )
            expected = max(expected or 0, provider_expected or 0) or None
        issue = catalog_manifest_issue(album.tracks, expected)
        if issue is not None:
            item.state = DiscographyBatchItemState.pending
            item.reason_code = _MANIFEST_REASONS[issue]
            reset.append(item.id)
            continue
        projection = (
            await project_catalog_album_queue_targets(
                db, [album.id], quality_profile=quality_profile
            )
        )[album.id]
        targets = set(projection.target_track_ids)
        active = (
            set(
                int(value)
                for value in (
                    await db.scalars(
                        select(AcquisitionDispatchClaim.catalog_track_id)
                        .join(Job, Job.id == AcquisitionDispatchClaim.job_id)
                        .where(
                            AcquisitionDispatchClaim.catalog_album_id == album.id,
                            AcquisitionDispatchClaim.catalog_track_id.in_(targets),
                            Job.status.in_((JobStatus.pending, JobStatus.running)),
                        )
                    )
                ).all()
            )
            if targets
            else set()
        )
        if not targets:
            item.state = DiscographyBatchItemState.complete
            item.reason_code = "verified_complete"
        elif active:
            item.state = DiscographyBatchItemState.waiting
            item.reason_code = "active_jobs"
        else:
            item.state = DiscographyBatchItemState.pending
            item.reason_code = None
            reset.append(item.id)
        item.lease_token = None
        item.heartbeat_at = None
    batch.state = DiscographyBatchState.queued
    batch.completed_at = None
    await db.commit()
    return DiscographyBatchControlResult(batch.id, batch.state, reset_item_ids=tuple(reset))


async def retry_discography_batch_items(
    db: AsyncSession, batch_id: int, item_ids: list[int] | tuple[int, ...]
) -> DiscographyBatchControlResult:
    batch = await db.get(DiscographyBatch, batch_id)
    if batch is None:
        raise DiscographyScopeError("discography batch does not exist")
    selected_ids = tuple(dict.fromkeys(int(value) for value in item_ids))
    items = (
        list(
            (
                await db.scalars(
                    select(DiscographyBatchItem).where(DiscographyBatchItem.id.in_(selected_ids))
                )
            ).all()
        )
        if selected_ids
        else []
    )
    if len(items) != len(selected_ids) or any(item.batch_id != batch_id for item in items):
        raise DiscographyScopeError("selected item does not belong to batch")
    reset: list[int] = []
    retryable_skips = {"already_active", *_MANIFEST_REASONS.values(), "hydration_failed"}
    for item in items:
        eligible = item.state in {
            DiscographyBatchItemState.failed,
            DiscographyBatchItemState.cancelled,
        } or (
            item.state == DiscographyBatchItemState.skipped and item.reason_code in retryable_skips
        )
        if not eligible:
            continue
        item.state = DiscographyBatchItemState.pending
        item.error_detail = None
        item.lease_token = None
        item.heartbeat_at = None
        if item.reason_code not in _MANIFEST_REASONS.values():
            item.reason_code = None
        reset.append(item.id)
    if reset:
        batch.state = DiscographyBatchState.queued
        batch.completed_at = None
    await db.commit()
    return DiscographyBatchControlResult(
        batch.id,
        batch.state,
        reset_item_ids=tuple(item_id for item_id in selected_ids if item_id in reset),
    )


preview_discography_batch = create_discography_batch_preview
confirm_batch = confirm_discography_batch
pause_batch = pause_discography_batch
resume_batch = resume_discography_batch
cancel_batch = cancel_discography_batch
retry_batch_items = retry_discography_batch_items
