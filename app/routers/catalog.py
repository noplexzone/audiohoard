from __future__ import annotations

import asyncio
import json
import logging
import posixpath
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any
from urllib.parse import unquote, urlencode, urlsplit
from uuid import uuid4

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, selectinload
from tenacity import RetryError

from app.auth import get_current_user, require_mutation
from app.config import Settings
from app.database import get_db, run_with_sqlite_lock_retry
from app.jobs.dispatcher import job_dispatcher
from app.models.acquisition_attempt import AcquisitionAttempt
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyScopeKind,
)
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release import Release
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import ImportWorkflowState, ReviewDecision
from app.services.catalog import (
    ReleaseProgress,
    aggregate_artist_release_rollup,
    get_artist_detail,
    get_library_artists_page,
    get_library_stats,
    get_missing_release_ids,
    get_missing_releases_page,
    get_release_progress,
    list_distinct_formats,
    list_distinct_sources,
    list_library_tracks,
    queue_catalog_album_missing_track_jobs,
)
from app.services.catalog_artist_credits import catalog_track_artist_name
from app.services.catalog_metadata import (
    VALID_METADATA_PROVIDERS,
    album_providers,
    available_artist_providers,
    build_metadata_provider,
    enrich_catalog_artist,
    ensure_legacy_provider_snapshots,
    fetch_and_store_album,
    fetch_catalog_artist_detail,
    release_bucket,
    upsert_catalog_artist,
    upsert_provider_release,
)
from app.services.catalog_ownership import reconcile_deezer_catalog_ownership
from app.services.discography_batches import (
    DiscographyScopeError,
    cancel_discography_batch,
    confirm_discography_batch,
    create_discography_batch_preview,
    is_discography_batch_item_retryable,
    pause_discography_batch,
    queue_discography_batch,
    resume_discography_batch,
    retry_discography_batch_items,
)
from app.services.library_import import ImportExecutionError, retag_catalog_album
from app.services.library_removal import (
    LibraryRemovalError,
    remove_catalog_album,
    remove_imported_release_group,
)
from app.services.quality_upgrade import reconcile_album_quality_duplicates
from app.services.release_editions import (
    apply_release_monitoring_policy,
    project_release_families,
    set_family_monitor_overrides,
    sync_canonical_monitoring,
)
from app.services.upgrade_monitoring import (
    sync_album_upgrade_monitoring,
    sync_artist_upgrade_monitoring,
)
from app.settings_service import RuntimeSettings, effective_settings_dep, get_runtime_settings

router = APIRouter(dependencies=[Depends(get_current_user)])
logger = logging.getLogger(__name__)
_discography_tasks: dict[tuple[int, str], asyncio.Task[None]] = {}


def _start_discography_task(artist_id: int, provider_name: str) -> bool:
    key = (artist_id, provider_name)
    existing = _discography_tasks.get(key)
    if existing is not None and not existing.done():
        return False
    task = asyncio.create_task(_refresh_discography_task(artist_id, provider_name))
    _discography_tasks[key] = task

    def forget_task(completed: asyncio.Task[None]) -> None:
        if _discography_tasks.get(key) is completed:
            _discography_tasks.pop(key, None)

    task.add_done_callback(forget_task)
    return True


async def _claim_discography_refresh(
    session: AsyncSession, identity: CatalogArtistIdentity
) -> str | None:
    original = identity.metadata_json
    try:
        metadata = json.loads(original or "{}")
    except (json.JSONDecodeError, TypeError):
        metadata = {}
    started_at = metadata.get("discography_started_at")
    if metadata.get("discography_state") == "loading" and isinstance(started_at, str):
        try:
            started = datetime.fromisoformat(started_at)
        except ValueError:
            started = None
        if started is not None and started > datetime.now(tz=UTC) - timedelta(minutes=10):
            await session.rollback()
            return None
    claim_id = uuid4().hex
    metadata["discography_state"] = "loading"
    metadata["discography_started_at"] = datetime.now(tz=UTC).isoformat()
    metadata["discography_claim_id"] = claim_id
    metadata.pop("discography_error", None)
    condition = (
        CatalogArtistIdentity.metadata_json.is_(None)
        if original is None
        else CatalogArtistIdentity.metadata_json == original
    )
    result = await session.execute(
        update(CatalogArtistIdentity)
        .where(CatalogArtistIdentity.id == identity.id, condition)
        .values(metadata_json=json.dumps(metadata, sort_keys=True))
    )
    await session.commit()
    return claim_id if getattr(result, "rowcount", 0) else None


def _is_fetch_request(request: Request | None) -> bool:
    return (
        request is not None and request.headers.get("x-requested-with", "").casefold() == "fetch"
    )


def _download_response(request: Request | None, *, queued: int, album_id: int) -> Response:
    if _is_fetch_request(request):
        return JSONResponse({"queued": queued, "album_id": album_id})
    return RedirectResponse("/downloads", status_code=303)


def _download_many_response(request: Request | None, *, queued: int, artist_id: int) -> Response:
    if _is_fetch_request(request):
        return JSONResponse({"queued": queued, "artist_id": artist_id})
    return RedirectResponse("/downloads", status_code=303)


def _wanted_queue_response(
    request: Request | None, *, queued: int, album_ids: list[int]
) -> Response:
    if _is_fetch_request(request):
        return JSONResponse({"queued": queued, "catalog_album_ids": album_ids})
    return RedirectResponse("/downloads", status_code=303)


async def _imported_catalog_track_ids(db: AsyncSession, album_id: int) -> set[int]:
    rows = (
        await db.execute(
            select(Track.catalog_track_id, ImportPlan.destination_path)
            .join(ImportPlan, ImportPlan.track_id == Track.id)
            .where(
                Track.catalog_album_id == album_id,
                Track.catalog_track_id.is_not(None),
                Track.import_state == ImportWorkflowState.imported,
                ImportPlan.status == ImportWorkflowState.imported,
                ImportPlan.destination_path != "",
            )
        )
    ).all()
    imported: set[int] = set()
    for track_id, path in rows:
        if track_id is not None and await asyncio.to_thread(Path(path).is_file):
            imported.add(int(track_id))
    return imported


async def _ensure_catalog_tracks(
    db: AsyncSession, settings: Settings, album: CatalogAlbum
) -> None:
    """Hydrate and persist CatalogAlbumTrack rows for *album* when absent but expected.

    Called at dispatch time (direct download and bulk monitored download) so the job
    runner always has a complete catalog manifest available for binding and gap-detection.
    Raises on failure so callers can surface a 502 rather than silently dispatching a
    job with an empty manifest.

    Uses a COUNT query rather than accessing album.tracks directly to avoid triggering
    a SQLAlchemy lazy-load on an uninitialized collection in async context.
    """
    from sqlalchemy import func

    existing = int(
        await db.scalar(
            select(func.count(CatalogAlbumTrack.id)).where(CatalogAlbumTrack.album_id == album.id)
        )
        or 0
    )
    expected_before_hydration = album.track_count or 0
    manifest_complete = bool(expected_before_hydration and existing >= expected_before_hydration)
    missing_artist_credits = False
    if album.is_compilation or (album.release_type or "").casefold() in {
        "compile",
        "compilation",
    }:
        missing_track_artists = int(
            await db.scalar(
                select(func.count(CatalogAlbumTrack.id)).where(
                    CatalogAlbumTrack.album_id == album.id,
                    CatalogAlbumTrack.artist_name.is_(None),
                )
            )
            or 0
        )
        missing_artist_credits = not album.album_artist_name or missing_track_artists > 0
    if manifest_complete and not missing_artist_credits:
        return
    try:
        await fetch_and_store_album(db, settings, album)
    except Exception:
        if manifest_complete:
            logger.warning(
                "Catalog artist-credit refresh failed for album %d; preserving acquisition",
                album.id,
            )
            return
        raise
    after = int(
        await db.scalar(
            select(func.count(CatalogAlbumTrack.id)).where(CatalogAlbumTrack.album_id == album.id)
        )
        or 0
    )
    expected = max(expected_before_hydration, album.track_count or 0)
    album.track_count = expected or None
    if not after or (expected and after < expected):
        raise RuntimeError(
            f"Catalog album {album.id} has track_count={album.track_count} "
            f"but only {after} catalog tracks could be loaded"
        )


def _selected_provider(
    requested: str,
    available: list[str],
    primary: str,
    watchlist_provider: str | None,
) -> str:
    if requested in VALID_METADATA_PROVIDERS and requested in available:
        return requested
    if primary in available:
        return primary
    if watchlist_provider in available:
        return watchlist_provider
    return available[0] if available else primary


def _artist_primary_provider(
    artist: CatalogArtist, runtime: RuntimeSettings, available: list[str]
) -> str:
    return _selected_provider(
        artist.primary_metadata_provider or runtime.primary_metadata_provider,
        available,
        runtime.primary_metadata_provider,
        artist.watchlist_provider,
    )


def _legacy_provider_album_rows(artist: CatalogArtist, provider_name: str) -> list[Any]:
    rows: list[Any] = []
    for album in artist.albums:
        if provider_name not in album_providers(album):
            continue
        bucket = release_bucket(album.release_type)
        rows.append(
            SimpleNamespace(
                id=-(album.id or 0),
                catalog_album_id=album.id,
                title=album.title,
                year=album.year,
                artwork_url=album.artwork_url,
                track_count=album.track_count,
                release_kind={
                    "album": "album",
                    "single_ep": "single",
                    "compilation": "compilation",
                }[bucket],
                release_type_raw=album.release_type,
                content_rating=album.content_rating,
                monitored=bool(album.monitored and artist.watchlist_provider == provider_name),
                provider=provider_name,
            )
        )
    return rows


def _artist_page_url(
    artist_id: int,
    *,
    provider: str = "",
    release_type: str = "",
    sort: str = "desc",
    enrichment: str = "",
) -> str:
    params: dict[str, str] = {}
    if provider in VALID_METADATA_PROVIDERS:
        params["provider"] = provider
    if release_type in {"Album", "single_ep", "Compilation"}:
        params["release_type"] = release_type
    if sort in {"asc", "desc"}:
        params["sort"] = sort
    if enrichment in {"ok", "ambiguous", "partial", "failed"}:
        params["enrichment"] = enrichment
    query = urlencode(params)
    return f"/artists/catalog/{artist_id}" + (f"?{query}" if query else "")


def _release_needs_track_count_refresh(release: CatalogAlbumProvider) -> bool:
    if release.content_rating in {None, "", "unknown"}:
        return True
    if release.track_count is not None:
        return False
    try:
        metadata = json.loads(release.metadata_json or "{}")
    except (json.JSONDecodeError, TypeError):
        return True
    return not isinstance(metadata, dict) or not metadata.get("track_count_checked", False)


def _sanitize_error_class(exc: BaseException) -> str:
    first_line = str(exc).splitlines()[0] if str(exc) else ""
    raw = f"{type(exc).__name__}: {first_line}" if first_line else type(exc).__name__
    return raw[:200]


def _form_bool(value: object) -> bool:
    return str(value or "").lower() in {"1", "true", "yes", "on"}


def _safe_discover_return_path(value: str) -> str | None:
    if not value:
        return None
    decoded = value
    for _ in range(4):
        if any(ord(char) < 32 or char == "\\" for char in decoded):
            return None
        next_value = unquote(decoded)
        if next_value == decoded:
            break
        decoded = next_value
    else:
        if unquote(decoded) != decoded:
            return None
    if any(ord(char) < 32 or char == "\\" for char in decoded):
        return None
    parsed = urlsplit(decoded)
    if parsed.scheme or parsed.netloc or decoded.startswith("//"):
        return None
    if posixpath.normpath(parsed.path) != parsed.path:
        return None
    if parsed.path != "/search" and not parsed.path.startswith("/discover/"):
        return None
    return value


def _apply_runtime_watchlist_defaults(artist: CatalogArtist, runtime: RuntimeSettings) -> None:
    artist.watchlist_release_albums = bool(runtime.default_watchlist_release_albums)
    artist.watchlist_release_singles = bool(runtime.default_watchlist_release_singles)
    artist.watchlist_release_eps = bool(runtime.default_watchlist_release_eps)
    artist.watchlist_monitor_upgrades = bool(runtime.default_watchlist_monitor_upgrades)
    artist.monitor_policy = "all"


async def _sync_artist_upgrade_monitoring(
    db: AsyncSession, artist: CatalogArtist, releases: list[CatalogAlbumProvider]
) -> None:
    await sync_artist_upgrade_monitoring(db, artist, releases)


async def _queue_artist_enrichment(db: AsyncSession, artist_id: int) -> bool:
    queued = False

    async def operation() -> None:
        nonlocal queued
        result = await db.execute(
            update(CatalogArtist)
            .where(
                CatalogArtist.id == artist_id,
                CatalogArtist.enrichment_state.not_in(("queued", "running")),
            )
            .values(enrichment_state="queued")
        )
        queued = bool(getattr(result, "rowcount", 0))
        await db.commit()

    await run_with_sqlite_lock_retry(db, operation)
    return queued


async def _enrich_artist_task(artist_id: int, providers: list[str]) -> None:
    from app.config import get_settings
    from app.database import get_session_factory
    from app.settings_service import build_effective_settings

    factory = get_session_factory()
    async with factory() as session:
        cfg = await build_effective_settings(session, get_settings())
        result = await session.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(selectinload(CatalogArtist.albums))
        )
        artist = result.scalar_one_or_none()
        if artist is not None:
            artist.enrichment_state = "running"
            await session.commit()
            try:
                enrichment = await enrich_catalog_artist(session, cfg, artist, providers)
                survivor_value = enrichment.get("artist_id")
                survivor_id = survivor_value if isinstance(survivor_value, int) else artist_id
                survivor = await session.get(CatalogArtist, survivor_id)
                if survivor is not None:
                    survivor.enrichment_state = "idle"
                await session.commit()
            except Exception as exc:
                logger.error(
                    "Catalog artist enrichment failed for artist %s", artist_id, exc_info=True
                )
                await session.rollback()
                artist = await session.get(CatalogArtist, artist_id)
                if artist is not None:
                    provenance = (
                        json.loads(artist.provenance_json or "{}")
                        if artist.provenance_json
                        else {}
                    )
                    provenance["last_enrichment_error"] = {
                        "at": datetime.now(tz=UTC).isoformat(),
                        "message": _sanitize_error_class(exc),
                    }
                    artist.provenance_json = json.dumps(provenance, sort_keys=True)
                    artist.enrichment_state = "failed"
                    await session.commit()


async def _refresh_discography_task(artist_id: int, provider_name: str) -> None:
    from app.config import get_settings
    from app.database import get_session_factory
    from app.settings_service import build_effective_settings

    factory = get_session_factory()
    claim_id: str | None = None
    try:
        async with factory() as session:
            cfg = await build_effective_settings(session, get_settings())
            identity = (
                await session.scalars(
                    select(CatalogArtistIdentity).where(
                        CatalogArtistIdentity.artist_id == artist_id,
                        CatalogArtistIdentity.provider == provider_name,
                    )
                )
            ).first()
            artist = await session.get(CatalogArtist, artist_id)
            if artist is None or identity is None:
                return
            claimed_identity: CatalogArtistIdentity = identity
            provider_id = identity.provider_artist_id
            claim_id = None

            async def claim() -> None:
                nonlocal claim_id
                claim_id = await _claim_discography_refresh(session, claimed_identity)

            await run_with_sqlite_lock_retry(session, claim, attempts=5, delay_seconds=0.35)
            if claim_id is None:
                return

        metadata_provider = build_metadata_provider(provider_name, cfg)
        if metadata_provider is None:
            raise RuntimeError("Metadata provider is unavailable")
        # Provider HTTP is deliberately completed before opening the SQLite write session.
        summaries = await metadata_provider.get_discography(provider_id)

        async with factory() as session:
            stored = False

            async def store() -> None:
                nonlocal stored
                load = selectinload(CatalogArtist.identities).selectinload(
                    CatalogArtistIdentity.releases
                )
                stored_artist = (
                    await session.execute(
                        select(CatalogArtist)
                        .where(CatalogArtist.id == artist_id)
                        .options(load, selectinload(CatalogArtist.albums))
                    )
                ).scalar_one_or_none()
                if stored_artist is None:
                    return
                stored_identity = next(
                    (item for item in stored_artist.identities if item.provider == provider_name),
                    None,
                )
                if stored_identity is None:
                    return
                try:
                    claim_metadata = json.loads(stored_identity.metadata_json or "{}")
                except (json.JSONDecodeError, TypeError):
                    claim_metadata = {}
                if claim_metadata.get("discography_claim_id") != claim_id:
                    await session.rollback()
                    return
                for summary in summaries:
                    await upsert_provider_release(session, stored_artist, stored_identity, summary)
                await session.flush()
                complete_releases = list(
                    (
                        await session.scalars(
                            select(CatalogAlbumProvider)
                            .where(CatalogAlbumProvider.artist_identity_id == stored_identity.id)
                            .options(
                                selectinload(CatalogAlbumProvider.artist_identity),
                                selectinload(CatalogAlbumProvider.catalog_album),
                            )
                        )
                    ).all()
                )
                apply_release_monitoring_policy(stored_artist, complete_releases)
                sync_canonical_monitoring(stored_artist, complete_releases)
                await sync_artist_upgrade_monitoring(session, stored_artist, complete_releases)
                stored_identity.last_discography_at = datetime.now(tz=UTC)
                try:
                    metadata = json.loads(stored_identity.metadata_json or "{}")
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
                metadata["discography_state"] = "ready"
                metadata.pop("discography_started_at", None)
                metadata.pop("discography_claim_id", None)
                metadata.pop("discography_error", None)
                stored_identity.metadata_json = json.dumps(metadata, sort_keys=True)
                runtime = await get_runtime_settings(session)
                effective_primary = (
                    stored_artist.primary_metadata_provider or runtime.primary_metadata_provider
                )
                if provider_name == effective_primary:
                    stored_artist.enrichment_state = "idle"
                await session.commit()
                stored = True

            await run_with_sqlite_lock_retry(session, store, attempts=5, delay_seconds=0.35)
            if not stored:
                return

        async with factory() as session:
            artist = await session.get(CatalogArtist, artist_id)
            runtime = await get_runtime_settings(session)
            effective_primary = (
                artist.primary_metadata_provider or runtime.primary_metadata_provider
                if artist is not None
                else runtime.primary_metadata_provider
            )
            if artist is not None and artist.monitored and provider_name == effective_primary:
                for secondary in runtime.enabled_metadata_providers:
                    if secondary == provider_name:
                        continue
                    secondary_identity = await session.scalar(
                        select(CatalogArtistIdentity)
                        .where(
                            CatalogArtistIdentity.artist_id == artist_id,
                            CatalogArtistIdentity.provider == secondary,
                        )
                        .options(selectinload(CatalogArtistIdentity.releases))
                    )
                    if secondary_identity is not None:
                        try:
                            secondary_metadata = json.loads(
                                secondary_identity.metadata_json or "{}"
                            )
                        except (json.JSONDecodeError, TypeError):
                            secondary_metadata = {}
                        if (
                            secondary_metadata.get("discography_state") != "ready"
                            and not secondary_identity.releases
                        ):
                            _start_discography_task(artist_id, secondary)
            await session.rollback()
        if provider_name == "deezer":
            try:
                await reconcile_deezer_catalog_ownership(
                    get_session_factory(), cfg, artist_id=artist_id
                )
            except Exception:
                logger.exception(
                    "Catalog ownership reconciliation failed for artist %s", artist_id
                )
    except Exception:
        if claim_id is None:
            logger.error(
                "Catalog discography refresh failed before claim for artist %s via %s",
                artist_id,
                provider_name,
                exc_info=True,
            )
            return
        async with factory() as session:
            failed = await session.get(CatalogArtist, artist_id)
            if failed is not None:
                runtime = await get_runtime_settings(session)
                effective_primary = (
                    failed.primary_metadata_provider or runtime.primary_metadata_provider
                )
                if provider_name == effective_primary:
                    failed.enrichment_state = "failed"
                identity = await session.scalar(
                    select(CatalogArtistIdentity).where(
                        CatalogArtistIdentity.artist_id == artist_id,
                        CatalogArtistIdentity.provider == provider_name,
                    )
                )
                if identity is not None:
                    try:
                        metadata = json.loads(identity.metadata_json or "{}")
                    except (json.JSONDecodeError, TypeError):
                        metadata = {}
                    if metadata.get("discography_claim_id") != claim_id:
                        await session.rollback()
                        return
                    metadata["discography_state"] = "failed"
                    metadata.pop("discography_started_at", None)
                    metadata.pop("discography_claim_id", None)
                    metadata["discography_error"] = "Discography refresh failed"
                    identity.metadata_json = json.dumps(metadata, sort_keys=True)
                await session.commit()
        logger.error(
            "Catalog discography refresh failed for artist %s via %s",
            artist_id,
            provider_name,
            exc_info=True,
        )


def _templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates  # type: ignore[no-any-return]


async def _confirmed_delete(request: Request) -> bool:
    if request.headers.get("content-type", "").casefold().startswith("application/json"):
        try:
            payload = await request.json()
        except ValueError:
            return False
        return isinstance(payload, dict) and payload.get("confirmation") == "delete"
    form = await request.form()
    return form.get("confirmation") == "delete"


def _wants_json(request: Request) -> bool:
    return (
        "application/json" in request.headers.get("accept", "").casefold()
        or request.headers.get("x-requested-with", "").casefold() == "fetch"
        or request.headers.get("content-type", "").casefold().startswith("application/json")
    )


def _artist_watchlist_payload(artist: CatalogArtist) -> dict[str, object]:
    return {
        "artist_id": artist.id,
        "watched": bool(artist.monitored),
        "watchlist_release_albums": bool(artist.watchlist_release_albums),
        "watchlist_release_singles": bool(artist.watchlist_release_singles),
        "watchlist_release_eps": bool(artist.watchlist_release_eps),
        "watchlist_monitor_upgrades": bool(artist.watchlist_monitor_upgrades),
        "configure_url": f"/artists/catalog/{artist.id}/monitor",
        "discography_url": f"/artists/catalog/{artist.id}",
    }


@router.get("/library", response_class=HTMLResponse)
async def library_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    view: str = "artists",
    q: str = "",
    sort: str = "name",
    artist: str = "",
    album: str = "",
    source: str = "",
    fmt: str = "",
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
) -> HTMLResponse:
    if view == "tracks":
        track_sort = (
            sort if sort in {"added", "title", "artist", "album", "year", "source"} else "added"
        )
        tracks = await list_library_tracks(
            db,
            q=q,
            artist=artist,
            album=album,
            source=source,
            fmt=fmt,
            sort=track_sort,
            page=page,
            per_page=per_page,
        )
        track_filter_params = {
            "view": "tracks",
            "sort": track_sort,
            "per_page": str(per_page),
        }
        for key, value in {
            "q": q,
            "artist": artist,
            "album": album,
            "source": source,
            "fmt": fmt,
        }.items():
            if value:
                track_filter_params[key] = value
        return _templates(request).TemplateResponse(
            request,
            "library_tracks.html",
            {
                "stats": await get_library_stats(db),
                "tracks": tracks,
                "q": q,
                "filter_artist": artist,
                "filter_album": album,
                "filter_source": source,
                "filter_fmt": fmt,
                "all_sources": await list_distinct_sources(db),
                "all_formats": await list_distinct_formats(db),
                "sort": track_sort,
                "per_page": per_page,
                "filter_qs": urlencode(track_filter_params),
            },
        )
    artists = await get_library_artists_page(db, q=q, sort=sort, page=page, per_page=per_page)
    filter_params: dict[str, str] = {}
    if q:
        filter_params["q"] = q
    filter_params["sort"] = sort
    filter_params["per_page"] = str(per_page)
    return _templates(request).TemplateResponse(
        request,
        "artists.html",
        {
            "artists": artists,
            "q": q,
            "sort": sort,
            "per_page": per_page,
            "filter_qs": urlencode(filter_params),
        },
    )


@router.get("/artists", include_in_schema=False)
async def artists_page(request: Request) -> RedirectResponse:
    query = request.scope.get("query_string", b"").decode("ascii")
    location = "/library" + (f"?{query}" if query else "")
    return RedirectResponse(location, status_code=307)


@router.get("/artists/detail", response_class=HTMLResponse)
async def artist_detail_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    name: str = "",
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
) -> HTMLResponse:
    name = name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Artist name is required")
    detail = await get_artist_detail(db, artist_name=name, page=page, per_page=per_page)
    return _templates(request).TemplateResponse(
        request,
        "artist_detail.html",
        {"detail": detail},
    )


@router.post("/library/releases/delete", include_in_schema=False)
async def delete_imported_release_files(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    if not await _confirmed_delete(request):
        raise HTTPException(status_code=422, detail="Explicit deletion confirmation is required")
    form = await request.form()
    artist_name = str(form.get("artist_name", "")).strip()
    album_title = str(form.get("album_title", "")).strip()
    if not artist_name or not album_title:
        raise HTTPException(status_code=422, detail="Imported release identity is invalid")
    year = str(form.get("year", "")).strip()
    release_value = str(form.get("release_id", "")).strip()
    try:
        release_id = int(release_value) if release_value else None
    except ValueError:
        raise HTTPException(
            status_code=422, detail="Imported release identity is invalid"
        ) from None
    try:
        result = await remove_imported_release_group(
            db,
            release_id=release_id,
            artist_name=artist_name,
            album_title=album_title,
            year=year,
            library_root=settings.library_root,
            cache_root=settings.artwork_cache_root.parent / "library-audio",
        )
    except LibraryRemovalError as exc:
        status = 404 if "not found" in str(exc).casefold() else 409
        raise HTTPException(status_code=status, detail=str(exc)) from None
    if _wants_json(request):
        return JSONResponse(
            {
                "release_id": release_id,
                "deleted_files": result.deleted_files,
                "track_ids": list(result.affected_track_ids),
                "already_removed": result.already_removed,
                "cleanup_pending": result.cleanup_pending,
            }
        )
    location = "/artists/detail?" + urlencode({"name": artist_name, "removed": "1"})
    return RedirectResponse(location, status_code=303)


@router.get("/artists/provider-preview", response_class=HTMLResponse)
async def provider_artist_preview(
    provider: str,
    provider_id: str,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    return_to: str = "",
) -> Response:
    await db.rollback()
    try:
        detail = await fetch_catalog_artist_detail(settings, provider, provider_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Provider artist not found") from None
    except (httpx.HTTPError, TimeoutError, RetryError):
        raise HTTPException(status_code=502, detail="Metadata provider unavailable") from None
    return _templates(request).TemplateResponse(
        request,
        "provider_artist_preview.html",
        {"artist": detail, "return_to": _safe_discover_return_path(return_to) or ""},
    )


@router.get("/artists/catalog/open", response_class=HTMLResponse)
async def open_catalog_artist_page(
    provider: str,
    provider_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    monitor: bool = False,
) -> Response:
    # Authentication and effective settings may have opened an implicit read
    # transaction on the request session. End it before provider network I/O.
    await db.rollback()
    try:
        detail = await fetch_catalog_artist_detail(settings, provider, provider_id)
    except ValueError:
        message = "The selected artist is no longer available from this provider."
        if request is not None and _wants_json(request):
            return JSONResponse(
                {"error": "invalid_artist_identity", "message": message}, status_code=422
            )
        return HTMLResponse(
            "<h1>Invalid artist identity</h1><p>" + message + "</p>", status_code=422
        )
    except (httpx.HTTPError, TimeoutError, RetryError):
        logger.warning("Metadata provider artist lookup failed", exc_info=True)
        message = "The metadata provider could not be reached. Please try again."
        if request is not None and _wants_json(request):
            return JSONResponse(
                {"error": "metadata_provider_unavailable", "message": message}, status_code=502
            )
        return HTMLResponse(
            "<h1>Metadata provider unavailable</h1><p>" + message + "</p>",
            status_code=502,
        )
    artist_id: int | None = None
    runtime: RuntimeSettings | None = None
    watchlist_payload: dict[str, object] | None = None

    async def save_artist() -> None:
        nonlocal artist_id, runtime, watchlist_payload
        artist = await upsert_catalog_artist(db, detail)
        runtime = await get_runtime_settings(db)
        if monitor:
            was_monitored = bool(artist.monitored)
            artist.monitored = True
            if not was_monitored:
                _apply_runtime_watchlist_defaults(artist, runtime)
                available = [provider] if provider in VALID_METADATA_PROVIDERS else []
                artist.watchlist_provider = _selected_provider(
                    runtime.primary_metadata_provider,
                    available,
                    runtime.primary_metadata_provider,
                    provider,
                )
        await db.commit()
        artist_id = artist.id
        watchlist_payload = _artist_watchlist_payload(artist)

    await run_with_sqlite_lock_retry(db, save_artist, attempts=5, delay_seconds=0.35)
    assert artist_id is not None and runtime is not None and watchlist_payload is not None
    primary_provider = _selected_provider(
        runtime.primary_metadata_provider,
        [provider],
        runtime.primary_metadata_provider,
        provider,
    )
    _start_discography_task(artist_id, primary_provider)
    if _wants_json(request):
        return JSONResponse(watchlist_payload)
    return RedirectResponse(f"/artists/catalog/{artist_id}", status_code=303)


@router.post("/artists/catalog/open", include_in_schema=False)
async def open_catalog_artist_post(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
    provider: Annotated[str, Form()],
    provider_id: Annotated[str, Form()],
    monitor: Annotated[str, Form()] = "",
    return_to: Annotated[str, Form()] = "",
) -> Response:
    response = await open_catalog_artist_page(
        provider,
        provider_id,
        request,
        background_tasks,
        db,
        settings,
        monitor=monitor.lower() in {"1", "true", "yes", "on"},
    )
    if (
        response.status_code < 400
        and not _wants_json(request)
        and (location := _safe_discover_return_path(return_to))
    ):
        return RedirectResponse(location, status_code=303)
    return response


@router.get("/artists/catalog/{artist_id}", response_class=HTMLResponse)
async def catalog_artist_page(
    request: Request,
    artist_id: int,
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    provider: str = "",
    release_type: str = "",
    sort: str = "desc",
    enrichment: str = "",
) -> HTMLResponse:
    identity_load = selectinload(CatalogArtist.identities).selectinload(
        CatalogArtistIdentity.releases
    )
    result = await db.execute(
        select(CatalogArtist)
        .where(CatalogArtist.id == artist_id)
        .options(identity_load, selectinload(CatalogArtist.albums))
    )
    artist = result.scalar_one_or_none()
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")
    runtime = await get_runtime_settings(db)
    available_providers = available_artist_providers(artist)
    effective_primary_provider = _artist_primary_provider(artist, runtime, available_providers)
    selected_provider = _selected_provider(
        provider, available_providers, effective_primary_provider, artist.watchlist_provider
    )
    selected_identity = next(
        (identity for identity in artist.identities if identity.provider == selected_provider),
        None,
    )
    try:
        selected_metadata = (
            json.loads(selected_identity.metadata_json or "{}") if selected_identity else {}
        )
    except (json.JSONDecodeError, TypeError):
        selected_metadata = {}
    discography_failed = selected_metadata.get("discography_state") == "failed"
    # GET navigation must stay read-only. Provider refreshes and legacy snapshot repairs
    # can involve slow network calls and SQLite writer locks; run them only from explicit
    # actions so artist pages remain usable while acquisition/monitoring jobs are active.
    provider_albums = (
        list(selected_identity.releases)
        if selected_identity is not None
        else _legacy_provider_album_rows(artist, selected_provider)
    )
    release_families = project_release_families(provider_albums)
    if (
        selected_provider == effective_primary_provider
        and not provider_albums
        and not discography_failed
    ):
        _start_discography_task(artist.id, selected_provider)
    discography_loading = not provider_albums and (
        (artist.id, selected_provider) in _discography_tasks
        or artist.enrichment_state in {"queued", "running"}
    )
    canonical_progress = await get_release_progress(
        db,
        {
            release.catalog_album_id
            for release in provider_albums
            if release.catalog_album_id is not None
        },
        library_root=settings.library_root,
    )
    release_progress: dict[int, ReleaseProgress] = {}
    for release in provider_albums:
        projected = canonical_progress.get(
            release.catalog_album_id or 0,
            ReleaseProgress(wanted_track_count=0, downloaded_track_count=0),
        )
        release_progress[release.id] = projected
    artist_rollup = aggregate_artist_release_rollup(
        release_progress[family.display_release.id] for family in release_families
    )
    albums = sorted(
        release_families,
        key=lambda family: (
            family.display_release.year or "0000",
            family.display_release.title.casefold(),
        ),
        reverse=sort != "asc",
    )
    requested_kinds = {
        "Album": {"album"},
        "single_ep": {"single", "ep"},
        "Compilation": {"compilation"},
    }.get(release_type)
    if requested_kinds:
        albums = [family for family in albums if family.key.release_kind in requested_kinds]
    else:
        release_type = ""
    release_types = sorted(
        {release.release_type_raw for release in provider_albums if release.release_type_raw}
    )
    counts_by_type = {"albums": 0, "singles_eps": 0, "compilations": 0}
    for family in release_families:
        if family.key.release_kind in {"single", "ep"}:
            counts_by_type["singles_eps"] += 1
        elif family.key.release_kind == "compilation":
            counts_by_type["compilations"] += 1
        else:
            counts_by_type["albums"] += 1
    grouped_albums = (
        [
            ("Albums", [family for family in albums if family.key.release_kind == "album"]),
            (
                "Singles & EPs",
                [family for family in albums if family.key.release_kind in {"single", "ep"}],
            ),
            (
                "Compilations",
                [family for family in albums if family.key.release_kind == "compilation"],
            ),
            (
                "Other",
                [family for family in albums if family.key.release_kind in {"other", "unknown"}],
            ),
        ]
        if not release_type
        else [(release_type, albums)]
    )
    filter_options = [
        ("", "All"),
        ("Album", "Albums"),
        ("single_ep", "Singles & EPs"),
        ("Compilation", "Compilations"),
    ]
    provider_links = [
        (
            name,
            _artist_page_url(artist.id, provider=name, release_type=release_type, sort=sort),
        )
        for name in available_providers
    ]
    filter_links = [
        (
            value,
            label,
            _artist_page_url(artist.id, provider=selected_provider, release_type=value, sort=sort),
        )
        for value, label in filter_options
    ]
    sort_url = _artist_page_url(
        artist.id,
        provider=selected_provider,
        release_type=release_type,
        sort="asc" if sort != "asc" else "desc",
    )
    return _templates(request).TemplateResponse(
        request,
        "catalog_artist.html",
        {
            "artist": artist,
            "display_identity": selected_identity,
            "albums": albums,
            "grouped_albums": grouped_albums,
            "release_types": release_types,
            "release_type": release_type,
            "sort": sort,
            "counts_by_type": counts_by_type,
            "filter_options": filter_options,
            "filter_links": filter_links,
            "provider_links": provider_links,
            "available_providers": available_providers,
            "selected_provider": selected_provider,
            "primary_metadata_provider": effective_primary_provider,
            "sort_url": sort_url,
            "enrichment": enrichment,
            "release_progress": release_progress,
            "artist_rollup": artist_rollup,
            "discography_loading": discography_loading,
            "discography_failed": discography_failed,
        },
    )


@router.post("/artists/catalog/{artist_id}/primary-source", include_in_schema=False)
async def set_catalog_artist_primary_source(
    artist_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    load = selectinload(CatalogArtist.identities)
    artist = (
        await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(load)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")
    form = await request.form()
    runtime = await get_runtime_settings(db)
    available = available_artist_providers(artist)
    requested = str(form.get("primary_metadata_provider", ""))
    selected = _selected_provider(
        requested, available, runtime.primary_metadata_provider, artist.watchlist_provider
    )
    artist.primary_metadata_provider = selected if selected in available else None
    await db.commit()
    release_type = str(form.get("release_type", ""))
    sort = str(form.get("sort", "desc"))
    return RedirectResponse(
        _artist_page_url(artist.id, provider=selected, release_type=release_type, sort=sort),
        status_code=303,
    )


@router.post("/artists/catalog/{artist_id}/enrich", include_in_schema=False)
async def enrich_catalog_artist_page(
    artist_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    result = await db.execute(
        select(CatalogArtist)
        .where(CatalogArtist.id == artist_id)
        .options(selectinload(CatalogArtist.albums))
    )
    artist = result.scalar_one_or_none()
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")
    runtime = await get_runtime_settings(db)
    if await _queue_artist_enrichment(db, artist.id):
        background_tasks.add_task(
            _enrich_artist_task, artist.id, runtime.enabled_metadata_providers
        )
    return RedirectResponse("/library", status_code=303)


@router.post("/artists/catalog/{artist_id}/monitor", include_in_schema=False)
async def monitor_catalog_artist_page(
    artist_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    load = selectinload(CatalogArtist.identities).selectinload(CatalogArtistIdentity.releases)
    artist = (
        await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(load)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")
    await ensure_legacy_provider_snapshots(db, artist)
    await db.commit()
    artist = (
        await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(load)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    form = await request.form()
    runtime = await get_runtime_settings(db)
    available = available_artist_providers(artist)
    requested_view_provider = str(form.get("provider", ""))
    view_provider = _selected_provider(
        requested_view_provider,
        available,
        runtime.primary_metadata_provider,
        artist.watchlist_provider,
    )
    release_type = str(form.get("release_type", ""))
    sort = str(form.get("sort", "desc"))

    quick = _form_bool(form.get("quick", ""))
    if quick:
        was_monitored = artist.monitored
        artist.monitored = not artist.monitored
        if artist.monitored and not was_monitored:
            _apply_runtime_watchlist_defaults(artist, runtime)
        if artist.monitored:
            primary_provider = _artist_primary_provider(artist, runtime, available)
            artist.watchlist_provider = _selected_provider(
                primary_provider,
                available,
                runtime.primary_metadata_provider,
                artist.watchlist_provider,
            )
    else:
        artist.monitored = _form_bool(form.get("monitored", ""))
        artist.watchlist_release_albums = _form_bool(form.get("watchlist_release_albums", ""))
        artist.watchlist_release_singles = _form_bool(form.get("watchlist_release_singles", ""))
        artist.watchlist_release_eps = _form_bool(form.get("watchlist_release_eps", ""))
        artist.watchlist_monitor_upgrades = _form_bool(form.get("watchlist_monitor_upgrades", ""))
        policy = str(form.get("monitor_policy", artist.monitor_policy or "all"))
        artist.monitor_policy = policy if policy in {"all", "albums_only", "none_new"} else "all"
        requested_watchlist = str(form.get("watchlist_provider", ""))
        artist.watchlist_provider = _selected_provider(
            requested_watchlist,
            available,
            _artist_primary_provider(artist, runtime, available),
            artist.watchlist_provider,
        )

    selected_identity = next(
        (
            identity
            for identity in artist.identities
            if identity.provider == artist.watchlist_provider
        ),
        None,
    )
    bulk = "all" if quick and artist.monitored else "none" if quick else str(form.get("bulk", ""))
    if bulk == "all":
        artist.watchlist_release_albums = True
        artist.watchlist_release_singles = True
        artist.watchlist_release_eps = True
    elif bulk == "albums_only" or bulk == "singles_off":
        artist.watchlist_release_albums = True
        artist.watchlist_release_singles = False
        artist.watchlist_release_eps = False
    elif bulk == "none":
        artist.watchlist_release_albums = False
        artist.watchlist_release_singles = False
        artist.watchlist_release_eps = False

    all_releases = [release for identity in artist.identities for release in identity.releases]
    if selected_identity is not None:
        apply_release_monitoring_policy(artist, all_releases)
        sync_canonical_monitoring(artist, all_releases)
    else:
        sync_canonical_monitoring(artist, [])
    if all_releases:
        await _sync_artist_upgrade_monitoring(db, artist, all_releases)
    primary_provider = _artist_primary_provider(artist, runtime, available)
    primary_identity = next(
        (identity for identity in artist.identities if identity.provider == primary_provider), None
    )
    refresh_provider = ""
    if artist.monitored and primary_identity is not None and not primary_identity.releases:
        refresh_provider = primary_provider
    elif artist.monitored and selected_identity is not None and not selected_identity.releases:
        refresh_provider = selected_identity.provider
    await db.commit()
    if refresh_provider:
        _start_discography_task(artist.id, refresh_provider)
    if _wants_json(request):
        return JSONResponse(_artist_watchlist_payload(artist))
    return RedirectResponse(
        _artist_page_url(artist.id, provider=view_provider, release_type=release_type, sort=sort),
        status_code=303,
    )


@router.post(
    "/artists/catalog/{artist_id}/release-families/{anchor_release_id}",
    include_in_schema=False,
)
async def set_catalog_release_family_editions(
    artist_id: int,
    anchor_release_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    load = (
        selectinload(CatalogArtist.identities)
        .selectinload(CatalogArtistIdentity.releases)
        .selectinload(CatalogAlbumProvider.catalog_album)
    )
    artist = (
        await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(load, selectinload(CatalogArtist.albums))
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")

    form = await request.form()
    submitted_watchlist_provider = str(form.get("family_provider", ""))
    if (
        not artist.watchlist_provider
        or submitted_watchlist_provider != artist.watchlist_provider
        or submitted_watchlist_provider not in VALID_METADATA_PROVIDERS
    ):
        raise HTTPException(status_code=400, detail="Release family provider is not selected")
    identity = next(
        (
            item
            for item in artist.identities
            if item.provider == artist.watchlist_provider
            and any(release.id == anchor_release_id for release in item.releases)
        ),
        None,
    )
    if identity is None:
        raise HTTPException(status_code=400, detail="Release family anchor is invalid")
    family = next(
        (
            projected
            for projected in project_release_families(list(identity.releases))
            if any(release.id == anchor_release_id for release in projected.releases)
        ),
        None,
    )
    if family is None:
        raise HTTPException(status_code=400, detail="Release family anchor is invalid")

    action = str(form.get("action", "save"))
    if action == "defaults":
        for release in family.releases:
            release.monitor_override = None
    elif action == "save":
        raw_ids = [str(value) for value in form.getlist("edition")]
        if any(not value.isdigit() or len(value) > 19 for value in raw_ids):
            raise HTTPException(status_code=400, detail="Release edition is invalid")
        selected_ids = {int(value) for value in raw_ids}
        representative_ids = {
            representative.id for representative in family.representatives.values()
        }
        if not selected_ids.issubset(representative_ids):
            raise HTTPException(status_code=400, detail="Release edition is not in this family")
        set_family_monitor_overrides(list(family.releases), selected_ids)
    else:
        raise HTTPException(status_code=400, detail="Release family action is invalid")

    all_releases = [release for item in artist.identities for release in item.releases]
    apply_release_monitoring_policy(artist, all_releases)
    sync_canonical_monitoring(artist, all_releases)
    await _sync_artist_upgrade_monitoring(db, artist, all_releases)
    await db.commit()
    location = _artist_page_url(
        artist.id,
        provider=str(form.get("provider", "")),
        release_type=str(form.get("release_type", "")),
        sort=str(form.get("sort", "desc")),
    )
    return RedirectResponse(f"{location}#release-family-{family.anchor.id}", status_code=303)


@router.get("/artists/catalog/{artist_id}/state", include_in_schema=False)
async def catalog_artist_state(
    artist_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> JSONResponse:
    artist = (
        await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(
                selectinload(CatalogArtist.identities).selectinload(CatalogArtistIdentity.releases)
            )
        )
    ).scalar_one_or_none()
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")
    providers: dict[str, dict[str, object]] = {}
    for identity in artist.identities:
        try:
            metadata = json.loads(identity.metadata_json or "{}")
        except (json.JSONDecodeError, TypeError):
            metadata = {}
        state = str(metadata.get("discography_state") or "idle")
        if (artist_id, identity.provider) in _discography_tasks:
            state = "loading"
        elif identity.releases:
            state = "ready"
        providers[identity.provider] = {
            "state": state,
            "release_count": len(project_release_families(list(identity.releases))),
            "error": metadata.get("discography_error") if state == "failed" else None,
        }
    return JSONResponse({"providers": providers})


@router.get("/artists/catalog/{artist_id}/discography", response_class=HTMLResponse)
async def catalog_artist_discography_fragment(
    request: Request,
    artist_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    provider: str = "",
    release_type: str = "",
    sort: str = "desc",
) -> HTMLResponse:
    page = await catalog_artist_page(
        request,
        artist_id,
        BackgroundTasks(),
        db,
        settings,
        provider=provider,
        release_type=release_type,
        sort=sort,
    )
    context = getattr(page, "context", None)
    if not isinstance(context, dict):
        raise HTTPException(status_code=500, detail="Discography fragment unavailable")
    return _templates(request).TemplateResponse(request, "partials/_discography.html", context)


@router.post("/artists/catalog/{artist_id}/discography/refresh", include_in_schema=False)
async def refresh_catalog_artist_discography(
    artist_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    form = await request.form()
    provider = str(form.get("provider", ""))
    identity = await db.scalar(
        select(CatalogArtistIdentity.id).where(
            CatalogArtistIdentity.artist_id == artist_id,
            CatalogArtistIdentity.provider == provider,
        )
    )
    if provider not in VALID_METADATA_PROVIDERS or identity is None:
        raise HTTPException(status_code=400, detail="Unavailable metadata provider")
    started = _start_discography_task(artist_id, provider)
    if _wants_json(request):
        return JSONResponse({"provider": provider, "state": "loading", "started": started})
    return RedirectResponse(
        _artist_page_url(
            artist_id,
            provider=provider,
            release_type=str(form.get("release_type", "")),
            sort=str(form.get("sort", "desc")),
        ),
        status_code=303,
    )


@router.get("/artists/monitored", include_in_schema=False)
async def monitored_artists_page() -> RedirectResponse:
    return RedirectResponse("/library", status_code=303)


async def _wanted_release_context(
    db: AsyncSession, album_ids: list[int]
) -> dict[int, SimpleNamespace]:
    if not album_ids:
        return {}
    latest_job = aliased(Job)
    latest_job_id = (
        select(Job.id)
        .where(Job.catalog_album_id == CatalogAlbum.id)
        .order_by(Job.updated_at.desc(), Job.id.desc())
        .limit(1)
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    review_count = (
        select(func.count(StagingReviewItem.id))
        .join(Release, Release.id == StagingReviewItem.release_id)
        .join(Job, Job.id == Release.job_id)
        .where(
            Job.catalog_album_id == CatalogAlbum.id,
            StagingReviewItem.review_state == ReviewDecision.pending,
            Release.review_dismissed_at.is_(None),
        )
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    rejected_count = (
        select(func.count(func.distinct(SourceCandidateBlock.id)))
        .select_from(SourceCandidateBlock)
        .join(
            AcquisitionAttempt,
            and_(
                AcquisitionAttempt.provider == SourceCandidateBlock.provider,
                AcquisitionAttempt.peer == SourceCandidateBlock.peer,
                AcquisitionAttempt.remote_path == SourceCandidateBlock.filename,
            ),
        )
        .where(
            AcquisitionAttempt.catalog_album_id == CatalogAlbum.id,
            or_(
                SourceCandidateBlock.blocked_until.is_(None),
                SourceCandidateBlock.blocked_until > datetime.now(UTC),
            ),
        )
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    rows = (
        await db.execute(
            select(
                CatalogAlbum.id,
                latest_job.status,
                latest_job.updated_at,
                latest_job.result_json,
                latest_job.source,
                review_count.label("review_count"),
                rejected_count.label("rejected_count"),
            )
            .outerjoin(latest_job, latest_job.id == latest_job_id)
            .where(CatalogAlbum.id.in_(album_ids))
        )
    ).all()
    result: dict[int, SimpleNamespace] = {}
    for row in rows:
        failure = ""
        if row.result_json:
            try:
                payload = json.loads(row.result_json)
                raw_error = payload.get("error") or payload.get("message") or payload.get("code")
                if isinstance(raw_error, dict):
                    raw_error = raw_error.get("message") or raw_error.get("code")
                failure = str(raw_error or "")
            except (json.JSONDecodeError, TypeError, AttributeError):
                failure = "Acquisition failed"
        state = "Needs search"
        if row.review_count:
            state = "Awaiting review"
        elif row.status == JobStatus.running:
            state = "Downloading"
        elif row.status == JobStatus.pending:
            state = "Search scheduled"
        elif row.status == JobStatus.partial:
            state = "Partially downloaded"
        elif row.status in {JobStatus.failed, JobStatus.cancelled}:
            state = "No usable candidates" if row.rejected_count else "Failed"
        result[int(row.id)] = SimpleNamespace(
            state=state,
            last_attempt=row.updated_at,
            failure=failure,
            provider=row.source,
            rejected_count=int(row.rejected_count or 0),
            review_count=int(row.review_count or 0),
        )
    return result


def _discography_error(message: str) -> HTMLResponse:
    """Return a native, escaped validation page without exposing exception internals."""
    import html

    detail = html.escape(message)
    return HTMLResponse(
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "<title>Batch scope needs review</title></head><body>"
        "<main><h1>Review the batch scope</h1>"
        f'<p>{detail}</p><p><a href="/wanted">Return to Wanted and correct the form</a>.'
        "</p></main></body></html>",
        status_code=400,
    )


def _discography_notice(value: str) -> str | None:
    return {
        "confirmed": "Batch confirmed and queued.",
        "queued": "Queued missing releases.",
        "complete": "No missing release jobs were needed; this batch is complete.",
        "paused": "Batch paused. Running downloads were left alone.",
        "resumed": "Batch resumed.",
        "cancelled": "Batch cancelled. Pending batch-owned jobs were cancelled.",
        "retried": "Selected releases were queued for retry.",
        "no-eligible-retries": "No selected releases were eligible for retry.",
    }.get(value)


async def _render_discography_batch(
    request: Request,
    db: AsyncSession,
    batch_id: int,
    *,
    scope_changed: bool = False,
) -> HTMLResponse:
    batch = (
        await db.execute(
            select(DiscographyBatch)
            .where(DiscographyBatch.id == batch_id)
            .options(
                selectinload(DiscographyBatch.items)
                .selectinload(DiscographyBatchItem.catalog_album)
                .selectinload(CatalogAlbum.artist),
                selectinload(DiscographyBatch.items)
                .selectinload(DiscographyBatchItem.provider_release)
                .selectinload(CatalogAlbumProvider.artist_identity)
                .selectinload(CatalogArtistIdentity.artist),
                selectinload(DiscographyBatch.items)
                .selectinload(DiscographyBatchItem.job_links)
                .selectinload(DiscographyBatchItemJob.job),
            )
        )
    ).scalar_one_or_none()
    if batch is None:
        raise HTTPException(status_code=404, detail="Discography batch not found")
    try:
        scope = json.loads(batch.scope_json)
    except (TypeError, json.JSONDecodeError):
        scope = {}
    items = sorted(
        batch.items,
        key=lambda item: (
            item.artist_name.casefold(),
            item.release_year or "",
            item.release_title.casefold(),
            item.id,
        ),
    )
    item_rows: list[dict[str, object]] = []
    hydration_reasons = {
        "catalog_manifest_missing",
        "catalog_manifest_incomplete",
        "catalog_manifest_overfull",
        "catalog_manifest_invalid_positions",
        "hydration_failed",
    }
    display_counts = {
        "matching": len(items),
        "complete": sum(item.state == DiscographyBatchItemState.complete for item in items),
        "active": sum(item.active_count for item in items),
        "hydration": sum(
            item.state == DiscographyBatchItemState.hydrating
            or item.reason_code in hydration_reasons
            for item in items
        ),
        "missing": sum(item.target_count for item in items),
        "estimated": sum(item.estimated_job_count for item in items),
    }
    for item in items:
        album = item.catalog_album
        provider_release = item.provider_release
        artist_id = album.artist_id if album is not None else None
        if artist_id is None and provider_release is not None:
            artist_id = provider_release.artist_identity.artist_id
        statuses: dict[str, int] = {}
        for link in item.job_links:
            key = link.job.status.value
            statuses[key] = statuses.get(key, 0) + 1
        item_rows.append(
            {
                "item": item,
                "artist_id": artist_id,
                "album_id": album.id if album is not None else None,
                "artwork_url": album.artwork_url if album is not None else None,
                "job_count": len(item.job_links),
                "job_statuses": statuses,
                "retryable": is_discography_batch_item_retryable(item.state, item.reason_code),
            }
        )
    return _templates(request).TemplateResponse(
        request,
        "discography_batch.html",
        {
            "batch": batch,
            "items": item_rows,
            "scope": scope if isinstance(scope, dict) else {},
            "scope_changed": scope_changed,
            "display_counts": display_counts,
            "notice": _discography_notice(request.query_params.get("notice", "")),
        },
    )


@router.post("/discography-batches/preview", include_in_schema=False)
async def preview_discography_batch_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    form = await request.form()
    scope_kind = str(form.get("scope_kind", ""))
    payload: dict[str, object]
    if scope_kind == DiscographyScopeKind.artist:
        payload = {
            "artist_id": form.get("artist_id"),
            "provider": form.get("provider", ""),
            "release_type": form.get("release_type", "all"),
            "year_from": form.get("year_from"),
            "year_to": form.get("year_to"),
            "monitoring_status": form.get("monitoring_status", "monitored"),
        }
    elif scope_kind in {
        DiscographyScopeKind.wanted_selected,
        DiscographyScopeKind.wanted_page,
    }:
        album_ids = list(form.getlist("catalog_album_ids"))
        if not album_ids:
            return _discography_error("Select at least one release to preview.")
        payload = {"album_ids": album_ids}
    elif scope_kind == DiscographyScopeKind.wanted_all_matching:
        payload = {
            "q": form.get("q", ""),
            "sort": form.get("sort", "year"),
            "status": form.get("status", "all"),
        }
    else:
        return _discography_error("Choose a supported preview scope and try again.")
    try:
        preview = await create_discography_batch_preview(db, scope_kind, payload)
    except DiscographyScopeError as exc:
        return _discography_error(str(exc))
    return RedirectResponse(f"/discography-batches/{preview.id}", status_code=303)


@router.post("/discography-batches/queue", include_in_schema=False)
async def queue_discography_batch_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    form = await request.form()
    scope_kind = str(form.get("scope_kind", ""))
    payload: dict[str, object]
    if scope_kind == DiscographyScopeKind.artist:
        payload = {
            "artist_id": form.get("artist_id"),
            "provider": form.get("provider", ""),
            "release_type": form.get("release_type", "all"),
            "year_from": form.get("year_from"),
            "year_to": form.get("year_to"),
            "monitoring_status": form.get("monitoring_status", "monitored"),
        }
    elif scope_kind in {
        DiscographyScopeKind.wanted_selected,
        DiscographyScopeKind.wanted_page,
    }:
        album_ids = list(form.getlist("catalog_album_ids"))
        if not album_ids:
            return _discography_error("Select at least one release to queue.")
        payload = {"album_ids": album_ids}
    elif scope_kind == DiscographyScopeKind.wanted_all_matching:
        payload = {
            "q": form.get("q", ""),
            "sort": form.get("sort", "year"),
            "status": form.get("status", "all"),
        }
    else:
        return _discography_error("Choose a supported queue scope and try again.")
    try:
        batch = await queue_discography_batch(db, scope_kind, payload)
    except DiscographyScopeError as exc:
        return _discography_error(str(exc))
    if batch.state == DiscographyBatchState.queued:
        _wake_discography_batch_runner(request)
        notice = "queued"
    else:
        notice = "complete"
    return RedirectResponse(f"/activity?notice={notice}", status_code=303)


@router.get("/discography-batches/{batch_id}", response_class=HTMLResponse)
async def discography_batch_page(
    request: Request,
    batch_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> HTMLResponse:
    return await _render_discography_batch(request, db, batch_id)


def _wake_discography_batch_runner(request: Request) -> None:
    runner = getattr(request.app.state, "discography_batch_runner", None)
    if runner is not None:
        runner.wake()


@router.post("/discography-batches/{batch_id}/confirm", include_in_schema=False)
async def confirm_discography_batch_page(
    request: Request,
    batch_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    try:
        result = await confirm_discography_batch(db, batch_id)
    except DiscographyScopeError as exc:
        return _discography_error(str(exc))
    if result.scope_changed:
        return await _render_discography_batch(request, db, batch_id, scope_changed=True)
    _wake_discography_batch_runner(request)
    return RedirectResponse(f"/discography-batches/{batch_id}?notice=confirmed", status_code=303)


@router.post("/discography-batches/{batch_id}/pause", include_in_schema=False)
async def pause_discography_batch_page(
    batch_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    try:
        await pause_discography_batch(db, batch_id)
    except DiscographyScopeError as exc:
        return _discography_error(str(exc))
    return RedirectResponse(f"/discography-batches/{batch_id}?notice=paused", status_code=303)


@router.post("/discography-batches/{batch_id}/resume", include_in_schema=False)
async def resume_discography_batch_page(
    request: Request,
    batch_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    try:
        await resume_discography_batch(db, batch_id)
    except DiscographyScopeError as exc:
        return _discography_error(str(exc))
    _wake_discography_batch_runner(request)
    return RedirectResponse(f"/discography-batches/{batch_id}?notice=resumed", status_code=303)


@router.post("/discography-batches/{batch_id}/cancel", include_in_schema=False)
async def cancel_discography_batch_page(
    batch_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    try:
        result = await cancel_discography_batch(db, batch_id)
    except DiscographyScopeError as exc:
        return _discography_error(str(exc))
    for job_id in result.cancel_job_ids:
        job_dispatcher.cancel(job_id)
    return RedirectResponse(f"/discography-batches/{batch_id}?notice=cancelled", status_code=303)


@router.post("/discography-batches/{batch_id}/retry", include_in_schema=False)
async def retry_discography_batch_page(
    request: Request,
    batch_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    form = await request.form()
    raw_ids = [str(value) for value in form.getlist("item_ids")]
    if not raw_ids or any(not value.isdigit() for value in raw_ids):
        return _discography_error("Select at least one failed release to retry.")
    try:
        result = await retry_discography_batch_items(
            db, batch_id, [int(value) for value in raw_ids]
        )
    except (DiscographyScopeError, ValueError) as exc:
        return _discography_error(str(exc))
    if result.reset_item_ids:
        _wake_discography_batch_runner(request)
        notice = "retried"
    else:
        notice = "no-eligible-retries"
    return RedirectResponse(f"/discography-batches/{batch_id}?notice={notice}", status_code=303)


@router.get("/wanted", response_class=HTMLResponse)
async def wanted_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    q: str = "",
    sort: str = "year",
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
    status: str = "all",
) -> HTMLResponse:
    valid_status = status if status in {"all", "needs-search", "active", "failed"} else "all"
    releases = await get_missing_releases_page(
        db, q=q, sort=sort, status=valid_status, page=page, per_page=per_page
    )
    wanted_context = await _wanted_release_context(db, [release.id for release in releases.items])
    filter_params: dict[str, str] = {
        "sort": sort,
        "status": valid_status,
        "per_page": str(per_page),
    }
    if q:
        filter_params["q"] = q
    return _templates(request).TemplateResponse(
        request,
        "wanted.html",
        {
            "releases": releases,
            "q": q,
            "sort": sort,
            "status": valid_status,
            "wanted_context": wanted_context,
            "per_page": per_page,
            "filter_qs": urlencode(filter_params),
        },
    )


@router.post("/wanted/queue", include_in_schema=False)
async def queue_wanted_releases(
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
    catalog_album_ids: Annotated[list[int] | None, Form()] = None,
    queue_scope: Annotated[str, Form()] = "selected",
    q: Annotated[str, Form()] = "",
    sort: Annotated[str, Form()] = "year",
    status: Annotated[str, Form()] = "all",
    request: Request = None,  # type: ignore[assignment]
) -> Response:
    if queue_scope == "all_matching":
        selected_ids = await get_missing_release_ids(db, q=q, sort=sort, status=status)
    else:
        selected_ids = list(dict.fromkeys(catalog_album_ids or []))
    if not selected_ids:
        return _wanted_queue_response(request, queued=0, album_ids=[])
    runtime = await get_runtime_settings(db)
    job_ids: list[int] = []
    queued_album_ids: list[int] = []
    for album_id in selected_ids:
        album = (
            await db.execute(
                select(CatalogAlbum)
                .where(CatalogAlbum.id == album_id)
                .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
            )
        ).scalar_one_or_none()
        if album is None:
            continue
        try:
            await _ensure_catalog_tracks(db, settings, album)
            refreshed = (
                await db.execute(
                    select(CatalogAlbum)
                    .where(CatalogAlbum.id == album.id)
                    .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
                    .execution_options(populate_existing=True)
                )
            ).scalar_one()
            ids = await queue_catalog_album_missing_track_jobs(
                db,
                refreshed,
                library_root=settings.library_root,
                quality_profile=runtime.quality_profile,
            )
        except Exception:
            logger.warning("Wanted queue failed for album %d", album_id, exc_info=True)
            await db.rollback()
            continue
        if ids:
            queued_album_ids.append(album_id)
            job_ids.extend(ids)
    for job_id in job_ids:
        await job_dispatcher.dispatch(job_id)
    return _wanted_queue_response(request, queued=len(job_ids), album_ids=queued_album_ids)


@router.get("/albums/{album_id}", response_class=HTMLResponse)
async def catalog_album_page(
    request: Request,
    album_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
) -> HTMLResponse:
    result = await db.execute(
        select(CatalogAlbum)
        .where(CatalogAlbum.id == album_id)
        .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
    )
    album = result.scalar_one_or_none()
    if album is None:
        raise HTTPException(status_code=404, detail="Catalog album not found")
    try:
        await _ensure_catalog_tracks(db, settings, album)
        await db.commit()
    except Exception:
        logger.exception("Catalog album hydration failed for album %s", album_id)
        await db.rollback()
    result = await db.execute(
        select(CatalogAlbum)
        .where(CatalogAlbum.id == album_id)
        .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
    )
    album = result.scalar_one()
    progress = (await get_release_progress(db, [album.id], library_root=settings.library_root))[
        album.id
    ]
    watched_release_ids = {
        row[0]
        for row in (
            await db.execute(
                select(MonitoringRecord.release_id)
                .join(Release, Release.id == MonitoringRecord.release_id)
                .join(Track, Track.release_id == Release.id)
                .where(
                    Track.catalog_album_id == album.id,
                    MonitoringRecord.status != MonitoringStatus.paused,
                )
                .distinct()
            )
        ).all()
    }
    total_runtime_sec = sum(track.duration_sec or 0 for track in album.tracks)
    retag_status = request.query_params.get("retag", "")
    flash_message: str | None = None
    flash_type = "info"
    if retag_status == "ok":
        try:
            count = max(0, int(request.query_params.get("count", "0")))
        except ValueError:
            count = 0
        noun = "file" if count == 1 else "files"
        flash_message = f"Retagged {count} audio {noun} from Audiohoard metadata."
        flash_type = "ok"
    quality_status = request.query_params.get("quality", "")
    if quality_status == "ok":
        try:
            count = max(0, int(request.query_params.get("deleted", "0")))
            review = max(0, int(request.query_params.get("review", "0")))
        except ValueError:
            count = 0
            review = 0
        noun = "duplicate" if count == 1 else "duplicates"
        flash_message = f"Removed {count} lower-quality {noun}."
        if review:
            flash_message += f" {review} ambiguous duplicate file(s) still need review."
        flash_type = "ok"
    elif retag_status == "error":
        detail = request.query_params.get("detail", "Metadata repair could not be completed")
        flash_message = f"Metadata repair failed: {detail}"
        flash_type = "error"
    return _templates(request).TemplateResponse(
        request,
        "catalog_album.html",
        {
            "album": album,
            "progress": progress,
            "total_runtime_sec": total_runtime_sec,
            "flash_message": flash_message,
            "flash_type": flash_type,
            "watching_for_upgrades": bool(watched_release_ids),
        },
    )


@router.post("/library/albums/{catalog_album_id}/delete", include_in_schema=False)
async def delete_catalog_album_files(
    catalog_album_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Response:
    if not await _confirmed_delete(request):
        raise HTTPException(status_code=422, detail="Explicit deletion confirmation is required")
    try:
        result = await remove_catalog_album(
            db,
            catalog_album_id,
            library_root=settings.library_root,
            cache_root=settings.artwork_cache_root.parent / "library-audio",
        )
    except LibraryRemovalError as exc:
        status = 404 if "not found" in str(exc).casefold() else 409
        raise HTTPException(status_code=status, detail=str(exc)) from None
    if _wants_json(request):
        return JSONResponse(
            {
                "catalog_album_id": catalog_album_id,
                "deleted_files": result.deleted_files,
                "track_ids": list(result.affected_track_ids),
                "already_removed": result.already_removed,
                "cleanup_pending": result.cleanup_pending,
            }
        )
    return RedirectResponse(f"/albums/{catalog_album_id}?removed=1", status_code=303)


@router.post("/albums/{album_id}/watch-upgrade", include_in_schema=False)
async def watch_album_quality_upgrade(
    album_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    values = [str(value).lower() for value in form.getlist("monitor_for_upgrades")]
    enabled = True if not values else any(value in {"1", "true", "yes", "on"} for value in values)
    album = await db.get(CatalogAlbum, album_id)
    if album is None:
        raise HTTPException(status_code=404, detail="Catalog album not found")
    await sync_album_upgrade_monitoring(db, album_id, enabled)
    await db.commit()
    return RedirectResponse(f"/albums/{album_id}", status_code=303)


@router.post("/albums/{album_id}/retag", include_in_schema=False)
async def retag_catalog_album_files(
    album_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    try:
        result = await retag_catalog_album(
            db,
            album_id,
            library_root=settings.library_root,
        )
    except ImportExecutionError as exc:
        query = urlencode({"retag": "error", "detail": str(exc)})
        return RedirectResponse(f"/albums/{album_id}?{query}", status_code=303)
    except Exception:
        logger.exception("Unexpected metadata repair failure for album %d", album_id)
        query = urlencode(
            {"retag": "error", "detail": "Unexpected error while repairing metadata"}
        )
        return RedirectResponse(f"/albums/{album_id}?{query}", status_code=303)
    query = urlencode({"retag": "ok", "count": str(result.files_retagged)})
    return RedirectResponse(f"/albums/{album_id}?{query}", status_code=303)


@router.post("/albums/{album_id}/quality-deduplicate", include_in_schema=False)
async def deduplicate_catalog_album_quality(
    album_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    runtime = await get_runtime_settings(db)
    result = await reconcile_album_quality_duplicates(
        db,
        album_id,
        library_root=settings.library_root,
        quality_profile=runtime.quality_profile,
        defer_filesystem_delete=True,
    )
    query = urlencode(
        {
            "quality": "ok",
            "deleted": str(result.deleted_files),
            "review": str(result.review_required),
        }
    )
    return RedirectResponse(f"/albums/{album_id}?{query}", status_code=303)


@router.post("/artists/catalog/{artist_id}/download-monitored", include_in_schema=False)
async def download_monitored_catalog_albums(
    artist_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
    request: Request = None,  # type: ignore[assignment]
) -> Response:
    load = selectinload(CatalogArtist.identities).selectinload(CatalogArtistIdentity.releases)
    artist = (
        await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(load)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")
    identity = next(
        (item for item in artist.identities if item.provider == artist.watchlist_provider),
        None,
    )
    canonical: dict[int, str] = {}
    if identity is not None:
        for release in identity.releases:
            album = release.catalog_album
            if release.monitored and album is not None and not album.in_library:
                canonical[album.id] = album.title
    artist_name = artist.name
    runtime = await get_runtime_settings(db)
    job_ids: list[int] = []
    for album_id, album_title in canonical.items():
        album_query = f"{artist_name} {album_title}".strip()
        album = (
            await db.execute(
                select(CatalogAlbum)
                .where(CatalogAlbum.id == album_id)
                .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
            )
        ).scalar_one()
        # Hydrate catalog tracks before dispatching so the runner has a complete
        # manifest. A failed hydration is retained as an actionable failed job
        # instead of silently disappearing from the bulk request.
        try:
            await _ensure_catalog_tracks(db, settings, album)
        except Exception:
            await db.rollback()
            logger.warning(
                "Pre-dispatch bulk hydration failed for album %d (%s)",
                album_id,
                album_title,
            )
            failed_job = Job(
                source="priority",
                query=album_query,
                status=JobStatus.failed,
                catalog_album_id=album_id,
                result_json=json.dumps(
                    {
                        "error": {
                            "code": "catalog_tracks_incomplete",
                            "operation": "hydrate",
                            "retryable": True,
                        }
                    },
                    sort_keys=True,
                ),
            )
            db.add(failed_job)
            await db.commit()
            continue
        refreshed = (
            await db.execute(
                select(CatalogAlbum)
                .where(CatalogAlbum.id == album_id)
                .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
                .execution_options(populate_existing=True)
            )
        ).scalar_one()
        ids = await queue_catalog_album_missing_track_jobs(
            db,
            refreshed,
            library_root=settings.library_root,
            quality_profile=runtime.quality_profile,
        )
        job_ids.extend(ids)
    for job_id in job_ids:
        await job_dispatcher.dispatch(job_id)
    return _download_many_response(request, queued=len(job_ids), artist_id=artist_id)


@router.post("/albums/{album_id}/download", include_in_schema=False)
async def download_catalog_album(
    album_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
    request: Request = None,  # type: ignore[assignment]
) -> Response:
    result = await db.execute(
        select(CatalogAlbum)
        .where(CatalogAlbum.id == album_id)
        .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
    )
    album = result.scalar_one_or_none()
    if album is None:
        raise HTTPException(status_code=404, detail="Catalog album not found")
    try:
        await _ensure_catalog_tracks(db, settings, album)
    except Exception:
        logger.warning("Pre-dispatch catalog hydration failed for album %d", album_id)
        raise HTTPException(
            status_code=502,
            detail="Could not load album tracklist from provider; download not started",
        ) from None
    refreshed = (
        await db.execute(
            select(CatalogAlbum)
            .where(CatalogAlbum.id == album.id)
            .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    runtime = await get_runtime_settings(db)
    job_ids = await queue_catalog_album_missing_track_jobs(
        db,
        refreshed,
        library_root=settings.library_root,
        quality_profile=runtime.quality_profile,
    )
    for job_id in job_ids:
        await job_dispatcher.dispatch(job_id)
    return _download_response(request, queued=len(job_ids), album_id=album_id)


@router.post("/albums/{album_id}/tracks/{track_id}/download", include_in_schema=False)
async def download_catalog_track(
    album_id: int,
    track_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
    request: Request = None,  # type: ignore[assignment]
) -> Response:
    result = await db.execute(
        select(CatalogAlbum)
        .where(CatalogAlbum.id == album_id)
        .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
    )
    album = result.scalar_one_or_none()
    if album is None:
        raise HTTPException(status_code=404, detail="Catalog album not found")
    try:
        await _ensure_catalog_tracks(db, settings, album)
    except Exception:
        logger.warning("Pre-dispatch catalog hydration failed for album %d", album_id)
        raise HTTPException(
            status_code=502,
            detail="Could not load album tracklist from provider; download not started",
        ) from None
    album = (
        await db.execute(
            select(CatalogAlbum)
            .where(CatalogAlbum.id == album_id)
            .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    track = next((t for t in album.tracks if t.id == track_id), None)
    if track is None:
        raise HTTPException(status_code=404, detail="Catalog track not found")
    query = f"{catalog_track_artist_name(album, track)} {track.title}".strip()
    job = Job(
        source="priority",
        query=query,
        status=JobStatus.pending,
        catalog_album_id=album.id,
        catalog_track_id=track.id,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    await job_dispatcher.dispatch(job.id)
    return _download_response(request, queued=1, album_id=album.id)
