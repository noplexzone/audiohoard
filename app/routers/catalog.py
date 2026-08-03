from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any
from urllib.parse import urlencode

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user, require_mutation
from app.config import Settings
from app.database import get_db, run_with_sqlite_lock_retry
from app.jobs.dispatcher import job_dispatcher
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.catalog import (
    ReleaseProgress,
    aggregate_artist_release_rollup,
    get_artist_detail,
    get_library_artists_page,
    get_library_stats,
    get_missing_releases_page,
    get_release_progress,
    list_distinct_formats,
    list_distinct_sources,
    list_library_tracks,
    queue_catalog_album_missing_track_jobs,
)
from app.services.catalog_metadata import (
    VALID_METADATA_PROVIDERS,
    album_providers,
    available_artist_providers,
    enrich_catalog_artist,
    ensure_legacy_provider_snapshots,
    fetch_and_store_album,
    fetch_and_store_discography,
    fetch_catalog_artist_detail,
    release_bucket,
    upsert_catalog_artist,
)
from app.services.catalog_ownership import reconcile_deezer_catalog_ownership
from app.services.library_import import ImportExecutionError, retag_catalog_album
from app.services.library_removal import (
    LibraryRemovalError,
    remove_catalog_album,
    remove_imported_release_group,
)
from app.services.monitoring import (
    _monitoring_profile_from_runtime,
    current_release_quality,
)
from app.services.quality_upgrade import reconcile_album_quality_duplicates
from app.settings_service import RuntimeSettings, effective_settings_dep, get_runtime_settings

router = APIRouter(dependencies=[Depends(get_current_user)])
logger = logging.getLogger(__name__)


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
    if expected_before_hydration and existing >= expected_before_hydration:
        return
    await fetch_and_store_album(db, settings, album)
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


def _artist_release_kind_default(artist: CatalogArtist, release_kind: str) -> bool:
    watch_albums = (
        True if artist.watchlist_release_albums is None else artist.watchlist_release_albums
    )
    watch_singles = (
        False if artist.watchlist_release_singles is None else artist.watchlist_release_singles
    )
    watch_eps = False if artist.watchlist_release_eps is None else artist.watchlist_release_eps
    return (
        (release_kind == "album" and watch_albums)
        or (release_kind == "single" and watch_singles)
        or (release_kind == "ep" and watch_eps)
    )


def _apply_runtime_watchlist_defaults(artist: CatalogArtist, runtime: RuntimeSettings) -> None:
    artist.watchlist_release_albums = bool(runtime.default_watchlist_release_albums)
    artist.watchlist_release_singles = bool(runtime.default_watchlist_release_singles)
    artist.watchlist_release_eps = bool(runtime.default_watchlist_release_eps)
    artist.watchlist_monitor_upgrades = bool(runtime.default_watchlist_monitor_upgrades)
    artist.monitor_policy = "all"


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
        record.desired_quality_json = profile.to_json()
        record.history_json = history
        record.candidate_id = None


async def _sync_album_upgrade_monitoring(db: AsyncSession, album_id: int, enabled: bool) -> None:
    release_id = await _latest_imported_release_id_for_album(db, album_id)
    if release_id is not None:
        await _set_release_upgrade_monitoring(db, release_id, enabled)


async def _sync_artist_upgrade_monitoring(
    db: AsyncSession, artist: CatalogArtist, releases: list[CatalogAlbumProvider]
) -> None:
    for release in releases:
        if release.catalog_album_id is None:
            continue
        enabled = bool(
            artist.monitored and artist.watchlist_monitor_upgrades and release.monitored
        )
        await _sync_album_upgrade_monitoring(db, release.catalog_album_id, enabled)


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
    async with factory() as session:
        cfg = await build_effective_settings(session, get_settings())
        load = selectinload(CatalogArtist.identities).selectinload(CatalogArtistIdentity.releases)
        artist = (
            await session.execute(
                select(CatalogArtist).where(CatalogArtist.id == artist_id).options(load)
            )
        ).scalar_one_or_none()
        if artist is None:
            return
        try:
            artist.enrichment_state = "running"
            await session.commit()
            await fetch_and_store_discography(
                session,
                cfg,
                artist,
                provider_name=provider_name,
            )
            artist.enrichment_state = "idle"
            await session.commit()
            try:
                await reconcile_deezer_catalog_ownership(
                    get_session_factory(), cfg, artist_id=artist_id
                )
            except Exception:
                logger.exception(
                    "Catalog ownership reconciliation failed for artist %s", artist_id
                )
        except Exception:
            await session.rollback()
            failed = await session.get(CatalogArtist, artist_id)
            if failed is not None:
                failed.enrichment_state = "failed"
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
    artist_id = None
    runtime = None

    async def save_artist() -> None:
        nonlocal artist_id, runtime
        artist = await upsert_catalog_artist(db, detail)
        runtime = await get_runtime_settings(db)
        if monitor:
            artist.monitored = True
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

    await run_with_sqlite_lock_retry(db, save_artist, attempts=5, delay_seconds=0.35)
    assert artist_id is not None and runtime is not None
    if await _queue_artist_enrichment(db, artist_id):
        background_tasks.add_task(
            _enrich_artist_task, artist_id, runtime.enabled_metadata_providers
        )
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
) -> RedirectResponse:
    return await open_catalog_artist_page(
        provider,
        provider_id,
        request,
        background_tasks,
        db,
        settings,
        monitor=monitor.lower() in {"1", "true", "yes", "on"},
    )


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
    # GET navigation must stay read-only. Provider refreshes and legacy snapshot repairs
    # can involve slow network calls and SQLite writer locks; run them only from explicit
    # actions so artist pages remain usable while acquisition/monitoring jobs are active.
    provider_albums = (
        list(selected_identity.releases)
        if selected_identity is not None
        else _legacy_provider_album_rows(artist, selected_provider)
    )
    discography_loading = artist.enrichment_state in {"queued", "running"} and not provider_albums
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
    artist_rollup = aggregate_artist_release_rollup(release_progress.values())
    albums = sorted(
        provider_albums,
        key=lambda release: (release.year or "0000", release.title.casefold()),
        reverse=sort != "asc",
    )
    requested_kinds = {
        "Album": {"album"},
        "single_ep": {"single", "ep"},
        "Compilation": {"compilation"},
    }.get(release_type)
    if requested_kinds:
        albums = [release for release in albums if release.release_kind in requested_kinds]
    else:
        release_type = ""
    release_types = sorted(
        {release.release_type_raw for release in provider_albums if release.release_type_raw}
    )
    counts_by_type = {"albums": 0, "singles_eps": 0, "compilations": 0}
    for release in provider_albums:
        if release.release_kind in {"single", "ep"}:
            counts_by_type["singles_eps"] += 1
        elif release.release_kind == "compilation":
            counts_by_type["compilations"] += 1
        else:
            counts_by_type["albums"] += 1
    grouped_albums = (
        [
            ("Albums", [release for release in albums if release.release_kind == "album"]),
            (
                "Singles & EPs",
                [release for release in albums if release.release_kind in {"single", "ep"}],
            ),
            (
                "Compilations",
                [release for release in albums if release.release_kind == "compilation"],
            ),
            (
                "Other",
                [release for release in albums if release.release_kind in {"other", "unknown"}],
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
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
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
    submitted_release_provider = (
        requested_view_provider if requested_view_provider in available else None
    )
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
    selected_ids = {
        int(str(value)) for value in form.getlist("album_monitored") if str(value).isdigit()
    }
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

    if selected_identity is not None:
        for release in selected_identity.releases:
            if not artist.monitored or bulk == "none":
                release.monitored = False
            elif bulk == "all":
                release.monitored = _artist_release_kind_default(artist, release.release_kind)
            elif bulk == "albums_only":
                release.monitored = release.release_kind == "album"
            elif bulk == "singles_off":
                release.monitored = release.release_kind not in {"single", "ep"}
            elif submitted_release_provider == artist.watchlist_provider:
                release.monitored = release.id in selected_ids

    for album in artist.albums:
        album.monitored = False
    if artist.monitored and selected_identity is not None:
        for release in selected_identity.releases:
            if release.monitored and release.catalog_album is not None:
                release.catalog_album.monitored = True
    if selected_identity is not None:
        await _sync_artist_upgrade_monitoring(db, artist, list(selected_identity.releases))
    should_queue_enrichment = artist.monitored and (
        selected_identity is None or not list(selected_identity.releases)
    )
    if should_queue_enrichment and await _queue_artist_enrichment(db, artist.id):
        background_tasks.add_task(
            _enrich_artist_task, artist.id, runtime.enabled_metadata_providers
        )
    await db.commit()
    return RedirectResponse(
        _artist_page_url(artist.id, provider=view_provider, release_type=release_type, sort=sort),
        status_code=303,
    )


@router.get("/artists/catalog/{artist_id}/state", include_in_schema=False)
async def catalog_artist_state(
    artist_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> JSONResponse:
    artist = await db.get(CatalogArtist, artist_id)
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")
    return JSONResponse({"enrichment_state": artist.enrichment_state})


@router.get("/artists/monitored", include_in_schema=False)
async def monitored_artists_page() -> RedirectResponse:
    return RedirectResponse("/library", status_code=303)


@router.get("/wanted", response_class=HTMLResponse)
async def wanted_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    q: str = "",
    sort: str = "year",
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
) -> HTMLResponse:
    releases = await get_missing_releases_page(db, q=q, sort=sort, page=page, per_page=per_page)
    filter_params: dict[str, str] = {"sort": sort, "per_page": str(per_page)}
    if q:
        filter_params["q"] = q
    return _templates(request).TemplateResponse(
        request,
        "wanted.html",
        {
            "releases": releases,
            "q": q,
            "sort": sort,
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
    request: Request = None,  # type: ignore[assignment]
) -> Response:
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
    await _sync_album_upgrade_monitoring(db, album_id, enabled)
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
    track = next((t for t in album.tracks if t.id == track_id), None)
    if track is None:
        raise HTTPException(status_code=404, detail="Catalog track not found")
    query = f"{album.artist.name} {track.title}".strip()
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
