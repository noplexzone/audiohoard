from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user, require_mutation
from app.config import Settings
from app.database import get_db
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
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.catalog import (
    ReleaseProgress,
    get_artist_detail,
    get_library_artists_page,
    get_library_stats,
    get_missing_releases_page,
    get_release_progress,
    list_distinct_formats,
    list_distinct_sources,
    list_library_tracks,
)
from app.services.catalog_metadata import (
    VALID_METADATA_PROVIDERS,
    available_artist_providers,
    enrich_catalog_artist,
    ensure_legacy_provider_snapshots,
    fetch_and_store_album,
    fetch_and_store_discography,
    open_catalog_artist,
)
from app.services.library_import import ImportExecutionError, retag_catalog_album
from app.services.quality_upgrade import reconcile_album_quality_duplicates
from app.settings_service import effective_settings_dep, get_runtime_settings

router = APIRouter(dependencies=[Depends(get_current_user)])
logger = logging.getLogger(__name__)


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


async def _queue_artist_enrichment(db: AsyncSession, artist_id: int) -> bool:
    result = await db.execute(
        update(CatalogArtist)
        .where(
            CatalogArtist.id == artist_id,
            CatalogArtist.enrichment_state.not_in(("queued", "running")),
        )
        .values(enrichment_state="queued")
    )
    await db.commit()
    return bool(getattr(result, "rowcount", 0))


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
            await fetch_and_store_discography(
                session,
                cfg,
                artist,
                provider_name=provider_name,
            )
            await session.commit()
        except Exception:
            await session.rollback()
            logger.error(
                "Catalog discography refresh failed for artist %s via %s",
                artist_id,
                provider_name,
                exc_info=True,
            )


def _templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates  # type: ignore[no-any-return]


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


@router.get("/artists/catalog/open", response_class=HTMLResponse)
async def open_catalog_artist_page(
    provider: str,
    provider_id: str,
    background_tasks: BackgroundTasks,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    monitor: bool = False,
) -> RedirectResponse:
    artist = await open_catalog_artist(db, settings, provider, provider_id)
    runtime = await get_runtime_settings(db)
    if monitor:
        artist.monitored = True
        artist.monitor_policy = "all"
        available = [provider] if provider in VALID_METADATA_PROVIDERS else []
        artist.watchlist_provider = _selected_provider(
            runtime.primary_metadata_provider,
            available,
            runtime.primary_metadata_provider,
            provider,
        )
    await db.commit()
    background_tasks.add_task(_enrich_artist_task, artist.id, runtime.enabled_metadata_providers)
    return RedirectResponse(f"/artists/catalog/{artist.id}", status_code=303)


@router.post("/artists/catalog/open", include_in_schema=False)
async def open_catalog_artist_post(
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
    load = selectinload(CatalogArtist.identities).selectinload(CatalogArtistIdentity.releases)
    result = await db.execute(
        select(CatalogArtist).where(CatalogArtist.id == artist_id).options(load)
    )
    artist = result.scalar_one_or_none()
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")
    runtime = await get_runtime_settings(db)
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
    if artist.last_enriched_at is None and await _queue_artist_enrichment(db, artist.id):
        background_tasks.add_task(
            _enrich_artist_task, artist.id, runtime.enabled_metadata_providers
        )
    available_providers = available_artist_providers(artist)
    selected_provider = _selected_provider(
        provider, available_providers, runtime.primary_metadata_provider, artist.watchlist_provider
    )
    selected_identity = next(
        (identity for identity in artist.identities if identity.provider == selected_provider),
        None,
    )
    if selected_identity is not None and (
        not selected_identity.releases
        or any(_release_needs_track_count_refresh(item) for item in selected_identity.releases)
    ):
        background_tasks.add_task(_refresh_discography_task, artist.id, selected_provider)
    artist = (
        await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(load)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    available_providers = available_artist_providers(artist)
    selected_provider = _selected_provider(
        provider, available_providers, runtime.primary_metadata_provider, artist.watchlist_provider
    )
    selected_identity = next(
        (identity for identity in artist.identities if identity.provider == selected_provider),
        None,
    )
    provider_albums = list(selected_identity.releases) if selected_identity is not None else []
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
        wanted = max(projected.wanted_track_count, release.track_count or 0)
        release_progress[release.id] = ReleaseProgress(
            wanted_track_count=wanted,
            downloaded_track_count=min(projected.downloaded_track_count, wanted),
            downloaded_catalog_track_ids=projected.downloaded_catalog_track_ids,
        )
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
            "sort_url": sort_url,
            "enrichment": enrichment,
            "release_progress": release_progress,
        },
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

    quick = str(form.get("quick", "")).lower() in {"1", "true", "on", "yes"}
    if quick:
        artist.monitored = not artist.monitored
        artist.monitor_policy = "all"
        if artist.monitored:
            artist.watchlist_provider = _selected_provider(
                runtime.primary_metadata_provider,
                available,
                runtime.primary_metadata_provider,
                artist.watchlist_provider,
            )
    else:
        artist.monitored = str(form.get("monitored", "")).lower() in {"1", "true", "on", "yes"}
        policy = str(form.get("monitor_policy", artist.monitor_policy or "all"))
        artist.monitor_policy = policy if policy in {"all", "albums_only", "none_new"} else "all"
        requested_watchlist = str(form.get("watchlist_provider", ""))
        artist.watchlist_provider = _selected_provider(
            requested_watchlist,
            available,
            runtime.primary_metadata_provider,
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
    if selected_identity is not None:
        for release in selected_identity.releases:
            if not artist.monitored or bulk == "none":
                release.monitored = False
            elif bulk == "all":
                release.monitored = True
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
    await db.commit()
    return RedirectResponse(
        _artist_page_url(artist.id, provider=view_provider, release_type=release_type, sort=sort),
        status_code=303,
    )


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
        },
    )


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
    identity = next(
        (item for item in artist.identities if item.provider == artist.watchlist_provider),
        None,
    )
    canonical: dict[int, CatalogAlbum] = {}
    if identity is not None:
        for release in identity.releases:
            album = release.catalog_album
            if release.monitored and album is not None and not album.in_library:
                canonical[album.id] = album
    job_ids: list[int] = []
    for album in canonical.values():
        album_id = album.id
        album_title = album.title
        album_query = f"{artist.name} {album_title}".strip()
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
        job = Job(
            source="priority",
            query=f"{artist.name} {album.title}".strip(),
            status=JobStatus.pending,
            catalog_album_id=album.id,
        )
        db.add(job)
        await db.commit()
        job_ids.append(job.id)
    for job_id in job_ids:
        await job_dispatcher.dispatch(job_id)
    return RedirectResponse("/downloads", status_code=303)


@router.post("/albums/{album_id}/download", include_in_schema=False)
async def download_catalog_album(
    album_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
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
    progress = (await get_release_progress(db, [album.id], library_root=settings.library_root))[
        album.id
    ]
    imported_ids = set(progress.downloaded_catalog_track_ids)
    query = f"{album.artist.name} {album.title}".strip()
    if not imported_ids:
        job = Job(
            source="priority", query=query, status=JobStatus.pending, catalog_album_id=album.id
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)
        await job_dispatcher.dispatch(job.id)
        return RedirectResponse("/downloads", status_code=303)

    missing_tracks = [track for track in album.tracks if track.id not in imported_ids]
    if not missing_tracks:
        return RedirectResponse("/downloads", status_code=303)
    job_ids: list[int] = []
    for track in missing_tracks:
        track_query = f"{album.artist.name} {track.title}".strip()
        job = Job(
            source="priority",
            query=track_query,
            status=JobStatus.pending,
            catalog_album_id=album.id,
            catalog_track_id=track.id,
        )
        db.add(job)
        await db.flush()
        job_ids.append(job.id)
    await db.commit()
    for job_id in job_ids:
        await job_dispatcher.dispatch(job_id)
    return RedirectResponse("/downloads", status_code=303)


@router.post("/albums/{album_id}/tracks/{track_id}/download", include_in_schema=False)
async def download_catalog_track(
    album_id: int,
    track_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
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
    return RedirectResponse("/downloads", status_code=303)
