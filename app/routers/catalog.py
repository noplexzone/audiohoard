from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user, require_mutation
from app.config import Settings
from app.database import get_db
from app.jobs.dispatcher import job_dispatcher
from app.models.catalog_entities import CatalogAlbum, CatalogArtist
from app.models.job import Job, JobStatus
from app.services.catalog import (
    get_artist_detail,
    get_artists_page,
    get_library_stats,
    list_distinct_formats,
    list_distinct_sources,
    list_library_tracks,
)
from app.services.catalog_metadata import (
    enrich_catalog_artist,
    fetch_and_store_album,
    fetch_and_store_discography,
    open_catalog_artist,
    reconcile_duplicate_catalog_albums,
)
from app.settings_service import effective_settings_dep, get_runtime_settings

router = APIRouter(dependencies=[Depends(get_current_user)])
logger = logging.getLogger(__name__)


def _sanitize_error_class(exc: BaseException) -> str:
    first_line = str(exc).splitlines()[0] if str(exc) else ""
    raw = f"{type(exc).__name__}: {first_line}" if first_line else type(exc).__name__
    return raw[:200]


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
            try:
                await enrich_catalog_artist(session, cfg, artist, providers)
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
                    await session.commit()


def _templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates  # type: ignore[no-any-return]


@router.get("/library", response_class=HTMLResponse)
async def library_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    q: str = "",
    artist: str = "",
    album: str = "",
    source: str = "",
    fmt: str = "",
    sort: str = "added",
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
) -> HTMLResponse:
    stats = await get_library_stats(db)
    all_sources = await list_distinct_sources(db)
    all_formats = await list_distinct_formats(db)
    tracks = await list_library_tracks(
        db,
        q=q,
        artist=artist,
        album=album,
        source=source,
        fmt=fmt,
        sort=sort,
        page=page,
        per_page=per_page,
    )

    filter_params: dict[str, str] = {}
    if q:
        filter_params["q"] = q
    if artist:
        filter_params["artist"] = artist
    if album:
        filter_params["album"] = album
    if source:
        filter_params["source"] = source
    if fmt:
        filter_params["fmt"] = fmt
    filter_params["sort"] = sort
    filter_params["per_page"] = str(per_page)
    filter_qs = urlencode(filter_params)

    return _templates(request).TemplateResponse(
        request,
        "library.html",
        {
            "stats": stats,
            "tracks": tracks,
            "all_sources": all_sources,
            "all_formats": all_formats,
            "q": q,
            "filter_artist": artist,
            "filter_album": album,
            "filter_source": source,
            "filter_fmt": fmt,
            "sort": sort,
            "per_page": per_page,
            "filter_qs": filter_qs,
        },
    )


@router.get("/artists", response_class=HTMLResponse)
async def artists_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    q: str = "",
    sort: str = "name",
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=200),
) -> HTMLResponse:
    artists = await get_artists_page(db, q=q, sort=sort, page=page, per_page=per_page)
    monitored_count = (
        (await db.execute(select(CatalogArtist).where(CatalogArtist.monitored.is_(True))))
        .scalars()
        .all()
    )
    wanted_count = (
        (
            await db.execute(
                select(CatalogAlbum).where(
                    CatalogAlbum.monitored.is_(True), CatalogAlbum.in_library.is_(False)
                )
            )
        )
        .scalars()
        .all()
    )

    filter_params: dict[str, str] = {}
    if q:
        filter_params["q"] = q
    filter_params["sort"] = sort
    filter_params["per_page"] = str(per_page)
    filter_qs = urlencode(filter_params)

    return _templates(request).TemplateResponse(
        request,
        "artists.html",
        {
            "artists": artists,
            "q": q,
            "sort": sort,
            "per_page": per_page,
            "filter_qs": filter_qs,
            "monitored_count": len(monitored_count),
            "wanted_count": len(wanted_count),
        },
    )


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
    if monitor:
        artist.monitored = True
        artist.monitor_policy = "all"
    runtime = await get_runtime_settings(db)
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
    release_type: str = "",
    sort: str = "desc",
    enrichment: str = "",
) -> HTMLResponse:
    result = await db.execute(
        select(CatalogArtist)
        .where(CatalogArtist.id == artist_id)
        .options(selectinload(CatalogArtist.albums))
    )
    artist = result.scalar_one_or_none()
    if artist is None:
        raise HTTPException(status_code=404, detail="Catalog artist not found")
    runtime = await get_runtime_settings(db)
    if artist.last_enriched_at is None and not artist.mbid:
        background_tasks.add_task(
            _enrich_artist_task, artist.id, runtime.enabled_metadata_providers
        )
    try:
        if not artist.albums:
            await fetch_and_store_discography(db, settings, artist)
        await reconcile_duplicate_catalog_albums(db, artist.id)
        await db.commit()
    except Exception:
        logger.exception("Catalog artist discography refresh failed for artist %s", artist_id)
        await db.rollback()
    result = await db.execute(
        select(CatalogArtist)
        .where(CatalogArtist.id == artist_id)
        .options(selectinload(CatalogArtist.albums))
    )
    artist = result.scalar_one()
    albums = sorted(
        artist.albums,
        key=lambda album: (album.year or "0000", album.title.casefold()),
        reverse=sort != "asc",
    )
    if release_type:
        if release_type == "single_ep":
            albums = [a for a in albums if (a.release_type or "").casefold() in {"single", "ep"}]
        else:
            albums = [
                a for a in albums if (a.release_type or "").casefold() == release_type.casefold()
            ]
    release_types = sorted({a.release_type for a in artist.albums if a.release_type})
    counts_by_type: dict[str, int] = {"albums": 0, "singles_eps": 0, "compilations": 0}
    for album in artist.albums:
        rt = (album.release_type or "album").casefold()
        if rt in {"single", "ep"}:
            counts_by_type["singles_eps"] += 1
        elif "compilation" in rt:
            counts_by_type["compilations"] += 1
        else:
            counts_by_type["albums"] += 1
    grouped_albums = (
        [
            (
                "Albums",
                [
                    a
                    for a in albums
                    if (a.release_type or "album").casefold()
                    not in {"single", "ep", "compilation"}
                ],
            ),
            (
                "Singles & EPs",
                [a for a in albums if (a.release_type or "").casefold() in {"single", "ep"}],
            ),
            (
                "Compilations",
                [a for a in albums if "compilation" in (a.release_type or "").casefold()],
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
    return _templates(request).TemplateResponse(
        request,
        "catalog_artist.html",
        {
            "artist": artist,
            "albums": albums,
            "grouped_albums": grouped_albums,
            "release_types": release_types,
            "release_type": release_type,
            "sort": sort,
            "counts_by_type": counts_by_type,
            "filter_options": filter_options,
            "enrichment": enrichment,
        },
    )


@router.post("/artists/catalog/{artist_id}/enrich", include_in_schema=False)
async def enrich_catalog_artist_page(
    artist_id: int,
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
    try:
        outcome = await enrich_catalog_artist(
            db, settings, artist, runtime.enabled_metadata_providers
        )
        redirect_value = outcome.get("artist_id", artist.id)
        redirect_artist_id = redirect_value if isinstance(redirect_value, int) else artist.id
        survivor = await db.get(CatalogArtist, redirect_artist_id)
        if survivor is None:
            raise RuntimeError("Enrichment survivor was not found")
        provenance = (
            json.loads(survivor.provenance_json or "{}") if survivor.provenance_json else {}
        )
        provenance.pop("last_enrichment_error", None)
        survivor.provenance_json = json.dumps(provenance, sort_keys=True)
        await db.commit()
        artist_id = redirect_artist_id
        suffix = (
            "?enrichment=ambiguous" if outcome.get("status") == "ambiguous" else "?enrichment=ok"
        )
    except Exception as exc:
        logger.error(
            "Manual catalog artist enrichment failed for artist %s", artist_id, exc_info=True
        )
        await db.rollback()
        fresh = await db.get(CatalogArtist, artist_id)
        if fresh is not None:
            provenance = json.loads(fresh.provenance_json or "{}") if fresh.provenance_json else {}
            provenance["last_enrichment_error"] = {
                "at": datetime.now(tz=UTC).isoformat(),
                "message": _sanitize_error_class(exc),
            }
            fresh.provenance_json = json.dumps(provenance, sort_keys=True)
        await db.commit()
        suffix = "?enrichment=failed"
    return RedirectResponse(f"/artists/catalog/{artist_id}{suffix}", status_code=303)


@router.post("/artists/catalog/{artist_id}/monitor", include_in_schema=False)
async def monitor_catalog_artist_page(
    artist_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
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
    form = await request.form()
    if str(form.get("quick", "")).lower() in {"1", "true", "on", "yes"}:
        enabled = not artist.monitored
        artist.monitored = enabled
        artist.monitor_policy = "all"
        for album in artist.albums:
            album.monitored = enabled
        await db.commit()
        return RedirectResponse(f"/artists/catalog/{artist.id}", status_code=303)
    artist.monitored = str(form.get("monitored", "")).lower() in {"1", "true", "on", "yes"}
    policy = str(form.get("monitor_policy", artist.monitor_policy or "all"))
    artist.monitor_policy = policy if policy in {"all", "albums_only", "none_new"} else "all"
    album_ids = {int(str(v)) for v in form.getlist("album_monitored") if str(v).isdigit()}
    bulk = str(form.get("bulk", ""))
    for album in artist.albums:
        if bulk == "all":
            album.monitored = True
        elif bulk == "none":
            album.monitored = False
        elif bulk == "albums_only":
            album.monitored = (album.release_type or "album").casefold() == "album"
        elif bulk == "singles_off":
            album.monitored = (album.release_type or "").casefold() not in {"single", "ep"}
        else:
            album.monitored = album.id in album_ids
    await db.commit()
    return RedirectResponse(f"/artists/catalog/{artist.id}", status_code=303)


@router.get("/artists/monitored", response_class=HTMLResponse, include_in_schema=False)
async def monitored_artists_page(
    request: Request, db: Annotated[AsyncSession, Depends(get_db)]
) -> HTMLResponse:
    result = await db.execute(
        select(CatalogArtist)
        .where(CatalogArtist.monitored.is_(True))
        .options(selectinload(CatalogArtist.albums))
    )
    artists = list(result.scalars().all())
    return _templates(request).TemplateResponse(
        request, "monitored.html", {"artists": artists, "monitored_count": len(artists)}
    )


@router.get("/wanted", response_class=HTMLResponse, include_in_schema=False)
async def wanted_page(
    request: Request, db: Annotated[AsyncSession, Depends(get_db)]
) -> HTMLResponse:
    result = await db.execute(
        select(CatalogAlbum)
        .where(CatalogAlbum.monitored.is_(True), CatalogAlbum.in_library.is_(False))
        .options(selectinload(CatalogAlbum.artist))
    )
    albums = list(result.scalars().all())
    monitored_count = len(
        (await db.execute(select(CatalogArtist).where(CatalogArtist.monitored.is_(True))))
        .scalars()
        .all()
    )
    return _templates(request).TemplateResponse(
        request,
        "wanted.html",
        {"albums": albums, "wanted_count": len(albums), "monitored_count": monitored_count},
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
        album = await fetch_and_store_album(db, settings, album)
        await db.commit()
    except Exception:
        await db.rollback()
    result = await db.execute(
        select(CatalogAlbum)
        .where(CatalogAlbum.id == album_id)
        .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
    )
    album = result.scalar_one()
    return _templates(request).TemplateResponse(request, "catalog_album.html", {"album": album})


@router.post("/artists/catalog/{artist_id}/download-monitored", include_in_schema=False)
async def download_monitored_catalog_albums(
    artist_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
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
    job_ids: list[int] = []
    for album in artist.albums:
        if not album.monitored or album.in_library:
            continue
        query = f"{artist.name} {album.title}".strip()
        job = Job(
            source="priority", query=query, status=JobStatus.pending, catalog_album_id=album.id
        )
        db.add(job)
        await db.flush()
        job_ids.append(job.id)
    await db.commit()
    for job_id in job_ids:
        await job_dispatcher.dispatch(job_id)
    return RedirectResponse("/downloads", status_code=303)


@router.post("/albums/{album_id}/download", include_in_schema=False)
async def download_catalog_album(
    album_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    result = await db.execute(
        select(CatalogAlbum)
        .where(CatalogAlbum.id == album_id)
        .options(selectinload(CatalogAlbum.artist))
    )
    album = result.scalar_one_or_none()
    if album is None:
        raise HTTPException(status_code=404, detail="Catalog album not found")
    query = f"{album.artist.name} {album.title}".strip()
    job = Job(source="priority", query=query, status=JobStatus.pending, catalog_album_id=album.id)
    db.add(job)
    await db.commit()
    await db.refresh(job)
    await job_dispatcher.dispatch(job.id)
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
