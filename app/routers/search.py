from __future__ import annotations

import asyncio
import logging
import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.config import Settings
from app.database import get_db
from app.models.catalog_entities import CatalogArtist, CatalogArtistIdentity
from app.schemas.health import SourceStatus
from app.schemas.search import SearchRequest, SearchResponse, SearchResult
from app.services.catalog_metadata import search_catalog_artists
from app.services.discovery import discovery_service
from app.settings_service import effective_settings_dep, get_runtime_settings
from app.sources.base import SourceAdapter
from app.sources.prowlarr import ProwlarrAdapter
from app.sources.slskd import SlskdAdapter
from app.sources.tidal import TidalAdapter
from app.sources.youtube import ProviderError, YouTubeAdapter

router = APIRouter(dependencies=[Depends(get_current_user)])
logger = logging.getLogger(__name__)

_VALID_SOURCES = {"slskd", "prowlarr", "youtube", "tidal"}
_DEFAULT_SOURCES = {"slskd", "prowlarr", "youtube"}
_INTERACTIVE_SLSKD_SEARCH_TIMEOUT_SEC = 60
_INTERACTIVE_YOUTUBE_SEARCH_TIMEOUT_SEC = 30


def _get_templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates  # type: ignore[no-any-return]


async def _watched_catalog_artists(
    db: AsyncSession,
) -> dict[str, dict[str, dict[str, object]]]:
    rows = (
        await db.execute(
            select(
                CatalogArtistIdentity.provider,
                CatalogArtistIdentity.provider_artist_id,
                CatalogArtist.id,
                CatalogArtist.watchlist_release_albums,
                CatalogArtist.watchlist_release_singles,
                CatalogArtist.watchlist_release_eps,
                CatalogArtist.watchlist_monitor_upgrades,
            )
            .join(CatalogArtist, CatalogArtist.id == CatalogArtistIdentity.artist_id)
            .where(CatalogArtist.monitored.is_(True))
        )
    ).all()
    watched: dict[str, dict[str, dict[str, object]]] = {}
    for provider, provider_id, artist_id, albums, singles, eps, upgrades in rows:
        watched.setdefault(provider, {})[provider_id] = {
            "artist_id": artist_id,
            "watchlist_release_albums": bool(albums),
            "watchlist_release_singles": bool(singles),
            "watchlist_release_eps": bool(eps),
            "watchlist_monitor_upgrades": bool(upgrades),
        }
    return watched


def _build_adapter(
    name: str, settings: Settings, budget_seconds: int | None = None
) -> SourceAdapter | None:
    if name == "slskd":
        return SlskdAdapter(
            settings.slskd_url,
            settings.slskd_api_key,
            float(
                min(
                    budget_seconds or _INTERACTIVE_SLSKD_SEARCH_TIMEOUT_SEC,
                    _INTERACTIVE_SLSKD_SEARCH_TIMEOUT_SEC,
                )
            ),
        )
    if name == "prowlarr":
        return ProwlarrAdapter(settings.prowlarr_url, settings.prowlarr_api_key)
    if name == "youtube":
        return YouTubeAdapter(
            settings.ytdlp_cookies_file,
            float(
                min(
                    budget_seconds or _INTERACTIVE_YOUTUBE_SEARCH_TIMEOUT_SEC,
                    _INTERACTIVE_YOUTUBE_SEARCH_TIMEOUT_SEC,
                )
            ),
        )
    if name == "tidal":
        return TidalAdapter(
            settings.tidal_config_path,
            settings.tidal_session_path,
            settings.tidal_quality,
        )
    return None


async def _search_source(
    name: str, settings: Settings, query: SearchRequest, budget_seconds: int | None = None
) -> tuple[str, list[SearchResult], SourceStatus]:
    started = time.perf_counter()
    adapter = _build_adapter(name, settings, budget_seconds)
    if adapter is None:
        return (
            name,
            [],
            SourceStatus(
                available=False, reason="Unknown source", details={"code": "unknown_source"}
            ),
        )

    cap = await adapter.health()
    if not cap.available:
        return name, [], SourceStatus(available=False, reason=cap.reason, details=cap.extra)

    try:
        results = await adapter.search(query)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return name, results, SourceStatus(available=True, details={"elapsed_ms": elapsed_ms})
    except ProviderError as exc:
        logger.warning("Search on %s failed with code %s", name, exc.code)
        return name, [], SourceStatus(available=False, reason=exc.message, details=exc.details())
    except Exception:
        logger.warning("Search on %s failed", name)
        return (
            name,
            [],
            SourceStatus(
                available=False,
                reason="Source search failed",
                details={"code": "search_failed", "operation": "search", "retryable": True},
            ),
        )


@router.post("/search", response_model=SearchResponse)
async def search(
    req: SearchRequest,
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> SearchResponse:
    runtime = await get_runtime_settings(db)
    await db.rollback()
    if req.sources == []:
        requested = [s for s in runtime.enabled_sources if s in _VALID_SOURCES]
    else:
        requested = [s for s in req.sources if s in _VALID_SOURCES]
    tasks = [
        _search_source(name, settings, req, runtime.source_search_budget_seconds)
        for name in requested
    ]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    all_results: list[SearchResult] = []
    source_states: dict[str, SourceStatus] = {}

    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            logger.warning("Search task failed")
            continue
        name, results, state = outcome
        all_results.extend(results)
        source_states[name] = state

    priority = {name: index for index, name in enumerate(requested)}
    all_results.sort(key=lambda r: priority.get(r.source, 999))
    return SearchResponse(results=all_results, source_states=source_states)


@router.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    db: Annotated[AsyncSession, Depends(get_db)],
    q: str = "",
    tab: str = "catalog",
    provider: str = "primary",
) -> HTMLResponse:
    templates = _get_templates(request)
    runtime = await get_runtime_settings(db)
    metadata_providers = runtime.enabled_metadata_providers
    if provider in {"", "primary"}:
        requested = (
            [runtime.primary_metadata_provider]
            if runtime.primary_metadata_provider in metadata_providers
            else []
        )
        provider = "primary"
    elif provider == "all":
        requested = metadata_providers
    else:
        requested = [provider] if provider in metadata_providers else []
    watched_catalog_artists = await _watched_catalog_artists(db)
    await db.rollback()
    catalog_outcomes = []
    primary_error = None
    discovery_sections = []
    if q and requested:
        catalog_outcomes = await search_catalog_artists(settings, q, requested)
        if provider == "primary" and catalog_outcomes and not catalog_outcomes[0].state.available:
            primary_error = catalog_outcomes[0].state.reason or "Primary provider unavailable"
    elif not q:
        discovery_sections = await discovery_service.landing(runtime.discovery_region)
    return templates.TemplateResponse(
        request,
        "search.html",
        {
            "tab": tab,
            "catalog_query": q,
            "catalog_provider": provider,
            "primary_metadata_provider": runtime.primary_metadata_provider,
            "primary_error": primary_error,
            "metadata_providers": runtime.metadata_providers,
            "catalog_outcomes": catalog_outcomes,
            "discovery_sections": discovery_sections,
            "discovery_region": runtime.discovery_region,
            "watched_catalog_artists": watched_catalog_artists,
            "watchlist_defaults": {
                "watchlist_release_albums": runtime.default_watchlist_release_albums,
                "watchlist_release_singles": runtime.default_watchlist_release_singles,
                "watchlist_release_eps": runtime.default_watchlist_release_eps,
                "watchlist_monitor_upgrades": runtime.default_watchlist_monitor_upgrades,
            },
            "metadata_enabled": metadata_providers,
            "results": None,
            "source_states": {},
            "query": "",
            "artist": "",
            "album": "",
            "track": "",
            "error": None,
        },
    )


async def _discover_page(
    request: Request,
    db: AsyncSession,
    feed: str,
    page: int,
    genre_id: str | None = None,
) -> HTMLResponse:
    runtime = await get_runtime_settings(db)
    watched = await _watched_catalog_artists(db)
    await db.rollback()
    section = await discovery_service.get(
        feed, runtime.discovery_region, page=page, limit=12, genre_id=genre_id
    )
    return _get_templates(request).TemplateResponse(
        request,
        "discover_list.html",
        {
            "section": section,
            "page": page,
            "genre_id": genre_id,
            "watched_catalog_artists": watched,
            "watchlist_defaults": {
                "watchlist_release_albums": runtime.default_watchlist_release_albums,
                "watchlist_release_singles": runtime.default_watchlist_release_singles,
                "watchlist_release_eps": runtime.default_watchlist_release_eps,
                "watchlist_monitor_upgrades": runtime.default_watchlist_monitor_upgrades,
            },
        },
    )


@router.get("/discover/popular", response_class=HTMLResponse)
async def discover_popular(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1, le=20)] = 1,
) -> HTMLResponse:
    return await _discover_page(request, db, "popular", page)


@router.get("/discover/genres", response_class=HTMLResponse)
async def discover_genres(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1, le=20)] = 1,
) -> HTMLResponse:
    return await _discover_page(request, db, "genres", page)


@router.get("/discover/genres/{genre_id}", response_class=HTMLResponse)
async def discover_genre(
    genre_id: str,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1, le=20)] = 1,
) -> HTMLResponse:
    if not genre_id.isdigit() or len(genre_id) > 12:
        raise HTTPException(status_code=404, detail="Unknown genre")
    return await _discover_page(request, db, "genre", page, genre_id)


@router.get("/discover/new", response_class=HTMLResponse)
async def discover_new(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1, le=20)] = 1,
) -> HTMLResponse:
    return await _discover_page(request, db, "new", page)


@router.get("/discover/trending", response_class=HTMLResponse)
async def discover_trending(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1, le=20)] = 1,
) -> HTMLResponse:
    return await _discover_page(request, db, "trending", page)


@router.get("/search/ui", include_in_schema=False)
async def search_ui_get() -> RedirectResponse:
    return RedirectResponse("/search", status_code=307)


@router.post("/search/ui", response_class=HTMLResponse)
async def search_ui(
    request: Request,
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> HTMLResponse:
    templates = _get_templates(request)
    form = await request.form()
    query_str = str(form.get("query", "")).strip()
    artist = str(form.get("artist", "")).strip()
    album = str(form.get("album", "")).strip()
    track = str(form.get("track", "")).strip()
    runtime = await get_runtime_settings(db)
    sources_raw = str(form.get("sources", ",".join(runtime.enabled_sources)))
    sources = [s.strip() for s in sources_raw.split(",") if s.strip() in _VALID_SOURCES]
    await db.rollback()

    if not (query_str or artist or album or track):
        return templates.TemplateResponse(
            request,
            "search.html",
            {
                "tab": "advanced",
                "catalog_query": "",
                "catalog_provider": "primary",
                "metadata_providers": runtime.metadata_providers,
                "catalog_outcomes": [],
                "metadata_enabled": runtime.enabled_metadata_providers,
                "results": None,
                "query": "",
                "artist": artist,
                "album": album,
                "track": track,
                "error": "At least one search field is required",
            },
        )

    req = SearchRequest(
        query=query_str,
        artist=artist or None,
        album=album or None,
        track=track or None,
        sources=sources,
    )
    ordered_sources = sources or [s for s in runtime.enabled_sources if s in _VALID_SOURCES]
    tasks = [
        _search_source(name, settings, req, runtime.source_search_budget_seconds)
        for name in ordered_sources
    ]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    all_results: list[SearchResult] = []
    source_states: dict[str, SourceStatus] = {}
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            continue
        name, results, state = outcome
        all_results.extend(results)
        source_states[name] = state
    priority = {name: index for index, name in enumerate(ordered_sources)}
    all_results.sort(key=lambda r: priority.get(r.source, 999))

    return templates.TemplateResponse(
        request,
        "search.html",
        {
            "tab": "advanced",
            "catalog_query": "",
            "catalog_provider": "primary",
            "metadata_providers": runtime.metadata_providers,
            "catalog_outcomes": [],
            "metadata_enabled": runtime.enabled_metadata_providers,
            "results": all_results,
            "source_states": source_states,
            "query": query_str,
            "artist": artist,
            "album": album,
            "track": track,
            "error": None,
        },
    )
