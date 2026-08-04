from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from importlib.resources import files
from pathlib import Path
from typing import Annotated
from urllib.parse import quote

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import Response

from app.auth import get_current_user, setup_complete
from app.config import Settings, get_settings
from app.database import get_db, get_session_factory
from app.display_names import display_name
from app.jobs.dispatcher import job_dispatcher
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.workflow import ReviewDecision
from app.routers import (
    artwork,
    auth,
    blocklist,
    health,
    imports,
    jobs,
    maintenance,
    naming,
    search,
    staging,
    tracks,
)
from app.routers import catalog as catalog_router
from app.routers import settings as settings_router
from app.services.acquisition_cleanup import (
    cleanup_imported_sources,
    pending_imported_source_cleanups,
    prune_orphaned_terminal_records,
    wait_for_imported_source_cleanups,
)
from app.services.acquisition_recovery import recover_approved_downloads
from app.services.artist_monitoring import DiscographyRefreshScheduler
from app.services.catalog_metadata import reconcile_duplicate_catalog_artists
from app.services.catalog_ownership import reconcile_deezer_catalog_ownership
from app.services.dashboard import get_dashboard_data
from app.services.health_status import get_health_status_service
from app.services.library_adoption_runner import LibraryAdoptionRunner
from app.services.library_reconciliation import LibraryReconciliationService
from app.services.library_removal import recover_deletion_operations
from app.services.maintenance_scheduler import MaintenanceScheduler
from app.services.maintenance_state import empty_maintenance_state
from app.services.monitoring import MonitoringScheduler, QualityUpgradeCycleScheduler
from app.settings_service import (
    build_effective_settings,
    effective_settings_dep,
    get_runtime_settings,
)
from app.version import APP_VERSION

_TEMPLATES_DIR = files("app") / "templates"
_STATIC_DIR = files("app") / "static"

logger = logging.getLogger(__name__)


async def _reconcile_catalog_ownership_at_startup(settings: Settings) -> None:
    try:
        ownership_repairs = await reconcile_deezer_catalog_ownership(
            get_session_factory(), settings
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Catalog ownership reconciliation failed at startup")
    else:
        if ownership_repairs:
            logger.info(
                "Reconciled %d imported track catalog ownership record(s) at startup",
                ownership_repairs,
            )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    scheduler = DiscographyRefreshScheduler()
    health_status = get_health_status_service()
    maintenance_state = getattr(app.state, "maintenance_state", empty_maintenance_state())
    maintenance_scheduler = MaintenanceScheduler(maintenance_state)
    monitoring_scheduler = MonitoringScheduler()
    quality_upgrade_scheduler = QualityUpgradeCycleScheduler()
    app.state.maintenance_state = maintenance_state
    app.state.discography_scheduler = scheduler
    app.state.health_status_service = health_status
    app.state.maintenance_scheduler = maintenance_scheduler
    app.state.monitoring_scheduler = monitoring_scheduler
    app.state.quality_upgrade_scheduler = quality_upgrade_scheduler
    async with get_session_factory()() as db:
        runtime = await get_runtime_settings(db)
        await job_dispatcher.set_max_concurrent_jobs(runtime.max_parallel_acquisitions)
        effective_settings = await build_effective_settings(db, get_settings())
    library_adoption_runner = LibraryAdoptionRunner(get_session_factory())
    app.state.library_adoption_runner = library_adoption_runner
    await recover_deletion_operations(
        get_session_factory(),
        library_root=effective_settings.library_root,
        cache_root=effective_settings.artwork_cache_root.parent / "library-audio",
    )
    library_reconciliation = LibraryReconciliationService(
        get_session_factory(), effective_settings.library_root
    )
    app.state.library_reconciliation_service = library_reconciliation
    await library_reconciliation.startup_reconcile()
    async with get_session_factory()() as db:
        pending_cleanups = await pending_imported_source_cleanups(db)
        pruned = await prune_orphaned_terminal_records(db)
        if pruned.tracks or pruned.releases or pruned.jobs:
            logger.info(
                "Pruned orphaned acquisition history at startup: "
                "%d track(s), %d release(s), %d job(s)",
                pruned.tracks,
                pruned.releases,
                pruned.jobs,
            )
        n = await reconcile_duplicate_catalog_artists(db)
        await db.commit()
        if n:
            logger.info("Reconciled %d duplicate catalog artist(s) at startup", n)
        repaired = await recover_approved_downloads(db, effective_settings)
        await db.commit()
        if repaired:
            logger.info("Recovered and imported %d approved release(s) at startup", repaired)
    ownership_task = asyncio.create_task(
        _reconcile_catalog_ownership_at_startup(effective_settings),
        name="catalog-ownership-startup-reconciliation",
    )
    app.state.catalog_ownership_reconciliation_task = ownership_task
    await cleanup_imported_sources(pending_cleanups)
    await job_dispatcher.recover()
    settings = get_settings()
    await job_dispatcher.start_watchdog(
        threshold_seconds=settings.job_watchdog_threshold_seconds,
        interval_seconds=settings.job_watchdog_interval_seconds,
    )
    await job_dispatcher.start_cleanup_reconciler(
        interval_seconds=settings.terminal_cleanup_interval_seconds
    )
    await scheduler.start()
    await library_adoption_runner.start()
    await maintenance_scheduler.start()
    await quality_upgrade_scheduler.start()
    await health_status.start()
    await library_reconciliation.start()
    try:
        yield
    finally:
        await library_adoption_runner.stop()
        await library_reconciliation.stop()
        if not ownership_task.done():
            ownership_task.cancel()
        with suppress(asyncio.CancelledError):
            await ownership_task
        await health_status.stop()
        await quality_upgrade_scheduler.stop()
        await maintenance_scheduler.stop()
        await scheduler.stop()
        await wait_for_imported_source_cleanups(raise_errors=False)
        await job_dispatcher.shutdown()


def create_app() -> FastAPI:
    settings = get_settings()
    app_version = APP_VERSION

    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    app = FastAPI(
        title="Audiohoard",
        version=app_version,
        description="Self-hosted music acquisition and library management",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        lifespan=lifespan,
    )

    app.state.maintenance_state = empty_maintenance_state()
    app.state.health_status_service = get_health_status_service()
    app.state.templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    app.state.templates.env.filters["from_json"] = lambda value: json.loads(value or "[]")
    app.state.templates.env.filters["display_name"] = display_name
    app.state.templates.env.globals["display_name"] = display_name
    app.state.templates.env.globals["app_version"] = app_version
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.middleware("http")
    async def pending_review_count_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request.state.pending_review_count = 0
        if request.method == "GET" and not request.url.path.startswith(
            ("/static/", "/api/", "/artwork")
        ):
            async with get_session_factory()() as db:
                count = await db.scalar(
                    select(func.count(StagingReviewItem.id))
                    .join(Release, StagingReviewItem.release_id == Release.id)
                    .where(
                        StagingReviewItem.review_state == ReviewDecision.pending,
                        Release.review_dismissed_at.is_(None),
                    )
                )
            request.state.pending_review_count = int(count or 0)
        return await call_next(request)

    @app.exception_handler(HTTPException)
    async def browser_auth_exception_handler(request: Request, exc: HTTPException) -> Response:
        accepts_html = "text/html" in request.headers.get("accept", "").casefold()
        if exc.status_code == 401 and request.method == "GET" and accepts_html:
            target = request.url.path
            if request.url.query:
                target = f"{target}?{request.url.query}"
            response = RedirectResponse(f"/login?next={quote(target, safe='')}", status_code=303)
            response.delete_cookie("session")
            response.delete_cookie("csrf")
            return response
        return await http_exception_handler(request, exc)

    @app.middleware("http")
    async def html_timing_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        started = time.perf_counter()
        response = await call_next(request)
        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type:
            duration_ms = int((time.perf_counter() - started) * 1000)
            logger.info(
                "%s %s %s %sms",
                request.method,
                request.url.path,
                response.status_code,
                duration_ms,
            )
        return response

    @app.middleware("http")
    async def html_security_headers_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        response = await call_next(request)
        if "text/html" in response.headers.get("content-type", ""):
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["Referrer-Policy"] = "no-referrer"
            if not request.url.path.startswith(("/api/docs", "/api/redoc")):
                response.headers["Content-Security-Policy"] = (
                    "default-src 'self'; img-src 'self' data:; media-src 'self'; "
                    "script-src 'self'; style-src 'self'; frame-ancestors 'none'; "
                    "base-uri 'self'"
                )
        return response

    app.include_router(health.router, tags=["health"])
    app.include_router(auth.router, tags=["auth"])
    app.include_router(artwork.router, tags=["artwork"])
    app.include_router(catalog_router.router, tags=["catalog"])
    app.include_router(search.router, tags=["search"])
    app.include_router(settings_router.router, tags=["settings"])
    app.include_router(blocklist.router, tags=["blocklist"])
    app.include_router(jobs.router, tags=["jobs", "downloads"])
    app.include_router(tracks.router, tags=["tracks"])
    app.include_router(naming.router, tags=["naming"])
    app.include_router(imports.router, tags=["imports"])
    app.include_router(staging.router, tags=["staging"])
    app.include_router(maintenance.router, tags=["maintenance"])

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def dashboard(
        request: Request,
        db: Annotated[AsyncSession, Depends(get_db)],
        effective_settings: Annotated[Settings, Depends(effective_settings_dep)],
    ) -> Response:
        if not await setup_complete(db):
            return RedirectResponse("/setup", status_code=307)
        try:
            await get_current_user(request, db)
        except HTTPException:
            return RedirectResponse("/login", status_code=307)
        dashboard_data = await get_dashboard_data(
            db, effective_settings, request.app.state.health_status_service.snapshot()
        )
        templates: Jinja2Templates = request.app.state.templates
        return templates.TemplateResponse(
            request,
            "index.html",
            {"dashboard": dashboard_data},
        )

    @app.get("/changelog", response_class=HTMLResponse, include_in_schema=False)
    async def changelog_page(request: Request) -> Response:
        try:
            text = await asyncio.to_thread(Path("CHANGELOG.md").read_text)
        except OSError:
            text = "# Changelog\n\nNo changelog packaged."

        def render(md: str) -> str:
            import html
            import re

            out = []
            in_list = False
            for raw in md.splitlines():
                line = raw.strip()
                if not line:
                    if in_list:
                        out.append("</ul>")
                        in_list = False
                    continue
                if line.startswith("### "):
                    if in_list:
                        out.append("</ul>")
                        in_list = False
                    out.append(f"<h3>{html.escape(line[4:])}</h3>")
                elif line.startswith("## "):
                    if in_list:
                        out.append("</ul>")
                        in_list = False
                    out.append(f"<h2>{html.escape(line[3:])}</h2>")
                elif line.startswith("# "):
                    if in_list:
                        out.append("</ul>")
                        in_list = False
                    out.append(f"<h1>{html.escape(line[2:])}</h1>")
                elif line.startswith("- "):
                    if not in_list:
                        out.append("<ul>")
                        in_list = True
                    item = html.escape(line[2:])
                    item = re.sub(
                        r"\[([^\]]+)\]\((https?://[^)]+)\)", r'<a href="\2">\1</a>', item
                    )
                    out.append(f"<li>{item}</li>")
                else:
                    if in_list:
                        out.append("</ul>")
                        in_list = False
                    out.append(f"<p>{html.escape(line)}</p>")
            if in_list:
                out.append("</ul>")
            return "\n".join(out)

        templates: Jinja2Templates = request.app.state.templates
        return templates.TemplateResponse(
            request, "changelog.html", {"html": render(text), "app_version": app.version}
        )

    return app


app = create_app()
