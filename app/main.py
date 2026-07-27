from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from importlib.resources import files
from pathlib import Path
from typing import Annotated
from urllib.parse import quote

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import Response

from app.auth import get_current_user, setup_complete
from app.config import Settings, get_settings
from app.database import get_db, get_session_factory
from app.display_names import display_name
from app.jobs.dispatcher import job_dispatcher
from app.routers import auth, health, imports, jobs, naming, search, staging, tracks
from app.routers import catalog as catalog_router
from app.routers import settings as settings_router
from app.services.acquisition_cleanup import (
    cleanup_imported_sources,
    pending_imported_source_cleanups,
)
from app.services.acquisition_recovery import recover_approved_downloads
from app.services.artist_monitoring import DiscographyRefreshScheduler
from app.services.catalog_metadata import reconcile_duplicate_catalog_artists
from app.services.dashboard import get_dashboard_data
from app.services.health_status import get_health_status_service
from app.settings_service import build_effective_settings, effective_settings_dep
from app.version import APP_VERSION

_TEMPLATES_DIR = files("app") / "templates"
_STATIC_DIR = files("app") / "static"

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    scheduler = DiscographyRefreshScheduler()
    health_status = get_health_status_service()
    app.state.discography_scheduler = scheduler
    app.state.health_status_service = health_status
    async with get_session_factory()() as db:
        pending_cleanups = await pending_imported_source_cleanups(db)
        n = await reconcile_duplicate_catalog_artists(db)
        await db.commit()
        if n:
            logger.info("Reconciled %d duplicate catalog artist(s) at startup", n)
        repaired = await recover_approved_downloads(
            db, await build_effective_settings(db, get_settings())
        )
        await db.commit()
        if repaired:
            logger.info("Recovered and imported %d approved release(s) at startup", repaired)
    await cleanup_imported_sources(pending_cleanups)
    await job_dispatcher.recover()
    settings = get_settings()
    await job_dispatcher.start_watchdog(
        threshold_seconds=settings.job_watchdog_threshold_seconds,
        interval_seconds=settings.job_watchdog_interval_seconds,
    )
    await scheduler.start()
    await health_status.start()
    try:
        yield
    finally:
        await health_status.stop()
        await scheduler.stop()
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

    app.state.templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    app.state.templates.env.filters["from_json"] = lambda value: json.loads(value or "[]")
    app.state.templates.env.filters["display_name"] = display_name
    app.state.templates.env.globals["display_name"] = display_name
    app.state.templates.env.globals["app_version"] = app_version
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

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

    app.include_router(health.router, tags=["health"])
    app.include_router(auth.router, tags=["auth"])
    app.include_router(catalog_router.router, tags=["catalog"])
    app.include_router(search.router, tags=["search"])
    app.include_router(settings_router.router, tags=["settings"])
    app.include_router(jobs.router, tags=["jobs", "downloads"])
    app.include_router(tracks.router, tags=["tracks"])
    app.include_router(naming.router, tags=["naming"])
    app.include_router(imports.router, tags=["imports"])
    app.include_router(staging.router, tags=["staging"])

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
        dashboard_data = await get_dashboard_data(db, effective_settings)
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
