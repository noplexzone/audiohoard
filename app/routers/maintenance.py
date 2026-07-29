from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from starlette.responses import Response

from app.auth import get_current_user, require_mutation
from app.config import Settings
from app.database import get_db, get_session_factory
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.services.library_import import ImportExecutionError
from app.services.library_scan import scan_library_filesystem
from app.services.maintenance_state import MaintenanceState, empty_maintenance_state
from app.services.maintenance_workflows import (
    clean_safe_library_duplicates,
    scan_library_duplicates,
)
from app.services.monitoring import current_release_quality, execute_quality_upgrade
from app.settings_service import effective_settings_dep, get_runtime_settings

router = APIRouter(prefix="/maintenance", dependencies=[Depends(get_current_user)])


def _state(request: Request) -> MaintenanceState:
    state = getattr(request.app.state, "maintenance_state", None)
    if state is None:
        state = empty_maintenance_state()
        request.app.state.maintenance_state = state
    return state


async def _run_scan(app_state: Any, settings: Settings, artist_id: int | None) -> None:
    async with get_session_factory()() as db:
        result = await scan_library_filesystem(
            db, library_root=settings.library_root, artist_id=artist_id
        )
    app_state.maintenance_state.store_library_scan(result)


async def _run_duplicate_scan(app_state: Any, settings: Settings) -> None:
    async with get_session_factory()() as db:
        runtime = await get_runtime_settings(db)
        summary = await scan_library_duplicates(
            db,
            library_root=settings.library_root,
            quality_profile=runtime.quality_profile,
        )
    app_state.maintenance_state.store_duplicate_scan(summary)


@router.get("", response_class=HTMLResponse, include_in_schema=False)
async def maintenance_page(
    request: Request, db: Annotated[AsyncSession, Depends(get_db)]
) -> Response:
    templates = request.app.state.templates
    upgrade_records = (
        await db.scalars(
            select(MonitoringRecord)
            .where(MonitoringRecord.status == MonitoringStatus.candidate_found)
            .options(
                selectinload(MonitoringRecord.release),
                selectinload(MonitoringRecord.candidate),
            )
            .order_by(MonitoringRecord.updated_at.desc())
        )
    ).all()
    response: Response = templates.TemplateResponse(
        request,
        "maintenance.html",
        {
            "maintenance": _state(request),
            "deleted": request.query_params.get("deleted", ""),
            "upgrade_records": upgrade_records,
            "upgrade_status": request.query_params.get("upgrade", ""),
            "upgrade_detail": request.query_params.get("detail", ""),
        },
    )
    return response


@router.post("/scan", include_in_schema=False)
async def maintenance_scan(
    request: Request,
    background_tasks: BackgroundTasks,
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    artist_raw = str(form.get("artist_id", "")).strip()
    artist_id = int(artist_raw) if artist_raw else None
    background_tasks.add_task(_run_scan, request.app.state, settings, artist_id)
    return RedirectResponse("/maintenance", status_code=303)


@router.post("/duplicates/scan", include_in_schema=False)
async def duplicate_scan(
    request: Request,
    background_tasks: BackgroundTasks,
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    background_tasks.add_task(_run_duplicate_scan, request.app.state, settings)
    return RedirectResponse("/maintenance", status_code=303)


@router.post("/duplicates/clean", include_in_schema=False)
async def duplicate_clean(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    runtime = await get_runtime_settings(db)
    result = await clean_safe_library_duplicates(
        db,
        library_root=settings.library_root,
        quality_profile=runtime.quality_profile,
        latest_scan=_state(request).duplicate_scan,
    )
    await db.commit()
    return RedirectResponse(f"/maintenance?deleted={result.deleted_files}", status_code=303)


@router.post("/orphans/import", include_in_schema=False)
async def import_orphan(
    request: Request,
    _user: Annotated[object, Depends(require_mutation)],
) -> None:
    raise HTTPException(
        status_code=501,
        detail="Raw orphan import entrypoint is not wired; use the staged import review pipeline.",
    )


@router.post("/orphans/ignore", include_in_schema=False)
async def ignore_orphan(
    request: Request,
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    path = str(form.get("path", "")).strip()
    if path:
        _state(request).ignored_orphans.add(path)
    return RedirectResponse("/maintenance", status_code=303)


@router.post("/upgrades/approve/{record_id}", include_in_schema=False)
async def approve_quality_upgrade(
    record_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    record = await db.get(
        MonitoringRecord,
        record_id,
        options=(selectinload(MonitoringRecord.candidate),),
    )
    if record is None:
        raise HTTPException(status_code=404, detail="Monitoring record not found")
    if record.candidate is None:
        return RedirectResponse(
            "/maintenance?upgrade=error&detail=No+approved+upgrade+candidate",
            status_code=303,
        )
    try:
        await execute_quality_upgrade(
            db,
            record,
            record.candidate,
            await current_release_quality(db, record.release_id),
            library_root=settings.library_root,
        )
    except ImportExecutionError as exc:
        await db.rollback()
        from urllib.parse import quote_plus

        return RedirectResponse(
            f"/maintenance?upgrade=error&detail={quote_plus(str(exc))}", status_code=303
        )
    await db.commit()
    return RedirectResponse("/maintenance?upgrade=ok", status_code=303)
