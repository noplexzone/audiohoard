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
from app.models.library_adoption import (
    AdoptionCandidateState,
    AdoptionScopeKind,
    LibraryAdoptionCandidate,
    LibraryAdoptionScan,
)
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.services.library_adoption import AdoptionScope, enqueue_library_adoption_scan
from app.services.library_import import ImportExecutionError
from app.services.maintenance_state import MaintenanceState, empty_maintenance_state
from app.services.maintenance_workflows import (
    clean_safe_library_duplicates,
    scan_library_duplicates,
)
from app.services.monitoring import (
    current_release_quality,
    execute_quality_upgrade,
    run_quality_upgrade_scan,
)
from app.settings_service import effective_settings_dep, get_runtime_settings

router = APIRouter(prefix="/maintenance", dependencies=[Depends(get_current_user)])


def _state(request: Request) -> MaintenanceState:
    state = getattr(request.app.state, "maintenance_state", None)
    if state is None:
        state = empty_maintenance_state()
        request.app.state.maintenance_state = state
    return state


async def _run_duplicate_scan(app_state: Any, settings: Settings) -> None:
    async with get_session_factory()() as db:
        runtime = await get_runtime_settings(db)
        summary = await scan_library_duplicates(
            db,
            library_root=settings.library_root,
            quality_profile=runtime.quality_profile,
        )
    app_state.maintenance_state.store_duplicate_scan(summary)


async def _run_upgrade_scan(app_state: Any, settings: Settings) -> None:
    async with get_session_factory()() as db:
        checked_records = await run_quality_upgrade_scan(db)
    app_state.maintenance_state.store_quality_upgrade_scan(checked_records)


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
    adoption_scans = (
        await db.scalars(
            select(LibraryAdoptionScan).order_by(LibraryAdoptionScan.id.desc()).limit(10)
        )
    ).all()
    adoption_candidates = (
        await db.scalars(
            select(LibraryAdoptionCandidate)
            .where(LibraryAdoptionCandidate.state == AdoptionCandidateState.review)
            .order_by(LibraryAdoptionCandidate.id.desc())
            .limit(50)
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
            "adoption_status": request.query_params.get("adoption", ""),
            "adoption_scans": adoption_scans,
            "adoption_candidates": adoption_candidates,
        },
    )
    return response


@router.post("/scan", include_in_schema=False)
async def maintenance_scan(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    scan_id = await enqueue_library_adoption_scan(
        db, library_root=settings.library_root, scope=AdoptionScope()
    )
    await db.commit()
    request.app.state.library_adoption_runner.wake()
    return RedirectResponse(f"/maintenance?adoption=queued&scan_id={scan_id}", status_code=303)


@router.post("/scan/artists/{artist_id}", include_in_schema=False)
async def maintenance_scan_artist(
    artist_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    try:
        await enqueue_library_adoption_scan(
            db,
            library_root=settings.library_root,
            scope=AdoptionScope(AdoptionScopeKind.catalog_artist, artist_id),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    await db.commit()
    request.app.state.library_adoption_runner.wake()
    return RedirectResponse(f"/artists/catalog/{artist_id}?scan=queued", status_code=303)


@router.post("/scan/albums/{album_id}", include_in_schema=False)
async def maintenance_scan_album(
    album_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    try:
        await enqueue_library_adoption_scan(
            db,
            library_root=settings.library_root,
            scope=AdoptionScope(AdoptionScopeKind.catalog_album, album_id),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    await db.commit()
    request.app.state.library_adoption_runner.wake()
    return RedirectResponse(f"/albums/{album_id}?scan=queued", status_code=303)


@router.post("/scan/imported", include_in_schema=False)
async def maintenance_scan_imported(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    artist_name = str(form.get("artist_name", "")).strip()
    album_title = str(form.get("album_title", "")).strip() or None
    year = str(form.get("year", "")).strip() or None
    scope = AdoptionScope(
        AdoptionScopeKind.imported_release if album_title else AdoptionScopeKind.imported_artist,
        artist_name=artist_name,
        album_title=album_title,
        year=year,
    )
    try:
        await enqueue_library_adoption_scan(db, library_root=settings.library_root, scope=scope)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    await db.commit()
    request.app.state.library_adoption_runner.wake()
    return RedirectResponse("/maintenance?adoption=queued", status_code=303)


@router.post("/adoption/candidates/{candidate_id}/ignore", include_in_schema=False)
async def ignore_adoption_candidate(
    candidate_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    candidate = await db.get(LibraryAdoptionCandidate, candidate_id)
    if candidate is None:
        raise HTTPException(status_code=404, detail="Adoption candidate not found")
    if candidate.state == AdoptionCandidateState.review:
        candidate.state = AdoptionCandidateState.ignored
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


@router.post("/upgrades/scan", include_in_schema=False)
async def upgrade_scan(
    request: Request,
    background_tasks: BackgroundTasks,
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    background_tasks.add_task(_run_upgrade_scan, request.app.state, settings)
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
