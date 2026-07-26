from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user, require_mutation
from app.config import Settings
from app.database import get_db
from app.models.import_plan import ImportPlan
from app.models.release import Release
from app.models.workflow import ImportWorkflowState
from app.services.library_import import (
    ImportExecutionError,
    execute_release_import,
    plan_release_import,
)
from app.settings_service import effective_settings_dep

router = APIRouter(prefix="/imports", dependencies=[Depends(get_current_user)])


def _get_templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates  # type: ignore[no-any-return]


def _plan_dict(plan: ImportPlan) -> dict[str, object]:
    return {
        "id": plan.id,
        "release_id": plan.release_id,
        "track_id": plan.track_id,
        "source_path": plan.source_path,
        "staging_path": plan.staging_path,
        "destination_path": plan.destination_path,
        "destination_temp_path": plan.destination_temp_path,
        "planned_operations": plan.planned_operations_json,
        "collision_state": plan.collision_state.value,
        "tag_verification_state": plan.tag_verification_state.value,
        "status": plan.status.value,
        "error_detail": plan.error_detail,
        "rollback_detail": plan.rollback_detail,
    }


@router.get("/plans")
async def list_import_plans(
    db: Annotated[AsyncSession, Depends(get_db)],
) -> list[dict[str, object]]:
    result = await db.execute(select(ImportPlan).order_by(ImportPlan.created_at.desc()).limit(200))
    return [_plan_dict(plan) for plan in result.scalars().all()]


@router.post("/releases/{release_id}/plan")
async def plan_release(
    release_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> list[dict[str, object]]:
    release = await db.get(Release, release_id)
    if release is None:
        raise HTTPException(status_code=404, detail="Release not found")
    plans = await plan_release_import(
        db, release, library_root=settings.library_root, naming_template=settings.naming_template
    )
    return [_plan_dict(plan) for plan in plans]


@router.post("/releases/{release_id}/execute")
async def execute_release(
    release_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> list[dict[str, object]]:
    release = await db.get(Release, release_id)
    if release is None:
        raise HTTPException(status_code=404, detail="Release not found")
    try:
        plans = await execute_release_import(db, release, library_root=settings.library_root)
    except ImportExecutionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return [_plan_dict(plan) for plan in plans]


@router.get("/ui/releases/{release_id}/plan", include_in_schema=False)
async def plan_release_from_review_get(release_id: int) -> RedirectResponse:
    del release_id
    return RedirectResponse("/imports/ui/review", status_code=307)


@router.post("/ui/releases/{release_id}/plan", response_class=RedirectResponse)
async def plan_release_from_review(
    release_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    release = await db.get(Release, release_id)
    if release is None:
        raise HTTPException(status_code=404, detail="Release not found")
    await plan_release_import(
        db, release, library_root=settings.library_root, naming_template=settings.naming_template
    )
    return RedirectResponse("/imports/ui/review", status_code=303)


@router.get("/ui/releases/{release_id}/execute", include_in_schema=False)
async def execute_release_from_review_get(release_id: int) -> RedirectResponse:
    del release_id
    return RedirectResponse("/imports/ui/review", status_code=307)


@router.post("/ui/releases/{release_id}/execute", response_class=RedirectResponse)
async def execute_release_from_review(
    release_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    release = await db.get(Release, release_id)
    if release is None:
        raise HTTPException(status_code=404, detail="Release not found")
    form = await request.form()
    if str(form.get("confirm_import", "")) != "yes":
        return RedirectResponse("/imports/ui/review?error=confirmation_required", status_code=303)
    plans = list(await db.scalars(select(ImportPlan).where(ImportPlan.release_id == release_id)))
    if not plans or any(plan.status != ImportWorkflowState.ready for plan in plans):
        return RedirectResponse("/imports/ui/review?error=plan_not_ready", status_code=303)
    try:
        confirmed_plan_ids = [int(str(value)) for value in form.getlist("confirmed_plan_id")]
    except ValueError:
        confirmed_plan_ids = []
    current_plan_ids = {plan.id for plan in plans}
    if (
        len(confirmed_plan_ids) != len(current_plan_ids)
        or set(confirmed_plan_ids) != current_plan_ids
    ):
        return RedirectResponse("/imports/ui/review?error=plan_changed", status_code=303)
    try:
        await execute_release_import(db, release, library_root=settings.library_root)
    except ImportExecutionError:
        await db.commit()
        return RedirectResponse("/imports/ui/review?error=execution_failed", status_code=303)
    return RedirectResponse("/imports/ui/review", status_code=303)


@router.get("/ui/review", response_class=HTMLResponse)
async def import_review_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> HTMLResponse:
    templates = _get_templates(request)
    releases = list(
        await db.scalars(select(Release).order_by(Release.created_at.desc()).limit(100))
    )
    release_ids = [release.id for release in releases]
    plans = (
        list(
            await db.scalars(
                select(ImportPlan)
                .where(ImportPlan.release_id.in_(release_ids))
                .order_by(ImportPlan.created_at.desc())
            )
        )
        if release_ids
        else []
    )
    errors = {
        "confirmation_required": "Confirm the reviewed file count before importing.",
        "plan_not_ready": "Generate and review a ready import plan before importing.",
        "plan_changed": "The import plan changed after review. Review it again before importing.",
        "execution_failed": "Import failed. Review the persisted plan details below.",
    }
    return templates.TemplateResponse(
        request,
        "imports.html",
        {
            "releases": releases,
            "plans": plans,
            "error_message": errors.get(request.query_params.get("error", "")),
        },
    )
