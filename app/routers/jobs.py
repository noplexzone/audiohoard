from __future__ import annotations

import json
import logging
from datetime import UTC
from datetime import datetime as dt
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user, require_mutation
from app.database import get_db
from app.jobs.dispatcher import JobNotFoundError, JobStateError, job_dispatcher
from app.models.job import Job, JobStatus
from app.schemas.job import JobCreate, JobRead, SelectedResultPayload

router = APIRouter(dependencies=[Depends(get_current_user)])
logger = logging.getLogger(__name__)
_ALLOWED_JOB_SOURCES: set[str] = {"priority", "slskd", "prowlarr", "youtube", "tidal"}


def _get_templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates  # type: ignore[no-any-return]


def _selected_json(payload: SelectedResultPayload | None) -> str | None:
    return payload.model_dump_json() if payload is not None else None


@router.post("/jobs", response_model=JobRead, status_code=201)
async def create_job(
    payload: JobCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> Job:
    job = Job(
        source=payload.source,
        query=payload.query,
        status=JobStatus.pending,
        selected_result_json=_selected_json(payload.selected_result),
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    await job_dispatcher.dispatch(job.id)
    return job


@router.get("/jobs", response_model=list[JobRead])
async def list_jobs(
    db: Annotated[AsyncSession, Depends(get_db)],
    status: JobStatus | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[Job]:
    q = select(Job).order_by(Job.created_at.desc()).offset(offset).limit(limit)
    if status is not None:
        q = q.where(Job.status == status)
    result = await db.execute(q)
    return list(result.scalars().all())


@router.get("/jobs/{job_id}", response_model=JobRead)
async def get_job(job_id: int, db: Annotated[AsyncSession, Depends(get_db)]) -> Job:
    job = await db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/downloads", response_class=HTMLResponse, include_in_schema=False)
async def downloads_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    status: JobStatus | None = None,
) -> HTMLResponse:
    templates = _get_templates(request)
    query = (
        select(Job).options(selectinload(Job.tracks)).order_by(Job.created_at.desc()).limit(100)
    )
    if status is not None:
        query = query.where(Job.status == status)
    result = await db.execute(query)
    downloads = list(result.scalars().all())
    notices = {
        "cancelled": ("Cancellation requested.", "info"),
        "retried": ("Retry scheduled.", "info"),
        "invalid_state": ("That job can no longer be changed.", "error"),
        "not_found": ("Job not found.", "error"),
    }
    notice, notice_type = notices.get(request.query_params.get("notice", ""), (None, "info"))
    now = dt.now(UTC)
    return templates.TemplateResponse(
        request,
        "downloads.html",
        {
            "downloads": downloads,
            "jobs": downloads,
            "notice": notice,
            "notice_type": notice_type,
            "selected_status": status.value if status is not None else None,
            "now": now,
        },
    )


@router.get("/jobs/ui/list", include_in_schema=False)
async def old_jobs_page() -> RedirectResponse:
    return RedirectResponse("/downloads", status_code=308)


@router.get("/jobs/ui/create", include_in_schema=False)
async def old_create_job_page() -> RedirectResponse:
    return RedirectResponse("/downloads", status_code=307)


@router.get("/downloads/create", include_in_schema=False)
async def downloads_create_page() -> RedirectResponse:
    return RedirectResponse("/downloads", status_code=307)


@router.post("/jobs/ui/create", response_class=HTMLResponse, include_in_schema=False)
@router.post("/downloads/create", response_class=HTMLResponse, include_in_schema=False)
async def create_job_ui(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    source = str(form.get("source", "priority")).strip()
    query = str(form.get("query", "")).strip()
    selected_raw = str(form.get("selected_result", "")).strip()
    selected: SelectedResultPayload | None = None
    if selected_raw:
        try:
            selected = SelectedResultPayload.model_validate(json.loads(selected_raw))
        except (json.JSONDecodeError, ValueError):
            logger.warning("Rejected invalid selected result payload from UI")
            return RedirectResponse("/downloads", status_code=303)
    if source not in _ALLOWED_JOB_SOURCES:
        logger.warning("Rejected unsupported download source from UI: %s", source)
        return RedirectResponse("/downloads", status_code=303)
    if not query and selected is None:
        return RedirectResponse("/downloads", status_code=303)
    job = Job(
        source=source,
        query=query or (selected.title if selected is not None else ""),
        status=JobStatus.pending,
        selected_result_json=_selected_json(selected),
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    await job_dispatcher.dispatch(job.id)
    return RedirectResponse("/downloads", status_code=303)


@router.post("/jobs/{job_id}/cancel", status_code=202)
async def cancel_job(
    job_id: int,
    _user: Annotated[object, Depends(require_mutation)],
) -> dict[str, str]:
    try:
        await job_dispatcher.cancel_job(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Job not found") from exc
    except JobStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "cancellation_requested"}


@router.post("/jobs/{job_id}/retry", status_code=202)
async def retry_job(
    job_id: int,
    _user: Annotated[object, Depends(require_mutation)],
) -> dict[str, str]:
    try:
        await job_dispatcher.retry(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Job not found") from exc
    except JobStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "retry_scheduled"}


async def _job_action_redirect(action: str, job_id: int) -> RedirectResponse:
    try:
        if action == "cancel":
            await job_dispatcher.cancel_job(job_id)
            notice = "cancelled"
        else:
            await job_dispatcher.retry(job_id)
            notice = "retried"
    except JobNotFoundError:
        notice = "not_found"
    except JobStateError:
        notice = "invalid_state"
    return RedirectResponse(f"/downloads?notice={notice}", status_code=303)


@router.post("/downloads/{job_id}/cancel", include_in_schema=False)
async def cancel_job_ui(
    job_id: int,
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    return await _job_action_redirect("cancel", job_id)


@router.post("/downloads/{job_id}/retry", include_in_schema=False)
async def retry_job_ui(
    job_id: int,
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    return await _job_action_redirect("retry", job_id)
