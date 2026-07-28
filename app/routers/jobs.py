from __future__ import annotations

import json
import logging
from datetime import UTC
from datetime import datetime as dt
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql.elements import ColumnElement

from app.auth import get_current_user, require_mutation
from app.database import get_db
from app.jobs.dispatcher import JobNotFoundError, JobStateError, job_dispatcher
from app.models.catalog_entities import CatalogAlbum
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.workflow import ImportWorkflowState, ReviewDecision
from app.schemas.job import JobCreate, JobRead, SelectedResultPayload
from app.services.download_queue import project_download_groups

router = APIRouter(dependencies=[Depends(get_current_user)])
logger = logging.getLogger(__name__)
_ALLOWED_JOB_SOURCES: set[str] = {"priority", "slskd", "prowlarr", "youtube", "tidal"}


def _get_templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates  # type: ignore[no-any-return]


def _selected_json(payload: SelectedResultPayload | None) -> str | None:
    return payload.model_dump_json() if payload is not None else None


def _review_item_reason(item: StagingReviewItem) -> str:
    track_label = item.track.track_no or item.track.id or "unknown"
    if item.verification_reason == "mismatch":
        return f"AcoustID mismatch on track {track_label}"
    return f"AcoustID verification unavailable on track {track_label}"


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
    q = (
        select(Job)
        .where(Job.queue_hidden.is_(False))
        .order_by(Job.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
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
    load_options = (
        selectinload(Job.tracks),
        selectinload(Job.catalog_album).selectinload(CatalogAlbum.artist),
        selectinload(Job.catalog_album).selectinload(CatalogAlbum.tracks),
    )
    metadata_query = (
        select(Job.id, Job.parent_job_id, Job.catalog_album_id)
        .where(Job.queue_hidden.is_(False))
        .order_by(Job.created_at.desc(), Job.id.desc())
        .limit(500)
    )
    if status is not None:
        metadata_query = metadata_query.where(Job.status == status)
    metadata_rows = (await db.execute(metadata_query)).all()
    parents = {
        int(job_id): int(parent_job_id) if parent_job_id is not None else None
        for job_id, parent_job_id, _catalog_album_id in metadata_rows
    }

    unresolved_parents = {
        parent_job_id
        for parent_job_id in parents.values()
        if parent_job_id is not None and parent_job_id not in parents
    }
    while unresolved_parents:
        ancestor_rows = (
            await db.execute(
                select(Job.id, Job.parent_job_id).where(Job.id.in_(unresolved_parents))
            )
        ).all()
        if not ancestor_rows:
            break
        for job_id, parent_job_id in ancestor_rows:
            parents[int(job_id)] = int(parent_job_id) if parent_job_id is not None else None
        unresolved_parents = {
            parent_job_id
            for parent_job_id in parents.values()
            if parent_job_id is not None and parent_job_id not in parents
        }

    candidate_root_ids = {
        int(job_id)
        for job_id, parent_job_id, _catalog_album_id in metadata_rows
        if parent_job_id is None
    }
    parent_ids = set(
        (
            await db.scalars(
                select(Job.parent_job_id)
                .where(Job.parent_job_id.in_(candidate_root_ids))
                .distinct()
            )
        ).all()
    )

    def root_id(job_id: int) -> int:
        seen: set[int] = set()
        current = job_id
        while current not in seen:
            parent_job_id = parents.get(current)
            if parent_job_id is None:
                break
            seen.add(current)
            current = parent_job_id
        return current

    selected_keys: set[tuple[str, int]] = set()
    for job_id, parent_job_id, catalog_album_id in metadata_rows:
        if catalog_album_id is not None:
            key = ("album", int(catalog_album_id))
        elif parent_job_id is not None or job_id in parent_ids:
            key = ("chain", root_id(int(job_id)))
        else:
            key = ("job", int(job_id))
        selected_keys.add(key)
        if len(selected_keys) == 100:
            break

    album_ids = {value for kind, value in selected_keys if kind == "album"}
    chain_roots = {value for kind, value in selected_keys if kind == "chain"}
    standalone_ids = {value for kind, value in selected_keys if kind == "job"}
    chain_ids = set(chain_roots)
    descendant_frontier = set(chain_roots)
    while descendant_frontier:
        descendant_rows = (
            await db.execute(
                select(Job.id, Job.parent_job_id).where(Job.parent_job_id.in_(descendant_frontier))
            )
        ).all()
        new_ids = {int(job_id) for job_id, _parent_job_id in descendant_rows} - chain_ids
        for job_id, parent_job_id in descendant_rows:
            parents[int(job_id)] = int(parent_job_id)
        chain_ids.update(new_ids)
        descendant_frontier = new_ids

    visible_conditions: list[ColumnElement[bool]] = []
    if album_ids:
        visible_conditions.append(Job.catalog_album_id.in_(album_ids))
    if chain_ids:
        visible_conditions.append(Job.catalog_album_id.is_(None) & Job.id.in_(chain_ids))
    if standalone_ids:
        visible_conditions.append(Job.id.in_(standalone_ids))

    downloads: list[Job] = []
    if visible_conditions:
        visible_query = (
            select(Job)
            .where(Job.queue_hidden.is_(False), or_(*visible_conditions))
            .options(*load_options)
        )
        downloads.extend((await db.scalars(visible_query)).all())

    hidden_conditions: list[ColumnElement[bool]] = []
    if album_ids:
        hidden_conditions.append(Job.catalog_album_id.in_(album_ids))
    if chain_ids:
        hidden_conditions.append(Job.catalog_album_id.is_(None) & Job.id.in_(chain_ids))
    if hidden_conditions:
        hidden_query = (
            select(Job)
            .where(Job.queue_hidden.is_(True), or_(*hidden_conditions))
            .options(*load_options)
        )
        downloads.extend((await db.scalars(hidden_query)).all())

    download_groups = project_download_groups(downloads, parents)
    if status is not None:
        download_groups = [
            group
            for group in download_groups
            if any(attempt.job.status == status for attempt in group.attempts)
        ]
    download_groups = download_groups[:100]
    review_result = await db.execute(
        select(StagingReviewItem)
        .where(
            StagingReviewItem.review_state == ReviewDecision.pending,
            StagingReviewItem.release.has(Release.review_dismissed_at.is_(None)),
        )
        .options(
            selectinload(StagingReviewItem.track),
            selectinload(StagingReviewItem.release),
        )
        .order_by(StagingReviewItem.created_at.asc())
    )
    pending_review_items = list(review_result.scalars().all())
    release_result = await db.execute(
        select(Release)
        .where(
            Release.import_state.in_(
                [
                    ImportWorkflowState.needs_review,
                    ImportWorkflowState.failed,
                    ImportWorkflowState.rolled_back,
                ]
            ),
            Release.review_dismissed_at.is_(None),
        )
        .options(
            selectinload(Release.tracks),
            selectinload(Release.import_plans),
        )
        .order_by(Release.created_at.asc())
    )
    releases_by_id = {release.id: release for release in release_result.scalars().all()}
    items_by_release: dict[int, list[StagingReviewItem]] = {}
    for item in pending_review_items:
        items_by_release.setdefault(item.release_id, []).append(item)
        releases_by_id.setdefault(item.release_id, item.release)
    release_reviews: list[dict[str, object]] = []
    for release in releases_by_id.values():
        items = items_by_release.get(release.id, [])
        plan_reason = next(
            (plan.error_detail for plan in release.import_plans if plan.error_detail), None
        )
        reason = release.error_detail or release.rollback_detail or plan_reason
        if not reason and items:
            reason = _review_item_reason(items[0])
        if not reason:
            reason = "review required: no structured reason was recorded"
        if (
            not items
            and str(release.error_detail or "").startswith("AcoustID mismatch on track ")
            and not release.rollback_detail
            and plan_reason is None
        ):
            continue
        release_reviews.append(
            {
                "release": release,
                "reason": reason,
                "missing_source": str(reason).startswith("missing staged source:"),
                "review_items": items,
            }
        )
    notices = {
        "cancelled": ("Cancellation requested.", "info"),
        "retried": ("Retry scheduled.", "info"),
        "invalid_state": ("That job can no longer be changed.", "error"),
        "not_found": ("Job not found.", "error"),
        "removed": ("Download removed from the queue.", "info"),
        "approved": ("Track approved — import pipeline resumed.", "ok"),
        "denied": ("Track denied — staged file and review item removed.", "info"),
        "already_reviewed": ("That item has already been reviewed.", "info"),
        "review_dismissed": ("Review entry dismissed; staged files were retained.", "info"),
        "reacquired": ("Re-acquisition queued for the missing staged source.", "info"),
    }
    notice_key = request.query_params.get("notice", "")
    notice, notice_type = notices.get(notice_key, (None, "info"))
    if notice_key == "cleared":
        try:
            count = max(0, int(request.query_params.get("count", "0")))
        except ValueError:
            count = 0
        notice = f"Cleared {count} download{'s' if count != 1 else ''} from the queue."
    now = dt.now(UTC)
    return templates.TemplateResponse(
        request,
        "downloads.html",
        {
            "downloads": downloads,
            "jobs": downloads,
            "download_groups": download_groups,
            "pending_review_items": pending_review_items,
            "release_reviews": release_reviews,
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


@router.post("/downloads/{job_id}/remove", include_in_schema=False)
async def remove_job_ui(
    job_id: int,
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    try:
        await job_dispatcher.remove(job_id)
        notice = "removed"
    except JobNotFoundError:
        notice = "not_found"
    except JobStateError:
        notice = "invalid_state"
    return RedirectResponse(f"/downloads?notice={notice}", status_code=303)


@router.post("/downloads/clear", include_in_schema=False)
async def clear_jobs_ui(
    request: Request,
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    form = await request.form()
    scope = str(form.get("scope", ""))
    scopes = {
        "failed": {JobStatus.failed},
        "finished": {
            JobStatus.done,
            JobStatus.failed,
            JobStatus.partial,
            JobStatus.cancelled,
        },
    }
    statuses = scopes.get(scope)
    if statuses is None:
        return RedirectResponse("/downloads?notice=invalid_state", status_code=303)
    count = await job_dispatcher.clear(statuses)
    return RedirectResponse(f"/downloads?notice=cleared&count={count}", status_code=303)
