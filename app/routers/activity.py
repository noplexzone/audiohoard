from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.database import get_db
from app.services.activity import ActivitySummary, get_batch_failure_page

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("/activity", response_class=HTMLResponse, include_in_schema=False)
async def activity_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> HTMLResponse:
    """Show a concise acquisition overview using the shared navigation aggregate."""
    templates: Jinja2Templates = request.app.state.templates
    summary: ActivitySummary | None = getattr(request.state, "activity_summary", None)
    batch_failure_page = await get_batch_failure_page(db)
    return templates.TemplateResponse(
        request,
        "activity.html",
        {
            "activity_summary": summary,
            "batch_failures": batch_failure_page.items,
            "batch_failure_total": batch_failure_page.total,
        },
    )


@router.get("/activity/queue-failures", response_class=HTMLResponse, include_in_schema=False)
async def queue_failures_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    page: Annotated[int, Query(ge=1)] = 1,
) -> HTMLResponse:
    templates: Jinja2Templates = request.app.state.templates
    failures = await get_batch_failure_page(db, page=page)
    return templates.TemplateResponse(
        request,
        "queue_failures.html",
        {"failures": failures},
    )
