from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.database import get_db
from app.services.activity import ActivitySummary, get_batch_failures

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("/activity", response_class=HTMLResponse, include_in_schema=False)
async def activity_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> HTMLResponse:
    """Show a concise acquisition overview using the shared navigation aggregate."""
    templates: Jinja2Templates = request.app.state.templates
    summary: ActivitySummary | None = getattr(request.state, "activity_summary", None)
    batch_failures = await get_batch_failures(db)
    return templates.TemplateResponse(
        request,
        "activity.html",
        {"activity_summary": summary, "batch_failures": batch_failures},
    )
