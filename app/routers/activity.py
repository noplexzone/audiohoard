from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.auth import get_current_user
from app.services.activity import ActivitySummary

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("/activity", response_class=HTMLResponse, include_in_schema=False)
async def activity_page(request: Request) -> HTMLResponse:
    """Show a concise acquisition overview using the shared navigation aggregate."""
    templates: Jinja2Templates = request.app.state.templates
    summary: ActivitySummary | None = getattr(request.state, "activity_summary", None)
    return templates.TemplateResponse(
        request,
        "activity.html",
        {"activity_summary": summary},
    )
