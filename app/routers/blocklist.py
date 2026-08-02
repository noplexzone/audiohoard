from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import Response

from app.auth import get_current_user, require_mutation
from app.database import get_db
from app.models.source_candidate_block import SourceCandidateBlock

router = APIRouter(prefix="/blocklist", dependencies=[Depends(get_current_user)])


@router.get("", response_class=HTMLResponse, include_in_schema=False)
async def blocklist_page(
    request: Request, db: Annotated[AsyncSession, Depends(get_db)]
) -> Response:
    blocks = (
        await db.scalars(
            select(SourceCandidateBlock).order_by(
                SourceCandidateBlock.created_at.desc(), SourceCandidateBlock.id.desc()
            )
        )
    ).all()
    templates = request.app.state.templates
    response: Response = templates.TemplateResponse(
        request,
        "blocklist.html",
        {
            "blocks": blocks,
            "removed": request.query_params.get("removed", ""),
        },
    )
    return response


@router.post("/{block_id}/remove", include_in_schema=False)
async def remove_blocked_source(
    block_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    block = await db.get(SourceCandidateBlock, block_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Blocked source not found")
    await db.delete(block)
    await db.commit()
    return RedirectResponse("/blocklist?removed=1", status_code=303)
