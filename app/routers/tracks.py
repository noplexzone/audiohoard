from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user
from app.config import Settings
from app.database import get_db
from app.models.track import Track
from app.schemas.track import TrackRead
from app.services.media_streaming import (
    MediaAssetError,
    TranscodeError,
    media_response,
    open_imported_track,
    open_or_create_mp3_preview,
)
from app.settings_service import effective_settings_dep

router = APIRouter(dependencies=[Depends(get_current_user)])


def _get_templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates  # type: ignore[no-any-return]


@router.api_route("/tracks/{track_id}/audio", methods=["GET", "HEAD"], include_in_schema=False)
@router.api_route(
    "/library/tracks/{track_id}/audio", methods=["GET", "HEAD"], include_in_schema=False
)
async def stream_imported_track(
    track_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    transcode: Annotated[str | None, Query(pattern="^mp3$")] = None,
) -> Response:
    try:
        asset = await open_imported_track(db, track_id, library_root=settings.library_root)
    except MediaAssetError:
        raise HTTPException(status_code=404, detail="Imported audio is unavailable") from None
    if transcode == "mp3":
        try:
            asset = await open_or_create_mp3_preview(
                asset,
                track_id=track_id,
                cache_root=settings.artwork_cache_root.parent / "library-audio",
            )
        except TranscodeError:
            raise HTTPException(
                status_code=422, detail="Browser-compatible audio could not be created"
            ) from None
    return media_response(request, asset)


@router.get("/tracks", response_model=list[TrackRead])
async def list_tracks(
    db: Annotated[AsyncSession, Depends(get_db)],
    job_id: int | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[Track]:
    q = (
        select(Track)
        .options(selectinload(Track.path_previews))
        .order_by(Track.id.desc())
        .offset(offset)
        .limit(limit)
    )
    if job_id is not None:
        q = q.where(Track.job_id == job_id)
    result = await db.execute(q)
    return list(result.scalars().all())


@router.get("/tracks/{track_id}", response_model=TrackRead)
async def get_track(
    track_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> Track:
    result = await db.execute(
        select(Track).options(selectinload(Track.path_previews)).where(Track.id == track_id)
    )
    track = result.scalar_one_or_none()
    if track is None:
        raise HTTPException(status_code=404, detail="Track not found")
    return track


@router.get("/tracks/{track_id}/ui", response_class=HTMLResponse)
async def track_detail_page(
    track_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> HTMLResponse:
    templates = _get_templates(request)
    result = await db.execute(
        select(Track).options(selectinload(Track.path_previews)).where(Track.id == track_id)
    )
    track = result.scalar_one_or_none()
    if track is None:
        raise HTTPException(status_code=404, detail="Track not found")
    return templates.TemplateResponse(request, "track.html", {"track": track})
