from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user
from app.config import Settings
from app.database import get_db
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import ImportWorkflowState, ReviewDecision
from app.services.staging import REVIEW_TAG_FIELDS, build_review_item
from app.settings_service import effective_settings_dep

router = APIRouter(dependencies=[Depends(get_current_user)])

_TAG_LABELS = {
    "title": "Title",
    "artist": "Artist",
    "album": "Album",
    "album_artist": "Album artist",
    "track_number": "Track number",
    "disc_number": "Disc number",
    "year": "Year",
    "genre": "Genre",
}


def _release_recovery(release: Release) -> dict[str, object] | None:
    plan_reason = next(
        (plan.error_detail for plan in release.import_plans if plan.error_detail), None
    )
    reason = release.error_detail or release.rollback_detail or plan_reason
    missing_source = bool(reason and reason.startswith("missing staged source:"))
    if release.import_state == ImportWorkflowState.needs_review and not missing_source:
        return None
    return {
        "release": release,
        "reason": reason or "review required: no structured reason was recorded",
        "missing_source": missing_source,
    }


@router.get("/review", response_class=HTMLResponse, include_in_schema=False)
async def review_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
) -> HTMLResponse:
    pending_filter = (
        StagingReviewItem.review_state == ReviewDecision.pending,
        StagingReviewItem.release.has(Release.review_dismissed_at.is_(None)),
    )
    pending_count = int(
        await db.scalar(select(func.count(StagingReviewItem.id)).where(*pending_filter)) or 0
    )
    item = await db.scalar(
        select(StagingReviewItem)
        .where(*pending_filter)
        .options(
            selectinload(StagingReviewItem.release),
            selectinload(StagingReviewItem.track)
            .selectinload(Track.catalog_track)
            .selectinload(CatalogAlbumTrack.album)
            .selectinload(CatalogAlbum.artist),
            selectinload(StagingReviewItem.track)
            .selectinload(Track.catalog_album)
            .selectinload(CatalogAlbum.artist),
        )
        .order_by(StagingReviewItem.created_at.asc(), StagingReviewItem.id.asc())
        .limit(1)
    )
    review = await build_review_item(item, settings) if item is not None else None
    release_recovery = None
    if item is None:
        releases = list(
            (
                await db.scalars(
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
                    .options(selectinload(Release.import_plans))
                    .order_by(Release.created_at.asc(), Release.id.asc())
                )
            ).all()
        )
        release_recovery = next(
            (projected for release in releases if (projected := _release_recovery(release))),
            None,
        )
    templates: Jinja2Templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "review.html",
        {
            "review": review,
            "release_recovery": release_recovery,
            "pending_count": pending_count,
            "tag_fields": [(field, _TAG_LABELS[field]) for field in REVIEW_TAG_FIELDS],
        },
    )
