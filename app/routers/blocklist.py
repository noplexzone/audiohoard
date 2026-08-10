from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from math import ceil
from typing import Annotated, cast
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from starlette.responses import Response

from app.auth import get_current_user, require_mutation
from app.database import get_db
from app.jobs.dispatcher import JobNotFoundError, JobNotRetryableError, job_dispatcher
from app.models.acquisition_attempt import AcquisitionAttempt
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.source_candidate_block import SourceCandidateBlock
from app.services.rejected_sources import RejectionClass, classify_rejection_reason

router = APIRouter(prefix="/blocklist", dependencies=[Depends(get_current_user)])
_RETRYABLE_JOB_STATUSES = {JobStatus.failed, JobStatus.partial, JobStatus.cancelled}


@dataclass(frozen=True, slots=True)
class RejectedSourceView:
    block: SourceCandidateBlock
    classification: RejectionClass
    artist_name: str | None
    album_id: int | None
    album_title: str | None
    track_title: str | None
    attempt_id: int | None
    job_id: int | None
    job_status: JobStatus | None

    @property
    def active(self) -> bool:
        if self.block.blocked_until is None:
            return True
        blocked_until = self.block.blocked_until
        if blocked_until.tzinfo is None:
            blocked_until = blocked_until.replace(tzinfo=UTC)
        return blocked_until > datetime.now(UTC)

    @property
    def can_retry(self) -> bool:
        return self.job_id is not None and self.job_status in _RETRYABLE_JOB_STATUSES


@router.get("", response_class=HTMLResponse, include_in_schema=False)
async def blocklist_page(
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    q: str = "",
    status: str = "all",
    provider: str = "",
    reason: str = "",
    page: int = Query(default=1, ge=1, le=10_000),
    per_page: int = Query(default=50, ge=1, le=100),
) -> Response:
    now = datetime.now(UTC)
    filters: list[ColumnElement[bool]] = []
    if q:
        pattern = f"%{q}%"
        filters.append(
            or_(
                SourceCandidateBlock.peer.ilike(pattern),
                SourceCandidateBlock.filename.ilike(pattern),
            )
        )
    if provider:
        filters.append(SourceCandidateBlock.provider == provider)
    if reason:
        filters.append(SourceCandidateBlock.reason == reason)
    valid_status = (
        status if status in {"all", "active", "permanent", "temporary", "eligible"} else "all"
    )
    if valid_status == "active":
        filters.append(
            or_(
                SourceCandidateBlock.blocked_until.is_(None),
                SourceCandidateBlock.blocked_until > now,
            )
        )
    elif valid_status == "permanent":
        filters.append(SourceCandidateBlock.blocked_until.is_(None))
    elif valid_status == "temporary":
        filters.append(SourceCandidateBlock.blocked_until.is_not(None))
    elif valid_status == "eligible":
        filters.append(SourceCandidateBlock.blocked_until <= now)

    total_result = await db.execute(select(func.count(SourceCandidateBlock.id)).where(*filters))
    total = int(total_result.scalar_one())
    total_pages = max(1, ceil(total / per_page))
    page = min(page, total_pages)
    latest_attempt_id = (
        select(AcquisitionAttempt.id)
        .where(
            AcquisitionAttempt.provider == SourceCandidateBlock.provider,
            AcquisitionAttempt.peer == SourceCandidateBlock.peer,
            AcquisitionAttempt.remote_path == SourceCandidateBlock.filename,
        )
        .order_by(AcquisitionAttempt.updated_at.desc(), AcquisitionAttempt.id.desc())
        .limit(1)
        .correlate(SourceCandidateBlock)
        .scalar_subquery()
    )
    stmt = (
        select(
            SourceCandidateBlock,
            AcquisitionAttempt.id.label("attempt_id"),
            Job.id.label("job_id"),
            Job.status.label("job_status"),
            CatalogAlbum.id.label("album_id"),
            CatalogAlbum.title.label("album_title"),
            CatalogArtist.name.label("artist_name"),
            CatalogAlbumTrack.title.label("track_title"),
        )
        .outerjoin(AcquisitionAttempt, AcquisitionAttempt.id == latest_attempt_id)
        .outerjoin(Job, Job.id == AcquisitionAttempt.job_id)
        .outerjoin(CatalogAlbum, CatalogAlbum.id == AcquisitionAttempt.catalog_album_id)
        .outerjoin(CatalogArtist, CatalogArtist.id == CatalogAlbum.artist_id)
        .outerjoin(CatalogAlbumTrack, CatalogAlbumTrack.id == AcquisitionAttempt.catalog_track_id)
        .where(*filters)
        .order_by(SourceCandidateBlock.created_at.desc(), SourceCandidateBlock.id.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    rows = (await db.execute(stmt)).all()
    blocks = [
        RejectedSourceView(
            block=row[0],
            classification=classify_rejection_reason(row[0].reason),
            attempt_id=row.attempt_id,
            job_id=row.job_id,
            job_status=row.job_status,
            album_id=row.album_id,
            album_title=row.album_title,
            artist_name=row.artist_name,
            track_title=row.track_title,
        )
        for row in rows
    ]
    filter_params = {
        "q": q,
        "status": valid_status,
        "provider": provider,
        "reason": reason,
        "per_page": str(per_page),
    }
    templates = request.app.state.templates
    return cast(
        Response,
        templates.TemplateResponse(
            request,
            "blocklist.html",
            {
                "blocks": blocks,
                "page": page,
                "total": total,
                "total_pages": total_pages,
                "q": q,
                "status": valid_status,
                "provider": provider,
                "reason": reason,
                "filter_qs": urlencode(filter_params),
                "allowed": request.query_params.get("allowed", ""),
                "retried": request.query_params.get("retried", ""),
                "retry_unavailable": request.query_params.get("retry_unavailable", ""),
            },
        ),
    )


async def _get_block_and_retry_job(
    db: AsyncSession, block_id: int
) -> tuple[SourceCandidateBlock, int | None]:
    block = await db.get(SourceCandidateBlock, block_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Rejected source not found")
    job_id = await db.scalar(
        select(AcquisitionAttempt.job_id)
        .join(Job, Job.id == AcquisitionAttempt.job_id)
        .where(
            AcquisitionAttempt.provider == block.provider,
            AcquisitionAttempt.peer == block.peer,
            AcquisitionAttempt.remote_path == block.filename,
            Job.status.in_(_RETRYABLE_JOB_STATUSES),
        )
        .order_by(AcquisitionAttempt.updated_at.desc(), AcquisitionAttempt.id.desc())
        .limit(1)
    )
    return block, int(job_id) if job_id is not None else None


@router.post("/{block_id}/allow", include_in_schema=False)
@router.post("/{block_id}/remove", include_in_schema=False)
async def allow_source_again(
    block_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    block, _job_id = await _get_block_and_retry_job(db, block_id)
    await db.delete(block)
    await db.commit()
    return RedirectResponse("/blocklist?allowed=1", status_code=303)


@router.post("/{block_id}/allow-retry", include_in_schema=False)
async def allow_and_retry_source(
    block_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    block, job_id = await _get_block_and_retry_job(db, block_id)
    await db.delete(block)
    await db.commit()
    if job_id is None:
        return RedirectResponse("/blocklist?allowed=1&retry_unavailable=1", status_code=303)
    try:
        await job_dispatcher.retry(job_id)
    except (JobNotFoundError, JobNotRetryableError):
        return RedirectResponse("/blocklist?allowed=1&retry_unavailable=1", status_code=303)
    return RedirectResponse("/blocklist?allowed=1&retried=1", status_code=303)
