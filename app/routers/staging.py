from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse, StreamingResponse
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user, require_mutation
from app.config import Settings
from app.database import get_db
from app.jobs.dispatcher import job_dispatcher
from app.media_formats import IMPORTABLE_AUDIO_SUFFIXES
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
    ReviewDecision,
)
from app.settings_service import effective_settings_dep, get_runtime_settings

router = APIRouter(prefix="/staging", dependencies=[Depends(get_current_user)])

_MIME_MAP: dict[str, str] = {
    ".flac": "audio/flac",
    ".mp3": "audio/mpeg",
    ".m4a": "audio/mp4",
    ".mp4": "audio/mp4",
    ".ogg": "audio/ogg",
    ".oga": "audio/ogg",
    ".opus": "audio/ogg; codecs=opus",
    ".wav": "audio/wav",
    ".aac": "audio/aac",
}


def _validate_audio_path(staging_path: str, staging_root: Path) -> Path:
    """Validate and resolve a staged audio path.

    Raises HTTPException on traversal, symlink, or non-audio extension.
    """
    raw = Path(staging_path)
    if not raw.is_absolute():
        raise HTTPException(status_code=400, detail="Staging path must be absolute")

    try:
        resolved = raw.resolve(strict=False)
        root_resolved = staging_root.resolve()
        resolved.relative_to(root_resolved)
    except ValueError:
        raise HTTPException(status_code=400, detail="Path escapes staging root") from None

    current = Path(raw.anchor)
    for component in raw.parts[1:]:
        current /= component
        if current.is_symlink():
            raise HTTPException(status_code=400, detail="Symlinks are not permitted")

    suffix = resolved.suffix.casefold()
    if suffix not in IMPORTABLE_AUDIO_SUFFIXES:
        raise HTTPException(status_code=400, detail="Not an importable audio file")

    if not resolved.is_file():
        raise HTTPException(status_code=404, detail="Staged file not found")

    return resolved


def _parse_range(range_header: str | None, file_size: int) -> tuple[int, int]:
    """Parse a simple Range: bytes=start-end header.  Returns (start, end) inclusive."""
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1
    spec = range_header[6:].strip()
    parts = spec.split("-", 1)
    try:
        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
    except ValueError:
        return 0, file_size - 1
    start = max(0, min(start, file_size - 1))
    end = max(start, min(end, file_size - 1))
    return start, end


@router.get("/audio/{item_id}", include_in_schema=False)
async def serve_staged_audio(
    item_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
) -> Response:
    """Serve a staged audio file for in-browser playback.

    Requires authentication.  Validates containment, rejects symlinks/non-audio.
    Supports HTTP Range requests.
    """
    item = await db.get(StagingReviewItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Review item not found")

    track = await db.get(Track, item.track_id)
    if track is None or not track.staging_path:
        raise HTTPException(status_code=404, detail="Staged track path not available")

    resolved = await __import__("asyncio").to_thread(
        _validate_audio_path, track.staging_path, settings.staging_root
    )
    file_size = resolved.stat().st_size
    suffix = resolved.suffix.casefold()
    mime = _MIME_MAP.get(suffix, "application/octet-stream")

    range_header = request.headers.get("range")
    start, end = _parse_range(range_header, file_size)
    chunk_size = end - start + 1

    def iterfile() -> Iterator[bytes]:
        with open(resolved, "rb") as f:
            f.seek(start)
            remaining = chunk_size
            buf = 64 * 1024
            while remaining > 0:
                data = f.read(min(buf, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data

    if range_header:
        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(chunk_size),
            "Content-Type": mime,
        }
        return StreamingResponse(iterfile(), status_code=206, headers=headers, media_type=mime)
    else:
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
            "Content-Type": mime,
        }
        return StreamingResponse(iterfile(), status_code=200, headers=headers, media_type=mime)


@router.post("/review/{item_id}/approve", include_in_schema=False)
async def approve_review_item(
    item_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    """Approve a staged track for import despite a verification flag."""
    item = await db.get(StagingReviewItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Review item not found")
    if item.review_state != ReviewDecision.pending:
        return RedirectResponse("/downloads?notice=already_reviewed", status_code=303)

    item.review_state = ReviewDecision.approved
    item.reviewed_at = datetime.now(UTC)

    track = await db.get(Track, item.track_id)
    if track is not None:
        track.acoustid_verification_state = AcoustIDVerificationState.approved

    release = await db.get(Release, item.release_id)
    if release is not None:
        await db.flush()
        from app.services.auto_import import try_auto_import_release

        await try_auto_import_release(
            db,
            release,
            library_root=settings.library_root,
            naming_template=settings.naming_template,
        )

    await db.commit()
    return RedirectResponse("/downloads?notice=approved", status_code=303)


@router.post("/review/{item_id}/deny", include_in_schema=False)
async def deny_review_item(
    item_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    """Deny a staged track, retain it, and schedule bounded reacquisition."""
    item = await db.get(StagingReviewItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Review item not found")
    if item.review_state != ReviewDecision.pending:
        return RedirectResponse("/downloads?notice=already_reviewed", status_code=303)

    item.review_state = ReviewDecision.denied
    item.reviewed_at = datetime.now(UTC)

    track = await db.get(Track, item.track_id)
    continuation_id: int | None = None
    if track is not None:
        track.acoustid_verification_state = AcoustIDVerificationState.denied
        track.acquisition_state = AcquisitionState.failed
        release = await db.get(Release, item.release_id)
        if release is not None:
            track_label = track.track_no or track.id or "unknown"
            release.error_detail = f"AcoustID mismatch on track {track_label}"
        parent = await db.get(Job, track.job_id)
        runtime = await get_runtime_settings(db)
        if (
            parent is not None
            and track.catalog_album_id is not None
            and track.catalog_track_id is not None
            and parent.partial_attempt < runtime.max_partial_attempts
        ):
            duplicate = await db.scalar(
                select(Job.id).where(
                    Job.catalog_album_id == track.catalog_album_id,
                    Job.catalog_track_id == track.catalog_track_id,
                    Job.status.in_([JobStatus.pending, JobStatus.running]),
                )
            )
            if duplicate is None:
                continuation = Job(
                    source="priority",
                    query=" ".join(
                        part for part in (track.artist, track.album, track.title) if part
                    ),
                    status=JobStatus.pending,
                    catalog_album_id=track.catalog_album_id,
                    catalog_track_id=track.catalog_track_id,
                    parent_job_id=parent.id,
                    partial_attempt=parent.partial_attempt + 1,
                )
                db.add(continuation)
                await db.flush()
                continuation_id = continuation.id

    await db.commit()
    if continuation_id is not None:
        await job_dispatcher.dispatch(continuation_id)
    return RedirectResponse("/downloads?notice=denied", status_code=303)


@router.post("/review/{item_id}/dismiss", include_in_schema=False)
async def dismiss_review_item(
    item_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    """Remove a stale review record without touching its staged source file."""
    item = await db.get(StagingReviewItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Review item not found")
    await db.delete(item)
    await db.commit()
    return RedirectResponse("/downloads?notice=review_dismissed", status_code=303)


@router.post("/release/{release_id}/dismiss", include_in_schema=False)
async def dismiss_release_review(
    release_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    """Dismiss a release-level review while retaining every source file."""
    release = await db.get(Release, release_id)
    if release is None:
        raise HTTPException(status_code=404, detail="Release not found")
    release.review_dismissed_at = datetime.now(UTC)
    await db.commit()
    return RedirectResponse("/downloads?notice=review_dismissed", status_code=303)


@router.post("/release/{release_id}/reacquire", include_in_schema=False)
async def reacquire_missing_release_sources(
    release_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    """Queue bounded continuation jobs for tracks whose staged source disappeared."""
    release = await db.get(Release, release_id)
    if release is None:
        raise HTTPException(status_code=404, detail="Release not found")
    if not (release.error_detail or "").startswith("missing staged source:"):
        return RedirectResponse("/downloads?notice=invalid_state", status_code=303)

    parent = await db.get(Job, release.job_id)
    runtime = await get_runtime_settings(db)
    if parent is None or parent.partial_attempt >= runtime.max_partial_attempts:
        return RedirectResponse("/downloads?notice=invalid_state", status_code=303)

    missing_track_ids = {
        plan.track_id
        for plan in release.import_plans
        if plan.error_detail
        and (
            plan.error_detail == "source path is not a regular file"
            or "no staged source path" in plan.error_detail
        )
    }
    if not missing_track_ids:
        missing_track_ids = {track.id for track in release.tracks if not track.staging_path}

    continuation_ids: list[int] = []
    existing_continuation = False
    for track in release.tracks:
        if track.id not in missing_track_ids:
            continue
        continuation_query = " ".join(
            part for part in (track.artist, track.album, track.title) if part
        )
        duplicate_query = select(Job.id).where(
            Job.parent_job_id == parent.id,
            Job.status.in_([JobStatus.pending, JobStatus.running]),
        )
        if track.catalog_track_id is not None:
            duplicate_query = duplicate_query.where(
                Job.catalog_album_id == track.catalog_album_id,
                Job.catalog_track_id == track.catalog_track_id,
            )
        else:
            duplicate_query = duplicate_query.where(Job.query == continuation_query)
        history_query = select(func.max(Job.partial_attempt)).where(Job.parent_job_id == parent.id)
        if track.catalog_track_id is not None:
            history_query = history_query.where(
                Job.catalog_album_id == track.catalog_album_id,
                Job.catalog_track_id == track.catalog_track_id,
            )
        else:
            history_query = history_query.where(Job.query == continuation_query)
        previous_attempt = await db.scalar(history_query)
        next_attempt = max(parent.partial_attempt, previous_attempt or 0) + 1
        duplicate = await db.scalar(duplicate_query)
        if duplicate is None and next_attempt > runtime.max_partial_attempts:
            continue
        track.acquisition_state = AcquisitionState.failed
        track.import_state = ImportWorkflowState.discovered
        track.acoustid_verification_state = AcoustIDVerificationState.pending
        track.staging_path = None
        if duplicate is not None:
            existing_continuation = True
            continue
        continuation = Job(
            source="priority",
            query=continuation_query,
            status=JobStatus.pending,
            catalog_album_id=track.catalog_album_id,
            catalog_track_id=track.catalog_track_id,
            parent_job_id=parent.id,
            partial_attempt=next_attempt,
        )
        db.add(continuation)
        await db.flush()
        continuation_ids.append(continuation.id)

    if not continuation_ids and not existing_continuation:
        return RedirectResponse("/downloads?notice=invalid_state", status_code=303)
    await db.execute(delete(ImportPlan).where(ImportPlan.release_id == release_id))
    release.import_state = ImportWorkflowState.discovered
    release.error_detail = None
    release.rollback_detail = None
    release.review_dismissed_at = None
    await db.commit()
    for continuation_id in continuation_ids:
        await job_dispatcher.dispatch(continuation_id)
    return RedirectResponse("/downloads?notice=reacquired", status_code=303)
