from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import uuid4
from weakref import WeakValueDictionary

import httpx
from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, Response
from fastapi.responses import RedirectResponse, StreamingResponse
from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.auth import get_current_user, require_mutation
from app.config import Settings
from app.database import get_db, run_with_sqlite_lock_retry
from app.jobs.dispatcher import job_dispatcher
from app.media_formats import IMPORTABLE_AUDIO_SUFFIXES
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.staging_review import ReviewAutomationAttempt, StagingReviewItem
from app.models.track import Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
    ReviewDecision,
)
from app.services.audio_alignment import COMMON_PREVIEW_OFFSET_SECONDS, align_deezer_preview
from app.services.review_automation import _source_identity
from app.services.source_candidate_identity import normalize_source_candidate_identity
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
_PREVIEW_CACHE_LIMIT = 128
_PREVIEW_CACHE_MAX_BYTES = 512 * 1024 * 1024
_PREVIEW_MAX_OUTPUT_BYTES = 64 * 1024 * 1024
_PREVIEW_MAX_DURATION_SECONDS = 30 * 60
_PREVIEW_TRANSCODE_LIMIT = asyncio.Semaphore(2)
_PREVIEW_ITEM_LOCKS: WeakValueDictionary[int, asyncio.Lock] = WeakValueDictionary()


def _browser_preview_path(source: Path, item_id: int, cache_root: Path) -> Path:
    """Return a cached MP3 browser preview without changing the source."""
    source_stat = source.stat()
    cache_key = hashlib.sha256(
        f"{source.resolve()}:{source_stat.st_size}:{source_stat.st_mtime_ns}".encode()
    ).hexdigest()[:20]
    cache_root.mkdir(parents=True, exist_ok=True)
    destination = cache_root / f"{item_id}-{cache_key}.mp3"
    if destination.is_file() and destination.stat().st_size > 0:
        return destination

    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        raise RuntimeError("ffmpeg is not available")
    temporary = cache_root / f".{destination.name}.{uuid4().hex}.tmp.mp3"
    try:
        completed = subprocess.run(
            [
                ffmpeg,
                "-nostdin",
                "-v",
                "error",
                "-y",
                "-i",
                str(source),
                "-map",
                "0:a:0",
                "-vn",
                "-t",
                str(_PREVIEW_MAX_DURATION_SECONDS),
                "-ac",
                "2",
                "-c:a",
                "libmp3lame",
                "-q:a",
                "4",
                str(temporary),
            ],
            capture_output=True,
            check=False,
            timeout=180,
        )
        output_size = temporary.stat().st_size if temporary.is_file() else 0
        if (
            completed.returncode != 0
            or output_size == 0
            or output_size > _PREVIEW_MAX_OUTPUT_BYTES
        ):
            raise RuntimeError("ffmpeg could not create a browser preview")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)

    for stale in cache_root.glob(f"{item_id}-*.mp3"):
        if stale != destination:
            stale.unlink(missing_ok=True)
    cached = sorted(
        cache_root.glob("*.mp3"), key=lambda path: path.stat().st_mtime_ns, reverse=True
    )
    retained_bytes = 0
    for index, stale in enumerate(cached):
        size = stale.stat().st_size
        if index >= _PREVIEW_CACHE_LIMIT or retained_bytes + size > _PREVIEW_CACHE_MAX_BYTES:
            stale.unlink(missing_ok=True)
        else:
            retained_bytes += size
    return destination


def _validate_audio_path(
    staging_path: str, staging_root: Path, *, require_importable_audio: bool = True
) -> Path:
    """Validate and resolve a staged path.

    Raises HTTPException on traversal, symlink, non-audio extension when required,
    or missing files.
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
    if require_importable_audio and suffix not in IMPORTABLE_AUDIO_SUFFIXES:
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


def _needs_browser_preview(suffix: str, user_agent: str | None) -> bool:
    """Use stable MP3 review audio where the native format cannot seek reliably."""
    if suffix in {".m4a", ".mp4"}:
        return True
    if suffix != ".flac" or not user_agent:
        return False
    normalized = user_agent.casefold()
    return any(marker in normalized for marker in ("mobile", "android", "iphone", "ipad", "ipod"))


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

    resolved = await asyncio.to_thread(
        _validate_audio_path, track.staging_path, settings.staging_root
    )
    suffix = resolved.suffix.casefold()
    vary_user_agent = suffix == ".flac"
    if _needs_browser_preview(suffix, request.headers.get("user-agent")):
        try:
            item_lock = _PREVIEW_ITEM_LOCKS.setdefault(item_id, asyncio.Lock())
            async with item_lock, _PREVIEW_TRANSCODE_LIMIT:
                resolved = await asyncio.to_thread(
                    _browser_preview_path,
                    resolved,
                    item_id,
                    settings.artwork_cache_root.parent / "review-audio",
                )
        except (OSError, RuntimeError, subprocess.SubprocessError):
            raise HTTPException(
                status_code=422, detail="Browser-compatible audio preview could not be created"
            ) from None
        suffix = ".mp3"
    file_size = resolved.stat().st_size
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
        if vary_user_agent:
            headers["Vary"] = "User-Agent"
        return StreamingResponse(iterfile(), status_code=206, headers=headers, media_type=mime)
    else:
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
            "Content-Type": mime,
        }
        if vary_user_agent:
            headers["Vary"] = "User-Agent"
        return StreamingResponse(iterfile(), status_code=200, headers=headers, media_type=mime)


@router.get("/review/{item_id}/alignment", include_in_schema=False)
async def align_review_audio(
    item_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    reference_source: Annotated[Literal["deezer", "itunes"] | None, Query()] = None,
    reference_url: Annotated[str | None, Query(max_length=2048)] = None,
) -> dict[str, object]:
    """Align the exact reference already rendered in the browser to its staged file."""
    item = await db.scalar(
        select(StagingReviewItem)
        .where(
            StagingReviewItem.id == item_id,
            StagingReviewItem.review_state == ReviewDecision.pending,
        )
        .options(
            selectinload(StagingReviewItem.release),
            selectinload(StagingReviewItem.track),
        )
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Pending review item not found")
    track = item.track
    if not track.staging_path:
        raise HTTPException(status_code=404, detail="Staged track path not available")
    source_path = await asyncio.to_thread(
        _validate_audio_path, track.staging_path, settings.staging_root
    )
    if reference_source is None:
        return {
            "status": "unavailable",
            "downloaded_offset_sec": None,
            "confidence": None,
            "method": None,
            "message": "No reference preview is available.",
            "linked_playback": False,
        }

    deezer_reference_valid = reference_source == "deezer" and bool(reference_url)
    if deezer_reference_valid and reference_url is not None:
        try:
            async with asyncio.timeout(65):
                result = await align_deezer_preview(source_path, reference_url)
        except ValueError:
            deezer_reference_valid = False
            result = None
        except (OSError, RuntimeError, httpx.HTTPError):
            result = None
        if result is not None:
            return {
                "status": "matched",
                "downloaded_offset_sec": result.offset_seconds,
                "confidence": result.confidence,
                "method": "chromaprint",
                "message": "Reference located in the downloaded file.",
                "linked_playback": True,
            }

    linked_playback = bool(deezer_reference_valid)
    message = (
        "Exact alignment was unavailable; using the common 47.926s preview start."
        if linked_playback
        else "Using the common 47.926s preview start; the reference player remains independent."
    )
    return {
        "status": "defaulted",
        "downloaded_offset_sec": COMMON_PREVIEW_OFFSET_SECONDS,
        "confidence": "defaulted",
        "method": "common-preview-offset",
        "message": message,
        "linked_playback": linked_playback,
    }


def _review_result_location(return_to: str | None, notice: str) -> str:
    base = "/review" if return_to == "/review" else "/downloads"
    return f"{base}?notice={notice}"


@router.post("/review/{item_id}/approve", include_in_schema=False)
async def approve_review_item(
    item_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
    return_to: Annotated[str | None, Form()] = None,
) -> RedirectResponse:
    """Approve a staged track for import despite a verification flag."""
    await db.rollback()
    pending_item = await db.get(StagingReviewItem, item_id)
    if pending_item is None:
        raise HTTPException(status_code=404, detail="Review item not found")
    pending_track = await db.get(Track, pending_item.track_id)
    manual_source_value = str(
        (pending_track.staging_path or pending_track.source_path or "")
        if pending_track is not None
        else ""
    ).strip()
    manual_source_path = Path(manual_source_value) if manual_source_value else None
    manual_source_identity = (
        await asyncio.to_thread(
            _source_identity,
            manual_source_path,
            Path(settings.staging_root),
        )
        if manual_source_path is not None
        else None
    )
    if manual_source_path is None or manual_source_identity is None:
        raise HTTPException(status_code=409, detail="Approved source is missing or changed")
    approved = False

    async def persist_approval() -> None:
        nonlocal approved
        await db.rollback()
        await db.execute(text("BEGIN IMMEDIATE"))
        current_item = await db.get(StagingReviewItem, item_id)
        if current_item is None:
            raise HTTPException(status_code=404, detail="Review item not found")
        if current_item.review_state != ReviewDecision.pending:
            await db.rollback()
            return

        track = await db.get(Track, current_item.track_id)
        current_source = str(
            (track.staging_path or track.source_path or "") if track is not None else ""
        ).strip()
        if current_source != str(manual_source_path):
            await db.rollback()
            raise HTTPException(status_code=409, detail="Approved source path changed")
        now = datetime.now(UTC)
        if current_item.automation_claim_token:
            attempt = await db.scalar(
                select(ReviewAutomationAttempt).where(
                    ReviewAutomationAttempt.claim_token == current_item.automation_claim_token
                )
            )
            if attempt is not None and attempt.state == "claimed":
                attempt.state = "abandoned"
                attempt.completed_at = now
                attempt.decision_json = json.dumps(
                    {"decision": "manual_approval", "reason": "manual_decision_precedence"},
                    sort_keys=True,
                )
        current_item.review_state = ReviewDecision.approved
        current_item.reviewed_at = now
        current_item.automation_state = "manual_approved"
        current_item.automation_claim_token = None
        current_item.automation_claimed_at = None
        current_item.automation_next_attempt_at = None
        current_item.automation_decision_json = json.dumps(
            {
                "decision": "manual_approval",
                "reason": "operator_approved",
                "source_path": str(manual_source_path),
                "source_sha256": manual_source_identity[0],
                "source_size": manual_source_identity[1],
                "source_mtime_ns": manual_source_identity[2],
            },
            sort_keys=True,
        )
        current_item.import_dispatch_state = "pending"
        current_item.import_dispatch_claim_token = None
        current_item.import_dispatch_claimed_at = None
        current_item.import_dispatch_next_attempt_at = now
        if track is not None:
            track.acoustid_verification_state = AcoustIDVerificationState.approved
        await db.commit()
        approved = True

    await run_with_sqlite_lock_retry(db, persist_approval, attempts=6, delay_seconds=0.2)
    if not approved:
        return RedirectResponse(
            _review_result_location(return_to, "already_reviewed"), status_code=303
        )

    return RedirectResponse(_review_result_location(return_to, "approved"), status_code=303)


async def _block_denied_slskd_candidate(db: AsyncSession, track: Track) -> bool:
    """Permanently block the exact slskd artifact, if provenance identifies it."""
    if track.source != "slskd" or not track.acquisition_provenance_json:
        return False
    try:
        provenance = json.loads(track.acquisition_provenance_json)
    except (json.JSONDecodeError, TypeError):
        return False
    if not isinstance(provenance, dict):
        return False
    identity = normalize_source_candidate_identity(
        provenance.get("source"), provenance.get("username"), provenance.get("filename")
    )
    if identity is None:
        return False
    provider, peer, filename = identity
    existing_block = await db.scalar(
        select(SourceCandidateBlock).where(
            SourceCandidateBlock.provider == provider,
            SourceCandidateBlock.peer == peer,
            SourceCandidateBlock.filename == filename,
        )
    )
    if existing_block is None:
        db.add(
            SourceCandidateBlock(
                provider=provider,
                peer=peer,
                filename=filename,
                reason="denied",
            )
        )
    else:
        existing_block.reason = "denied"
        existing_block.blocked_until = None
    return True


@router.post("/review/{item_id}/deny", include_in_schema=False)
async def deny_review_item(
    item_id: int,
    request: Request,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(effective_settings_dep)],
    _user: Annotated[object, Depends(require_mutation)],
    return_to: Annotated[str | None, Form()] = None,
) -> RedirectResponse:
    """Deny and remove a staged track, then schedule bounded reacquisition."""
    await db.rollback()
    await db.execute(text("BEGIN IMMEDIATE"))
    item = await db.get(StagingReviewItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Review item not found")
    if item.review_state != ReviewDecision.pending:
        await db.rollback()
        return RedirectResponse(
            _review_result_location(return_to, "already_reviewed"), status_code=303
        )

    if item.automation_claim_token:
        attempt = await db.scalar(
            select(ReviewAutomationAttempt).where(
                ReviewAutomationAttempt.claim_token == item.automation_claim_token
            )
        )
        if attempt is not None and attempt.state == "claimed":
            attempt.state = "abandoned"
            attempt.completed_at = datetime.now(UTC)
            attempt.decision_json = json.dumps(
                {"decision": "manual_denial", "reason": "manual_decision_precedence"},
                sort_keys=True,
            )

    track = await db.get(Track, item.track_id)
    source_blocked = False
    continuation_id: int | None = None
    if track is not None:
        staged_path = track.staging_path
        resolved_staged_path: Path | None = None
        if staged_path:
            try:
                resolved_staged_path = await asyncio.to_thread(
                    _validate_audio_path,
                    staged_path,
                    settings.staging_root,
                    require_importable_audio=False,
                )
            except HTTPException:
                # Denying a review is the user's escape hatch for stale or unsafe
                # staged paths. Refuse to serve/delete the file, but still clear
                # the review row and reset the track state below.
                resolved_staged_path = None
            track.staging_path = None
            if track.source_path == staged_path:
                track.source_path = None
        source_blocked = await _block_denied_slskd_candidate(db, track)
        track.acoustid_verification_state = AcoustIDVerificationState.denied
        track.acquisition_state = AcquisitionState.failed
        release = await db.get(Release, item.release_id)
        if release is not None:
            track_label = track.track_no or track.id or "unknown"
            if not release.error_detail or release.error_detail.startswith(
                "AcoustID mismatch on track "
            ):
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

    await db.delete(item)
    quarantine_path: Path | None = None
    if track is not None and resolved_staged_path is not None:
        quarantine_path = resolved_staged_path.with_name(
            f".{resolved_staged_path.name}.denied-{uuid4().hex}"
        )

    def restore_quarantine() -> None:
        if (
            quarantine_path is not None
            and resolved_staged_path is not None
            and quarantine_path.exists()
        ):
            quarantine_path.replace(resolved_staged_path)

    def delete_quarantine() -> None:
        if quarantine_path is not None:
            quarantine_path.unlink(missing_ok=True)

    async def rollback_and_restore() -> None:
        try:
            await db.rollback()
        finally:
            restore_quarantine()  # noqa: ASYNC240

    if quarantine_path is not None and resolved_staged_path is not None:
        resolved_staged_path.replace(quarantine_path)  # noqa: ASYNC240
    commit_task = asyncio.create_task(db.commit())

    def clear_delivered_cancellation() -> None:
        current_task = asyncio.current_task()
        if current_task is not None:
            for _ in range(current_task.cancelling()):
                current_task.uncancel()

    async def settle_commit() -> None:
        while not commit_task.done():
            try:
                await asyncio.shield(commit_task)
            except asyncio.CancelledError:
                clear_delivered_cancellation()
        commit_task.result()

    try:
        await asyncio.shield(commit_task)
    except asyncio.CancelledError:
        clear_delivered_cancellation()
        try:
            await settle_commit()
        except BaseException:
            await rollback_and_restore()
        else:
            delete_quarantine()  # noqa: ASYNC240
        raise
    except BaseException:
        await rollback_and_restore()
        raise
    delete_quarantine()  # noqa: ASYNC240
    if continuation_id is not None:
        await job_dispatcher.dispatch(continuation_id)
    notice = "source_blocked" if source_blocked else "source_identity_unavailable"
    return RedirectResponse(_review_result_location(return_to, notice), status_code=303)


@router.post("/release/{release_id}/dismiss", include_in_schema=False)
async def dismiss_release_review(
    release_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    _user: Annotated[object, Depends(require_mutation)],
) -> RedirectResponse:
    async def dismiss_release() -> None:
        current = await db.get(Release, release_id)
        if current is None:
            raise HTTPException(status_code=404, detail="Release not found")
        current.review_dismissed_at = datetime.now(UTC).replace(tzinfo=None)
        await db.commit()

    await run_with_sqlite_lock_retry(db, dismiss_release, attempts=6, delay_seconds=0.2)
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
