from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import stat
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from sqlalchemy import and_, case, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload
from sqlalchemy.sql.elements import ColumnElement

from app.config import get_settings
from app.database import get_session_factory
from app.models.acquisition_attempt import (
    AcquisitionAttempt,
    ArtifactState,
    AttemptOutcome,
    CleanupState,
    ProviderTransferState,
    RetentionDisposition,
)
from app.models.catalog_entities import CatalogAlbumTrack
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.settings_service import build_effective_settings
from app.sources.slskd import SlskdAdapter

logger = logging.getLogger(__name__)

_SOURCE_CLEANUP_COMPLETED_AT = "source_cleanup_completed_at"
_TERMINAL_CLEANUP_LOCK = asyncio.Lock()
_IMPORTED_SOURCE_CLEANUP_TASKS: set[asyncio.Task[None]] = set()


def _provenance(provenance_json: str | None) -> dict[str, object]:
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        value = json.loads(provenance_json or "{}")
        if isinstance(value, dict):
            return value
    return {}


def _source_cleanup_completed(provenance_json: str | None) -> bool:
    return bool(_provenance(provenance_json).get(_SOURCE_CLEANUP_COMPLETED_AT))


@dataclass(frozen=True)
class ImportedSourceCleanup:
    plan_id: int | None
    staged_path: Path
    provenance_json: str | None
    source_job_id: str | None = None
    track_id: int | None = None
    expected_device: int | None = None
    expected_inode: int | None = None
    expected_mtime_ns: int | None = None
    expected_size: int | None = None
    expected_digest: str | None = None
    session_factory: async_sessionmaker[AsyncSession] | None = field(
        default=None, compare=False, repr=False
    )

    def __post_init__(self) -> None:
        if all(
            value is not None
            for value in (
                self.expected_device,
                self.expected_inode,
                self.expected_mtime_ns,
                self.expected_size,
            )
        ):
            return
        try:
            stat_result = self.staged_path.stat()
        except OSError:
            return
        object.__setattr__(self, "expected_device", stat_result.st_dev)
        object.__setattr__(self, "expected_inode", stat_result.st_ino)
        object.__setattr__(self, "expected_mtime_ns", stat_result.st_mtime_ns)
        object.__setattr__(self, "expected_size", stat_result.st_size)


class SlskdCleanupAdapter(Protocol):
    async def downloads(self, *, force_refresh: bool = False) -> list[dict[str, object]]: ...

    async def remove_exact(self, username: str, provider_uuid: str) -> None: ...


class AttemptCleanupResult(StrEnum):
    removed = "removed"
    already_absent = "already_absent"
    blocked = "blocked"
    retryable_failure = "retryable_failure"
    claimed_elsewhere = "claimed_elsewhere"
    not_eligible = "not_eligible"


@dataclass(frozen=True)
class _CleanupClaim:
    attempt_id: int
    token: str
    version: int
    peer: str | None
    remote_path: str | None
    provider_uuid: str | None
    staged_path: str | None
    quarantine_path: str | None
    device: int | None
    inode: int | None
    mtime_ns: int | None
    size: int | None
    sha256: str | None
    attempt_count: int


def _slskd_identity(provenance_json: str | None) -> tuple[str, str] | None:
    with contextlib.suppress(json.JSONDecodeError, TypeError):
        provenance = json.loads(provenance_json or "{}")
        if (
            provenance.get("source") == "slskd"
            and provenance.get("username")
            and provenance.get("filename")
        ):
            return str(provenance["username"]), str(provenance["filename"])
    return None


async def _hide_completed_and_timed_out_jobs_once(
    session_factory: async_sessionmaker[AsyncSession],
    job_ids: set[int] | None = None,
) -> list[int]:
    """Hide durable successful/timeout attempts without deleting acquisition history."""
    terminal = {JobStatus.done, JobStatus.failed, JobStatus.partial, JobStatus.cancelled}
    async with session_factory() as db:
        query = (
            select(Job)
            .where(Job.queue_hidden.is_(False), Job.status.in_(terminal))
            .options(selectinload(Job.tracks))
            .order_by(Job.id)
        )
        if job_ids is not None:
            query = query.where(Job.id.in_(job_ids))
        jobs = list((await db.scalars(query)).all())
        hidden: set[int] = set()
        for job in jobs:
            timed_out = any(track.source_status == "transfer_timeout" for track in job.tracks)
            has_other_incomplete = any(
                track.acquisition_state != AcquisitionState.downloaded
                and track.source_status != "transfer_timeout"
                for track in job.tracks
            )
            completed_standalone = job.status == JobStatus.done and job.catalog_album_id is None
            timed_out_standalone = (
                job.catalog_album_id is None and timed_out and not has_other_incomplete
            )
            if completed_standalone or timed_out_standalone:
                job.queue_hidden = True
                hidden.add(job.id)

        album_ids = {job.catalog_album_id for job in jobs if job.catalog_album_id is not None}
        if album_ids:
            wanted_rows = await db.execute(
                select(CatalogAlbumTrack.album_id, CatalogAlbumTrack.id).where(
                    CatalogAlbumTrack.album_id.in_(album_ids)
                )
            )
            imported_rows = await db.execute(
                select(Track.catalog_album_id, Track.catalog_track_id).where(
                    Track.catalog_album_id.in_(album_ids),
                    Track.catalog_track_id.is_not(None),
                    Track.import_state == ImportWorkflowState.imported,
                )
            )
            wanted: dict[int, set[int]] = {}
            imported: dict[int, set[int]] = {}
            for album_id, track_id in wanted_rows:
                wanted.setdefault(album_id, set()).add(track_id)
            for album_id, track_id in imported_rows:
                if album_id is not None and track_id is not None:
                    imported.setdefault(album_id, set()).add(track_id)
            completed_album_ids = {
                album_id
                for album_id, track_ids in wanted.items()
                if track_ids and track_ids.issubset(imported.get(album_id, set()))
            }
            if completed_album_ids:
                completed_jobs = list(
                    (
                        await db.scalars(
                            select(Job).where(
                                Job.catalog_album_id.in_(completed_album_ids),
                                Job.queue_hidden.is_(False),
                                Job.status.in_(terminal),
                            )
                        )
                    ).all()
                )
                for job in completed_jobs:
                    job.queue_hidden = True
                    hidden.add(job.id)
        await db.commit()
        return sorted(hidden)


async def hide_completed_and_timed_out_jobs(
    session_factory: async_sessionmaker[AsyncSession],
    job_ids: set[int] | None = None,
    *,
    max_attempts: int = 3,
) -> list[int]:
    """Hide terminal jobs in a fresh rollback-safe transaction on each attempt."""
    for attempt in range(1, max_attempts + 1):
        try:
            return await _hide_completed_and_timed_out_jobs_once(session_factory, job_ids)
        except Exception as exc:
            if attempt == max_attempts or not _transient_cleanup_error(exc):
                raise
            await _cleanup_retry_delay(attempt)
    raise RuntimeError("cleanup retry loop exited unexpectedly")


def _normalized_remote_path(value: object) -> str:
    return str(value or "").replace("\\", "/")


def _snapshot_provider_uuid(item: dict[str, object]) -> str | None:
    from app.services.acquisition_attempts import canonical_provider_uuid

    return canonical_provider_uuid(item.get("id") or item.get("transferId"))


async def _block_cleanup(
    session_factory: async_sessionmaker[AsyncSession],
    attempt_id: int,
    *,
    provider: bool,
    code: str,
) -> None:
    state_column = (
        AcquisitionAttempt.provider_cleanup_state
        if provider
        else AcquisitionAttempt.file_cleanup_state
    )
    async with session_factory() as db:
        await db.execute(
            update(AcquisitionAttempt)
            .where(
                AcquisitionAttempt.id == attempt_id,
                state_column != CleanupState.completed,
            )
            .values(
                {
                    state_column.key: CleanupState.blocked,
                    "error_code": code,
                    "error_detail": "cleanup requires manual review",
                    "cleanup_claim_token": None,
                    "cleanup_claimed_at": None,
                    "cleanup_lease_expires_at": None,
                }
            )
        )
        await db.commit()


async def _claim_provider_cleanup(
    session_factory: async_sessionmaker[AsyncSession],
    attempt_id: int,
    *,
    lease_seconds: int,
) -> tuple[_CleanupClaim | None, AttemptCleanupResult | None]:
    now = datetime.now(UTC)
    async with session_factory() as db:
        attempt = await db.get(AcquisitionAttempt, attempt_id)
        if attempt is None or attempt.provider != "slskd":
            return None, AttemptCleanupResult.not_eligible
        if attempt.provider_cleanup_state == CleanupState.completed:
            return None, AttemptCleanupResult.already_absent
        if attempt.provider_uuid is None:
            await db.rollback()
            await _block_cleanup(
                session_factory,
                attempt_id,
                provider=True,
                code="cleanup_missing_provider_uuid",
            )
            return None, AttemptCleanupResult.blocked
        terminal_states = {
            ProviderTransferState.completed,
            ProviderTransferState.failed,
            ProviderTransferState.cancelled,
        }
        if attempt.provider_state not in terminal_states:
            return None, AttemptCleanupResult.not_eligible
        if (
            attempt.provider_cleanup_retry_at is not None
            and attempt.provider_cleanup_retry_at.replace(tzinfo=UTC) > now
        ):
            return None, AttemptCleanupResult.not_eligible
        if (
            attempt.provider_cleanup_state == CleanupState.claimed
            and attempt.cleanup_lease_expires_at is not None
            and attempt.cleanup_lease_expires_at.replace(tzinfo=UTC) > now
        ):
            return None, AttemptCleanupResult.claimed_elsewhere
        old_version = attempt.cleanup_claim_version
        token = str(uuid4())
        version = old_version + 1
        eligible_state = or_(
            AcquisitionAttempt.provider_cleanup_state.in_(
                {CleanupState.pending, CleanupState.failed, CleanupState.blocked}
            ),
            and_(
                AcquisitionAttempt.provider_cleanup_state == CleanupState.claimed,
                AcquisitionAttempt.cleanup_lease_expires_at <= now,
            ),
        )
        result = await db.execute(
            update(AcquisitionAttempt)
            .execution_options(synchronize_session=False)
            .where(
                AcquisitionAttempt.id == attempt_id,
                AcquisitionAttempt.cleanup_claim_version == old_version,
                eligible_state,
            )
            .values(
                provider_cleanup_state=CleanupState.claimed,
                provider_cleanup_attempt_count=attempt.provider_cleanup_attempt_count + 1,
                provider_cleanup_last_attempted_at=now,
                provider_cleanup_retry_at=None,
                cleanup_claim_token=token,
                cleanup_claim_version=version,
                cleanup_claimed_at=now,
                cleanup_lease_expires_at=now + timedelta(seconds=lease_seconds),
                error_code=None,
                error_detail=None,
            )
        )
        if not isinstance(result, CursorResult) or result.rowcount != 1:
            await db.rollback()
            return None, AttemptCleanupResult.claimed_elsewhere
        await db.commit()
        return (
            _CleanupClaim(
                attempt_id=attempt.id,
                token=token,
                version=version,
                peer=attempt.peer,
                remote_path=attempt.remote_path,
                provider_uuid=attempt.provider_uuid,
                staged_path=attempt.staged_path,
                quarantine_path=attempt.quarantine_path,
                device=attempt.artifact_device,
                inode=attempt.artifact_inode,
                mtime_ns=attempt.artifact_mtime_ns,
                size=attempt.artifact_size,
                sha256=attempt.artifact_sha256,
                attempt_count=attempt.provider_cleanup_attempt_count + 1,
            ),
            None,
        )


async def _finish_provider_claim(
    session_factory: async_sessionmaker[AsyncSession],
    claim: _CleanupClaim,
    *,
    state: CleanupState,
    error_code: str | None = None,
) -> bool:
    now = datetime.now(UTC)
    values: dict[str, object] = {
        "provider_cleanup_state": state,
        "cleanup_claim_token": None,
        "cleanup_claimed_at": None,
        "cleanup_lease_expires_at": None,
        "error_code": error_code,
        "error_detail": "provider cleanup will be retried"
        if state == CleanupState.failed
        else None,
    }
    if state == CleanupState.completed:
        values["provider_cleanup_completed_at"] = now
        values["provider_cleanup_retry_at"] = None
    elif state == CleanupState.failed:
        values["provider_cleanup_retry_at"] = now + timedelta(
            seconds=min(3600, 30 * (2 ** max(0, claim.attempt_count - 1)))
        )
    async with session_factory() as db:
        result = await db.execute(
            update(AcquisitionAttempt)
            .execution_options(synchronize_session=False)
            .where(
                AcquisitionAttempt.id == claim.attempt_id,
                AcquisitionAttempt.provider_cleanup_state == CleanupState.claimed,
                AcquisitionAttempt.cleanup_claim_token == claim.token,
                AcquisitionAttempt.cleanup_claim_version == claim.version,
            )
            .values(**values)
        )
        await db.commit()
        return isinstance(result, CursorResult) and result.rowcount == 1


async def cleanup_attempt_provider(
    session_factory: async_sessionmaker[AsyncSession],
    adapter: SlskdCleanupAdapter,
    attempt_id: int,
    *,
    lease_seconds: int = 300,
) -> AttemptCleanupResult:
    """Claim, exactly remove, freshly verify, and CAS-finalize one slskd attempt."""
    claim, immediate = await _claim_provider_cleanup(
        session_factory, attempt_id, lease_seconds=lease_seconds
    )
    if claim is None:
        assert immediate is not None
        return immediate
    assert claim.provider_uuid is not None
    try:
        before = await adapter.downloads(force_refresh=True)
        exact = [item for item in before if _snapshot_provider_uuid(item) == claim.provider_uuid]
        if exact:
            if len(exact) != 1 or any(
                str(item.get("username") or "") != (claim.peer or "")
                or _normalized_remote_path(item.get("filename"))
                != _normalized_remote_path(claim.remote_path)
                for item in exact
            ):
                await _finish_provider_claim(
                    session_factory,
                    claim,
                    state=CleanupState.blocked,
                    error_code="provider_cleanup_identity_mismatch",
                )
                return AttemptCleanupResult.blocked
            await adapter.remove_exact(claim.peer or "", claim.provider_uuid)
            after = await adapter.downloads(force_refresh=True)
            if any(_snapshot_provider_uuid(item) == claim.provider_uuid for item in after):
                await _finish_provider_claim(
                    session_factory,
                    claim,
                    state=CleanupState.failed,
                    error_code="provider_cleanup_still_present",
                )
                return AttemptCleanupResult.retryable_failure
            finalized = await _finish_provider_claim(
                session_factory, claim, state=CleanupState.completed
            )
            return (
                AttemptCleanupResult.removed
                if finalized
                else AttemptCleanupResult.claimed_elsewhere
            )
        finalized = await _finish_provider_claim(
            session_factory, claim, state=CleanupState.completed
        )
        return (
            AttemptCleanupResult.already_absent
            if finalized
            else AttemptCleanupResult.claimed_elsewhere
        )
    except asyncio.CancelledError:
        await _finish_provider_claim(
            session_factory,
            claim,
            state=CleanupState.failed,
            error_code="provider_cleanup_cancelled",
        )
        raise
    except Exception:
        logger.warning("exact slskd attempt cleanup failed", exc_info=True)
        await _finish_provider_claim(
            session_factory,
            claim,
            state=CleanupState.failed,
            error_code="provider_cleanup_failed",
        )
        return AttemptCleanupResult.retryable_failure


def _file_quarantine_path(path: Path, attempt_id: int) -> Path:
    return path.with_name(f".{path.name}.audiohoard-attempt-{attempt_id}")


async def _claim_file_cleanup(
    session_factory: async_sessionmaker[AsyncSession],
    attempt_id: int,
    *,
    lease_seconds: int,
) -> tuple[_CleanupClaim | None, AttemptCleanupResult | None]:
    now = datetime.now(UTC)
    async with session_factory() as db:
        attempt = await db.get(AcquisitionAttempt, attempt_id)
        if attempt is None:
            return None, AttemptCleanupResult.not_eligible
        if attempt.file_cleanup_state == CleanupState.completed:
            return None, AttemptCleanupResult.already_absent
        eligible = (
            attempt.file_cleanup_eligible
            and attempt.retention_disposition == RetentionDisposition.cleanup_eligible
            and attempt.provider_cleanup_state == CleanupState.completed
            and attempt.staged_path is not None
            and all(
                value is not None
                for value in (
                    attempt.artifact_device,
                    attempt.artifact_inode,
                    attempt.artifact_mtime_ns,
                    attempt.artifact_size,
                    attempt.artifact_sha256,
                )
            )
        )
        if not eligible:
            await db.rollback()
            await _block_cleanup(
                session_factory,
                attempt_id,
                provider=False,
                code="file_cleanup_not_eligible",
            )
            return None, AttemptCleanupResult.blocked
        if (
            attempt.file_cleanup_retry_at is not None
            and attempt.file_cleanup_retry_at.replace(tzinfo=UTC) > now
        ):
            return None, AttemptCleanupResult.not_eligible
        if (
            attempt.file_cleanup_state == CleanupState.claimed
            and attempt.cleanup_lease_expires_at is not None
            and attempt.cleanup_lease_expires_at.replace(tzinfo=UTC) > now
        ):
            return None, AttemptCleanupResult.claimed_elsewhere
        old_version = attempt.cleanup_claim_version
        token = str(uuid4())
        version = old_version + 1
        staged_path = attempt.staged_path
        assert staged_path is not None
        quarantine = attempt.quarantine_path or str(
            _file_quarantine_path(Path(staged_path), attempt.id)
        )
        state_ok = or_(
            AcquisitionAttempt.file_cleanup_state.in_(
                {CleanupState.pending, CleanupState.failed, CleanupState.blocked}
            ),
            and_(
                AcquisitionAttempt.file_cleanup_state == CleanupState.claimed,
                AcquisitionAttempt.cleanup_lease_expires_at <= now,
            ),
        )
        result = await db.execute(
            update(AcquisitionAttempt)
            .execution_options(synchronize_session=False)
            .where(
                AcquisitionAttempt.id == attempt_id,
                AcquisitionAttempt.cleanup_claim_version == old_version,
                state_ok,
            )
            .values(
                file_cleanup_state=CleanupState.claimed,
                file_cleanup_attempt_count=attempt.file_cleanup_attempt_count + 1,
                file_cleanup_last_attempted_at=now,
                file_cleanup_retry_at=None,
                quarantine_path=quarantine,
                cleanup_claim_token=token,
                cleanup_claim_version=version,
                cleanup_claimed_at=now,
                cleanup_lease_expires_at=now + timedelta(seconds=lease_seconds),
                error_code=None,
                error_detail=None,
            )
        )
        if not isinstance(result, CursorResult) or result.rowcount != 1:
            await db.rollback()
            return None, AttemptCleanupResult.claimed_elsewhere
        await db.commit()
        return _CleanupClaim(
            attempt.id,
            token,
            version,
            attempt.peer,
            attempt.remote_path,
            attempt.provider_uuid,
            attempt.staged_path,
            quarantine,
            attempt.artifact_device,
            attempt.artifact_inode,
            attempt.artifact_mtime_ns,
            attempt.artifact_size,
            attempt.artifact_sha256,
            attempt.file_cleanup_attempt_count + 1,
        ), None


def _path_is_contained_regular(path: Path, root: Path) -> bool:
    try:
        root_resolved = root.resolve(strict=True)
        if path.is_symlink():
            return False
        resolved = path.resolve(strict=True)
        return resolved.is_relative_to(root_resolved) and stat.S_ISREG(
            path.stat(follow_symlinks=False).st_mode
        )
    except OSError:
        return False


def _identity_without_hash(path: Path) -> tuple[int, int, int, int] | None:
    try:
        current = path.stat(follow_symlinks=False)
    except OSError:
        return None
    if not stat.S_ISREG(current.st_mode):
        return None
    return current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size


def _hash_open_regular(path: Path) -> tuple[tuple[int, int, int, int], str] | None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    try:
        current = os.fstat(fd)
        if not stat.S_ISREG(current.st_mode):
            return None
        digest = hashlib.sha256()
        while chunk := os.read(fd, 1024 * 1024):
            digest.update(chunk)
        return (
            (current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size),
            digest.hexdigest(),
        )
    finally:
        os.close(fd)


def _remove_claimed_file(claim: _CleanupClaim, root: Path) -> AttemptCleanupResult:
    assert claim.staged_path is not None and claim.quarantine_path is not None
    source = Path(claim.staged_path)
    quarantine = Path(claim.quarantine_path)
    expected = (claim.device, claim.inode, claim.mtime_ns, claim.size)
    if quarantine.exists() or quarantine.is_symlink():
        target = quarantine
    elif source.exists() or source.is_symlink():
        if not _path_is_contained_regular(source, root):
            return AttemptCleanupResult.blocked
        if _identity_without_hash(source) != expected:
            return AttemptCleanupResult.blocked
        try:
            os.replace(source, quarantine)
        except OSError:
            return AttemptCleanupResult.retryable_failure
        target = quarantine
    else:
        return AttemptCleanupResult.already_absent
    if not _path_is_contained_regular(target, root):
        return AttemptCleanupResult.blocked
    current = _hash_open_regular(target)
    if current is None or current != (expected, claim.sha256):
        if not source.exists() and not source.is_symlink():
            with contextlib.suppress(OSError):
                os.replace(target, source)
        return AttemptCleanupResult.blocked
    if _identity_without_hash(target) != expected:
        return AttemptCleanupResult.blocked
    try:
        target.unlink()
    except OSError:
        return AttemptCleanupResult.retryable_failure
    return AttemptCleanupResult.removed


async def _finish_file_claim(
    session_factory: async_sessionmaker[AsyncSession],
    claim: _CleanupClaim,
    result: AttemptCleanupResult,
) -> bool:
    now = datetime.now(UTC)
    if result in {AttemptCleanupResult.removed, AttemptCleanupResult.already_absent}:
        state = CleanupState.completed
    elif result == AttemptCleanupResult.blocked:
        state = CleanupState.blocked
    else:
        state = CleanupState.failed
    values: dict[str, object] = {
        "file_cleanup_state": state,
        "cleanup_claim_token": None,
        "cleanup_claimed_at": None,
        "cleanup_lease_expires_at": None,
        "error_code": None
        if state == CleanupState.completed
        else "file_cleanup_identity_mismatch"
        if state == CleanupState.blocked
        else "file_cleanup_failed",
        "error_detail": None
        if state == CleanupState.completed
        else "file retained; cleanup requires review"
        if state == CleanupState.blocked
        else "file cleanup will be retried",
    }
    if state == CleanupState.completed:
        values.update(
            file_cleanup_completed_at=now,
            file_cleanup_retry_at=None,
            retention_disposition=RetentionDisposition.removed,
            artifact_state=ArtifactState.missing,
        )
    elif state == CleanupState.failed:
        values["file_cleanup_retry_at"] = now + timedelta(
            seconds=min(3600, 30 * (2 ** max(0, claim.attempt_count - 1)))
        )
    async with session_factory() as db:
        updated = await db.execute(
            update(AcquisitionAttempt)
            .execution_options(synchronize_session=False)
            .where(
                AcquisitionAttempt.id == claim.attempt_id,
                AcquisitionAttempt.file_cleanup_state == CleanupState.claimed,
                AcquisitionAttempt.cleanup_claim_token == claim.token,
                AcquisitionAttempt.cleanup_claim_version == claim.version,
            )
            .values(**values)
        )
        await db.commit()
        return isinstance(updated, CursorResult) and updated.rowcount == 1


async def cleanup_attempt_file(
    session_factory: async_sessionmaker[AsyncSession],
    attempt_id: int,
    root: Path,
    *,
    lease_seconds: int = 300,
) -> AttemptCleanupResult:
    """Remove only an explicitly eligible artifact bound to exact persisted content."""
    claim, immediate = await _claim_file_cleanup(
        session_factory, attempt_id, lease_seconds=lease_seconds
    )
    if claim is None:
        assert immediate is not None
        return immediate
    try:
        result = await asyncio.to_thread(_remove_claimed_file, claim, root)
    except asyncio.CancelledError:
        await _finish_file_claim(session_factory, claim, AttemptCleanupResult.retryable_failure)
        raise
    finalized = await _finish_file_claim(session_factory, claim, result)
    return result if finalized else AttemptCleanupResult.claimed_elsewhere


async def cleanup_durable_slskd_transfers(
    session_factory: async_sessionmaker[AsyncSession],
    adapter: SlskdCleanupAdapter,
    job_ids: set[int] | None = None,
    *,
    max_attempts: int = 3,
) -> int:
    """Consume exact attempt-backed obligations; legacy Track rows remain report-only."""
    del max_attempts  # Claim retries/backoff are persisted per attempt.
    async with session_factory() as db:
        query = select(AcquisitionAttempt.id).where(
            AcquisitionAttempt.provider == "slskd",
            AcquisitionAttempt.provider_state.in_(
                {
                    ProviderTransferState.completed,
                    ProviderTransferState.failed,
                    ProviderTransferState.cancelled,
                }
            ),
            AcquisitionAttempt.provider_cleanup_state != CleanupState.completed,
        )
        if job_ids is not None:
            query = query.where(AcquisitionAttempt.job_id.in_(job_ids))
        attempt_ids = list((await db.scalars(query.order_by(AcquisitionAttempt.id))).all())

    completed = 0
    for attempt_id in attempt_ids:
        result = await cleanup_attempt_provider(session_factory, adapter, attempt_id)
        if result in {AttemptCleanupResult.removed, AttemptCleanupResult.already_absent}:
            completed += 1
    return completed


def _transient_cleanup_error(exc: BaseException) -> bool:
    if isinstance(exc, SQLAlchemyTimeoutError):
        return True
    return isinstance(exc, OperationalError) and any(
        marker in str(exc).casefold() for marker in ("locked", "busy")
    )


async def _cleanup_retry_delay(attempt: int) -> None:
    delay = 0.25 * (2 ** (attempt - 1))
    logger.warning("Cleanup database contention; retrying in %.2fs", delay)
    await asyncio.sleep(delay)


async def _mark_durable_source_cleanups(
    session_factory: async_sessionmaker[AsyncSession],
    cleaned_track_identities: dict[int, tuple[tuple[str, str], str]],
    *,
    max_attempts: int,
) -> None:
    for attempt in range(1, max_attempts + 1):
        async with session_factory() as db:
            try:
                cleaned_tracks = list(
                    (
                        await db.scalars(
                            select(Track).where(Track.id.in_(cleaned_track_identities))
                        )
                    ).all()
                )
                completed_at = datetime.now(UTC).isoformat()
                for track in cleaned_tracks:
                    expected_identity, expected_source_job_id = cleaned_track_identities[track.id]
                    if (
                        track.source_job_id != expected_source_job_id
                        or _slskd_identity(track.acquisition_provenance_json) != expected_identity
                    ):
                        continue
                    provenance = _provenance(track.acquisition_provenance_json)
                    provenance[_SOURCE_CLEANUP_COMPLETED_AT] = completed_at
                    track.acquisition_provenance_json = json.dumps(provenance, sort_keys=True)
                await db.commit()
                return
            except Exception as exc:
                await db.rollback()
                if attempt == max_attempts or not _transient_cleanup_error(exc):
                    raise
        await _cleanup_retry_delay(attempt)


async def cleanup_terminal_acquisitions(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    slskd_url: str,
    slskd_api_key: str,
    job_ids: set[int] | None = None,
    max_attempts: int = 3,
) -> tuple[list[int], int]:
    """Serialize cleanup while retrying only short database transitions."""
    async with _TERMINAL_CLEANUP_LOCK:
        hidden = await hide_completed_and_timed_out_jobs(
            session_factory, job_ids, max_attempts=max_attempts
        )
        removed = await cleanup_durable_slskd_transfers(
            session_factory,
            SlskdAdapter(slskd_url, slskd_api_key),
            job_ids,
            max_attempts=max_attempts,
        )
        return hidden, removed


@dataclass(frozen=True)
class OrphanPruneResult:
    tracks: int = 0
    releases: int = 0
    jobs: int = 0


def _prune_empty_parents(path: Path, staging_root: Path) -> None:
    """Remove empty ancestors up to, but never including, the staging root."""
    root = staging_root.absolute()
    current = path.absolute().parent
    while current != root:
        if not current.is_relative_to(root) or current.is_symlink():
            return
        try:
            current.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            return
        current = current.parent


def _track_has_file(track: Track) -> bool:
    paths = [track.staging_path, track.source_path]
    paths.extend(plan.destination_path for plan in track.import_plans if plan.destination_path)
    return any(Path(raw).is_file() for raw in paths if raw)


async def prune_orphaned_terminal_records(
    db: AsyncSession, *, batch_size: int = 500
) -> OrphanPruneResult:
    """Remove all terminal acquisition history that has no surviving file artifact."""
    terminal = {JobStatus.done, JobStatus.failed, JobStatus.partial, JobStatus.cancelled}
    removed_tracks = 0
    last_track_id = 0
    while True:
        tracks = list(
            (
                await db.scalars(
                    select(Track)
                    .join(Job, Job.id == Track.job_id)
                    .where(Job.status.in_(terminal), Track.id > last_track_id)
                    .options(selectinload(Track.import_plans))
                    .order_by(Track.id)
                    .limit(batch_size)
                )
            ).all()
        )
        if not tracks:
            break
        last_track_id = tracks[-1].id
        has_files = await asyncio.gather(
            *(asyncio.to_thread(_track_has_file, track) for track in tracks)
        )
        for track, has_file in zip(tracks, has_files, strict=True):
            has_library_removal_evidence = any(
                plan.file_state in {LibraryFileState.missing, LibraryFileState.removed}
                for plan in track.import_plans
            )
            if not has_file and not has_library_removal_evidence:
                await db.delete(track)
                removed_tracks += 1
        await db.flush()
        db.expire_all()

    removed_releases = 0
    while True:
        releases = list(
            (
                await db.scalars(
                    select(Release)
                    .join(Job, Job.id == Release.job_id)
                    .where(
                        Job.status.in_(terminal),
                        ~Release.tracks.any(),
                        ~Release.monitoring_records.any(),
                    )
                    .order_by(Release.id)
                    .limit(batch_size)
                )
            ).all()
        )
        if not releases:
            break
        for release in releases:
            await db.delete(release)
        removed_releases += len(releases)
        await db.flush()
        db.expire_all()

    removed_jobs = 0
    while True:
        jobs = list(
            (
                await db.scalars(
                    select(Job)
                    .where(
                        Job.status.in_(terminal),
                        ~Job.tracks.any(),
                        ~Job.releases.any(),
                    )
                    .order_by(Job.id)
                    .limit(batch_size)
                )
            ).all()
        )
        if not jobs:
            break
        for job in jobs:
            await db.delete(job)
        removed_jobs += len(jobs)
        await db.flush()
        db.expire_all()
    return OrphanPruneResult(removed_tracks, removed_releases, removed_jobs)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cleanup_quarantine_path(
    path: Path,
    plan_id: int,
    device: int,
    inode: int,
    mtime_ns: int,
    size: int,
    digest: str,
) -> Path:
    marker = f".audiohoard-cleanup-{plan_id}-{device}-{inode}-{mtime_ns}-{size}-{digest}"
    if path.name.endswith(marker):
        return path
    return path.with_name(marker)


def _claimed_identity(path: Path, marker: str) -> tuple[int, int, int, int, str] | None:
    if marker not in path.name:
        return None
    try:
        values = path.name.rsplit(marker, 1)[1].split("-", 4)
        if len(values) != 5 or len(values[4]) != 64:
            return None
        return int(values[0]), int(values[1]), int(values[2]), int(values[3]), values[4]
    except ValueError:
        return None


def _current_identity(path: Path) -> tuple[int, int, int, int, str] | None:
    try:
        current = path.stat(follow_symlinks=False)
    except OSError:
        return None
    if not stat.S_ISREG(current.st_mode):
        return None
    try:
        digest = _file_sha256(path)
    except OSError:
        return None
    return current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size, digest


def _quarantine_claim_matches(path: Path, configured: Path, plan_id: int) -> bool:
    markers = (
        f".audiohoard-cleanup-{plan_id}-",
        f".{configured.name}.audiohoard-cleanup-{plan_id}-",
    )
    claimed_identities = {
        claimed for marker in markers if (claimed := _claimed_identity(path, marker)) is not None
    }
    if not claimed_identities:
        return False
    current_identity = _current_identity(path)
    return current_identity is not None and current_identity in claimed_identities


def _persisted_quarantine_claim_matches(path: Path, plan_id: int) -> bool:
    marker = f".audiohoard-cleanup-{plan_id}-"
    claimed_identity = _claimed_identity(path, marker)
    if claimed_identity is None:
        return False
    current_identity = _current_identity(path)
    return current_identity is not None and claimed_identity == current_identity


def _pending_cleanup_path_sync(plan: ImportPlan) -> Path | None:
    configured = Path(plan.staging_path or plan.source_path)
    if f".audiohoard-cleanup-{plan.id}-" in configured.name:
        if _persisted_quarantine_claim_matches(configured, plan.id):
            return configured
        logger.error(
            "refusing persisted cleanup quarantine with mismatched identity: plan=%s path=%s",
            plan.id,
            configured,
        )
        return None
    patterns = (
        f".audiohoard-cleanup-{plan.id}-*",
        f".*.audiohoard-cleanup-{plan.id}-*",
    )
    candidates = [
        candidate
        for pattern in patterns
        for candidate in configured.parent.glob(pattern)
        if _quarantine_claim_matches(candidate, configured, plan.id)
    ]
    return candidates[0] if len(candidates) == 1 else configured


async def _pending_cleanup_path(plan: ImportPlan) -> Path | None:
    return await asyncio.to_thread(_pending_cleanup_path_sync, plan)


async def pending_imported_source_cleanups(
    db: AsyncSession, *, limit: int = 500
) -> tuple[ImportedSourceCleanup, ...]:
    """Load durable post-import obligations represented by a retained staging path."""
    plans = list(
        (
            await db.scalars(
                select(ImportPlan)
                .where(
                    ImportPlan.status == ImportWorkflowState.imported,
                    ImportPlan.staging_path.is_not(None),
                    ImportPlan.staging_path != "",
                )
                .options(selectinload(ImportPlan.track))
                .order_by(
                    case((ImportPlan.cleanup_attempted_at.is_(None), 0), else_=1),
                    ImportPlan.cleanup_attempted_at,
                    ImportPlan.id,
                )
                .limit(limit)
            )
        ).all()
    )
    items: list[ImportedSourceCleanup] = []
    for plan in plans:
        cleanup_path = await _pending_cleanup_path(plan)
        if cleanup_path is None:
            continue
        items.append(
            ImportedSourceCleanup(
                plan.id,
                cleanup_path,
                plan.track.acquisition_provenance_json if plan.track else None,
                plan.track.source_job_id if plan.track else None,
                plan.track_id,
                session_factory=async_sessionmaker(db.bind, expire_on_commit=False)
                if db.bind is not None
                else None,
            )
        )
    return tuple(items)


def _active_destination_owner_condition() -> ColumnElement[bool]:
    return or_(
        and_(
            ImportPlan.status.in_({ImportWorkflowState.ready, ImportWorkflowState.importing}),
            ImportPlan.file_state != LibraryFileState.removed,
        ),
        and_(
            ImportPlan.status == ImportWorkflowState.imported,
            ImportPlan.file_state == LibraryFileState.present,
        ),
    )


async def _cleanup_obligation_is_current(
    db: AsyncSession, item: ImportedSourceCleanup, *, protect_destination: bool
) -> bool:
    if item.plan_id is None:
        return True
    plan = await db.get(ImportPlan, item.plan_id, options=(selectinload(ImportPlan.track),))
    if (
        plan is None
        or plan.status != ImportWorkflowState.imported
        or plan.track_id != item.track_id
    ):
        return False
    configured_path = Path(plan.staging_path or plan.source_path)
    accepted_paths = {str(configured_path)}
    claim_matches = await asyncio.to_thread(
        _quarantine_claim_matches, item.staged_path, configured_path, plan.id
    )
    if claim_matches:
        accepted_paths.add(str(item.staged_path))
    if str(item.staged_path) not in accepted_paths:
        return False
    track = plan.track
    if item.track_id is not None and (
        track is None
        or track.id != item.track_id
        or track.staging_path not in accepted_paths
        or track.source_job_id != item.source_job_id
        or _slskd_identity(track.acquisition_provenance_json)
        != _slskd_identity(item.provenance_json)
    ):
        return False
    if protect_destination:
        owner = await db.scalar(
            select(ImportPlan.id)
            .where(
                ImportPlan.destination_path.in_(accepted_paths),
                _active_destination_owner_condition(),
            )
            .limit(1)
        )
        if owner is not None:
            return False
    return True


async def _revalidate_cleanup_obligation(
    item: ImportedSourceCleanup, *, protect_destination: bool = True
) -> bool:
    if item.plan_id is None:
        return True
    session_factory = item.session_factory or get_session_factory()
    async with session_factory() as db:
        return await _cleanup_obligation_is_current(
            db, item, protect_destination=protect_destination
        )


async def _claim_cleanup_quarantine(item: ImportedSourceCleanup) -> ImportedSourceCleanup | None:
    """Move legacy imported content outside DB transactions, then persist the path."""
    if item.plan_id is None or item.track_id is None:
        return None
    factory = item.session_factory or get_session_factory()
    async with factory() as db:
        if not await _cleanup_obligation_is_current(db, item, protect_destination=True):
            return None
        plan = await db.get(ImportPlan, item.plan_id)
        track = await db.get(Track, item.track_id)
        if plan is None or track is None:
            return None
        configured = Path(plan.staging_path or plan.source_path)
        await db.commit()

    if any(
        value is None
        for value in (
            item.expected_device,
            item.expected_inode,
            item.expected_mtime_ns,
            item.expected_size,
        )
    ):
        return item
    try:
        digest = item.expected_digest or await asyncio.to_thread(_file_sha256, item.staged_path)
    except OSError:
        return None
    assert digest is not None
    current_item = replace(item, expected_digest=digest)
    assert current_item.expected_device is not None
    assert current_item.expected_inode is not None
    assert current_item.expected_mtime_ns is not None
    assert current_item.expected_size is not None
    quarantine = _cleanup_quarantine_path(
        configured,
        item.plan_id,
        current_item.expected_device,
        current_item.expected_inode,
        current_item.expected_mtime_ns,
        current_item.expected_size,
        digest,
    )
    try:
        if current_item.staged_path != quarantine:
            if quarantine.exists() or quarantine.is_symlink():
                return None
            await asyncio.to_thread(os.replace, current_item.staged_path, quarantine)
            current_item = replace(current_item, staged_path=quarantine)
        current = await asyncio.to_thread(quarantine.stat, follow_symlinks=False)
        current_digest = await asyncio.to_thread(_file_sha256, quarantine)
    except OSError:
        return None
    if (
        current.st_dev,
        current.st_ino,
        current.st_mtime_ns,
        current.st_size,
        current_digest,
    ) != (
        current_item.expected_device,
        current_item.expected_inode,
        current_item.expected_mtime_ns,
        current_item.expected_size,
        current_item.expected_digest,
    ):
        if not await asyncio.to_thread(configured.exists):
            with contextlib.suppress(OSError):
                await asyncio.to_thread(os.replace, quarantine, configured)
        return None

    for attempt_no in range(1, 4):
        async with factory() as db:
            try:
                if not await _cleanup_obligation_is_current(db, item, protect_destination=True):
                    if not await asyncio.to_thread(configured.exists):
                        with contextlib.suppress(OSError):
                            await asyncio.to_thread(os.replace, quarantine, configured)
                    return None
                plan = await db.get(ImportPlan, item.plan_id)
                track = await db.get(Track, item.track_id)
                if plan is None or track is None:
                    return None
                plan.staging_path = str(quarantine)
                track.staging_path = str(quarantine)
                await db.commit()
                return current_item
            except Exception as exc:
                await db.rollback()
                if attempt_no == 3 or not _transient_cleanup_error(exc):
                    raise
        await _cleanup_retry_delay(attempt_no)
    return None


def _unlink_if_identity_matches(item: ImportedSourceCleanup) -> bool:
    if any(
        value is None
        for value in (
            item.expected_device,
            item.expected_inode,
            item.expected_mtime_ns,
            item.expected_size,
        )
    ):
        return False
    try:
        current = item.staged_path.stat(follow_symlinks=False)
    except OSError:
        return False
    try:
        current_digest = _file_sha256(item.staged_path)
    except OSError:
        return False
    if (
        current.st_dev,
        current.st_ino,
        current.st_mtime_ns,
        current.st_size,
        current_digest,
    ) != (
        item.expected_device,
        item.expected_inode,
        item.expected_mtime_ns,
        item.expected_size,
        item.expected_digest,
    ):
        return False
    try:
        item.staged_path.unlink()
    except OSError:
        return False
    return True


async def _mark_cleanup_attempted(
    item: ImportedSourceCleanup, *, completed: bool, max_attempts: int = 3
) -> None:
    if item.plan_id is None:
        return
    session_factory = item.session_factory or get_session_factory()
    for attempt in range(1, max_attempts + 1):
        async with session_factory() as db:
            try:
                if not await _cleanup_obligation_is_current(db, item, protect_destination=False):
                    return
                plan = await db.get(ImportPlan, item.plan_id)
                if plan is None:
                    return
                plan.cleanup_attempted_at = datetime.now(UTC)
                if completed:
                    plan.staging_path = None
                    if item.track_id is not None:
                        track = await db.get(Track, item.track_id)
                        if track is not None and track.staging_path == str(item.staged_path):
                            track.staging_path = None
                await db.commit()
                return
            except Exception as exc:
                await db.rollback()
                if attempt == max_attempts or not _transient_cleanup_error(exc):
                    raise
        await _cleanup_retry_delay(attempt)


async def _provider_cleanup_completed_current(item: ImportedSourceCleanup) -> bool:
    if item.track_id is None:
        return _source_cleanup_completed(item.provenance_json)
    factory = item.session_factory or get_session_factory()
    async with factory() as db:
        track = await db.get(Track, item.track_id)
        return bool(
            track is not None
            and track.source_job_id == item.source_job_id
            and _slskd_identity(track.acquisition_provenance_json)
            == _slskd_identity(item.provenance_json)
            and _source_cleanup_completed(track.acquisition_provenance_json)
        )


async def _prepare_attempt_cleanup_for_imported_item(
    item: ImportedSourceCleanup,
) -> int | None:
    """Durably make the exact staged attempt eligible after the import committed."""
    if item.track_id is None:
        return None
    factory = item.session_factory or get_session_factory()
    async with factory() as db:
        attempts = list(
            (
                await db.scalars(
                    select(AcquisitionAttempt).where(
                        AcquisitionAttempt.track_id == item.track_id,
                        AcquisitionAttempt.provider == "slskd",
                        AcquisitionAttempt.staged_path == str(item.staged_path),
                        AcquisitionAttempt.outcome.in_(
                            {AttemptOutcome.downloaded, AttemptOutcome.imported}
                        ),
                    )
                )
            ).all()
        )
        if len(attempts) != 1:
            return None
        attempt = attempts[0]
        attempt.outcome = AttemptOutcome.imported
        attempt.artifact_state = ArtifactState.imported
        attempt.file_cleanup_eligible = True
        attempt.retention_disposition = RetentionDisposition.cleanup_eligible
        await db.commit()
        return attempt.id


async def cleanup_imported_sources(items: tuple[ImportedSourceCleanup, ...]) -> None:
    """Idempotently finish currently-owned cleanup obligations after import commit."""
    staging_root = get_settings().staging_root
    adapter = None
    if any(_slskd_identity(item.provenance_json) for item in items):
        try:
            async with get_session_factory()() as db:
                settings = await build_effective_settings(db, get_settings())
            adapter = SlskdAdapter(settings.slskd_url, settings.slskd_api_key)
        except Exception:
            logger.exception("post-import slskd cleanup setup failed")

    for original_item in items:
        attempt_id = await _prepare_attempt_cleanup_for_imported_item(original_item)
        if attempt_id is not None:
            factory = original_item.session_factory or get_session_factory()
            provider_result = (
                await cleanup_attempt_provider(factory, adapter, attempt_id)
                if adapter is not None
                else AttemptCleanupResult.retryable_failure
            )
            if provider_result in {
                AttemptCleanupResult.removed,
                AttemptCleanupResult.already_absent,
            }:
                file_result = await cleanup_attempt_file(factory, attempt_id, staging_root)
                if file_result in {
                    AttemptCleanupResult.removed,
                    AttemptCleanupResult.already_absent,
                }:
                    await _mark_cleanup_attempted(original_item, completed=True)
            continue
        if _slskd_identity(
            original_item.provenance_json
        ) is not None and not await _provider_cleanup_completed_current(original_item):
            # Unfenced legacy provider ownership is report-only. Do not move or
            # delete its recoverable artifact while that obligation is unresolved.
            continue
        item = (
            await _claim_cleanup_quarantine(original_item)
            if original_item.plan_id is not None
            else original_item
        )
        if item is None:
            continue
        failed = False
        has_local_identity = all(
            value is not None
            for value in (
                item.expected_device,
                item.expected_inode,
                item.expected_mtime_ns,
                item.expected_size,
            )
        )
        if has_local_identity and item.expected_digest is None:
            try:
                item = replace(
                    item,
                    expected_digest=await asyncio.to_thread(_file_sha256, item.staged_path),
                )
            except OSError:
                failed = True
        identity = _slskd_identity(item.provenance_json)
        provider_cleanup_completed = await _provider_cleanup_completed_current(item)
        if identity is not None and not provider_cleanup_completed:
            # Legacy Track projections have no claim/version fence. They remain
            # report-safe and are never destructively deleted by peer/path or an
            # unowned transfer ID; only attempt-backed cleanup performs provider I/O.
            failed = True
        if not await _revalidate_cleanup_obligation(item):
            continue
        if not failed:
            artifact_was_present = has_local_identity
            unlinked = (
                await asyncio.to_thread(_unlink_if_identity_matches, item)
                if artifact_was_present
                else True
            )
            if not unlinked:
                failed = True
                logger.warning(
                    "post-import staging cleanup refused changed artifact %s",
                    item.staged_path,
                )
            elif artifact_was_present:
                try:
                    await asyncio.to_thread(_prune_empty_parents, item.staged_path, staging_root)
                except Exception:
                    failed = True
                    logger.warning("post-import directory prune failed for %s", item.staged_path)
        try:
            await _mark_cleanup_attempted(item, completed=not failed)
        except Exception:
            logger.exception("failed to record post-import cleanup attempt")


def _finish_imported_source_cleanup(task: asyncio.Task[None]) -> None:
    _IMPORTED_SOURCE_CLEANUP_TASKS.discard(task)
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        logger.exception("detached post-import source cleanup failed")


def schedule_imported_source_cleanup(
    items: tuple[ImportedSourceCleanup, ...],
) -> asyncio.Task[None] | None:
    if not items:
        return None
    task = asyncio.get_running_loop().create_task(cleanup_imported_sources(items))
    _IMPORTED_SOURCE_CLEANUP_TASKS.add(task)
    task.add_done_callback(_finish_imported_source_cleanup)
    return task


async def wait_for_imported_source_cleanups(*, raise_errors: bool = True) -> None:
    """Drain source-cleanup tasks so tests and shutdown do not abandon obligations."""
    tasks = tuple(_IMPORTED_SOURCE_CLEANUP_TASKS)
    if not tasks:
        return
    results = await asyncio.gather(*tasks, return_exceptions=True)
    if raise_errors:
        for result in results:
            if isinstance(result, BaseException):
                raise result
