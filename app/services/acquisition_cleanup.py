from __future__ import annotations

import asyncio
import contextlib
import ctypes
import errno
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

from sqlalchemy import and_, case, or_, select, text, update
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
from app.models.discography_batch import DiscographyBatchItemJob
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.settings_service import build_effective_settings
from app.sources.slskd import ProvisionalTransferMatch, SlskdAdapter

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

    async def match_provisional_transfer(
        self, username: str, filename: str, *, force_refresh: bool = False
    ) -> ProvisionalTransferMatch: ...

    async def remove_exact(self, username: str, provider_uuid: str) -> None: ...


class AttemptCleanupResult(StrEnum):
    removed = "removed"
    quarantined = "quarantined"
    already_absent = "already_absent"
    blocked = "blocked"
    retryable_failure = "retryable_failure"
    claimed_elsewhere = "claimed_elsewhere"
    not_eligible = "not_eligible"


@dataclass(frozen=True)
class DirectorySweepItem:
    path: Path
    root: Path
    reason: str


@dataclass(frozen=True)
class DirectorySweepResult:
    snapshot_available: bool
    eligible: tuple[Path, ...] = ()
    not_eligible: tuple[DirectorySweepItem, ...] = ()
    removed: tuple[Path, ...] = ()
    error_code: str | None = None


_TERMINAL_TRANSFER_MARKERS = frozenset(
    {"complete", "completed", "succeeded", "failed", "cancelled", "canceled", "aborted"}
)
_LOCAL_PATH_KEYS = frozenset(
    {
        "localpath",
        "local_path",
        "downloadpath",
        "download_path",
        "incompletepath",
        "incomplete_path",
    }
)


def _active_transfer_local_paths(snapshot: list[dict[str, object]]) -> tuple[Path, ...]:
    paths: set[Path] = set()

    def collect(value: object, key: str = "") -> None:
        if isinstance(value, dict):
            for child_key, child in value.items():
                collect(child, str(child_key).casefold())
        elif isinstance(value, list):
            for child in value:
                collect(child, key)
        elif key in _LOCAL_PATH_KEYS and isinstance(value, str) and value.strip():
            candidate = Path(value.strip())
            if candidate.is_absolute():
                paths.add(candidate.absolute())

    for item in snapshot:
        raw_state = str(item.get("state") or item.get("status") or "").casefold()
        words = {word for word in raw_state.replace(",", " ").split() if word}
        if words & _TERMINAL_TRANSFER_MARKERS:
            continue
        collect(item)
    return tuple(sorted(paths, key=str))


def _paths_intersect(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def inspect_empty_slskd_directories(
    roots: tuple[Path, ...],
    snapshot: list[dict[str, object]],
    *,
    minimum_age: timedelta,
    now: datetime | None = None,
) -> DirectorySweepResult:
    """Classify directory-only trees without following links or mutating the filesystem."""
    cutoff = (now or datetime.now(UTC)).timestamp() - minimum_age.total_seconds()
    active_paths = _active_transfer_local_paths(snapshot)
    eligible: list[Path] = []
    rejected: list[DirectorySweepItem] = []

    def visit(path: Path, root: Path) -> bool:
        try:
            current = path.stat(follow_symlinks=False)
            entries = list(os.scandir(path))
        except OSError:
            rejected.append(DirectorySweepItem(path, root, "unavailable"))
            return False
        if stat.S_ISLNK(current.st_mode) or not stat.S_ISDIR(current.st_mode):
            rejected.append(DirectorySweepItem(path, root, "symlink_content"))
            return False
        if any(_paths_intersect(path, active) for active in active_paths):
            rejected.append(DirectorySweepItem(path, root, "active_transfer"))
            return False
        child_directories: list[Path] = []
        for entry in entries:
            try:
                if entry.is_symlink():
                    rejected.append(DirectorySweepItem(path, root, "symlink_content"))
                    return False
                if entry.is_dir(follow_symlinks=False):
                    child_directories.append(Path(entry.path))
                else:
                    rejected.append(DirectorySweepItem(path, root, "nonempty"))
                    return False
            except OSError:
                rejected.append(DirectorySweepItem(path, root, "unavailable"))
                return False
        children_eligible = all(visit(child, root) for child in child_directories)
        if not children_eligible:
            if not any(item.path == path for item in rejected):
                rejected.append(DirectorySweepItem(path, root, "nonempty"))
            return False
        if current.st_mtime > cutoff:
            rejected.append(DirectorySweepItem(path, root, "too_new"))
            return False
        eligible.append(path)
        return True

    seen_roots: set[Path] = set()
    for configured in roots:
        root = configured.absolute()
        if root in seen_roots:
            continue
        seen_roots.add(root)
        try:
            root_stat = root.stat(follow_symlinks=False)
            if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
                continue
            entries = list(os.scandir(root))
        except OSError:
            continue
        for entry in entries:
            try:
                if entry.is_symlink():
                    rejected.append(DirectorySweepItem(Path(entry.path), root, "symlink_content"))
                elif entry.is_dir(follow_symlinks=False):
                    visit(Path(entry.path), root)
            except OSError:
                rejected.append(DirectorySweepItem(Path(entry.path), root, "unavailable"))

    return DirectorySweepResult(
        snapshot_available=True,
        eligible=tuple(eligible),
        not_eligible=tuple(rejected),
    )


async def sweep_empty_slskd_directories(
    adapter: SlskdCleanupAdapter,
    roots: tuple[Path, ...],
    *,
    minimum_age: timedelta,
) -> DirectorySweepResult:
    """Remove only old empty directories after one forced-fresh live transfer snapshot."""
    if not roots:
        return DirectorySweepResult(snapshot_available=True)
    try:
        snapshot = await adapter.downloads(force_refresh=True)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning("slskd directory sweep skipped: provider snapshot unavailable")
        return DirectorySweepResult(
            snapshot_available=False,
            error_code="provider_unavailable",
        )
    inspected = inspect_empty_slskd_directories(roots, snapshot, minimum_age=minimum_age)
    removed: list[Path] = []
    # inspect_empty_slskd_directories emits children before parents.
    for candidate in inspected.eligible:
        try:
            if candidate.is_symlink():
                continue
            candidate.rmdir()
        except OSError:
            continue
        removed.append(candidate)
    return replace(inspected, removed=tuple(removed))


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


@dataclass(frozen=True)
class _PartialCleanupClaim:
    attempt_id: int
    token: str
    version: int
    provider_uuid: str
    partial_path: str
    device: int
    inode: int
    mtime_ns: int
    size: int
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


def _completed_transfer_has_durable_artifact(attempt: AcquisitionAttempt) -> bool:
    return (
        attempt.outcome
        in {
            AttemptOutcome.downloaded,
            AttemptOutcome.review_retained,
            AttemptOutcome.imported,
            AttemptOutcome.superseded,
        }
        and attempt.terminal_at is not None
        and attempt.provider_terminal_at is not None
        and attempt.artifact_state in {ArtifactState.staged, ArtifactState.imported}
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


def _provider_cleanup_eligible(attempt: AcquisitionAttempt) -> bool:
    if attempt.terminal_at is None or attempt.provider_terminal_at is None:
        return False
    if attempt.provider_state == ProviderTransferState.completed:
        return _completed_transfer_has_durable_artifact(attempt)
    return (
        attempt.provider_state in {ProviderTransferState.failed, ProviderTransferState.cancelled}
        and attempt.outcome != AttemptOutcome.pending
    )


def _provider_cleanup_eligibility_expression() -> ColumnElement[bool]:
    bound_completed = and_(
        AcquisitionAttempt.provider_state == ProviderTransferState.completed,
        AcquisitionAttempt.outcome.in_(
            {
                AttemptOutcome.downloaded,
                AttemptOutcome.review_retained,
                AttemptOutcome.imported,
                AttemptOutcome.superseded,
            }
        ),
        AcquisitionAttempt.artifact_state.in_({ArtifactState.staged, ArtifactState.imported}),
        AcquisitionAttempt.staged_path.is_not(None),
        AcquisitionAttempt.artifact_device.is_not(None),
        AcquisitionAttempt.artifact_inode.is_not(None),
        AcquisitionAttempt.artifact_mtime_ns.is_not(None),
        AcquisitionAttempt.artifact_size.is_not(None),
        AcquisitionAttempt.artifact_sha256.is_not(None),
    )
    failed_or_cancelled = and_(
        AcquisitionAttempt.provider_state.in_(
            {ProviderTransferState.failed, ProviderTransferState.cancelled}
        ),
        AcquisitionAttempt.outcome != AttemptOutcome.pending,
    )
    return and_(
        AcquisitionAttempt.terminal_at.is_not(None),
        AcquisitionAttempt.provider_terminal_at.is_not(None),
        or_(bound_completed, failed_or_cancelled),
    )


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
        if not _provider_cleanup_eligible(attempt):
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


def _open_cleanup_parent(path: Path, quarantine: Path, root: Path) -> int | None:
    """Pin a non-symlinked artifact parent beneath the configured cleanup root."""
    try:
        root_resolved = root.resolve(strict=True)
        parent_resolved = path.parent.resolve(strict=True)
        quarantine_parent = quarantine.parent.resolve(strict=True)
        relative_parent = parent_resolved.relative_to(root_resolved)
    except (OSError, ValueError):
        return None
    if quarantine_parent != parent_resolved or path.name in {"", ".", ".."}:
        return None
    if quarantine.name in {"", ".", ".."}:
        return None

    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        parent_fd = os.open(root_resolved, directory_flags)
    except OSError:
        return None
    try:
        if not stat.S_ISDIR(os.fstat(parent_fd).st_mode):
            os.close(parent_fd)
            return None
        for part in relative_parent.parts:
            next_fd = os.open(part, directory_flags, dir_fd=parent_fd)
            os.close(parent_fd)
            parent_fd = next_fd
            if not stat.S_ISDIR(os.fstat(parent_fd).st_mode):
                os.close(parent_fd)
                return None
        return parent_fd
    except OSError:
        os.close(parent_fd)
        return None


def _entry_exists_at(parent_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


def _regular_identity_at(parent_fd: int, name: str) -> tuple[int, int, int, int] | None:
    try:
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        return None
    if not stat.S_ISREG(current.st_mode):
        return None
    return current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size


def _identity_without_hash(path: Path) -> tuple[int, int, int, int] | None:
    """Untrusted pathname probe retained only as a pre-final-validation race hook."""
    try:
        current = path.stat(follow_symlinks=False)
    except OSError:
        return None
    if not stat.S_ISREG(current.st_mode):
        return None
    return current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size


def _hash_open_regular_at(
    parent_fd: int, name: str
) -> tuple[tuple[int, int, int, int], str] | None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(name, flags, dir_fd=parent_fd)
    except OSError:
        return None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            return None
        digest = hashlib.sha256()
        while chunk := os.read(fd, 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(fd)
        before_identity = (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size)
        after_identity = (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_size)
        if before_identity != after_identity:
            return None
        return after_identity, digest.hexdigest()
    finally:
        os.close(fd)


def _before_final_file_erase(_parent_fd: int, _name: str) -> None:
    """Test seam after final fd-bound validation and before content erasure."""


def _erase_validated_regular_at(
    parent_fd: int,
    name: str,
    expected_content: tuple[tuple[int | None, int | None, int | None, int | None], str | None],
) -> AttemptCleanupResult:
    """Erase only the validated inode, retaining its directory entry as a tombstone."""
    expected_identity, expected_sha256 = expected_content
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(name, flags, dir_fd=parent_fd)
    except OSError:
        return AttemptCleanupResult.blocked
    try:
        before = os.fstat(fd)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_mtime_ns,
            before.st_size,
        )
        if not stat.S_ISREG(before.st_mode) or before_identity != expected_identity:
            return AttemptCleanupResult.blocked
        digest = hashlib.sha256()
        while chunk := os.read(fd, 1024 * 1024):
            digest.update(chunk)
        after_hash = os.fstat(fd)
        after_hash_identity = (
            after_hash.st_dev,
            after_hash.st_ino,
            after_hash.st_mtime_ns,
            after_hash.st_size,
        )
        if after_hash_identity != expected_identity or digest.hexdigest() != expected_sha256:
            return AttemptCleanupResult.blocked

        # Pathname unlink cannot be conditional. From this point onward operate
        # only on the validated open inode and deliberately retain the entry.
        _before_final_file_erase(parent_fd, name)
        os.ftruncate(fd, 0)
        os.fsync(fd)
        erased = os.fstat(fd)
        if (erased.st_dev, erased.st_ino, erased.st_size) != (
            expected_identity[0],
            expected_identity[1],
            0,
        ):
            return AttemptCleanupResult.retryable_failure
    except OSError:
        return AttemptCleanupResult.retryable_failure
    finally:
        os.close(fd)

    current = _regular_identity_at(parent_fd, name)
    if current is None:
        return AttemptCleanupResult.blocked
    if (current[0], current[1], current[3]) != (
        expected_identity[0],
        expected_identity[1],
        0,
    ):
        return AttemptCleanupResult.blocked
    return AttemptCleanupResult.quarantined


def _rename_noreplace_at(parent_fd: int, source_name: str, quarantine_name: str) -> None:
    """Atomically quarantine without ever replacing an existing claim (Linux renameat2)."""
    renameat2 = getattr(ctypes.CDLL(None, use_errno=True), "renameat2", None)
    if renameat2 is None:
        raise OSError(errno.ENOSYS, "renameat2 is unavailable")
    result = renameat2(
        parent_fd,
        os.fsencode(source_name),
        parent_fd,
        os.fsencode(quarantine_name),
        1,  # RENAME_NOREPLACE
    )
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), quarantine_name)


def _restore_quarantine_at(parent_fd: int, quarantine_name: str, source_name: str) -> None:
    with contextlib.suppress(OSError):
        _rename_noreplace_at(parent_fd, quarantine_name, source_name)


def _remove_claimed_file(claim: _CleanupClaim, root: Path) -> AttemptCleanupResult:
    assert claim.staged_path is not None and claim.quarantine_path is not None
    source = Path(claim.staged_path)
    quarantine = Path(claim.quarantine_path)
    expected = (claim.device, claim.inode, claim.mtime_ns, claim.size)
    expected_content = (expected, claim.sha256)
    parent_fd = _open_cleanup_parent(source, quarantine, root)
    if parent_fd is None:
        return AttemptCleanupResult.blocked
    try:
        quarantine_identity = _regular_identity_at(parent_fd, quarantine.name)
        if quarantine_identity is not None and (
            quarantine_identity[0],
            quarantine_identity[1],
            quarantine_identity[3],
        ) == (expected[0], expected[1], 0):
            # Recovery after content erasure succeeded but durable claim
            # finalization did not. The empty inode is the retained tombstone.
            return AttemptCleanupResult.quarantined
        if quarantine_identity is None:
            if _entry_exists_at(parent_fd, quarantine.name):
                return AttemptCleanupResult.blocked
            source_identity = _identity_without_hash(source)
            if source_identity is None:
                return (
                    AttemptCleanupResult.blocked
                    if _entry_exists_at(parent_fd, source.name)
                    else AttemptCleanupResult.already_absent
                )
            if (
                source_identity != expected
                or _regular_identity_at(parent_fd, source.name) != expected
            ):
                return AttemptCleanupResult.blocked
            try:
                _rename_noreplace_at(parent_fd, source.name, quarantine.name)
            except FileExistsError:
                return AttemptCleanupResult.blocked
            except OSError:
                return AttemptCleanupResult.retryable_failure

        # Validate after the atomic move/recovery, then validate and erase via
        # one open fd. Never unlink the pathname: POSIX offers no conditional
        # unlink, so retaining an empty tombstone is the fail-closed boundary.
        if _hash_open_regular_at(parent_fd, quarantine.name) != expected_content:
            _restore_quarantine_at(parent_fd, quarantine.name, source.name)
            return AttemptCleanupResult.blocked
        if _identity_without_hash(quarantine) != expected:
            return AttemptCleanupResult.blocked
        return _erase_validated_regular_at(parent_fd, quarantine.name, expected_content)
    finally:
        os.close(parent_fd)


async def _finish_file_claim(
    session_factory: async_sessionmaker[AsyncSession],
    claim: _CleanupClaim,
    result: AttemptCleanupResult,
) -> bool:
    now = datetime.now(UTC)
    if result in {
        AttemptCleanupResult.removed,
        AttemptCleanupResult.quarantined,
        AttemptCleanupResult.already_absent,
    }:
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
            retention_disposition=(
                RetentionDisposition.retained
                if result == AttemptCleanupResult.quarantined
                else RetentionDisposition.removed
            ),
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


async def reconcile_terminal_slskd_intents(
    session_factory: async_sessionmaker[AsyncSession],
    adapter: SlskdCleanupAdapter,
    job_ids: set[int] | None = None,
) -> int:
    """Turn terminal jobs' provisional enqueue intents into exact cleanup obligations."""
    terminal_jobs = {JobStatus.done, JobStatus.failed, JobStatus.partial, JobStatus.cancelled}
    async with session_factory() as db:
        query = (
            select(
                AcquisitionAttempt.id,
                AcquisitionAttempt.provisional_transfer_id,
                AcquisitionAttempt.provider_uuid,
                AcquisitionAttempt.peer,
                AcquisitionAttempt.remote_path,
            )
            .join(Job, Job.id == AcquisitionAttempt.job_id)
            .where(
                Job.status.in_(terminal_jobs),
                AcquisitionAttempt.provider == "slskd",
                AcquisitionAttempt.provisional_transfer_id.is_not(None),
                AcquisitionAttempt.provider_state.in_(
                    {
                        ProviderTransferState.pending,
                        ProviderTransferState.enqueued,
                        ProviderTransferState.queued,
                        ProviderTransferState.downloading,
                    }
                ),
                AcquisitionAttempt.provider_cleanup_state != CleanupState.completed,
            )
            .order_by(AcquisitionAttempt.id)
        )
        if job_ids is not None:
            query = query.where(AcquisitionAttempt.job_id.in_(job_ids))
        intents = list((await db.execute(query)).all())

    reconciled = 0
    for attempt_id, fallback_id, persisted_uuid, peer, remote_path in intents:
        assert fallback_id is not None
        if not peer or not remote_path:
            continue
        try:
            evidence = await adapter.match_provisional_transfer(
                peer, remote_path, force_refresh=True
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("terminal slskd enqueue-intent probe failed", exc_info=True)
            continue
        extra = evidence.transfer
        if evidence.match_count != 1 or extra is None:
            continue
        discovered = _snapshot_provider_uuid(extra)
        if discovered is None:
            continue
        from app.services.acquisition_attempts import canonical_provider_uuid

        persisted_canonical = canonical_provider_uuid(persisted_uuid)
        if persisted_uuid is not None and persisted_canonical is None:
            continue
        if persisted_canonical is not None and persisted_canonical != discovered:
            logger.warning("terminal slskd enqueue-intent canonical UUID mismatch")
            continue
        if str(extra.get("username") or "") != peer or _normalized_remote_path(
            extra.get("filename")
        ) != _normalized_remote_path(remote_path):
            logger.warning("terminal slskd enqueue-intent identity mismatch")
            continue
        now = datetime.now(UTC)
        async with session_factory() as db:
            try:
                # Reserve SQLite's single writer before rechecking ownership. A replacement
                # attempt commits its provisional identity before POSTing, so it is visible
                # here; a concurrent replacement cannot commit and POST until we finish.
                await db.execute(text("BEGIN IMMEDIATE"))
                attempt = await db.get(AcquisitionAttempt, attempt_id)
                if attempt is None:
                    continue
                job = await db.get(Job, attempt.job_id)
                if (
                    job is None
                    or job.status not in terminal_jobs
                    or attempt.provider_state
                    not in {
                        ProviderTransferState.pending,
                        ProviderTransferState.enqueued,
                        ProviderTransferState.queued,
                        ProviderTransferState.downloading,
                    }
                    or attempt.provisional_transfer_id != fallback_id
                    or attempt.peer != peer
                    or _normalized_remote_path(attempt.remote_path)
                    != _normalized_remote_path(remote_path)
                ):
                    continue
                current_canonical = canonical_provider_uuid(attempt.provider_uuid)
                if attempt.provider_uuid is not None and current_canonical is None:
                    continue
                if current_canonical is not None and current_canonical != discovered:
                    continue
                active_provider_states = (
                    ProviderTransferState.pending,
                    ProviderTransferState.enqueued,
                    ProviderTransferState.queued,
                    ProviderTransferState.downloading,
                )
                owned = await db.scalar(
                    select(AcquisitionAttempt.id).where(
                        AcquisitionAttempt.id != attempt.id,
                        AcquisitionAttempt.provider == "slskd",
                        or_(
                            AcquisitionAttempt.provider_uuid == discovered,
                            and_(
                                or_(
                                    AcquisitionAttempt.provisional_transfer_id == fallback_id,
                                    and_(
                                        AcquisitionAttempt.peer == peer,
                                        AcquisitionAttempt.remote_path == remote_path,
                                    ),
                                ),
                                or_(
                                    AcquisitionAttempt.terminal_at.is_(None),
                                    AcquisitionAttempt.provider_state.in_(active_provider_states),
                                ),
                            ),
                        ),
                    )
                )
                if owned is not None:
                    logger.warning(
                        "terminal slskd enqueue-intent UUID or provisional "
                        "generation already owned"
                    )
                    continue
                attempt.provider_uuid = discovered
                attempt.provider_uuid_discovered_at = attempt.provider_uuid_discovered_at or now
                attempt.provider_enqueued_at = attempt.provider_enqueued_at or now
                attempt.provider_state = ProviderTransferState.cancelled
                attempt.provider_terminal_at = now
                attempt.outcome = AttemptOutcome.failed
                attempt.terminal_at = now
                attempt.error_code = "cancelled"
                attempt.error_detail = "terminal job left an accepted slskd enqueue intent"
                await db.commit()
                reconciled += 1
            except OperationalError:
                await db.rollback()
                logger.warning(
                    "terminal slskd enqueue-intent ownership transaction failed closed",
                    exc_info=True,
                )
    return reconciled


async def _claim_partial_cleanup(
    session_factory: async_sessionmaker[AsyncSession],
    attempt_id: int,
    *,
    lease_seconds: int,
) -> tuple[_PartialCleanupClaim | None, AttemptCleanupResult | None]:
    now = datetime.now(UTC)
    async with session_factory() as db:
        attempt = await db.get(AcquisitionAttempt, attempt_id)
        if attempt is None or attempt.provider != "slskd":
            return None, AttemptCleanupResult.not_eligible
        if attempt.file_cleanup_state == CleanupState.completed:
            return None, AttemptCleanupResult.already_absent
        required = (
            attempt.provider_cleanup_state == CleanupState.completed,
            attempt.provider_uuid,
            attempt.partial_path,
            attempt.file_cleanup_eligible,
            attempt.retention_disposition == RetentionDisposition.cleanup_eligible,
            attempt.artifact_state == ArtifactState.partial,
            attempt.artifact_device,
            attempt.artifact_inode,
            attempt.artifact_mtime_ns,
            attempt.artifact_size,
        )
        if not all(value is not None and value is not False for value in required):
            return None, AttemptCleanupResult.not_eligible
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
        state_ok = or_(
            AcquisitionAttempt.file_cleanup_state.in_(
                {CleanupState.pending, CleanupState.failed, CleanupState.blocked}
            ),
            and_(
                AcquisitionAttempt.file_cleanup_state == CleanupState.claimed,
                AcquisitionAttempt.cleanup_lease_expires_at <= now,
            ),
        )
        updated = await db.execute(
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
                cleanup_claim_token=token,
                cleanup_claim_version=version,
                cleanup_claimed_at=now,
                cleanup_lease_expires_at=now + timedelta(seconds=lease_seconds),
                error_code=None,
                error_detail=None,
            )
        )
        if not isinstance(updated, CursorResult) or updated.rowcount != 1:
            await db.rollback()
            return None, AttemptCleanupResult.claimed_elsewhere
        await db.commit()
        assert attempt.provider_uuid is not None
        assert attempt.partial_path is not None
        assert attempt.artifact_device is not None
        assert attempt.artifact_inode is not None
        assert attempt.artifact_mtime_ns is not None
        assert attempt.artifact_size is not None
        return (
            _PartialCleanupClaim(
                attempt.id,
                token,
                version,
                attempt.provider_uuid,
                attempt.partial_path,
                attempt.artifact_device,
                attempt.artifact_inode,
                attempt.artifact_mtime_ns,
                attempt.artifact_size,
                attempt.file_cleanup_attempt_count + 1,
            ),
            None,
        )


def _remove_owned_partial(
    claim: _PartialCleanupClaim, root: Path, *, minimum_age: timedelta
) -> AttemptCleanupResult:
    path = Path(claim.partial_path)
    try:
        root_resolved = root.resolve(strict=True)
        root_stat = root.stat(follow_symlinks=False)
        if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
            return AttemptCleanupResult.blocked
        if not path.is_absolute() or not path.absolute().is_relative_to(root.absolute()):
            return AttemptCleanupResult.blocked
        parent = path.parent.resolve(strict=True)
        if not parent.is_relative_to(root_resolved):
            return AttemptCleanupResult.blocked
        current = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return AttemptCleanupResult.already_absent
    except OSError:
        return AttemptCleanupResult.blocked
    expected = (claim.device, claim.inode, claim.mtime_ns, claim.size)
    if stat.S_ISLNK(current.st_mode) or not stat.S_ISREG(current.st_mode):
        return AttemptCleanupResult.blocked
    if (current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size) != expected:
        return AttemptCleanupResult.blocked
    cutoff = datetime.now(UTC).timestamp() - minimum_age.total_seconds()
    if current.st_mtime > cutoff:
        return AttemptCleanupResult.not_eligible
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        parent_fd = os.open(parent, flags)
    except OSError:
        return AttemptCleanupResult.blocked
    try:
        rechecked = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            stat.S_ISLNK(rechecked.st_mode)
            or (
                rechecked.st_dev,
                rechecked.st_ino,
                rechecked.st_mtime_ns,
                rechecked.st_size,
            )
            != expected
        ):
            return AttemptCleanupResult.blocked
        os.unlink(path.name, dir_fd=parent_fd)
    except FileNotFoundError:
        return AttemptCleanupResult.already_absent
    except OSError:
        return AttemptCleanupResult.retryable_failure
    finally:
        os.close(parent_fd)
    _prune_empty_parents(path, root)
    return AttemptCleanupResult.removed


async def _finish_partial_claim(
    session_factory: async_sessionmaker[AsyncSession],
    claim: _PartialCleanupClaim,
    result: AttemptCleanupResult,
) -> bool:
    now = datetime.now(UTC)
    if result in {AttemptCleanupResult.removed, AttemptCleanupResult.already_absent}:
        state = CleanupState.completed
    elif result == AttemptCleanupResult.blocked:
        state = CleanupState.blocked
    elif result == AttemptCleanupResult.not_eligible:
        state = CleanupState.pending
    else:
        state = CleanupState.failed
    values: dict[str, object] = {
        "file_cleanup_state": state,
        "cleanup_claim_token": None,
        "cleanup_claimed_at": None,
        "cleanup_lease_expires_at": None,
        "error_code": None,
        "error_detail": None,
    }
    if state == CleanupState.completed:
        values.update(
            file_cleanup_completed_at=now,
            file_cleanup_retry_at=None,
            retention_disposition=RetentionDisposition.removed,
            artifact_state=ArtifactState.missing,
        )
    elif state == CleanupState.blocked:
        values.update(
            error_code="partial_cleanup_identity_mismatch",
            error_detail="partial retained; cleanup requires review",
        )
    elif state == CleanupState.failed:
        values.update(
            error_code="partial_cleanup_failed",
            error_detail="partial cleanup will be retried",
            file_cleanup_retry_at=now
            + timedelta(seconds=min(3600, 30 * (2 ** max(0, claim.attempt_count - 1)))),
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


async def cleanup_attempt_partial(
    session_factory: async_sessionmaker[AsyncSession],
    adapter: SlskdCleanupAdapter,
    attempt_id: int,
    incomplete_root: Path,
    *,
    minimum_age: timedelta,
    lease_seconds: int = 300,
) -> AttemptCleanupResult:
    """Remove one exact provider-owned partial after UUID cleanup and a fresh live check."""
    claim, immediate = await _claim_partial_cleanup(
        session_factory, attempt_id, lease_seconds=lease_seconds
    )
    if claim is None:
        assert immediate is not None
        return immediate
    try:
        snapshot = await adapter.downloads(force_refresh=True)
    except asyncio.CancelledError:
        await _finish_partial_claim(session_factory, claim, AttemptCleanupResult.retryable_failure)
        raise
    except Exception:
        await _finish_partial_claim(session_factory, claim, AttemptCleanupResult.retryable_failure)
        return AttemptCleanupResult.retryable_failure
    partial = Path(claim.partial_path)
    exact_live = any(_snapshot_provider_uuid(item) == claim.provider_uuid for item in snapshot)
    path_live = any(
        _paths_intersect(partial, active) for active in _active_transfer_local_paths(snapshot)
    )
    if exact_live or path_live:
        result = AttemptCleanupResult.not_eligible
    else:
        result = await asyncio.to_thread(
            _remove_owned_partial, claim, incomplete_root, minimum_age=minimum_age
        )
    finalized = await _finish_partial_claim(session_factory, claim, result)
    return result if finalized else AttemptCleanupResult.claimed_elsewhere


async def cleanup_durable_slskd_transfers(
    session_factory: async_sessionmaker[AsyncSession],
    adapter: SlskdCleanupAdapter,
    job_ids: set[int] | None = None,
    *,
    max_attempts: int = 3,
    complete_root: Path | None = None,
    incomplete_root: Path | None = None,
    partial_minimum_age: timedelta = timedelta(days=1),
) -> int:
    """Consume exact attempt-backed obligations; legacy Track rows remain report-only."""
    del max_attempts  # Claim retries/backoff are persisted per attempt.
    async with session_factory() as db:
        query = select(AcquisitionAttempt.id).where(
            AcquisitionAttempt.provider == "slskd",
            _provider_cleanup_eligibility_expression(),
            AcquisitionAttempt.provider_cleanup_state != CleanupState.completed,
        )
        if job_ids is not None:
            query = query.where(AcquisitionAttempt.job_id.in_(job_ids))
        attempt_ids = list((await db.scalars(query.order_by(AcquisitionAttempt.id))).all())

        file_query = select(
            AcquisitionAttempt.id,
            AcquisitionAttempt.artifact_state,
            AcquisitionAttempt.staged_path,
        ).where(
            AcquisitionAttempt.provider == "slskd",
            AcquisitionAttempt.provider_cleanup_state == CleanupState.completed,
            AcquisitionAttempt.file_cleanup_state.in_({CleanupState.pending, CleanupState.failed}),
            AcquisitionAttempt.file_cleanup_eligible.is_(True),
            AcquisitionAttempt.retention_disposition == RetentionDisposition.cleanup_eligible,
            AcquisitionAttempt.artifact_state.in_({ArtifactState.staged, ArtifactState.partial}),
        )
        if job_ids is not None:
            file_query = file_query.where(AcquisitionAttempt.job_id.in_(job_ids))
        file_attempts = list(
            (
                await db.execute(
                    file_query.order_by(
                        AcquisitionAttempt.file_cleanup_last_attempted_at.asc().nulls_first(),
                        AcquisitionAttempt.id,
                    )
                )
            ).all()
        )

    completed = 0
    for attempt_id in attempt_ids:
        result = await cleanup_attempt_provider(session_factory, adapter, attempt_id)
        if result in {AttemptCleanupResult.removed, AttemptCleanupResult.already_absent}:
            completed += 1
    configured_roots = tuple(root for root in (complete_root, incomplete_root) if root is not None)
    for attempt_id, artifact_state, staged_path in file_attempts:
        if artifact_state == ArtifactState.partial:
            if incomplete_root is not None:
                await cleanup_attempt_partial(
                    session_factory,
                    adapter,
                    attempt_id,
                    incomplete_root,
                    minimum_age=partial_minimum_age,
                )
            continue
        if staged_path is None:
            continue
        cleanup_root = _configured_attempt_root(Path(staged_path), configured_roots)
        if cleanup_root is not None:
            await cleanup_attempt_file(session_factory, attempt_id, cleanup_root)
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
    slskd_complete_root: Path | None = None,
    slskd_incomplete_root: Path | None = None,
    partial_minimum_age: timedelta = timedelta(days=1),
) -> tuple[list[int], int]:
    """Serialize cleanup while retrying only short database transitions."""
    async with _TERMINAL_CLEANUP_LOCK:
        hidden = await hide_completed_and_timed_out_jobs(
            session_factory, job_ids, max_attempts=max_attempts
        )
        adapter = SlskdAdapter(slskd_url, slskd_api_key)
        await reconcile_terminal_slskd_intents(session_factory, adapter, job_ids)
        removed = await cleanup_durable_slskd_transfers(
            session_factory,
            adapter,
            job_ids,
            max_attempts=max_attempts,
            complete_root=slskd_complete_root,
            incomplete_root=slskd_incomplete_root,
            partial_minimum_age=partial_minimum_age,
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


async def _yield_to_interactive_writers() -> None:
    # A zero-duration yield lets this task reacquire SQLite immediately and can
    # starve browser writes for the entire maintenance pass. Give already-waiting
    # login/settings transactions a real scheduling window between prune batches.
    await asyncio.sleep(0.1)


async def prune_orphaned_terminal_records(
    db: AsyncSession,
    *,
    batch_size: int = 500,
    commit_batches: bool = False,
    max_batches: int | None = None,
) -> OrphanPruneResult:
    """Remove terminal history in optionally bounded writer-friendly batches."""

    async def release_batch_lock(*, changed: bool) -> None:
        if not commit_batches:
            return
        if changed:
            await db.commit()
            await _yield_to_interactive_writers()
        else:
            await db.rollback()

    terminal = {JobStatus.done, JobStatus.failed, JobStatus.partial, JobStatus.cancelled}
    removed_tracks = 0
    removed_releases = 0
    removed_jobs = 0
    batches = 0

    def batch_limit_reached() -> bool:
        return max_batches is not None and batches >= max_batches

    def result() -> OrphanPruneResult:
        return OrphanPruneResult(removed_tracks, removed_releases, removed_jobs)

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
        removed_in_batch = 0
        for track, has_file in zip(tracks, has_files, strict=True):
            has_library_removal_evidence = any(
                plan.file_state in {LibraryFileState.missing, LibraryFileState.removed}
                for plan in track.import_plans
            )
            if not has_file and not has_library_removal_evidence:
                await db.delete(track)
                removed_tracks += 1
                removed_in_batch += 1
        await db.flush()
        await release_batch_lock(changed=removed_in_batch > 0)
        if removed_in_batch:
            batches += 1
        db.expire_all()
        if batch_limit_reached():
            return result()

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
        await release_batch_lock(changed=True)
        batches += 1
        db.expire_all()
        if batch_limit_reached():
            return result()

    while True:
        jobs = list(
            (
                await db.scalars(
                    select(Job)
                    .where(
                        Job.status.in_(terminal),
                        ~Job.tracks.any(),
                        ~Job.releases.any(),
                        ~Job.id.in_(select(DiscographyBatchItemJob.job_id)),
                        ~Job.acquisition_attempts.any(
                            or_(
                                AcquisitionAttempt.provider_cleanup_state.not_in(
                                    (CleanupState.completed, CleanupState.not_required)
                                ),
                                AcquisitionAttempt.file_cleanup_state.not_in(
                                    (CleanupState.completed, CleanupState.not_required)
                                ),
                            )
                        ),
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
        await release_batch_lock(changed=True)
        batches += 1
        db.expire_all()
        if batch_limit_reached():
            return result()
    if commit_batches:
        await db.rollback()
    return result()


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
    if db.bind is None:
        return ()
    session_factory = async_sessionmaker(db.bind, expire_on_commit=False)
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
                session_factory=session_factory,
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


async def _mark_attempt_cleanup_scheduling(
    item: ImportedSourceCleanup, *, completed: bool, max_attempts: int = 3
) -> None:
    """Rotate an exact imported obligation without filesystem I/O in a DB transaction."""
    if item.plan_id is None:
        return
    session_factory = item.session_factory or get_session_factory()
    accepted_path = str(item.staged_path)
    for attempt_no in range(1, max_attempts + 1):
        async with session_factory() as db:
            try:
                plan = await db.get(
                    ImportPlan, item.plan_id, options=(selectinload(ImportPlan.track),)
                )
                if (
                    plan is None
                    or plan.status != ImportWorkflowState.imported
                    or plan.track_id != item.track_id
                    or plan.staging_path != accepted_path
                ):
                    return
                track = plan.track
                if item.track_id is not None and (
                    track is None
                    or track.id != item.track_id
                    or track.staging_path != accepted_path
                    or track.source_job_id != item.source_job_id
                    or _slskd_identity(track.acquisition_provenance_json)
                    != _slskd_identity(item.provenance_json)
                ):
                    return
                plan.cleanup_attempted_at = datetime.now(UTC)
                if completed:
                    plan.staging_path = None
                    if track is not None and track.staging_path == accepted_path:
                        track.staging_path = None
                await db.commit()
                return
            except Exception as exc:
                await db.rollback()
                if attempt_no == max_attempts or not _transient_cleanup_error(exc):
                    raise
        await _cleanup_retry_delay(attempt_no)


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


def _configured_attempt_root(path: Path, roots: tuple[Path, ...]) -> Path | None:
    """Select the narrowest explicit provider root containing a persisted artifact path."""
    absolute = path.absolute()
    candidates = [root.absolute() for root in roots if absolute.is_relative_to(root.absolute())]
    return max(candidates, key=lambda root: len(root.parts), default=None)


async def cleanup_imported_sources(
    items: tuple[ImportedSourceCleanup, ...],
    *,
    complete_root: Path | None = None,
    incomplete_root: Path | None = None,
) -> None:
    """Idempotently finish currently-owned cleanup obligations after import commit."""
    if not items:
        return
    factory = items[0].session_factory or get_session_factory()
    configured = get_settings()
    settings = configured
    loaded_effective = False
    try:
        async with factory() as db:
            settings = await build_effective_settings(db, configured)
        loaded_effective = True
    except Exception:
        # Legacy cleanup remains available if settings storage is unavailable, but
        # attempt-backed deletion still requires roots supplied by a lifecycle caller.
        logger.warning("effective post-import cleanup settings unavailable", exc_info=True)
    staging_root = getattr(settings, "staging_root", configured.staging_root)
    effective_complete_root = (
        getattr(settings, "slskd_complete_root", None) if loaded_effective else None
    )
    effective_incomplete_root = (
        getattr(settings, "slskd_incomplete_root", None) if loaded_effective else None
    )
    configured_attempt_roots = tuple(
        root
        for root in (
            complete_root if complete_root is not None else effective_complete_root,
            incomplete_root if incomplete_root is not None else effective_incomplete_root,
        )
        if root is not None
    )
    adapter = None
    if any(_slskd_identity(item.provenance_json) for item in items):
        adapter = SlskdAdapter(
            getattr(settings, "slskd_url", configured.slskd_url),
            getattr(settings, "slskd_api_key", configured.slskd_api_key),
        )

    for original_item in items:
        attempt_id = await _prepare_attempt_cleanup_for_imported_item(original_item)
        if attempt_id is not None:
            factory = original_item.session_factory or get_session_factory()
            provider_result = (
                await cleanup_attempt_provider(factory, adapter, attempt_id)
                if adapter is not None
                else AttemptCleanupResult.retryable_failure
            )
            completed = False
            if provider_result in {
                AttemptCleanupResult.removed,
                AttemptCleanupResult.already_absent,
            }:
                cleanup_root = _configured_attempt_root(
                    original_item.staged_path, configured_attempt_roots
                )
                if cleanup_root is not None:
                    file_result = await cleanup_attempt_file(factory, attempt_id, cleanup_root)
                    completed = file_result in {
                        AttemptCleanupResult.removed,
                        AttemptCleanupResult.quarantined,
                        AttemptCleanupResult.already_absent,
                    }
            try:
                await _mark_attempt_cleanup_scheduling(original_item, completed=completed)
            except Exception:
                logger.exception("failed to record attempt-backed post-import cleanup")
            continue
        if _slskd_identity(
            original_item.provenance_json
        ) is not None and not await _provider_cleanup_completed_current(original_item):
            # Unfenced legacy provider ownership is report-only. Do not move or
            # delete its recoverable artifact while that obligation is unresolved.
            try:
                await _mark_attempt_cleanup_scheduling(original_item, completed=False)
            except Exception:
                logger.exception("failed to record blocked legacy post-import cleanup")
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
    *,
    complete_root: Path | None = None,
    incomplete_root: Path | None = None,
) -> asyncio.Task[None] | None:
    if not items:
        return None
    task = asyncio.get_running_loop().create_task(
        cleanup_imported_sources(
            items, complete_root=complete_root, incomplete_root=incomplete_root
        )
    )
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
