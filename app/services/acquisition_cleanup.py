from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from sqlalchemy import and_, case, or_, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload
from sqlalchemy.sql.elements import ColumnElement

from app.config import get_settings
from app.database import get_session_factory
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
    async def cancel(
        self, username: str, filename: str, transfer_id: str | None = None
    ) -> bool | None: ...


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


async def cleanup_durable_slskd_transfers(
    session_factory: async_sessionmaker[AsyncSession],
    adapter: SlskdCleanupAdapter,
    job_ids: set[int] | None = None,
    *,
    max_attempts: int = 3,
) -> int:
    """Remove durable completed/timeout transfers with no DB transaction held during I/O."""
    async with session_factory() as db:
        query = select(Track).where(
            Track.source == "slskd",
            Track.source_job_id.is_not(None),
            Track.acquisition_provenance_json.is_not(None),
        )
        if job_ids is not None:
            query = query.where(Track.job_id.in_(job_ids))
        tracks = list((await db.scalars(query.order_by(Track.id))).all())
        attempts: dict[tuple[str, str], list[tuple[int, str]]] = {}
        for track in tracks:
            if _source_cleanup_completed(track.acquisition_provenance_json):
                continue
            durable = (
                track.acquisition_state == AcquisitionState.downloaded and bool(track.staging_path)
            ) or track.source_status == "transfer_timeout"
            identity = _slskd_identity(track.acquisition_provenance_json)
            source_job_id = track.source_job_id
            if durable and identity is not None and source_job_id is not None:
                attempts.setdefault(identity, []).append((track.id, source_job_id))

    completed = 0
    cleaned_track_identities: dict[int, tuple[tuple[str, str], str]] = {}
    for identity, track_refs in sorted(attempts.items()):
        # Revalidate immediately before provider I/O. The adapter additionally targets
        # the exact provider transfer ID, so a replacement with the same peer/path
        # cannot be removed after this transaction is released.
        async with session_factory() as db:
            current_tracks = list(
                (
                    await db.scalars(
                        select(Track).where(Track.id.in_([ref[0] for ref in track_refs]))
                    )
                ).all()
            )
            current_refs = [
                (track.id, expected_source_job_id)
                for track in current_tracks
                for expected_track_id, expected_source_job_id in track_refs
                if track.id == expected_track_id
                and track.source_job_id == expected_source_job_id
                and _slskd_identity(track.acquisition_provenance_json) == identity
                and not _source_cleanup_completed(track.acquisition_provenance_json)
                and (
                    (
                        track.acquisition_state == AcquisitionState.downloaded
                        and bool(track.staging_path)
                    )
                    or track.source_status == "transfer_timeout"
                )
            ]
        if not current_refs:
            continue
        # Every row grouped under an identity should refer to the same provider
        # transfer. Refuse ambiguous historical groups rather than deleting by path.
        transfer_ids = {source_job_id for _, source_job_id in current_refs}
        if len(transfer_ids) != 1:
            logger.warning("skipping ambiguous durable slskd transfer cleanup identity")
            continue
        transfer_id = next(iter(transfer_ids))
        try:
            cleanup_result = await adapter.cancel(*identity, transfer_id)
            if cleanup_result is False:
                continue
        except Exception:
            logger.exception("durable slskd transfer cleanup failed")
        else:
            completed += 1
            cleaned_track_identities.update(
                {track_id: (identity, source_job_id) for track_id, source_job_id in current_refs}
            )

    if cleaned_track_identities:
        await _mark_durable_source_cleanups(
            session_factory, cleaned_track_identities, max_attempts=max_attempts
        )
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
        digest = _file_sha256(path)
    except OSError:
        return None
    return current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size, digest


def _quarantine_claim_matches(path: Path, configured: Path, plan_id: int) -> bool:
    current_identity = _current_identity(path)
    if current_identity is None:
        return False
    markers = (
        f".audiohoard-cleanup-{plan_id}-",
        f".{configured.name}.audiohoard-cleanup-{plan_id}-",
    )
    return any(_claimed_identity(path, marker) == current_identity for marker in markers)


def _persisted_quarantine_claim_matches(path: Path, plan_id: int) -> bool:
    marker = f".audiohoard-cleanup-{plan_id}-"
    return _claimed_identity(path, marker) == _current_identity(path)


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
        f".{configured.name}.audiohoard-cleanup-{plan.id}-*",
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
    """Durably claim an owned inode at a deterministic crash-recoverable path."""
    if item.plan_id is None or item.track_id is None:
        return None
    factory = item.session_factory or get_session_factory()
    current_item = item
    for attempt in range(1, 4):
        async with factory() as db:
            try:
                await db.execute(text("BEGIN IMMEDIATE"))
                if not await _cleanup_obligation_is_current(
                    db, current_item, protect_destination=True
                ):
                    await db.rollback()
                    return None
                plan = await db.get(ImportPlan, current_item.plan_id)
                track = await db.get(Track, current_item.track_id)
                if plan is None or track is None:
                    await db.rollback()
                    return None
                configured = Path(plan.staging_path or plan.source_path)
                if any(
                    value is None
                    for value in (
                        current_item.expected_device,
                        current_item.expected_inode,
                        current_item.expected_mtime_ns,
                        current_item.expected_size,
                    )
                ):
                    # The old artifact is already absent. Preserve any later file at
                    # the configured name and finish provider cleanup only.
                    await db.rollback()
                    return current_item
                expected_device = current_item.expected_device
                expected_inode = current_item.expected_inode
                expected_mtime_ns = current_item.expected_mtime_ns
                expected_size = current_item.expected_size
                assert expected_device is not None
                assert expected_inode is not None
                assert expected_mtime_ns is not None
                assert expected_size is not None
                expected_digest = current_item.expected_digest
                if expected_digest is None:
                    expected_digest = await asyncio.to_thread(
                        _file_sha256, current_item.staged_path
                    )
                    current_item = replace(current_item, expected_digest=expected_digest)
                quarantine = _cleanup_quarantine_path(
                    configured,
                    plan.id,
                    expected_device,
                    expected_inode,
                    expected_mtime_ns,
                    expected_size,
                    expected_digest,
                )
                if current_item.staged_path != quarantine:
                    if quarantine.exists():
                        await db.rollback()
                        return None
                    await asyncio.to_thread(os.replace, current_item.staged_path, quarantine)
                    current_item = replace(current_item, staged_path=quarantine)
                current = await asyncio.to_thread(quarantine.stat, follow_symlinks=False)
                current_digest = await asyncio.to_thread(_file_sha256, quarantine)
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
                    configured_exists = await asyncio.to_thread(configured.exists)
                    if not configured_exists:
                        await asyncio.to_thread(os.replace, quarantine, configured)
                    await db.rollback()
                    return None
                plan.staging_path = str(quarantine)
                track.staging_path = str(quarantine)
                await db.commit()
                return current_item
            except Exception as exc:
                await db.rollback()
                if attempt == 3 or not _transient_cleanup_error(exc):
                    raise
        await _cleanup_retry_delay(attempt)
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
            if adapter is None:
                failed = True
            else:
                try:
                    if item.source_job_id is None:
                        raise RuntimeError("slskd cleanup requires an exact transfer ID")
                    cleanup_result = await adapter.cancel(*identity, item.source_job_id)
                    if cleanup_result is False:
                        failed = True
                    elif item.track_id is not None:
                        factory = item.session_factory or get_session_factory()
                        await _mark_durable_source_cleanups(
                            factory,
                            {item.track_id: (identity, item.source_job_id)},
                            max_attempts=3,
                        )
                except Exception:
                    failed = True
                    logger.exception("post-import slskd transfer cleanup failed")
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
