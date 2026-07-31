from __future__ import annotations

import asyncio
import errno
import os
import stat
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4
from weakref import WeakValueDictionary

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.models.import_plan import (
    DeletionOperation,
    DeletionOperationState,
    ImportPlan,
    LibraryFileState,
)
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState

_TARGET_LOCKS: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()


class LibraryRemovalError(RuntimeError):
    """A requested permanent removal was absent, ambiguous, or unsafe."""


@dataclass(frozen=True)
class RemovalResult:
    deleted_files: int
    affected_track_ids: tuple[int, ...]
    already_removed: bool = False
    cleanup_pending: bool = False


@dataclass
class _Target:
    plan: ImportPlan
    original: Path
    temporary: Path
    root: Path
    root_fd: int
    parent_fd: int | None
    parent_parts: tuple[str, ...]
    expected_device: int | None
    expected_inode: int | None
    missing: bool
    staged: bool = False

    @property
    def original_name(self) -> str:
        return self.original.name

    @property
    def temporary_name(self) -> str:
        return self.temporary.name

    def close(self) -> None:
        if self.parent_fd is not None:
            os.close(self.parent_fd)
            self.parent_fd = None
        if self.root_fd >= 0:
            os.close(self.root_fd)
            self.root_fd = -1


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _open_target(
    library_root: Path, destination: Path, *, temporary: Path | None = None
) -> _Target:
    if (
        not library_root.is_absolute()
        or not destination.is_absolute()
        or ".." in destination.parts
    ):
        raise LibraryRemovalError("Library file is not safe to remove")
    try:
        root = library_root.resolve(strict=True)
        relative = destination.relative_to(root)
    except (OSError, ValueError) as exc:
        raise LibraryRemovalError("Library file is not safe to remove") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise LibraryRemovalError("Library file is not safe to remove")

    root_fd = -1
    parent_fd: int | None = None
    missing = False
    try:
        root_fd = os.open(root, _directory_flags())
        parent_fd = os.dup(root_fd)
        for component in relative.parts[:-1]:
            try:
                next_fd = os.open(component, _directory_flags(), dir_fd=parent_fd)
            except FileNotFoundError:
                missing = True
                os.close(parent_fd)
                parent_fd = None
                break
            os.close(parent_fd)
            parent_fd = next_fd
        metadata: os.stat_result | None = None
        if parent_fd is not None:
            try:
                metadata = os.stat(relative.parts[-1], dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                missing = True
            if metadata is not None and not stat.S_ISREG(metadata.st_mode):
                raise LibraryRemovalError("Library file is not safe to remove")
        temporary_path = temporary or destination.with_name(
            f".{destination.name}.audiohoard-delete-{uuid4().hex}"
        )
        if temporary_path.parent != destination.parent or temporary_path.name == destination.name:
            raise LibraryRemovalError("Library file is not safe to remove")
        target = _Target(
            plan=None,  # type: ignore[arg-type]
            original=destination,
            temporary=temporary_path,
            root=root,
            root_fd=root_fd,
            parent_fd=parent_fd,
            parent_parts=tuple(relative.parts[:-1]),
            expected_device=metadata.st_dev if metadata is not None else None,
            expected_inode=metadata.st_ino if metadata is not None else None,
            missing=missing,
        )
        root_fd = -1
        parent_fd = None
        return target
    except LibraryRemovalError:
        raise
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise LibraryRemovalError("Library file is not safe to remove") from exc
        raise LibraryRemovalError("Library file could not be inspected safely") from exc
    finally:
        if parent_fd is not None:
            os.close(parent_fd)
        if root_fd >= 0:
            os.close(root_fd)


def _verify_attached(target: _Target) -> None:
    if target.parent_fd is None:
        return
    current_fd = os.dup(target.root_fd)
    try:
        for component in target.parent_parts:
            next_fd = os.open(component, _directory_flags(), dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        if not _same_inode(os.fstat(current_fd), os.fstat(target.parent_fd)):
            raise LibraryRemovalError("Library file changed before removal")
    except LibraryRemovalError:
        raise
    except OSError as exc:
        raise LibraryRemovalError("Library file changed before removal") from exc
    finally:
        os.close(current_fd)


def _matches_expected(target: _Target, name: str) -> bool:
    if target.parent_fd is None or target.expected_device is None or target.expected_inode is None:
        return False
    try:
        metadata = os.stat(name, dir_fd=target.parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_dev == target.expected_device
        and metadata.st_ino == target.expected_inode
    )


def _entry_exists(target: _Target, name: str) -> bool:
    if target.parent_fd is None:
        return False
    try:
        os.stat(name, dir_fd=target.parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _stage_target(target: _Target) -> None:
    if target.missing:
        return
    if target.parent_fd is None:
        raise LibraryRemovalError("Library file changed before removal")
    _verify_attached(target)
    if not _matches_expected(target, target.original_name):
        raise LibraryRemovalError("Library file changed before removal")
    if target.temporary_name != target.original_name:
        try:
            os.stat(target.temporary_name, dir_fd=target.parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise LibraryRemovalError("Library file changed before removal")
    os.rename(
        target.original_name,
        target.temporary_name,
        src_dir_fd=target.parent_fd,
        dst_dir_fd=target.parent_fd,
    )
    os.fsync(target.parent_fd)
    target.staged = True


def _restore_target(target: _Target) -> None:
    if not target.staged or target.parent_fd is None:
        return
    _verify_attached(target)
    if not _matches_expected(target, target.temporary_name):
        raise LibraryRemovalError("Library file could not be restored safely")
    try:
        os.stat(target.original_name, dir_fd=target.parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        pass
    else:
        raise LibraryRemovalError("Library file could not be restored safely")
    os.rename(
        target.temporary_name,
        target.original_name,
        src_dir_fd=target.parent_fd,
        dst_dir_fd=target.parent_fd,
    )
    os.fsync(target.parent_fd)
    target.staged = False


def _unlink_target(target: _Target) -> bool:
    if not target.staged or target.parent_fd is None:
        return True
    _verify_attached(target)
    if not _matches_expected(target, target.temporary_name):
        return False
    os.unlink(target.temporary_name, dir_fd=target.parent_fd)
    os.fsync(target.parent_fd)
    target.staged = False
    return True


def _remove_empty_parents(root: Path, parent: Path) -> None:
    """Remove empty non-symlink directories from album upward, never root itself."""
    try:
        relative = parent.relative_to(root)
    except ValueError:
        return
    parts = relative.parts
    for depth in range(len(parts), 0, -1):
        grandparent_fd = os.open(root, _directory_flags())
        child_fd = -1
        try:
            for component in parts[: depth - 1]:
                next_fd = os.open(component, _directory_flags(), dir_fd=grandparent_fd)
                os.close(grandparent_fd)
                grandparent_fd = next_fd
            child = parts[depth - 1]
            child_fd = os.open(child, _directory_flags(), dir_fd=grandparent_fd)
            if os.listdir(child_fd):
                return
            os.close(child_fd)
            child_fd = -1
            os.rmdir(child, dir_fd=grandparent_fd)
            os.fsync(grandparent_fd)
        except (FileNotFoundError, OSError):
            return
        finally:
            if child_fd >= 0:
                os.close(child_fd)
            os.close(grandparent_fd)


def invalidate_track_previews(cache_root: Path, track_ids: tuple[int, ...]) -> None:
    for track_id in track_ids:
        for entry in cache_root.glob(f"{track_id}-*.mp3"):
            with suppress(OSError):
                if stat.S_ISREG(entry.stat(follow_symlinks=False).st_mode):
                    entry.unlink()


async def _recompute_database_truth(
    db: AsyncSession, plans: list[ImportPlan], operations: list[DeletionOperation]
) -> tuple[int, ...]:
    now = datetime.now(UTC)
    track_ids = tuple(sorted({int(plan.track_id) for plan in plans if plan.track_id is not None}))
    release_ids = tuple(sorted({plan.release_id for plan in plans}))
    album_ids = tuple(
        sorted(
            {
                int(album_id)
                for album_id in await db.scalars(
                    select(Track.catalog_album_id).where(
                        Track.id.in_(track_ids), Track.catalog_album_id.is_not(None)
                    )
                )
                if album_id is not None
            }
        )
    )
    for plan in plans:
        await db.refresh(plan)
        if (
            plan.status != ImportWorkflowState.imported
            or plan.file_state != LibraryFileState.present
        ):
            raise LibraryRemovalError("Library state changed before removal")
        plan.status = ImportWorkflowState.removed
        plan.file_state = LibraryFileState.removed
        plan.file_checked_at = now
        plan.file_removed_at = now
        plan.file_removal_reason = "user"
    await db.flush()

    for track_id in track_ids:
        survivors = int(
            await db.scalar(
                select(func.count(ImportPlan.id)).where(
                    ImportPlan.track_id == track_id,
                    ImportPlan.status == ImportWorkflowState.imported,
                    ImportPlan.file_state == LibraryFileState.present,
                )
            )
            or 0
        )
        track = await db.get(Track, track_id)
        if track is not None:
            track.import_state = (
                ImportWorkflowState.imported if survivors else ImportWorkflowState.removed
            )
            if not survivors:
                track.file_size_bytes = None
    await db.flush()

    for release_id in release_ids:
        release = await db.get(Release, release_id)
        if release is None:
            continue
        imported_count = int(
            await db.scalar(
                select(func.count(Track.id)).where(
                    Track.release_id == release_id,
                    Track.import_state == ImportWorkflowState.imported,
                )
            )
            or 0
        )
        if not imported_count:
            release.import_state = ImportWorkflowState.removed
        elif release.track_count and imported_count < release.track_count:
            release.import_state = ImportWorkflowState.needs_review
        else:
            release.import_state = ImportWorkflowState.imported

    for album_id in album_ids:
        expected = set(
            await db.scalars(
                select(CatalogAlbumTrack.id).where(CatalogAlbumTrack.album_id == album_id)
            )
        )
        present = set(
            await db.scalars(
                select(Track.catalog_track_id)
                .join(ImportPlan, ImportPlan.track_id == Track.id)
                .where(
                    Track.catalog_album_id == album_id,
                    Track.catalog_track_id.is_not(None),
                    Track.import_state == ImportWorkflowState.imported,
                    ImportPlan.status == ImportWorkflowState.imported,
                    ImportPlan.file_state == LibraryFileState.present,
                )
            )
        )
        album = await db.get(CatalogAlbum, album_id)
        if album is not None:
            album.in_library = bool(expected) and expected <= present
    for operation in operations:
        operation.state = DeletionOperationState.committed
        operation.error_detail = None
    return track_ids


async def _operations_prepared(db: AsyncSession, operation_ids: tuple[int, ...]) -> bool:
    states = list(
        await db.scalars(
            select(DeletionOperation.state).where(DeletionOperation.id.in_(operation_ids))
        )
    )
    return len(states) == len(operation_ids) and all(
        state == DeletionOperationState.prepared for state in states
    )


async def _mark_finalized(
    db: AsyncSession, operation_ids: tuple[int, ...], *, error: str | None = None
) -> None:
    now = datetime.now(UTC)
    rows = list(
        await db.scalars(select(DeletionOperation).where(DeletionOperation.id.in_(operation_ids)))
    )
    for row in rows:
        row.state = DeletionOperationState.finalized
        row.finalized_at = now
        row.error_detail = error
    await db.commit()


async def _remove_plans(
    db: AsyncSession,
    plans: list[ImportPlan],
    *,
    library_root: Path,
    cache_root: Path,
) -> RemovalResult:
    if not plans:
        return RemovalResult(0, (), already_removed=True)
    lock_keys = sorted({f"plan:{plan.id}" for plan in plans})
    async with AsyncExitStack() as stack:
        for key in lock_keys:
            await stack.enter_async_context(_TARGET_LOCKS.setdefault(key, asyncio.Lock()))
        targets: list[_Target] = []
        database_committed = False
        try:
            seen_paths: set[Path] = set()
            for plan in plans:
                destination = Path(plan.destination_path)
                if destination in seen_paths:
                    raise LibraryRemovalError("Library removal targets are ambiguous")
                seen_paths.add(destination)
                target = await asyncio.to_thread(_open_target, library_root, destination)
                target.plan = plan
                targets.append(target)

            group_id = str(uuid4())
            operations = [
                DeletionOperation(
                    group_id=group_id,
                    import_plan_id=target.plan.id,
                    original_path=str(target.original),
                    temporary_path=str(target.temporary),
                    expected_device=target.expected_device,
                    expected_inode=target.expected_inode,
                    file_was_missing=target.missing,
                    state=DeletionOperationState.prepared,
                )
                for target in targets
            ]
            db.add_all(operations)
            await db.commit()
            for target in targets:
                await asyncio.to_thread(_stage_target, target)
            operation_ids = tuple(operation.id for operation in operations)
            try:
                track_ids = await _recompute_database_truth(db, plans, operations)
                await db.commit()
                database_committed = True
            except BaseException:
                # Once commit acknowledgement is lost, restoration is safe only when
                # the durable journal can be positively verified as entirely prepared.
                # Any unreadable, mixed, committed, or finalized state is recovery work.
                database_committed = True
                await db.rollback()
                restore_is_safe = await _operations_prepared(db, operation_ids)
                if restore_is_safe:
                    database_committed = False
                    for target in reversed(targets):
                        await asyncio.to_thread(_restore_target, target)
                    with suppress(Exception):
                        await _mark_finalized(
                            db,
                            operation_ids,
                            error="rolled back before database commit",
                        )
                raise

            cleanup_pending = False
            for target in targets:
                try:
                    if not await asyncio.to_thread(_unlink_target, target):
                        cleanup_pending = True
                except OSError:
                    cleanup_pending = True
            await asyncio.to_thread(invalidate_track_previews, cache_root, track_ids)
            if not cleanup_pending:
                await _mark_finalized(db, tuple(operation.id for operation in operations))
                for target in targets:
                    await asyncio.to_thread(
                        _remove_empty_parents, target.root, target.original.parent
                    )
            return RemovalResult(
                deleted_files=sum(not target.missing for target in targets),
                affected_track_ids=track_ids,
                cleanup_pending=cleanup_pending,
            )
        except LibraryRemovalError:
            await db.rollback()
            if not database_committed:
                for target in reversed(targets):
                    with suppress(Exception):
                        await asyncio.to_thread(_restore_target, target)
            raise
        except BaseException as exc:
            await db.rollback()
            restore_failed = False
            if not database_committed:
                for target in reversed(targets):
                    try:
                        await asyncio.to_thread(_restore_target, target)
                    except Exception:
                        restore_failed = True
            if isinstance(exc, asyncio.CancelledError):
                raise
            detail = (
                "Library removal recovery is required"
                if database_committed or restore_failed
                else "Library removal could not be completed"
            )
            raise LibraryRemovalError(detail) from exc
        finally:
            for target in targets:
                target.close()


async def remove_imported_track(
    db: AsyncSession,
    track_id: int,
    *,
    library_root: Path,
    cache_root: Path,
) -> RemovalResult:
    track = await db.get(Track, track_id)
    if track is None:
        raise LibraryRemovalError("Imported track was not found")
    plan = (
        await db.scalars(
            select(ImportPlan)
            .where(
                ImportPlan.track_id == track_id,
                ImportPlan.status == ImportWorkflowState.imported,
                ImportPlan.file_state == LibraryFileState.present,
            )
            .order_by(ImportPlan.id.desc())
            .limit(1)
        )
    ).first()
    if plan is None:
        return RemovalResult(0, (track_id,), already_removed=True)
    return await _remove_plans(db, [plan], library_root=library_root, cache_root=cache_root)


async def remove_catalog_album(
    db: AsyncSession,
    catalog_album_id: int,
    *,
    library_root: Path,
    cache_root: Path,
) -> RemovalResult:
    album = await db.get(CatalogAlbum, catalog_album_id)
    if album is None:
        raise LibraryRemovalError("Catalog album was not found")
    plans = list(
        await db.scalars(
            select(ImportPlan)
            .join(Track, ImportPlan.track_id == Track.id)
            .where(
                Track.catalog_album_id == catalog_album_id,
                ImportPlan.status == ImportWorkflowState.imported,
                ImportPlan.file_state == LibraryFileState.present,
            )
            .order_by(ImportPlan.id)
        )
    )
    return await _remove_plans(db, plans, library_root=library_root, cache_root=cache_root)


async def remove_imported_release_group(
    db: AsyncSession,
    *,
    release_id: int | None,
    artist_name: str,
    album_title: str,
    year: str,
    library_root: Path,
    cache_root: Path,
) -> RemovalResult:
    if release_id is not None:
        if await db.get(Release, release_id) is None:
            raise LibraryRemovalError("Imported release was not found")
        track_filter = Track.release_id == release_id
    else:
        artist_name = artist_name.strip()
        album_title = album_title.strip()
        if not artist_name or not album_title:
            raise LibraryRemovalError("Imported release identity is invalid")
        track_filter = (
            (Track.release_id.is_(None))
            & (
                func.coalesce(
                    func.nullif(Track.album_artist, ""),
                    func.nullif(Track.artist, ""),
                    "Unknown",
                )
                == artist_name
            )
            & (func.coalesce(func.nullif(Track.album, ""), "Unknown") == album_title)
            & (func.coalesce(func.nullif(Track.year, ""), "") == year.strip())
        )
    track_ids = tuple(
        sorted(int(value) for value in await db.scalars(select(Track.id).where(track_filter)))
    )
    if not track_ids:
        raise LibraryRemovalError("Imported release was not found")
    plans = list(
        await db.scalars(
            select(ImportPlan)
            .join(Track, ImportPlan.track_id == Track.id)
            .where(
                track_filter,
                ImportPlan.status == ImportWorkflowState.imported,
                ImportPlan.file_state == LibraryFileState.present,
            )
            .order_by(ImportPlan.id)
        )
    )
    if not plans:
        return RemovalResult(0, track_ids, already_removed=True)
    return await _remove_plans(db, plans, library_root=library_root, cache_root=cache_root)


def _target_from_operation(root: Path, operation: DeletionOperation) -> _Target:
    target = _open_target(
        root, Path(operation.original_path), temporary=Path(operation.temporary_path)
    )
    target.expected_device = operation.expected_device
    target.expected_inode = operation.expected_inode
    target.missing = operation.file_was_missing
    return target


async def recover_deletion_operations(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    library_root: Path,
    cache_root: Path,
) -> None:
    async with session_factory() as db:
        groups = list(
            await db.scalars(
                select(DeletionOperation.group_id)
                .where(DeletionOperation.state != DeletionOperationState.finalized)
                .distinct()
            )
        )
    for group_id in groups:
        async with session_factory() as db:
            operations = list(
                await db.scalars(
                    select(DeletionOperation)
                    .where(DeletionOperation.group_id == group_id)
                    .order_by(DeletionOperation.id)
                )
            )
            active = [row for row in operations if row.state != DeletionOperationState.finalized]
            if not active:
                continue
            targets: list[_Target] = []
            try:
                for operation in active:
                    targets.append(
                        await asyncio.to_thread(_target_from_operation, library_root, operation)
                    )
                if all(row.state == DeletionOperationState.prepared for row in active):
                    for target in reversed(targets):
                        if target.parent_fd is None:
                            continue
                        if _matches_expected(target, target.temporary_name):
                            target.staged = True
                            await asyncio.to_thread(_restore_target, target)
                        elif _matches_expected(target, target.original_name) or target.missing:
                            continue
                        else:
                            raise LibraryRemovalError("Prepared library removal is ambiguous")
                    await _mark_finalized(
                        db,
                        tuple(row.id for row in active),
                        error="restored during startup recovery",
                    )
                elif all(row.state == DeletionOperationState.committed for row in active):
                    track_ids: set[int] = set()
                    cleanup_pending = False
                    for row, target in zip(active, targets, strict=True):
                        plan = await db.get(ImportPlan, row.import_plan_id)
                        if plan is not None and plan.track_id is not None:
                            track_ids.add(plan.track_id)
                        if target.parent_fd is None:
                            if not row.file_was_missing:
                                cleanup_pending = True
                        elif _matches_expected(target, target.original_name):
                            target.missing = False
                            try:
                                await asyncio.to_thread(_stage_target, target)
                            except (LibraryRemovalError, OSError):
                                cleanup_pending = True
                            else:
                                if not await asyncio.to_thread(_unlink_target, target):
                                    cleanup_pending = True
                        elif _matches_expected(target, target.temporary_name):
                            target.staged = True
                            if not await asyncio.to_thread(_unlink_target, target):
                                cleanup_pending = True
                        elif _entry_exists(target, target.original_name) or _entry_exists(
                            target, target.temporary_name
                        ):
                            cleanup_pending = True
                    if not cleanup_pending:
                        await asyncio.to_thread(
                            invalidate_track_previews, cache_root, tuple(sorted(track_ids))
                        )
                        await _mark_finalized(db, tuple(row.id for row in active))
                        for target in targets:
                            await asyncio.to_thread(
                                _remove_empty_parents, target.root, target.original.parent
                            )
                else:
                    raise LibraryRemovalError("Deletion journal group has inconsistent state")
            except Exception:
                await db.rollback()
                continue
            finally:
                for target in targets:
                    target.close()
