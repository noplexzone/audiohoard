from __future__ import annotations

import asyncio
import errno
import logging
import os
import stat
from collections.abc import AsyncIterator, Callable, Collection
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from watchfiles import awatch

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.release import Release
from app.models.settings import AppSetting
from app.models.track import Track
from app.models.workflow import ImportWorkflowState

logger = logging.getLogger(__name__)

_CURSOR_KEY = "library_reconciliation_cursor"
_ELIGIBLE_STATES = (
    LibraryFileState.unknown,
    LibraryFileState.present,
    LibraryFileState.missing,
)
type WatchBatch = set[tuple[object, str]]
type WatchChanges = Callable[[Path], AsyncIterator[WatchBatch]]


@dataclass(frozen=True)
class FileInspection:
    present: bool
    size: int | None = None


def _default_watch(root: Path) -> AsyncIterator[WatchBatch]:
    return awatch(root, debounce=500, step=100)  # type: ignore[return-value]


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _inspect_library_path(root: Path, path: Path) -> FileInspection | None:
    """Return regular-file presence, or None when the path cannot be checked safely."""
    if not root.is_absolute() or not path.is_absolute() or ".." in path.parts:
        return None
    try:
        relative = path.relative_to(root)
    except ValueError:
        return None
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        return None

    current_fd = -1
    try:
        current_fd = os.open(root, _directory_flags())
        for component in relative.parts[:-1]:
            try:
                next_fd = os.open(component, _directory_flags(), dir_fd=current_fd)
            except FileNotFoundError:
                return FileInspection(False)
            os.close(current_fd)
            current_fd = next_fd
        try:
            metadata = os.stat(relative.parts[-1], dir_fd=current_fd, follow_symlinks=False)
        except FileNotFoundError:
            return FileInspection(False)
        if not stat.S_ISREG(metadata.st_mode):
            return None
        return FileInspection(metadata.st_size > 0, metadata.st_size or None)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR, errno.ENOENT}:
            return None
        return None
    finally:
        if current_fd >= 0:
            os.close(current_fd)


async def _recompute_truth(
    db: AsyncSession,
    track_ids: set[int],
    present_sizes: dict[int, int],
) -> None:
    if not track_ids:
        return
    release_ids = set(
        release_id
        for release_id in await db.scalars(
            select(Track.release_id).where(Track.id.in_(track_ids), Track.release_id.is_not(None))
        )
        if release_id is not None
    )
    album_ids = set(
        album_id
        for album_id in await db.scalars(
            select(Track.catalog_album_id).where(
                Track.id.in_(track_ids), Track.catalog_album_id.is_not(None)
            )
        )
        if album_id is not None
    )

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
                ImportWorkflowState.imported if survivors else ImportWorkflowState.needs_review
            )
            if survivors and track_id in present_sizes:
                track.file_size_bytes = present_sizes[track_id]
            elif not survivors:
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
        release.import_state = (
            ImportWorkflowState.imported
            if imported_count
            and (not release.track_count or imported_count >= release.track_count)
            else ImportWorkflowState.needs_review
        )

    for album_id in album_ids:
        expected = set(
            await db.scalars(
                select(CatalogAlbumTrack.id).where(CatalogAlbumTrack.album_id == album_id)
            )
        )
        present = set(
            catalog_track_id
            for catalog_track_id in await db.scalars(
                select(Track.catalog_track_id)
                .join(ImportPlan, ImportPlan.track_id == Track.id)
                .where(
                    Track.catalog_album_id == album_id,
                    Track.catalog_track_id.is_not(None),
                    ImportPlan.status == ImportWorkflowState.imported,
                    ImportPlan.file_state == LibraryFileState.present,
                )
            )
            if catalog_track_id is not None
        )
        album = await db.get(CatalogAlbum, album_id)
        if album is not None:
            album.in_library = bool(expected) and expected <= present


class LibraryReconciliationService:
    """Bounded watcher and sweep reconciliation for imported library files."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        library_root: Path,
        *,
        batch_size: int = 100,
        startup_batches: int = 4,
        periodic_interval: float = 300.0,
        watcher_retry_interval: float = 5.0,
        watch_changes: WatchChanges = _default_watch,
    ) -> None:
        self._session_factory = session_factory
        self._root = Path(os.path.abspath(library_root))
        self._batch_size = max(1, batch_size)
        self._startup_batches = max(1, startup_batches)
        self._periodic_interval = max(0.01, periodic_interval)
        self._watcher_retry_interval = max(0.01, watcher_retry_interval)
        self._watch_changes = watch_changes
        self._lock = asyncio.Lock()
        self._tasks: tuple[asyncio.Task[None], ...] = ()

    @property
    def tasks(self) -> tuple[asyncio.Task[None], ...]:
        return self._tasks

    def _safe_event_path(self, path: Path) -> Path | None:
        normalized = Path(os.path.abspath(path))
        try:
            normalized.relative_to(self._root)
        except ValueError:
            return None
        if ".audiohoard-delete-" in normalized.name:
            return None
        return normalized

    async def _inspect_paths(self, paths: list[Path]) -> dict[Path, FileInspection | None]:
        values = await asyncio.gather(
            *(asyncio.to_thread(_inspect_library_path, self._root, path) for path in paths)
        )
        return dict(zip(paths, values, strict=True))

    async def _apply(
        self,
        candidates: list[tuple[int, Path]],
        inspections: dict[Path, FileInspection | None],
        *,
        cursor: int | None = None,
    ) -> int:
        now = datetime.now(UTC)
        changed = 0
        track_ids: set[int] = set()
        present_sizes: dict[int, int] = {}
        async with self._session_factory() as db:
            for plan_id, expected_path in candidates:
                inspection = inspections.get(expected_path)
                if inspection is None:
                    continue
                presence = inspection.present
                plan = await db.get(ImportPlan, plan_id)
                if (
                    plan is None
                    or plan.status != ImportWorkflowState.imported
                    or plan.file_state not in _ELIGIBLE_STATES
                    or Path(plan.destination_path) != expected_path
                ):
                    continue
                plan.file_state = (
                    LibraryFileState.present if presence else LibraryFileState.missing
                )
                plan.file_checked_at = now
                if presence:
                    plan.file_removed_at = None
                    plan.file_removal_reason = None
                else:
                    plan.file_removed_at = now
                    plan.file_removal_reason = "external"
                if plan.track_id is not None:
                    track_ids.add(plan.track_id)
                    if presence and inspection.size is not None:
                        present_sizes[plan.track_id] = inspection.size
                changed += 1
            await db.flush()
            await _recompute_truth(db, track_ids, present_sizes)
            if cursor is not None:
                row = await db.get(AppSetting, _CURSOR_KEY)
                if row is None:
                    db.add(AppSetting(key=_CURSOR_KEY, value=str(cursor)))
                else:
                    row.value = str(cursor)
            await db.commit()
        return changed

    async def reconcile_paths(self, paths: Collection[Path]) -> int:
        safe_paths = sorted(
            {safe for path in paths if (safe := self._safe_event_path(path)) is not None}
        )
        if not safe_paths:
            return 0
        total = 0
        async with self._lock:
            for start in range(0, len(safe_paths), self._batch_size):
                path_batch = safe_paths[start : start + self._batch_size]
                cursor = 0
                while True:
                    async with self._session_factory() as db:
                        rows = list(
                            await db.execute(
                                select(ImportPlan.id, ImportPlan.destination_path)
                                .where(
                                    ImportPlan.id > cursor,
                                    ImportPlan.destination_path.in_(
                                        [str(path) for path in path_batch]
                                    ),
                                    ImportPlan.status == ImportWorkflowState.imported,
                                    ImportPlan.file_state.in_(_ELIGIBLE_STATES),
                                )
                                .order_by(ImportPlan.id)
                                .limit(self._batch_size)
                            )
                        )
                    if not rows:
                        break
                    candidates = [(plan_id, Path(path)) for plan_id, path in rows]
                    inspections = await self._inspect_paths(
                        sorted({path for _, path in candidates})
                    )
                    total += await self._apply(candidates, inspections)
                    cursor = rows[-1][0]
                    if len(rows) < self._batch_size:
                        break
        if total:
            logger.info(
                "Reconciled %d imported library file record(s) from filesystem events", total
            )
        return total

    async def reconcile_sweep(self, *, max_batches: int | None = None) -> int:
        batches = self._startup_batches if max_batches is None else max(1, max_batches)
        total = 0
        async with self._lock:
            for _ in range(batches):
                async with self._session_factory() as db:
                    cursor_row = await db.get(AppSetting, _CURSOR_KEY)
                    try:
                        cursor = max(0, int(cursor_row.value)) if cursor_row is not None else 0
                    except ValueError:
                        cursor = 0
                    rows = list(
                        await db.execute(
                            select(ImportPlan.id, ImportPlan.destination_path)
                            .where(
                                ImportPlan.id > cursor,
                                ImportPlan.status == ImportWorkflowState.imported,
                                ImportPlan.file_state.in_(_ELIGIBLE_STATES),
                            )
                            .order_by(ImportPlan.id)
                            .limit(self._batch_size)
                        )
                    )
                if not rows:
                    await self._apply([], {}, cursor=0)
                    break
                candidates = [(plan_id, Path(path)) for plan_id, path in rows]
                inspections = await self._inspect_paths(sorted({path for _, path in candidates}))
                total += await self._apply(candidates, inspections, cursor=rows[-1][0])
                if len(rows) < self._batch_size:
                    await self._apply([], {}, cursor=0)
                    break
        if total:
            logger.info(
                "Reconciled %d imported library file record(s) during bounded sweep", total
            )
        return total

    async def startup_reconcile(self) -> int:
        return await self.reconcile_sweep(max_batches=self._startup_batches)

    async def _watch_loop(self) -> None:
        while True:
            try:
                async for changes in self._watch_changes(self._root):
                    paths = {Path(path) for _change, path in changes}
                    await self.reconcile_paths(paths)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Library filesystem watcher failed; retrying")
                logger.debug("Library filesystem watcher failure", exc_info=True)
                await asyncio.sleep(self._watcher_retry_interval)

    async def _periodic_loop(self) -> None:
        while True:
            try:
                await self.reconcile_sweep(max_batches=1)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Periodic library reconciliation failed; retrying later")
                logger.debug("Periodic library reconciliation failure", exc_info=True)
            await asyncio.sleep(self._periodic_interval)

    async def start(self) -> None:
        if any(not task.done() for task in self._tasks):
            return
        self._tasks = (
            asyncio.create_task(self._watch_loop(), name="library-filesystem-watcher"),
            asyncio.create_task(self._periodic_loop(), name="library-periodic-reconciliation"),
        )

    async def stop(self) -> None:
        tasks = self._tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
