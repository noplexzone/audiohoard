from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.media_formats import IMPORTABLE_AUDIO_SUFFIXES
from app.models.catalog_entities import CatalogArtist
from app.models.import_plan import ImportPlan
from app.models.workflow import ImportWorkflowState
from app.naming.convention import _sanitize_segment
from app.services.catalog import _has_symlink_component

_MAX_RESULTS = 500


@dataclass(frozen=True)
class LibraryScanResult:
    matched: int
    orphans: tuple[str, ...]
    missing: tuple[str, ...]
    scanned_files: int


def _safe_file_path(root: Path, path: Path) -> Path | None:
    root_resolved = root.resolve()
    try:
        if _has_symlink_component(root_resolved, path):
            return None
        resolved = path.resolve(strict=False)
        resolved.relative_to(root_resolved)
    except (OSError, ValueError):
        return None
    return resolved


def _audio_files_under(root: Path, scan_root: Path) -> set[Path]:
    files: set[Path] = set()
    if not scan_root.exists():
        return files
    for path in scan_root.rglob("*"):
        if not path.is_file() or path.suffix.casefold() not in IMPORTABLE_AUDIO_SUFFIXES:
            continue
        safe = _safe_file_path(root, path)
        if safe is not None:
            files.add(safe)
    return files


def _resolve_scan_roots(
    library_root: Path, artist_name: str | None
) -> tuple[Path, Path, Path | None]:
    root = library_root.resolve()
    scan_root = root
    artist_filter: Path | None = None
    if artist_name is not None:
        candidate = root / _sanitize_segment(artist_name)
        if not _has_symlink_component(root, candidate):
            try:
                candidate.resolve(strict=False).relative_to(root)
            except (OSError, ValueError):
                pass
            else:
                scan_root = candidate
                artist_filter = candidate.resolve(strict=False)
    return root, scan_root, artist_filter


async def scan_library_filesystem(
    db: AsyncSession, *, library_root: Path, artist_id: int | None = None
) -> LibraryScanResult:
    artist_name: str | None = None
    if artist_id is not None:
        artist = await db.get(CatalogArtist, artist_id)
        if artist is None:
            raise ValueError("catalog artist not found")
        artist_name = artist.name
    root, scan_root, artist_filter = await asyncio.to_thread(
        _resolve_scan_roots, library_root, artist_name
    )

    disk_files = await asyncio.to_thread(_audio_files_under, root, scan_root)
    rows = await db.scalars(
        select(ImportPlan.destination_path).where(
            ImportPlan.status == ImportWorkflowState.imported,
            ImportPlan.destination_path != "",
        )
    )
    db_files: set[Path] = set()
    for destination in rows:
        safe = _safe_file_path(root, Path(destination))
        if safe is None:
            continue
        if artist_filter is not None:
            try:
                safe.relative_to(artist_filter)
            except ValueError:
                continue
        db_files.add(safe)

    orphans_all = sorted(str(path) for path in disk_files - db_files)
    missing_all = sorted(str(path) for path in db_files - disk_files)
    return LibraryScanResult(
        matched=len(disk_files & db_files),
        orphans=tuple(orphans_all[:_MAX_RESULTS]),
        missing=tuple(missing_all[:_MAX_RESULTS]),
        scanned_files=len(disk_files),
    )
