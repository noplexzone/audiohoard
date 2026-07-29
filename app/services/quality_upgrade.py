from __future__ import annotations

import contextlib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from mutagen import File as MutagenFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import register_transaction_callbacks
from app.models.import_plan import CollisionState, ImportPlan
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.settings_service import QualityProfile

_LOSSLESS_FORMATS = frozenset({"flac", "alac", "wav", "aiff", "aif"})


@dataclass(frozen=True)
class QualityDuplicateResult:
    deleted_files: int = 0
    review_required: int = 0


@dataclass(frozen=True)
class _ImportedFile:
    track: Track
    plan: ImportPlan
    path: Path
    format_family: str
    bitrate_kbps: int | None
    sample_rate: int | None


def _format_family(value: str | None) -> str:
    normalized = (value or "").casefold().lstrip(".")
    if normalized in {"m4a", "mp4", "aac"}:
        return "m4a/aac"
    return normalized


def _audio_info(path: Path) -> tuple[int | None, int | None]:
    with contextlib.suppress(Exception):
        parsed = MutagenFile(path)
        if parsed is not None and parsed.info is not None:
            bitrate = getattr(parsed.info, "bitrate", None)
            sample_rate = getattr(parsed.info, "sample_rate", None)
            return (
                int(bitrate // 1000) if isinstance(bitrate, int) and bitrate > 0 else None,
                int(sample_rate) if isinstance(sample_rate, int) and sample_rate > 0 else None,
            )
    return None, None


def _audio_bitrate_kbps(path: Path) -> int | None:
    bitrate, _sample_rate = _audio_info(path)
    return bitrate


def _quality_sort_key(item: _ImportedFile, profile: QualityProfile) -> tuple[int, int, int, int]:
    preferred = [_format_family(value) for value in profile.format_preference]
    try:
        preference_score = len(preferred) - preferred.index(item.format_family)
    except ValueError:
        preference_score = -1
    lossless_score = 1 if item.format_family in _LOSSLESS_FORMATS else 0
    bitrate = item.bitrate_kbps or 0
    if item.format_family == "mp3" and bitrate and bitrate < profile.min_mp3_bitrate:
        # Below-threshold MP3s are still comparable, but lose to any in-profile MP3.
        bitrate -= profile.min_mp3_bitrate
    sample_rate = item.sample_rate or 0
    return preference_score, lossless_score, bitrate, sample_rate


def _safe_imported_path(library_root: Path, destination_path: str) -> Path | None:
    if not destination_path:
        return None
    path = Path(destination_path)
    root = library_root.resolve()
    resolved = path.resolve(strict=False)
    if resolved != root and root not in resolved.parents:
        return None
    if path.is_symlink() or not path.is_file():
        return None
    return path


def _delete_paths(paths: tuple[Path, ...]) -> None:
    for path in paths:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()


async def reconcile_album_quality_duplicates(
    db: AsyncSession,
    album_id: int,
    *,
    library_root: Path,
    quality_profile: QualityProfile,
    defer_filesystem_delete: bool = False,
) -> QualityDuplicateResult:
    """Remove lower-quality imported duplicates for one catalog album.

    A duplicate is considered only when two imported rows resolve to the same catalog track
    and their destination files are regular files in the same album folder. A lower-quality
    file is deleted only when the configured quality profile produces a single clear winner;
    tied/ambiguous groups are left in place and marked for review on their import plans.
    """

    rows = (
        await db.execute(
            select(Track, ImportPlan)
            .join(ImportPlan, ImportPlan.track_id == Track.id)
            .where(
                Track.catalog_album_id == album_id,
                Track.catalog_track_id.is_not(None),
                Track.import_state == ImportWorkflowState.imported,
                ImportPlan.status == ImportWorkflowState.imported,
                ImportPlan.destination_path != "",
            )
            .order_by(Track.catalog_track_id, ImportPlan.id)
        )
    ).all()

    by_track_and_folder: dict[tuple[int, Path], list[_ImportedFile]] = defaultdict(list)
    review_required = 0
    for track, plan in rows:
        path = _safe_imported_path(library_root, plan.destination_path)
        if path is None or track.catalog_track_id is None:
            continue
        suffix_family = _format_family(track.file_format or path.suffix)
        bitrate, sample_rate = _audio_info(path)
        if suffix_family == "mp3" and bitrate is None:
            bitrate = _audio_bitrate_kbps(path)
        by_track_and_folder[(track.catalog_track_id, path.parent.resolve())].append(
            _ImportedFile(
                track=track,
                plan=plan,
                path=path,
                format_family=suffix_family,
                bitrate_kbps=bitrate,
                sample_rate=sample_rate,
            )
        )

    to_delete: list[Path] = []
    for duplicates in by_track_and_folder.values():
        if len(duplicates) < 2:
            continue
        ranked = sorted(
            duplicates,
            key=lambda item: (_quality_sort_key(item, quality_profile), item.plan.id or 0),
            reverse=True,
        )
        winner = ranked[0]
        if len(ranked) > 1 and _quality_sort_key(winner, quality_profile) == _quality_sort_key(
            ranked[1], quality_profile
        ):
            review_required += len(ranked)
            for item in ranked:
                item.plan.collision_state = CollisionState.needs_review
                item.plan.error_detail = "same-folder duplicate quality is ambiguous"
                item.plan.status = ImportWorkflowState.needs_review
                item.track.import_state = ImportWorkflowState.needs_review
            continue
        for loser in ranked[1:]:
            to_delete.append(loser.path)
            loser.plan.status = ImportWorkflowState.rolled_back
            loser.plan.rollback_detail = (
                f"lower-quality duplicate removed; retained {winner.path.name}"
            )
            loser.track.import_state = ImportWorkflowState.rolled_back
            loser.track.staging_path = None
            loser.track.source_path = None

    if to_delete:
        unique_paths = tuple(dict.fromkeys(to_delete))
        if defer_filesystem_delete:
            register_transaction_callbacks(
                db, after_commit=lambda: _delete_paths(unique_paths), after_rollback=lambda: None
            )
        else:
            _delete_paths(unique_paths)
    await db.flush()
    return QualityDuplicateResult(
        deleted_files=len(set(to_delete)), review_required=review_required
    )
