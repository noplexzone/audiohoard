from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import String, and_, case, cast, exists, func, literal, or_, select, text
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import run_with_sqlite_lock_retry
from app.media_formats import IMPORTABLE_AUDIO_SUFFIXES
from app.models.acquisition_claim import (
    AcquisitionDispatchClaim,
    CatalogReleaseAcquisitionClaim,
)
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyJobOwnership,
)
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState, ReviewDecision
from app.naming.convention import _sanitize_segment
from app.services.catalog_artist_credits import catalog_track_artist_name
from app.services.catalog_manifest import catalog_manifest_issue
from app.services.release_editions import project_release_families
from app.services.session_contract import reject_pending_orm_changes
from app.settings_service import QualityProfile, get_runtime_settings

UNKNOWN = "Unknown"
_DEFAULT_PAGE_SIZE = 50
_MAX_PAGE_SIZE = 200

_VALID_LIBRARY_SORTS = frozenset({"title", "artist", "album", "year", "source", "added"})
_VALID_ARTIST_SORTS = frozenset({"name", "tracks", "albums", "duration"})
_VALID_WATCHLIST_SORTS = frozenset({"name", "downloaded", "wanted"})
_BITRATE_RE = re.compile(r"(?P<bitrate>\d{2,4})\s*(?:kbps|k)?", re.IGNORECASE)


def _format_family(value: str | None) -> str:
    normalized = (value or "").strip().casefold().lstrip(".")
    if normalized in {"m4a", "mp4", "aac"}:
        return "m4a/aac"
    if normalized.startswith("mp3"):
        return "mp3"
    return normalized


def _known_mp3_bitrate(file_format: str | None) -> int | None:
    normalized = (file_format or "").casefold()
    if "mp3" not in normalized:
        return None
    for match in _BITRATE_RE.finditer(normalized):
        bitrate = int(match.group("bitrate"))
        if bitrate != 3:
            return bitrate
    return None


def track_meets_quality(file_format: str | None, profile: QualityProfile) -> bool:
    """Return whether an imported catalog track already satisfies the runtime profile."""
    family = _format_family(file_format)
    if not family:
        return True
    enabled = [_format_family(value) for value in profile.enabled_formats]
    preferred = [
        _format_family(value)
        for value in profile.format_preference
        if _format_family(value) in enabled
    ]
    if family not in preferred:
        return False
    best_family = next((value for value in preferred if value), "")
    if best_family and family != best_family:
        return False
    if family != "mp3":
        return True
    bitrate = _known_mp3_bitrate(file_format)
    return bitrate is None or bitrate >= profile.min_mp3_bitrate


def _artist_expr() -> Any:
    return func.coalesce(
        func.nullif(Track.album_artist, ""),
        func.nullif(Track.artist, ""),
        UNKNOWN,
    )


def _album_expr() -> Any:
    return func.coalesce(func.nullif(Track.album, ""), UNKNOWN)


def _year_expr() -> Any:
    return func.coalesce(func.nullif(Track.year, ""), "")


async def _count_album_groups(db: AsyncSession, *filters: Any) -> int:
    release_stmt = select(func.count(func.distinct(Track.release_id))).where(
        Track.release_id.is_not(None), *filters
    )
    release_count = int((await db.scalar(release_stmt)) or 0)
    fallback_groups = (
        select(
            _artist_expr().label("artist"),
            _album_expr().label("album"),
            _year_expr().label("year"),
        )
        .where(Track.release_id.is_(None), *filters)
        .group_by(_artist_expr(), _album_expr(), _year_expr())
        .subquery()
    )
    fallback_count = int((await db.scalar(select(func.count()).select_from(fallback_groups))) or 0)
    return release_count + fallback_count


@dataclass
class LibraryStats:
    track_count: int
    artist_count: int
    album_count: int
    total_duration_sec: int
    total_bytes: int
    format_breakdown: dict[str, int]
    source_breakdown: dict[str, int]


@dataclass
class TrackRow:
    id: int
    title: str
    artist: str
    album: str
    year: str | None
    source: str
    source_path: str | None
    file_path: str
    acquisition_state: str
    import_state: str
    duration_sec: int | None
    mbid: str | None
    fmt: str
    disc: int | None
    disc_total: int | None
    track_no: int | None
    file_size_bytes: int | None
    fingerprint_state: str
    deezer_id: str | None
    acoustid: str | None
    release_id: int | None
    artwork_url: str | None = None
    library_file_state: str = LibraryFileState.unknown


@dataclass
class Page[T]:
    items: list[T]
    total: int
    page: int
    per_page: int

    @property
    def total_pages(self) -> int:
        if self.per_page <= 0:
            return 1
        return max(1, math.ceil(self.total / self.per_page))

    @property
    def has_prev(self) -> bool:
        return self.page > 1

    @property
    def has_next(self) -> bool:
        return self.page < self.total_pages


@dataclass
class LibraryArtistRow:
    id: int | None
    name: str
    detail_url: str
    artwork_url: str | None
    primary_metadata_provider: str | None
    release_count: int
    downloaded_file_count: int
    wanted_release_count: int
    watchlisted: bool
    complete_release_count: int = 0
    partial_release_count: int = 0
    unknown_release_count: int = 0
    local_release_count: int = 0


@dataclass(frozen=True)
class _ProviderFamilyCounts:
    release_count: int = 0
    complete_count: int = 0
    partial_count: int = 0
    unknown_count: int = 0
    local_count: int = 0


async def _provider_family_counts(
    db: AsyncSession,
    identity_ids: set[int],
    album_progress: Any,
) -> dict[int, _ProviderFamilyCounts]:
    """Aggregate exact-edition progress over centralized provider release families."""
    if not identity_ids:
        return {}
    releases = list(
        (
            await db.scalars(
                select(CatalogAlbumProvider)
                .where(CatalogAlbumProvider.artist_identity_id.in_(identity_ids))
                .options(selectinload(CatalogAlbumProvider.artist_identity))
            )
        ).all()
    )
    releases_by_identity: dict[int, list[CatalogAlbumProvider]] = {}
    for release in releases:
        releases_by_identity.setdefault(release.artist_identity_id, []).append(release)
    album_ids = {release.catalog_album_id for release in releases if release.catalog_album_id}
    progress_by_album = {
        int(row["album_id"]): (int(row["manifest_count"]), int(row["present_count"]))
        for row in (
            (
                await db.execute(
                    select(album_progress).where(album_progress.c.album_id.in_(album_ids))
                )
            ).mappings()
            if album_ids
            else []
        )
    }
    result: dict[int, _ProviderFamilyCounts] = {}
    for identity_id in identity_ids:
        complete = partial = unknown = local = 0
        families = project_release_families(releases_by_identity.get(identity_id, []))
        for family in families:
            # Progress belongs to the displayed canonical edition only. A compatible
            # sibling may collapse the count, but can never contribute its files.
            album_id = family.display_release.catalog_album_id
            if album_id is None:
                unknown += 1
                continue
            manifest, present = progress_by_album.get(album_id, (0, 0))
            if manifest == 0:
                unknown += 1
            elif present >= manifest:
                complete += 1
            elif present > 0:
                partial += 1
            if present > 0:
                local += 1
        result[identity_id] = _ProviderFamilyCounts(
            release_count=len(families),
            complete_count=complete,
            partial_count=partial,
            unknown_count=unknown,
            local_count=local,
        )
    return result


@dataclass
class MissingReleaseRow:
    id: int
    artist_name: str
    title: str
    year: str | None
    artwork_url: str | None
    wanted_track_count: int
    downloaded_track_count: int
    manifest_known: bool = False


@dataclass(frozen=True)
class ReleaseLibraryFile:
    catalog_track_id: int
    track_id: int
    file_format: str
    file_size_bytes: int | None
    source: str


@dataclass(frozen=True)
class ArtistReleaseRollup:
    tracks_in_library: int
    tracks_total: int
    releases_complete: int
    releases_total: int


@dataclass(frozen=True)
class ReleaseProgress:
    wanted_track_count: int
    downloaded_track_count: int
    downloaded_catalog_track_ids: frozenset[int] = frozenset()
    library_track_ids: tuple[tuple[int, int], ...] = ()
    library_files: tuple[ReleaseLibraryFile, ...] = ()
    manifest_known: bool = False

    @property
    def complete(self) -> bool:
        return (
            self.manifest_known
            and self.wanted_track_count > 0
            and (self.downloaded_track_count >= self.wanted_track_count)
        )

    def track_state(self, catalog_track_id: int) -> str:
        return (
            LibraryFileState.present
            if catalog_track_id in self.downloaded_catalog_track_ids
            else LibraryFileState.missing
        )

    def library_track_id(self, catalog_track_id: int) -> int | None:
        return dict(self.library_track_ids).get(catalog_track_id)

    def library_file(self, catalog_track_id: int) -> ReleaseLibraryFile | None:
        return next(
            (item for item in self.library_files if item.catalog_track_id == catalog_track_id),
            None,
        )


def aggregate_artist_release_rollup(
    release_progress: Iterable[ReleaseProgress],
) -> ArtistReleaseRollup:
    manifest_known = [progress for progress in release_progress if progress.manifest_known]
    return ArtistReleaseRollup(
        tracks_in_library=sum(progress.downloaded_track_count for progress in manifest_known),
        tracks_total=sum(progress.wanted_track_count for progress in manifest_known),
        releases_complete=sum(
            1
            for progress in manifest_known
            if progress.downloaded_track_count >= progress.wanted_track_count
            and progress.wanted_track_count > 0
        ),
        releases_total=len(manifest_known),
    )


@dataclass
class WatchlistedArtistRow:
    id: int
    name: str
    artwork_url: str | None
    album_count: int
    single_ep_count: int
    compilation_count: int
    total_releases: int
    watchlisted: bool = True


@dataclass
class ArtistRow:
    display_name: str
    track_count: int
    album_count: int
    total_duration_sec: int | None
    min_year: str | None
    max_year: str | None
    formats: list[str] = field(default_factory=list)


@dataclass
class AlbumGroup:
    album: str
    year: str | None
    release_id: int | None
    release_mbid: str | None
    label: str | None
    country: str | None
    catalog_number: str | None
    tracks: list[TrackRow] = field(default_factory=list)


@dataclass
class ArtistDetail:
    display_name: str
    track_count: int
    album_count: int
    total_duration_sec: int
    albums: list[AlbumGroup]
    page: int = 1
    per_page: int = _DEFAULT_PAGE_SIZE
    total_track_pages: int = 1
    has_prev: bool = False
    has_next: bool = False


def _normalize_title(t: Track) -> str:
    return t.title or UNKNOWN


def _normalize_artist(t: Track) -> str:
    return (t.album_artist or None) or (t.artist or None) or UNKNOWN


def _normalize_album(t: Track) -> str:
    return t.album or UNKNOWN


def _track_file_path(t: Track) -> str:
    if "import_plans" not in sa_inspect(t).unloaded:
        imported_destinations = [
            plan.destination_path.strip()
            for plan in t.import_plans
            if plan.status == ImportWorkflowState.imported
            and plan.file_state == LibraryFileState.present
            and plan.destination_path.strip()
        ]
        if imported_destinations:
            return imported_destinations[-1]
    return ""


def to_track_row(t: Track) -> TrackRow:
    file_state = LibraryFileState.unknown
    if "import_plans" not in sa_inspect(t).unloaded:
        imported_plans = [
            plan for plan in t.import_plans if plan.status == ImportWorkflowState.imported
        ]
        if imported_plans:
            file_state = imported_plans[-1].file_state
    return TrackRow(
        id=t.id,
        title=_normalize_title(t),
        artist=_normalize_artist(t),
        album=_normalize_album(t),
        year=t.year,
        source=t.source,
        source_path=t.source_path,
        file_path=_track_file_path(t),
        acquisition_state=str(t.acquisition_state),
        import_state=str(t.import_state),
        duration_sec=t.duration_sec,
        mbid=t.mbid,
        fmt=t.file_format or "",
        disc=t.disc,
        disc_total=t.disc_total,
        track_no=t.track_no,
        file_size_bytes=t.file_size_bytes,
        fingerprint_state=str(t.fingerprint_state),
        deezer_id=t.deezer_id,
        acoustid=t.acoustid,
        release_id=t.release_id,
        artwork_url=t.catalog_album.artwork_url if t.catalog_album else None,
        library_file_state=file_state,
    )


def _clamp_per_page(per_page: int) -> int:
    return max(1, min(per_page, _MAX_PAGE_SIZE))


def _page_offset(page: int, per_page: int) -> int:
    return (max(1, page) - 1) * per_page


def _clamp_page(page: int, total: int, per_page: int) -> int:
    if total == 0:
        return 1
    last = max(1, math.ceil(total / per_page))
    return min(page, last)


def _non_empty(column: Any) -> Any:
    return and_(column.is_not(None), func.length(func.trim(column)) > 0)


def _library_artifact_filter() -> Any:
    imported_destination = exists(
        select(ImportPlan.id).where(
            ImportPlan.track_id == Track.id,
            ImportPlan.status == ImportWorkflowState.imported,
            _non_empty(ImportPlan.destination_path),
        )
    )
    return and_(
        Track.acquisition_state == AcquisitionState.downloaded,
        Track.import_state == ImportWorkflowState.imported,
        Track.file_size_bytes.is_not(None),
        Track.file_size_bytes > 0,
        imported_destination,
    )


def _present_import_plan_exists() -> Any:
    return exists(
        select(ImportPlan.id).where(
            ImportPlan.track_id == Track.id,
            ImportPlan.status == ImportWorkflowState.imported,
            ImportPlan.file_state == LibraryFileState.present,
            _non_empty(ImportPlan.destination_path),
        )
    )


def _present_library_artifact_filter() -> Any:
    return and_(_library_artifact_filter(), _present_import_plan_exists())


@dataclass(frozen=True, slots=True)
class _FilesystemReleaseEvidence:
    file_count: int
    track_keys: frozenset[tuple[int, int]]


_TRACK_NUMBER_PREFIX = re.compile(r"^(?:(?P<disc>\d{1,2})[-_.])?(?P<track>\d{1,3})(?:\D|$)")
_DISC_FOLDER = re.compile(r"^(?:cd|disc)[ _.-]?(\d{1,2})$", re.IGNORECASE)
_DirectoryState = tuple[tuple[str, int], ...]
_RELEASE_EVIDENCE_CACHE: dict[
    tuple[int, str], tuple[_DirectoryState, _FilesystemReleaseEvidence]
] = {}
_RELEASE_EVIDENCE_CACHE_MAX_ENTRIES = 2000


def _clear_release_evidence_cache() -> None:
    _RELEASE_EVIDENCE_CACHE.clear()


def _has_symlink_component(root: Path, path: Path) -> bool:
    try:
        parts = path.relative_to(root).parts
    except ValueError:
        return True
    current = root
    for part in parts:
        current /= part
        if current.is_symlink():
            return True
    return False


def _directory_state(folder: Path) -> _DirectoryState:
    paths = [folder]
    paths.extend(path for path in folder.rglob("*") if not path.is_symlink())
    return tuple(
        sorted((str(path.relative_to(folder)), path.stat().st_mtime_ns) for path in paths)
    )


def _catalog_progress_title_key(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", title.casefold()).strip()


def _filesystem_release_evidence(
    library_root: Path,
    albums: list[tuple[int, int | None, str, str | None, str]],
) -> dict[int, _FilesystemReleaseEvidence]:
    """Inspect exact release folders while refusing paths that leave the library root."""
    evidence: dict[int, _FilesystemReleaseEvidence] = {}
    root = library_root.resolve()
    for album_id, _track_count, title, year, artist_name in albums:
        artist_segment = _sanitize_segment(artist_name)
        title_segment = _sanitize_segment(title)
        candidates = []
        if year:
            candidates.extend(
                [
                    root / artist_segment / _sanitize_segment(f"{title} ({year})"),
                    root / artist_segment / _sanitize_segment(f"{year} - {title}"),
                ]
            )
        else:
            candidates.extend(
                [
                    root / artist_segment / _sanitize_segment(f"{title} (0000)"),
                    root / artist_segment / title_segment,
                ]
            )
        for folder in candidates:
            if _has_symlink_component(root, folder) or not folder.is_dir():
                continue
            resolved_folder = folder.resolve()
            try:
                resolved_folder.relative_to(root)
            except ValueError:
                continue
            directory_state = _directory_state(resolved_folder)
            cache_key = (album_id, str(resolved_folder))
            cached = _RELEASE_EVIDENCE_CACHE.get(cache_key)
            if cached is not None and cached[0] == directory_state:
                candidate = cached[1]
            else:
                file_count = 0
                positions: set[tuple[int, int]] = set()
                for path in folder.rglob("*"):
                    if (
                        not path.is_file()
                        or path.is_symlink()
                        or path.suffix.casefold() not in IMPORTABLE_AUDIO_SUFFIXES
                    ):
                        continue
                    try:
                        path.resolve().relative_to(resolved_folder)
                    except ValueError:
                        continue
                    file_count += 1
                    match = _TRACK_NUMBER_PREFIX.match(path.stem)
                    if match:
                        disc = int(match.group("disc") or 1)
                        track = int(match.group("track"))
                        if match.group("disc") is None:
                            for parent_part in path.relative_to(folder).parts[:-1]:
                                disc_match = _DISC_FOLDER.match(parent_part)
                                if disc_match:
                                    disc = int(disc_match.group(1))
                            if track >= 100 and track <= 999:
                                disc, track = divmod(track, 100)
                        positions.add((disc, track))
                candidate = _FilesystemReleaseEvidence(file_count, frozenset(positions))
                _RELEASE_EVIDENCE_CACHE[cache_key] = (directory_state, candidate)
                if len(_RELEASE_EVIDENCE_CACHE) > _RELEASE_EVIDENCE_CACHE_MAX_ENTRIES:
                    _RELEASE_EVIDENCE_CACHE.clear()
            file_count = candidate.file_count
            current = evidence.get(album_id)
            if file_count and (current is None or file_count > current.file_count):
                evidence[album_id] = candidate
    return evidence


async def get_release_progress(
    db: AsyncSession,
    album_ids: list[int] | set[int] | tuple[int, ...],
    *,
    library_root: Path | None = None,
) -> dict[int, ReleaseProgress]:
    """Project ownership from hydrated manifests and confirmed-present imports.

    Provider track-count metadata is not a manifest: a release without persisted
    ``CatalogAlbumTrack`` rows has an unknown denominator and can never be reported
    complete. Repeated acquisition attempts collapse to distinct catalog tracks.
    ``library_root`` remains accepted for caller compatibility, but page-time filesystem
    discovery is intentionally not used as ownership evidence.
    """
    ids = sorted(set(album_ids))
    if not ids:
        return {}

    del library_root
    album_rows = (await db.execute(select(CatalogAlbum.id).where(CatalogAlbum.id.in_(ids)))).all()
    manifest_tracks: dict[int, dict[tuple[int, int], int]] = {}
    manifest_rows = await db.execute(
        select(
            CatalogAlbumTrack.album_id,
            CatalogAlbumTrack.id,
            CatalogAlbumTrack.disc,
            CatalogAlbumTrack.position,
        ).where(CatalogAlbumTrack.album_id.in_(ids))
    )
    for album_id, track_id, disc, position in manifest_rows:
        manifest_tracks.setdefault(int(album_id), {})[(int(disc), int(position))] = int(track_id)
    manifest_counts = {album_id: len(tracks) for album_id, tracks in manifest_tracks.items()}
    local_file_counts = {
        int(album_id): int(file_count)
        for album_id, file_count in (
            await db.execute(
                select(Track.catalog_album_id, func.count(func.distinct(Track.id)))
                .where(Track.catalog_album_id.in_(ids), _present_library_artifact_filter())
                .group_by(Track.catalog_album_id)
            )
        ).all()
        if album_id is not None
    }
    imported_by_album: dict[int, dict[int, ReleaseLibraryFile]] = {}
    imported_rows = await db.execute(
        select(
            Track.catalog_album_id,
            Track.catalog_track_id,
            Track.id,
            Track.file_format,
            Track.file_size_bytes,
            Track.source,
        )
        .join(CatalogAlbumTrack, CatalogAlbumTrack.id == Track.catalog_track_id)
        .where(
            Track.catalog_album_id.in_(ids),
            CatalogAlbumTrack.album_id == Track.catalog_album_id,
            _present_library_artifact_filter(),
        )
        .distinct()
    )
    for (
        album_id,
        catalog_track_id,
        library_track_id,
        file_format,
        file_size,
        source,
    ) in imported_rows:
        if album_id is not None and catalog_track_id is not None:
            album_tracks = imported_by_album.setdefault(int(album_id), {})
            catalog_id = int(catalog_track_id)
            candidate = ReleaseLibraryFile(
                catalog_track_id=catalog_id,
                track_id=int(library_track_id),
                file_format=str(file_format or ""),
                file_size_bytes=int(file_size) if file_size is not None else None,
                source=str(source),
            )
            current = album_tracks.get(catalog_id)
            if current is None or candidate.track_id > current.track_id:
                album_tracks[catalog_id] = candidate

    progress: dict[int, ReleaseProgress] = {}
    for (album_id,) in album_rows:
        release_id = int(album_id)
        manifest_count = manifest_counts.get(release_id, 0)
        library_files = tuple(
            sorted(
                imported_by_album.get(release_id, {}).values(),
                key=lambda item: item.catalog_track_id,
            )
        )
        library_track_ids = tuple((item.catalog_track_id, item.track_id) for item in library_files)
        downloaded_ids = frozenset(catalog_id for catalog_id, _track_id in library_track_ids)
        progress[release_id] = ReleaseProgress(
            wanted_track_count=manifest_count,
            downloaded_track_count=(
                len(downloaded_ids) if manifest_count else local_file_counts.get(release_id, 0)
            ),
            downloaded_catalog_track_ids=downloaded_ids,
            library_track_ids=library_track_ids,
            library_files=library_files,
            manifest_known=manifest_count > 0,
        )
    return progress


@dataclass(frozen=True, slots=True)
class CatalogAlbumQueueProjection:
    """Imported ownership and exact missing/sub-quality queue targets for one album."""

    imported_track_ids: frozenset[int]
    target_track_ids: tuple[int, ...]


def _existing_destination_paths(paths: tuple[str, ...]) -> frozenset[str]:
    return frozenset(path for path in paths if Path(path).is_file())


async def project_catalog_album_queue_targets(
    db: AsyncSession,
    album_ids: Iterable[int],
    *,
    library_root: Path | None = None,
    quality_profile: QualityProfile,
) -> dict[int, CatalogAlbumQueueProjection]:
    """Batch-project the exact track targets used by ordinary catalog expansion."""
    ids = sorted(set(album_ids))
    if not ids:
        return {}

    progress_by_album = await get_release_progress(db, ids, library_root=library_root)
    imported_by_album = {
        album_id: set(progress.downloaded_catalog_track_ids)
        for album_id, progress in progress_by_album.items()
    }
    quality_rows = (
        await db.execute(
            select(
                Track.catalog_album_id,
                Track.catalog_track_id,
                Track.file_format,
                ImportPlan.destination_path,
            )
            .join(ImportPlan, ImportPlan.track_id == Track.id)
            .where(
                Track.catalog_album_id.in_(ids),
                Track.catalog_track_id.is_not(None),
                Track.import_state == ImportWorkflowState.imported,
                ImportPlan.status == ImportWorkflowState.imported,
                ImportPlan.destination_path != "",
            )
        )
    ).all()
    quality_candidates = [
        (int(album_id), int(catalog_track_id), file_format, str(destination_path))
        for album_id, catalog_track_id, file_format, destination_path in quality_rows
        if album_id is not None
        and catalog_track_id is not None
        and int(catalog_track_id) in imported_by_album.get(int(album_id), set())
        and not track_meets_quality(file_format, quality_profile)
    ]
    existing_paths = await asyncio.to_thread(
        _existing_destination_paths,
        tuple(sorted({row[3] for row in quality_candidates})),
    )
    subquality_by_album: dict[int, set[int]] = {}
    for album_id, catalog_track_id, _file_format, destination_path in quality_candidates:
        if destination_path in existing_paths:
            subquality_by_album.setdefault(album_id, set()).add(catalog_track_id)

    manifest_rows = (
        await db.execute(
            select(CatalogAlbumTrack.album_id, CatalogAlbumTrack.id)
            .where(CatalogAlbumTrack.album_id.in_(ids))
            .order_by(
                CatalogAlbumTrack.album_id,
                CatalogAlbumTrack.disc,
                CatalogAlbumTrack.position,
                CatalogAlbumTrack.id,
            )
        )
    ).all()
    manifest_by_album: dict[int, list[int]] = {album_id: [] for album_id in ids}
    for album_id, track_id in manifest_rows:
        manifest_by_album[int(album_id)].append(int(track_id))

    return {
        album_id: CatalogAlbumQueueProjection(
            imported_track_ids=frozenset(imported_by_album.get(album_id, set())),
            target_track_ids=tuple(
                track_id
                for track_id in manifest_by_album[album_id]
                if track_id not in imported_by_album.get(album_id, set())
                or track_id in subquality_by_album.get(album_id, set())
            ),
        )
        for album_id in ids
    }


async def queue_catalog_album_missing_track_jobs(
    db: AsyncSession,
    album: CatalogAlbum,
    *,
    library_root: Path | None = None,
    quality_profile: QualityProfile,
) -> list[int]:
    """Compatibility wrapper returning only newly committed ordinary job IDs."""
    outcome = await expand_catalog_album_missing_track_jobs(
        db, album, library_root=library_root, quality_profile=quality_profile
    )
    return list(outcome.created_job_ids)


@dataclass(frozen=True, slots=True)
class CatalogQueueOutcome:
    created_job_ids: tuple[int, ...]
    observed_job_ids: tuple[int, ...]
    complete_track_ids: frozenset[int]
    hydration_required: bool
    missing_count: int


_EXPANSION_ELIGIBLE_ITEM_STATES = frozenset(
    {
        DiscographyBatchItemState.preview,
        DiscographyBatchItemState.pending,
        DiscographyBatchItemState.hydrating,
        DiscographyBatchItemState.expanding,
        DiscographyBatchItemState.waiting,
        DiscographyBatchItemState.failed,
    }
)
_ACTIVE_JOB_STATUSES = (JobStatus.pending, JobStatus.running)
_TERMINAL_JOB_STATUSES = (
    JobStatus.done,
    JobStatus.failed,
    JobStatus.partial,
    JobStatus.cancelled,
)


def _item_is_expansion_eligible(item: DiscographyBatchItem) -> bool:
    return item.state in _EXPANSION_ELIGIBLE_ITEM_STATES or (
        item.state == DiscographyBatchItemState.skipped and item.reason_code == "already_active"
    )


async def _link_discography_job(
    db: AsyncSession,
    item_id: int | None,
    generation: int | None,
    catalog_track_id: int,
    job_id: int,
    ownership: DiscographyJobOwnership,
) -> None:
    if item_id is None or generation is None:
        return
    existing = await db.scalar(
        select(DiscographyBatchItemJob.id).where(
            DiscographyBatchItemJob.item_id == item_id,
            DiscographyBatchItemJob.generation == generation,
            DiscographyBatchItemJob.catalog_track_id == catalog_track_id,
        )
    )
    if existing is None:
        db.add(
            DiscographyBatchItemJob(
                item_id=item_id,
                generation=generation,
                catalog_track_id=catalog_track_id,
                job_id=job_id,
                ownership=ownership,
            )
        )


class DiscographyLeaseLostError(RuntimeError):
    """The durable batch item is no longer owned by this materializer."""


async def expand_catalog_album_missing_track_jobs(
    db: AsyncSession,
    album: CatalogAlbum,
    *,
    quality_profile: QualityProfile,
    batch_item_id: int | None = None,
    batch_lease_token: str | None = None,
    max_new_jobs: int = 25,
    library_root: Path | None = None,
) -> CatalogQueueOutcome:
    """Create or observe exact ordinary jobs behind the acquisition claim fence."""
    reject_pending_orm_changes(db, allowed_entities=(album,))
    album_id = album.id
    if album_id is None:
        raise ValueError("catalog album must be persisted")
    new_job_limit = max(0, min(int(max_new_jobs), 25))

    # The filesystem-sensitive quality projection precedes the short writer reservation.
    projection = (
        await project_catalog_album_queue_targets(
            db,
            [album_id],
            library_root=library_root,
            quality_profile=quality_profile,
        )
    )[album_id]
    projected_quality_targets = set(projection.target_track_ids) & set(
        projection.imported_track_ids
    )
    await db.commit()

    created: list[int] = []
    observed: list[int] = []
    complete_track_ids: frozenset[int] = frozenset()
    hydration_required = False
    missing_count = 0

    async def reserve_and_expand() -> None:
        nonlocal complete_track_ids, hydration_required, missing_count
        created.clear()
        observed.clear()
        complete_track_ids = frozenset()
        hydration_required = False
        missing_count = 0
        await db.execute(text("BEGIN IMMEDIATE"))
        current_album = await db.scalar(
            select(CatalogAlbum)
            .where(CatalogAlbum.id == album_id)
            .options(selectinload(CatalogAlbum.artist))
        )
        if current_album is None:
            raise ValueError("catalog album does not exist")
        tracks = list(
            (
                await db.scalars(
                    select(CatalogAlbumTrack)
                    .where(CatalogAlbumTrack.album_id == album_id)
                    .order_by(
                        CatalogAlbumTrack.disc,
                        CatalogAlbumTrack.position,
                        CatalogAlbumTrack.id,
                    )
                )
            ).all()
        )
        expected_count = current_album.track_count
        item_generation: int | None = None
        if batch_item_id is not None:
            item = await db.get(DiscographyBatchItem, batch_item_id)
            if item is None:
                raise ValueError("discography batch item does not exist")
            if item.catalog_album_id != album_id:
                raise ValueError("discography batch item belongs to a different catalog album")
            if batch_lease_token is not None:
                batch_state = await db.scalar(
                    select(DiscographyBatch.state).where(DiscographyBatch.id == item.batch_id)
                )
                if (
                    item.state != DiscographyBatchItemState.expanding
                    or item.lease_token != batch_lease_token
                    or batch_state
                    not in (DiscographyBatchState.queued, DiscographyBatchState.running)
                ):
                    raise DiscographyLeaseLostError("discography batch lease is no longer active")
            if not _item_is_expansion_eligible(item):
                raise ValueError("discography batch item is not expansion-eligible")
            item_generation = item.execution_generation
            expected_count = max(expected_count or 0, item.expected_track_count or 0) or None
            if item.provider_release_id is not None:
                provider_expected = await db.scalar(
                    select(CatalogAlbumProvider.track_count).where(
                        CatalogAlbumProvider.id == item.provider_release_id
                    )
                )
                expected_count = max(expected_count or 0, provider_expected or 0) or None

        if catalog_manifest_issue(tracks, expected_count) is not None:
            hydration_required = True
            complete_track_ids = frozenset(projection.imported_track_ids)
            await db.commit()
            return

        progress = (await get_release_progress(db, [album_id]))[album_id]
        imported_ids = set(progress.downloaded_catalog_track_ids)
        manifest_ids = {track.id for track in tracks}
        target_ids = {
            track_id
            for track_id in manifest_ids
            if track_id not in imported_ids or track_id in projected_quality_targets
        }
        missing_count = len(target_ids)
        complete_track_ids = frozenset(manifest_ids - target_ids)

        # A release-root owns the physical folder acquisition for the whole album.
        # This check shares the same BEGIN IMMEDIATE reservation as exact-track
        # claims, so root-first and track-first races serialize without overlap.
        release_claim = (
            await db.execute(
                select(CatalogReleaseAcquisitionClaim, Job)
                .outerjoin(Job, Job.id == CatalogReleaseAcquisitionClaim.job_id)
                .where(CatalogReleaseAcquisitionClaim.catalog_album_id == album_id)
            )
        ).one_or_none()
        if release_claim is not None:
            _claim, release_owner = release_claim
            if release_owner is not None and release_owner.status in _ACTIVE_JOB_STATUSES:
                if (
                    release_owner.catalog_album_id != album_id
                    or release_owner.catalog_track_id is not None
                ):
                    raise ValueError(
                        "active catalog release claim does not own an exact release root"
                    )
                observed.append(release_owner.id)
                await db.commit()
                return

        generation_links: dict[int, Job] = {}
        if batch_item_id is not None and item_generation is not None and target_ids:
            generation_links = {
                int(track_id): linked_job
                for track_id, linked_job in (
                    await db.execute(
                        select(DiscographyBatchItemJob.catalog_track_id, Job)
                        .join(Job, Job.id == DiscographyBatchItemJob.job_id)
                        .where(
                            DiscographyBatchItemJob.item_id == batch_item_id,
                            DiscographyBatchItemJob.generation == item_generation,
                            DiscographyBatchItemJob.catalog_track_id.in_(target_ids),
                        )
                    )
                ).all()
                if track_id is not None
            }

        for track in tracks:
            if track.id not in target_ids:
                continue
            prior_attempt = generation_links.get(track.id)
            if prior_attempt is not None:
                if prior_attempt.status in _ACTIVE_JOB_STATUSES:
                    observed.append(prior_attempt.id)
                continue
            claim = (
                await db.execute(
                    select(AcquisitionDispatchClaim, Job)
                    .outerjoin(Job, Job.id == AcquisitionDispatchClaim.job_id)
                    .where(
                        AcquisitionDispatchClaim.catalog_album_id == album_id,
                        AcquisitionDispatchClaim.catalog_track_id == track.id,
                    )
                )
            ).one_or_none()
            if claim is not None:
                claim_row, owner = claim
                if owner is not None and owner.status in _ACTIVE_JOB_STATUSES:
                    observed.append(owner.id)
                    await _link_discography_job(
                        db,
                        batch_item_id,
                        item_generation,
                        track.id,
                        owner.id,
                        DiscographyJobOwnership.observed,
                    )
                    continue
                if owner is None:
                    await db.delete(claim_row)
                    await db.flush()
            if len(created) >= new_job_limit:
                continue

            query = " ".join(
                part
                for part in (catalog_track_artist_name(current_album, track), track.title)
                if part
            )
            contender = Job(
                source="priority",
                query=query,
                status=JobStatus.pending,
                catalog_album_id=album_id,
                catalog_track_id=track.id,
            )
            db.add(contender)
            await db.flush()
            claimed_id = await db.scalar(
                sqlite_insert(AcquisitionDispatchClaim)
                .values(
                    catalog_album_id=album_id,
                    catalog_track_id=track.id,
                    job_id=contender.id,
                )
                .on_conflict_do_update(
                    index_elements=["catalog_album_id", "catalog_track_id"],
                    set_={"job_id": contender.id},
                    where=or_(
                        AcquisitionDispatchClaim.job_id == contender.id,
                        exists(
                            select(Job.id).where(
                                Job.id == AcquisitionDispatchClaim.job_id,
                                Job.status.in_(_TERMINAL_JOB_STATUSES),
                            )
                        ),
                    ),
                )
                .returning(AcquisitionDispatchClaim.job_id)
            )
            if claimed_id == contender.id:
                created.append(contender.id)
                await _link_discography_job(
                    db,
                    batch_item_id,
                    item_generation,
                    track.id,
                    contender.id,
                    DiscographyJobOwnership.created,
                )
                continue

            await db.delete(contender)
            await db.flush()
            winner_id = await db.scalar(
                select(AcquisitionDispatchClaim.job_id)
                .join(Job, Job.id == AcquisitionDispatchClaim.job_id)
                .where(
                    AcquisitionDispatchClaim.catalog_album_id == album_id,
                    AcquisitionDispatchClaim.catalog_track_id == track.id,
                    Job.status.in_(_ACTIVE_JOB_STATUSES),
                )
            )
            if winner_id is None:
                raise RuntimeError("exact acquisition claim lost without an active owner")
            observed.append(int(winner_id))
            await _link_discography_job(
                db,
                batch_item_id,
                item_generation,
                track.id,
                int(winner_id),
                DiscographyJobOwnership.observed,
            )
        await db.commit()

    try:
        await run_with_sqlite_lock_retry(db, reserve_and_expand, attempts=6, delay_seconds=0.2)
    except Exception:
        await db.rollback()
        raise
    return CatalogQueueOutcome(
        created_job_ids=tuple(created),
        observed_job_ids=tuple(observed),
        complete_track_ids=complete_track_ids,
        hydration_required=hydration_required,
        missing_count=missing_count,
    )


async def get_library_stats(db: AsyncSession) -> LibraryStats:
    artist_expr = _artist_expr()
    agg = await db.execute(
        select(
            func.count(Track.id).label("track_count"),
            func.count(func.distinct(artist_expr)).label("artist_count"),
            func.coalesce(func.sum(Track.duration_sec), 0).label("total_duration_sec"),
        ).where(_library_artifact_filter())
    )
    row = agg.one()
    album_count = await _count_album_groups(db, _library_artifact_filter())

    src_rows = await db.execute(
        select(Track.source, func.count(Track.id).label("cnt"))
        .where(_library_artifact_filter())
        .group_by(Track.source)
        .order_by(func.count(Track.id).desc())
    )
    source_breakdown: dict[str, int] = {r.source: int(r.cnt) for r in src_rows}

    fmt_rows = await db.execute(
        select(Track.file_format, func.count(Track.id).label("cnt"))
        .where(_library_artifact_filter(), Track.file_format.is_not(None))
        .group_by(Track.file_format)
        .order_by(func.count(Track.id).desc())
    )
    format_breakdown: dict[str, int] = {str(r.file_format): int(r.cnt) for r in fmt_rows}

    total_bytes = int(
        (
            await db.scalar(
                select(func.coalesce(func.sum(Track.file_size_bytes), 0)).where(
                    _library_artifact_filter()
                )
            )
        )
        or 0
    )

    return LibraryStats(
        track_count=int(row.track_count),
        artist_count=int(row.artist_count),
        album_count=album_count,
        total_duration_sec=int(row.total_duration_sec),
        total_bytes=total_bytes,
        format_breakdown=format_breakdown,
        source_breakdown=source_breakdown,
    )


def _build_library_filters(
    q: str,
    artist: str,
    album: str,
    source: str,
    fmt: str,
) -> list[Any]:
    artist_expr = _artist_expr()
    filters: list[Any] = [_library_artifact_filter()]
    if q:
        pattern = f"%{q}%"
        filters.append(
            or_(
                Track.title.ilike(pattern),
                Track.artist.ilike(pattern),
                Track.album_artist.ilike(pattern),
                Track.album.ilike(pattern),
            )
        )
    if artist:
        filters.append(artist_expr.ilike(f"%{artist}%"))
    if album:
        filters.append(Track.album.ilike(f"%{album}%"))
    if source:
        filters.append(Track.source == source)
    if fmt:
        filters.append(Track.file_format == fmt)
    return filters


async def list_library_tracks(
    db: AsyncSession,
    *,
    q: str = "",
    artist: str = "",
    album: str = "",
    source: str = "",
    fmt: str = "",
    sort: str = "added",
    page: int = 1,
    per_page: int = _DEFAULT_PAGE_SIZE,
) -> Page[TrackRow]:
    per_page = _clamp_per_page(per_page)
    page = max(1, page)
    artist_expr = _artist_expr()

    filters = _build_library_filters(q, artist, album, source, fmt)

    count_stmt = select(func.count(Track.id))
    if filters:
        count_stmt = count_stmt.where(and_(*filters))
    total = int((await db.scalar(count_stmt)) or 0)

    page = _clamp_page(page, total, per_page)

    data_stmt = select(Track).options(
        selectinload(Track.import_plans), selectinload(Track.catalog_album)
    )
    if filters:
        data_stmt = data_stmt.where(and_(*filters))

    valid_sort = sort if sort in _VALID_LIBRARY_SORTS else "added"
    if valid_sort == "title":
        data_stmt = data_stmt.order_by(Track.title, Track.id)
    elif valid_sort == "artist":
        data_stmt = data_stmt.order_by(artist_expr, Track.title, Track.id)
    elif valid_sort == "album":
        data_stmt = data_stmt.order_by(Track.album, Track.track_no, Track.id)
    elif valid_sort == "year":
        data_stmt = data_stmt.order_by(Track.year.desc(), Track.album, Track.id)
    elif valid_sort == "source":
        data_stmt = data_stmt.order_by(Track.source, Track.id)
    else:
        data_stmt = data_stmt.order_by(Track.id.desc())

    data_stmt = data_stmt.offset(_page_offset(page, per_page)).limit(per_page)
    rows = list((await db.execute(data_stmt)).scalars().all())

    return Page(
        items=[to_track_row(r) for r in rows],
        total=total,
        page=page,
        per_page=per_page,
    )


async def list_distinct_sources(db: AsyncSession) -> list[str]:
    rows = (
        await db.execute(
            select(Track.source)
            .where(_library_artifact_filter())
            .distinct()
            .order_by(Track.source)
        )
    ).scalars()
    return sorted({str(s) for s in rows})


async def list_distinct_formats(db: AsyncSession) -> list[str]:
    rows = (
        await db.execute(
            select(Track.file_format)
            .where(_library_artifact_filter(), Track.file_format.is_not(None))
            .distinct()
            .order_by(Track.file_format)
        )
    ).scalars()
    return sorted({str(s) for s in rows})


async def get_library_artists_page(
    db: AsyncSession,
    *,
    q: str = "",
    sort: str = "name",
    page: int = 1,
    per_page: int = _DEFAULT_PAGE_SIZE,
) -> Page[LibraryArtistRow]:
    """Return watchlisted artists and artists with persisted library artifacts.

    Card totals are computed in grouped passes over albums, provider releases, and
    imported tracks. Avoid per-artist correlated scans: production libraries can
    contain thousands of provider releases even when the artist page is small.
    """
    runtime = await get_runtime_settings(db)
    per_page = _clamp_per_page(per_page)
    page = max(1, page)
    normalized_track_artist = func.lower(func.trim(_artist_expr()))
    normalized_catalog_artist = func.lower(func.trim(CatalogArtist.name))

    imported_plan_tracks = (
        select(ImportPlan.track_id.label("track_id"))
        .where(
            ImportPlan.track_id.is_not(None),
            ImportPlan.status == ImportWorkflowState.imported,
            _non_empty(ImportPlan.destination_path),
        )
        .group_by(ImportPlan.track_id)
        .subquery()
    )
    present_plan_tracks = (
        select(ImportPlan.track_id.label("track_id"))
        .where(
            ImportPlan.track_id.is_not(None),
            ImportPlan.status == ImportWorkflowState.imported,
            ImportPlan.file_state == LibraryFileState.present,
            _non_empty(ImportPlan.destination_path),
        )
        .group_by(ImportPlan.track_id)
        .subquery()
    )
    artifact_track_state = and_(
        Track.acquisition_state == AcquisitionState.downloaded,
        Track.import_state == ImportWorkflowState.imported,
        Track.file_size_bytes.is_not(None),
        Track.file_size_bytes > 0,
    )

    manifest_counts = (
        select(
            CatalogAlbumTrack.album_id.label("album_id"),
            func.count(CatalogAlbumTrack.id).label("manifest_count"),
        )
        .group_by(CatalogAlbumTrack.album_id)
        .subquery()
    )
    present_catalog_tracks = (
        select(
            Track.catalog_album_id.label("album_id"),
            func.count(func.distinct(Track.catalog_track_id)).label("present_count"),
        )
        .join(present_plan_tracks, present_plan_tracks.c.track_id == Track.id)
        .join(
            CatalogAlbumTrack,
            and_(
                CatalogAlbumTrack.id == Track.catalog_track_id,
                CatalogAlbumTrack.album_id == Track.catalog_album_id,
            ),
        )
        .where(Track.catalog_album_id.is_not(None), artifact_track_state)
        .group_by(Track.catalog_album_id)
        .subquery()
    )
    manifest_count = func.coalesce(manifest_counts.c.manifest_count, 0)
    present_count = func.coalesce(present_catalog_tracks.c.present_count, 0)
    album_progress = (
        select(
            CatalogAlbum.id.label("album_id"),
            CatalogAlbum.artist_id.label("artist_id"),
            manifest_count.label("manifest_count"),
            present_count.label("present_count"),
        )
        .outerjoin(manifest_counts, manifest_counts.c.album_id == CatalogAlbum.id)
        .outerjoin(present_catalog_tracks, present_catalog_tracks.c.album_id == CatalogAlbum.id)
        .subquery()
    )
    album_manifest = album_progress.c.manifest_count
    album_present = album_progress.c.present_count

    provider_release_counts = (
        select(
            CatalogAlbumProvider.artist_identity_id.label("identity_id"),
            func.count(func.distinct(CatalogAlbumProvider.catalog_album_id)).label(
                "release_count"
            ),
            func.sum(case((CatalogAlbumProvider.catalog_album_id.is_(None), 1), else_=0)).label(
                "provider_only_count"
            ),
            func.count(
                func.distinct(
                    case(
                        (
                            and_(album_manifest > 0, album_present >= album_manifest),
                            CatalogAlbumProvider.catalog_album_id,
                        )
                    )
                )
            ).label("complete_count"),
            func.count(
                func.distinct(
                    case(
                        (
                            and_(
                                album_manifest > 0,
                                album_present > 0,
                                album_present < album_manifest,
                            ),
                            CatalogAlbumProvider.catalog_album_id,
                        )
                    )
                )
            ).label("partial_count"),
            func.count(
                func.distinct(case((album_manifest == 0, CatalogAlbumProvider.catalog_album_id)))
            ).label("unknown_count"),
            func.count(
                func.distinct(case((album_present > 0, CatalogAlbumProvider.catalog_album_id)))
            ).label("local_count"),
        )
        .outerjoin(
            album_progress,
            album_progress.c.album_id == CatalogAlbumProvider.catalog_album_id,
        )
        .group_by(CatalogAlbumProvider.artist_identity_id)
        .subquery()
    )
    artist_release_counts = (
        select(
            album_progress.c.artist_id,
            func.count(album_progress.c.album_id).label("release_count"),
            func.sum(
                case(
                    (and_(album_manifest > 0, album_present >= album_manifest), 1),
                    else_=0,
                )
            ).label("complete_count"),
            func.sum(
                case(
                    (
                        and_(
                            album_manifest > 0,
                            album_present > 0,
                            album_present < album_manifest,
                        ),
                        1,
                    ),
                    else_=0,
                )
            ).label("partial_count"),
            func.sum(case((album_manifest == 0, 1), else_=0)).label("unknown_count"),
            func.sum(case((album_present > 0, 1), else_=0)).label("local_count"),
        )
        .group_by(album_progress.c.artist_id)
        .subquery()
    )

    identity_priority = case(
        (
            CatalogArtistIdentity.provider == CatalogArtist.primary_metadata_provider,
            0,
        ),
        (CatalogArtistIdentity.provider == CatalogArtist.watchlist_provider, 1),
        (CatalogArtistIdentity.provider == runtime.primary_metadata_provider, 2),
        else_=3,
    )
    ranked_identities = (
        select(
            CatalogArtistIdentity.artist_id,
            CatalogArtistIdentity.id.label("identity_id"),
            CatalogArtistIdentity.provider,
            func.row_number()
            .over(
                partition_by=CatalogArtistIdentity.artist_id,
                order_by=(identity_priority, CatalogArtistIdentity.id),
            )
            .label("priority_rank"),
        )
        .join(CatalogArtist, CatalogArtist.id == CatalogArtistIdentity.artist_id)
        .subquery()
    )
    canonical_identities = (
        select(
            ranked_identities.c.artist_id,
            ranked_identities.c.identity_id,
            ranked_identities.c.provider,
        )
        .where(ranked_identities.c.priority_rank == 1)
        .subquery()
    )

    catalog_present_counts = (
        select(
            CatalogAlbum.artist_id,
            func.count(func.distinct(Track.id)).label("file_count"),
        )
        .select_from(Track)
        .join(present_plan_tracks, present_plan_tracks.c.track_id == Track.id)
        .join(CatalogAlbum, CatalogAlbum.id == Track.catalog_album_id)
        .where(artifact_track_state)
        .group_by(CatalogAlbum.artist_id)
        .subquery()
    )
    uncatalogued_present_counts = (
        select(
            normalized_track_artist.label("artist_key"),
            func.count(func.distinct(Track.id)).label("file_count"),
        )
        .select_from(Track)
        .join(present_plan_tracks, present_plan_tracks.c.track_id == Track.id)
        .where(Track.catalog_album_id.is_(None), artifact_track_state)
        .group_by(normalized_track_artist)
        .subquery()
    )
    catalog_imported_counts = (
        select(
            CatalogAlbum.artist_id,
            func.count(func.distinct(Track.id)).label("file_count"),
        )
        .select_from(Track)
        .join(imported_plan_tracks, imported_plan_tracks.c.track_id == Track.id)
        .join(CatalogAlbum, CatalogAlbum.id == Track.catalog_album_id)
        .where(artifact_track_state)
        .group_by(CatalogAlbum.artist_id)
        .subquery()
    )
    uncatalogued_imported_counts = (
        select(
            normalized_track_artist.label("artist_key"),
            func.count(func.distinct(Track.id)).label("file_count"),
        )
        .select_from(Track)
        .join(imported_plan_tracks, imported_plan_tracks.c.track_id == Track.id)
        .where(Track.catalog_album_id.is_(None), artifact_track_state)
        .group_by(normalized_track_artist)
        .subquery()
    )

    provider_only_count = func.coalesce(provider_release_counts.c.provider_only_count, 0)
    provider_total = func.coalesce(provider_release_counts.c.release_count, 0)
    all_total = func.coalesce(artist_release_counts.c.release_count, 0)
    no_canonical_identity = canonical_identities.c.identity_id.is_(None)
    release_count = case(
        (no_canonical_identity, all_total),
        else_=provider_total + provider_only_count,
    )
    complete_count = case(
        (no_canonical_identity, func.coalesce(artist_release_counts.c.complete_count, 0)),
        else_=func.coalesce(provider_release_counts.c.complete_count, 0),
    )
    partial_count = case(
        (no_canonical_identity, func.coalesce(artist_release_counts.c.partial_count, 0)),
        else_=func.coalesce(provider_release_counts.c.partial_count, 0),
    )
    unknown_count = case(
        (no_canonical_identity, func.coalesce(artist_release_counts.c.unknown_count, 0)),
        else_=func.coalesce(provider_release_counts.c.unknown_count, 0) + provider_only_count,
    )
    local_count = case(
        (no_canonical_identity, func.coalesce(artist_release_counts.c.local_count, 0)),
        else_=func.coalesce(provider_release_counts.c.local_count, 0),
    )
    downloaded_count = func.coalesce(catalog_present_counts.c.file_count, 0) + func.coalesce(
        uncatalogued_present_counts.c.file_count, 0
    )
    imported_count = func.coalesce(catalog_imported_counts.c.file_count, 0) + func.coalesce(
        uncatalogued_imported_counts.c.file_count, 0
    )

    catalog_from = (
        CatalogArtist.__table__.outerjoin(
            canonical_identities, canonical_identities.c.artist_id == CatalogArtist.id
        )
        .outerjoin(
            provider_release_counts,
            provider_release_counts.c.identity_id == canonical_identities.c.identity_id,
        )
        .outerjoin(
            artist_release_counts,
            artist_release_counts.c.artist_id == CatalogArtist.id,
        )
        .outerjoin(
            catalog_present_counts,
            catalog_present_counts.c.artist_id == CatalogArtist.id,
        )
        .outerjoin(
            uncatalogued_present_counts,
            uncatalogued_present_counts.c.artist_key == normalized_catalog_artist,
        )
        .outerjoin(
            catalog_imported_counts,
            catalog_imported_counts.c.artist_id == CatalogArtist.id,
        )
        .outerjoin(
            uncatalogued_imported_counts,
            uncatalogued_imported_counts.c.artist_key == normalized_catalog_artist,
        )
    )
    catalog_filters: list[Any] = [or_(CatalogArtist.monitored.is_(True), imported_count > 0)]
    if q:
        catalog_filters.append(CatalogArtist.name.ilike(f"%{q}%"))
    catalog_rows = (
        select(
            CatalogArtist.id.label("catalog_id"),
            CatalogArtist.name.label("name"),
            CatalogArtist.artwork_url.label("artwork_url"),
            CatalogArtist.monitored.label("monitored"),
            canonical_identities.c.identity_id.label("identity_id"),
            canonical_identities.c.provider.label("primary_metadata_provider"),
            release_count.label("release_count"),
            downloaded_count.label("downloaded_file_count"),
            (release_count - complete_count).label("wanted_release_count"),
            complete_count.label("complete_release_count"),
            partial_count.label("partial_release_count"),
            unknown_count.label("unknown_release_count"),
            local_count.label("local_release_count"),
        )
        .select_from(catalog_from)
        .where(*catalog_filters)
    )
    catalog_total = int(
        (
            await db.scalar(
                select(func.count())
                .select_from(
                    CatalogArtist.__table__.outerjoin(
                        catalog_imported_counts,
                        catalog_imported_counts.c.artist_id == CatalogArtist.id,
                    ).outerjoin(
                        uncatalogued_imported_counts,
                        uncatalogued_imported_counts.c.artist_key == normalized_catalog_artist,
                    )
                )
                .where(*catalog_filters)
            )
        )
        or 0
    )

    matching_catalog_artist = exists(
        select(CatalogArtist.id).where(normalized_catalog_artist == normalized_track_artist)
    )
    legacy_filters: list[Any] = [
        Track.catalog_album_id.is_(None),
        artifact_track_state,
        ~matching_catalog_artist,
    ]
    if q:
        legacy_filters.append(_artist_expr().ilike(f"%{q}%"))
    legacy_release_key = case(
        (Track.release_id.is_not(None), literal("release:") + cast(Track.release_id, String)),
        else_=literal("album:") + _album_expr() + literal(":") + _year_expr(),
    )
    legacy_projection = (
        select(
            _artist_expr().label("name"),
            func.count(func.distinct(Track.id)).label("downloaded_file_count"),
            func.count(func.distinct(legacy_release_key)).label("local_release_count"),
        )
        .select_from(Track)
        .join(present_plan_tracks, present_plan_tracks.c.track_id == Track.id)
        .where(*legacy_filters)
        .group_by(_artist_expr())
        .subquery()
    )
    legacy_rows = select(
        literal(None).label("catalog_id"),
        legacy_projection.c.name,
        literal(None).label("artwork_url"),
        literal(False).label("monitored"),
        literal(None).label("identity_id"),
        literal(None).label("primary_metadata_provider"),
        literal(0).label("release_count"),
        legacy_projection.c.downloaded_file_count,
        literal(0).label("wanted_release_count"),
        literal(0).label("complete_release_count"),
        literal(0).label("partial_release_count"),
        legacy_projection.c.local_release_count.label("unknown_release_count"),
        legacy_projection.c.local_release_count,
    )
    legacy_total = int((await db.scalar(select(func.count()).select_from(legacy_projection))) or 0)
    total = catalog_total + legacy_total
    page = _clamp_page(page, total, per_page)

    combined = catalog_rows.union_all(legacy_rows).subquery()
    stmt = select(combined)
    valid_sort = sort if sort in _VALID_WATCHLIST_SORTS else "name"
    if valid_sort == "downloaded":
        stmt = stmt.order_by(
            combined.c.downloaded_file_count.desc(), combined.c.name, combined.c.catalog_id
        )
    elif valid_sort == "wanted":
        stmt = stmt.order_by(
            combined.c.wanted_release_count.desc(), combined.c.name, combined.c.catalog_id
        )
    else:
        stmt = stmt.order_by(combined.c.name, combined.c.catalog_id)
    if valid_sort != "wanted":
        stmt = stmt.offset(_page_offset(page, per_page)).limit(per_page)
    rows = [dict(row) for row in (await db.execute(stmt)).mappings()]
    identity_ids = {int(row["identity_id"]) for row in rows if row["identity_id"] is not None}
    family_counts = await _provider_family_counts(db, identity_ids, album_progress)
    for row in rows:
        identity_id = row["identity_id"]
        if identity_id is None:
            continue
        counts = family_counts[int(identity_id)]
        row["release_count"] = counts.release_count
        row["complete_release_count"] = counts.complete_count
        row["partial_release_count"] = counts.partial_count
        row["unknown_release_count"] = counts.unknown_count
        row["local_release_count"] = counts.local_count
        row["wanted_release_count"] = counts.release_count - counts.complete_count

    if valid_sort == "downloaded":
        rows.sort(
            key=lambda row: (
                -int(row["downloaded_file_count"] or 0),
                str(row["name"]),
                int(row["catalog_id"] or 0),
            )
        )
    elif valid_sort == "wanted":
        rows.sort(
            key=lambda row: (
                -int(row["wanted_release_count"] or 0),
                str(row["name"]),
                int(row["catalog_id"] or 0),
            )
        )
    else:
        rows.sort(key=lambda row: (str(row["name"]), int(row["catalog_id"] or 0)))
    if valid_sort == "wanted":
        rows = rows[_page_offset(page, per_page) : _page_offset(page, per_page) + per_page]

    items: list[LibraryArtistRow] = []
    for row in rows:
        catalog_id = int(row["catalog_id"]) if row["catalog_id"] is not None else None
        name = str(row["name"])
        detail_url = (
            f"/artists/catalog/{catalog_id}"
            if catalog_id is not None
            else f"/artists/detail?{urlencode({'name': name})}"
        )
        items.append(
            LibraryArtistRow(
                id=catalog_id,
                name=name,
                detail_url=detail_url,
                artwork_url=str(row["artwork_url"]) if row["artwork_url"] else None,
                primary_metadata_provider=str(row["primary_metadata_provider"])
                if row["primary_metadata_provider"]
                else None,
                release_count=int(row["release_count"] or 0),
                downloaded_file_count=int(row["downloaded_file_count"] or 0),
                wanted_release_count=int(row["wanted_release_count"] or 0),
                watchlisted=bool(row["monitored"]),
                complete_release_count=int(row["complete_release_count"] or 0),
                partial_release_count=int(row["partial_release_count"] or 0),
                unknown_release_count=int(row["unknown_release_count"] or 0),
                local_release_count=int(row["local_release_count"] or 0),
            )
        )
    return Page(items=items, total=total, page=page, per_page=per_page)


def _missing_releases_query(q: str = "", status: str = "all") -> Any:
    manifest_count = (
        select(func.count(CatalogAlbumTrack.id))
        .where(CatalogAlbumTrack.album_id == CatalogAlbum.id)
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    wanted_count = manifest_count
    downloaded_count = (
        select(func.count(func.distinct(Track.catalog_track_id)))
        .where(
            Track.catalog_album_id == CatalogAlbum.id,
            Track.catalog_track_id.is_not(None),
            _present_library_artifact_filter(),
        )
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    filters: list[Any] = [
        CatalogArtist.monitored.is_(True),
        CatalogAlbum.monitored.is_(True),
        or_(manifest_count == 0, downloaded_count < wanted_count),
    ]
    if q:
        pattern = f"%{q}%"
        filters.append(or_(CatalogArtist.name.ilike(pattern), CatalogAlbum.title.ilike(pattern)))
    pending_review = exists(
        select(StagingReviewItem.id)
        .join(Release, Release.id == StagingReviewItem.release_id)
        .join(Job, Job.id == Release.job_id)
        .where(
            Job.catalog_album_id == CatalogAlbum.id,
            StagingReviewItem.review_state == ReviewDecision.pending,
            Release.review_dismissed_at.is_(None),
        )
    )
    latest_job_status = (
        select(Job.status)
        .where(Job.catalog_album_id == CatalogAlbum.id)
        .order_by(Job.updated_at.desc(), Job.id.desc())
        .limit(1)
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    if status == "active":
        filters.append(
            or_(pending_review, latest_job_status.in_((JobStatus.pending, JobStatus.running)))
        )
    elif status == "failed":
        filters.append(
            and_(
                ~pending_review,
                latest_job_status.in_((JobStatus.failed, JobStatus.partial, JobStatus.cancelled)),
            )
        )
    elif status == "needs-search":
        filters.append(
            and_(
                ~pending_review,
                or_(latest_job_status.is_(None), latest_job_status == JobStatus.done),
            )
        )
    return (
        select(
            CatalogAlbum.id.label("album_id"),
            CatalogArtist.name.label("artist_name"),
            CatalogAlbum.title.label("title"),
            CatalogAlbum.year.label("year"),
            CatalogAlbum.artwork_url.label("artwork_url"),
            wanted_count.label("wanted_track_count"),
            downloaded_count.label("downloaded_track_count"),
        )
        .join(CatalogArtist, CatalogArtist.id == CatalogAlbum.artist_id)
        .where(*filters)
    )


def _sort_missing_releases_query(stmt: Any, sort: str) -> Any:
    valid_sort = sort if sort in {"year", "artist", "title"} else "year"
    if valid_sort == "artist":
        return stmt.order_by(CatalogArtist.name, CatalogAlbum.title, CatalogAlbum.id)
    if valid_sort == "title":
        return stmt.order_by(CatalogAlbum.title, CatalogArtist.name, CatalogAlbum.id)
    return stmt.order_by(
        CatalogAlbum.year.desc(), CatalogArtist.name, CatalogAlbum.title, CatalogAlbum.id
    )


async def get_missing_release_ids(
    db: AsyncSession,
    *,
    q: str = "",
    sort: str = "year",
    status: str = "all",
    limit: int = 10_000,
) -> list[int]:
    stmt = _sort_missing_releases_query(_missing_releases_query(q, status), sort)
    rows = await db.scalars(select(stmt.subquery().c.album_id).limit(max(1, min(limit, 10_001))))
    return [int(album_id) for album_id in rows.all()]


async def get_missing_releases_page(
    db: AsyncSession,
    *,
    q: str = "",
    sort: str = "year",
    status: str = "all",
    page: int = 1,
    per_page: int = _DEFAULT_PAGE_SIZE,
) -> Page[MissingReleaseRow]:
    per_page = _clamp_per_page(per_page)
    page = max(1, page)
    rows_query = _missing_releases_query(q, status)
    total = int((await db.scalar(select(func.count()).select_from(rows_query.subquery()))) or 0)
    page = _clamp_page(page, total, per_page)
    rows_query = _sort_missing_releases_query(rows_query, sort)
    rows = (
        await db.execute(rows_query.offset(_page_offset(page, per_page)).limit(per_page))
    ).mappings()
    items = [
        MissingReleaseRow(
            id=int(row["album_id"]),
            artist_name=str(row["artist_name"]),
            title=str(row["title"]),
            year=str(row["year"]) if row["year"] else None,
            artwork_url=str(row["artwork_url"]) if row["artwork_url"] else None,
            wanted_track_count=int(row["wanted_track_count"] or 0),
            downloaded_track_count=int(row["downloaded_track_count"] or 0),
            manifest_known=int(row["wanted_track_count"] or 0) > 0,
        )
        for row in rows
    ]
    return Page(items=items, total=total, page=page, per_page=per_page)


async def get_watchlisted_artists_page(
    db: AsyncSession,
    *,
    q: str = "",
    sort: str = "name",
    page: int = 1,
    per_page: int = _DEFAULT_PAGE_SIZE,
) -> Page[WatchlistedArtistRow]:
    per_page = _clamp_per_page(per_page)
    page = max(1, page)
    stmt = (
        select(CatalogArtist)
        .where(CatalogArtist.monitored.is_(True))
        .options(
            selectinload(CatalogArtist.albums),
            selectinload(CatalogArtist.identities).selectinload(CatalogArtistIdentity.releases),
        )
    )
    if q:
        stmt = stmt.where(CatalogArtist.name.ilike(f"%{q}%"))
    catalog_artists = list((await db.execute(stmt)).scalars().all())
    items: list[WatchlistedArtistRow] = []
    for artist in catalog_artists:
        albums = singles_eps = compilations = 0
        identity = next(
            (
                item
                for item in artist.identities
                if item.provider == artist.watchlist_provider and item.releases
            ),
            None,
        )
        if identity is not None:
            release_types = [
                family.key.release_kind
                for family in project_release_families(list(identity.releases))
            ]
        else:
            release_types = [
                (release.release_type or "album").casefold() for release in artist.albums
            ]
        for release_type in release_types:
            if release_type in {"single", "ep"}:
                singles_eps += 1
            elif "compilation" in release_type:
                compilations += 1
            else:
                albums += 1
        items.append(
            WatchlistedArtistRow(
                id=artist.id,
                name=artist.name,
                artwork_url=artist.artwork_url,
                album_count=albums,
                single_ep_count=singles_eps,
                compilation_count=compilations,
                total_releases=albums + singles_eps + compilations,
            )
        )

    valid_sort = sort if sort in _VALID_WATCHLIST_SORTS else "name"
    if valid_sort == "releases":
        items.sort(key=lambda item: (-item.total_releases, item.name.casefold(), item.id))
    elif valid_sort == "albums":
        items.sort(key=lambda item: (-item.album_count, item.name.casefold(), item.id))
    elif valid_sort == "singles":
        items.sort(key=lambda item: (-item.single_ep_count, item.name.casefold(), item.id))
    elif valid_sort == "compilations":
        items.sort(key=lambda item: (-item.compilation_count, item.name.casefold(), item.id))
    else:
        items.sort(key=lambda item: (item.name.casefold(), item.id))

    total = len(items)
    page = _clamp_page(page, total, per_page)
    start = _page_offset(page, per_page)
    return Page(items=items[start : start + per_page], total=total, page=page, per_page=per_page)


async def get_artists_page(
    db: AsyncSession,
    *,
    q: str = "",
    sort: str = "name",
    page: int = 1,
    per_page: int = _DEFAULT_PAGE_SIZE,
) -> Page[ArtistRow]:
    per_page = _clamp_per_page(per_page)
    page = max(1, page)
    artist_expr = _artist_expr()
    artist_label = artist_expr.label("display_name")

    track_stats_stmt = (
        select(
            artist_label,
            func.count(Track.id).label("track_count"),
            func.coalesce(func.sum(Track.duration_sec), 0).label("total_duration_sec"),
            func.min(Track.year).label("min_year"),
            func.max(Track.year).label("max_year"),
        )
        .where(_library_artifact_filter())
        .group_by(artist_expr)
    )
    if q:
        track_stats_stmt = track_stats_stmt.where(artist_expr.ilike(f"%{q}%"))
    track_stats = track_stats_stmt.subquery()

    release_counts = (
        select(
            artist_expr.label("display_name"),
            func.count(func.distinct(Track.release_id)).label("release_count"),
        )
        .where(Track.release_id.is_not(None), _library_artifact_filter())
        .group_by(artist_expr)
        .subquery()
    )
    fallback_groups = (
        select(
            artist_expr.label("display_name"),
            _album_expr().label("album"),
            _year_expr().label("year"),
        )
        .where(Track.release_id.is_(None), _library_artifact_filter())
        .group_by(artist_expr, _album_expr(), _year_expr())
        .subquery()
    )
    fallback_counts = (
        select(
            fallback_groups.c.display_name,
            func.count().label("fallback_count"),
        )
        .group_by(fallback_groups.c.display_name)
        .subquery()
    )
    album_count_expr = (
        func.coalesce(release_counts.c.release_count, 0)
        + func.coalesce(fallback_counts.c.fallback_count, 0)
    ).label("album_count")

    total = int((await db.scalar(select(func.count()).select_from(track_stats))) or 0)
    page = _clamp_page(page, total, per_page)

    data_stmt = (
        select(
            track_stats.c.display_name,
            track_stats.c.track_count,
            album_count_expr,
            track_stats.c.total_duration_sec,
            track_stats.c.min_year,
            track_stats.c.max_year,
        )
        .outerjoin(
            release_counts,
            release_counts.c.display_name == track_stats.c.display_name,
        )
        .outerjoin(
            fallback_counts,
            fallback_counts.c.display_name == track_stats.c.display_name,
        )
    )

    valid_sort = sort if sort in _VALID_ARTIST_SORTS else "name"
    if valid_sort == "tracks":
        data_stmt = data_stmt.order_by(
            track_stats.c.track_count.desc(), track_stats.c.display_name
        )
    elif valid_sort == "albums":
        data_stmt = data_stmt.order_by(album_count_expr.desc(), track_stats.c.display_name)
    elif valid_sort == "duration":
        data_stmt = data_stmt.order_by(
            track_stats.c.total_duration_sec.desc(), track_stats.c.display_name
        )
    else:
        data_stmt = data_stmt.order_by(track_stats.c.display_name)

    data_stmt = data_stmt.offset(_page_offset(page, per_page)).limit(per_page)
    artist_rows = (await db.execute(data_stmt)).mappings().all()
    items = [
        ArtistRow(
            display_name=str(row["display_name"]),
            track_count=int(row["track_count"]),
            album_count=int(row["album_count"]),
            total_duration_sec=int(row["total_duration_sec"]),
            min_year=str(row["min_year"]) if row["min_year"] is not None else None,
            max_year=str(row["max_year"]) if row["max_year"] is not None else None,
        )
        for row in artist_rows
    ]

    if items:
        display_names = [item.display_name for item in items]
        artist_fmt_stmt = (
            select(artist_expr.label("display_name"), Track.file_format)
            .where(
                and_(
                    artist_expr.in_(display_names),
                    Track.file_format.is_not(None),
                    _library_artifact_filter(),
                )
            )
            .distinct()
            .order_by(artist_expr, Track.file_format)
        )
        formats_by_artist: dict[str, list[str]] = {}
        for fmt_row in (await db.execute(artist_fmt_stmt)).mappings():
            formats_by_artist.setdefault(str(fmt_row["display_name"]), []).append(
                str(fmt_row["file_format"])
            )
        for item in items:
            item.formats = formats_by_artist.get(item.display_name, [])

    return Page(items=items, total=total, page=page, per_page=per_page)


async def get_artist_detail(
    db: AsyncSession,
    *,
    artist_name: str,
    page: int = 1,
    per_page: int = _DEFAULT_PAGE_SIZE,
) -> ArtistDetail:
    per_page = _clamp_per_page(per_page)
    page = max(1, page)
    artist_expr = _artist_expr()

    count_stmt = select(func.count(Track.id)).where(
        artist_expr == artist_name, _present_library_artifact_filter()
    )
    total_tracks = int((await db.scalar(count_stmt)) or 0)

    page = _clamp_page(page, total_tracks, per_page)

    stmt = (
        select(Track)
        .options(
            selectinload(Track.release),
            selectinload(Track.import_plans),
            selectinload(Track.catalog_album),
        )
        .where(artist_expr == artist_name, _present_library_artifact_filter())
        .order_by(Track.year, Track.album, Track.disc, Track.track_no, Track.title, Track.id)
        .offset(_page_offset(page, per_page))
        .limit(per_page)
    )
    tracks = list((await db.execute(stmt)).scalars().all())

    total_duration_stmt = select(func.coalesce(func.sum(Track.duration_sec), 0)).where(
        artist_expr == artist_name, _present_library_artifact_filter()
    )
    total_duration = int((await db.scalar(total_duration_stmt)) or 0)

    album_count = await _count_album_groups(
        db, artist_expr == artist_name, _present_library_artifact_filter()
    )

    album_map: dict[tuple[int | None, str | None, str | None], AlbumGroup] = {}
    for t in tracks:
        if t.release_id is not None:
            key: tuple[int | None, str | None, str | None] = (t.release_id, None, None)
        else:
            key = (None, t.album or UNKNOWN, t.year or "")

        if key not in album_map:
            rel = t.release
            album_map[key] = AlbumGroup(
                album=(rel.title if rel and rel.title else t.album) or UNKNOWN,
                year=(rel.year if rel and rel.year else t.year),
                release_id=t.release_id,
                release_mbid=rel.release_mbid if rel else None,
                label=rel.label if rel else None,
                country=rel.country if rel else None,
                catalog_number=rel.catalog_number if rel else None,
            )
        album_map[key].tracks.append(to_track_row(t))

    total_pages = max(1, math.ceil(total_tracks / per_page))

    return ArtistDetail(
        display_name=artist_name,
        track_count=total_tracks,
        album_count=album_count,
        total_duration_sec=total_duration,
        albums=list(album_map.values()),
        page=page,
        per_page=per_page,
        total_track_pages=total_pages,
        has_prev=page > 1,
        has_next=page < total_pages,
    )
