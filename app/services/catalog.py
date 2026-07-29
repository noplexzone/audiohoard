from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import and_, exists, func, literal, or_, select
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.media_formats import IMPORTABLE_AUDIO_SUFFIXES
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.import_plan import ImportPlan
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.naming.convention import _sanitize_segment
from app.settings_service import QualityProfile

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
    preferred = {_format_family(value) for value in profile.format_preference}
    if family not in preferred:
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
    release_count: int
    downloaded_file_count: int
    wanted_release_count: int
    watchlisted: bool


@dataclass
class MissingReleaseRow:
    id: int
    artist_name: str
    title: str
    year: str | None
    artwork_url: str | None
    wanted_track_count: int
    downloaded_track_count: int


@dataclass(frozen=True)
class ReleaseProgress:
    wanted_track_count: int
    downloaded_track_count: int
    downloaded_catalog_track_ids: frozenset[int] = frozenset()

    @property
    def complete(self) -> bool:
        return self.wanted_track_count > 0 and (
            self.downloaded_track_count >= self.wanted_track_count
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
            if plan.status == ImportWorkflowState.imported and plan.destination_path.strip()
        ]
        if imported_destinations:
            return imported_destinations[-1]
    return (t.source_path or "").strip()


def to_track_row(t: Track) -> TrackRow:
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
    """Project catalog release ownership from wanted tracks and committed imports.

    ``CatalogAlbum.track_count`` remains the truthful denominator when a provider has
    reported a count but the track manifest has not been hydrated yet. Imported counts
    are distinct catalog tracks so repeated acquisition attempts cannot inflate progress.
    """
    ids = sorted(set(album_ids))
    if not ids:
        return {}

    album_rows = (
        await db.execute(
            select(
                CatalogAlbum.id,
                CatalogAlbum.track_count,
                CatalogAlbum.title,
                CatalogAlbum.year,
                CatalogArtist.name,
            )
            .join(CatalogArtist, CatalogArtist.id == CatalogAlbum.artist_id)
            .where(CatalogAlbum.id.in_(ids))
        )
    ).all()
    album_data = [(int(row[0]), row[1], row[2], row[3], row[4]) for row in album_rows]
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
    filesystem_evidence = (
        await asyncio.to_thread(_filesystem_release_evidence, library_root, album_data)
        if library_root is not None
        else {}
    )
    imported_by_album: dict[int, set[int]] = {}
    imported_rows = await db.execute(
        select(Track.catalog_album_id, Track.catalog_track_id)
        .join(CatalogAlbumTrack, CatalogAlbumTrack.id == Track.catalog_track_id)
        .where(
            Track.catalog_album_id.in_(ids),
            CatalogAlbumTrack.album_id == Track.catalog_album_id,
            _library_artifact_filter(),
        )
        .distinct()
    )
    for album_id, catalog_track_id in imported_rows:
        if album_id is not None and catalog_track_id is not None:
            imported_by_album.setdefault(int(album_id), set()).add(int(catalog_track_id))

    progress: dict[int, ReleaseProgress] = {}
    for album_id, known_track_count, _title, _year, _artist_name in album_data:
        release_id = int(album_id)
        wanted = max(int(known_track_count or 0), manifest_counts.get(release_id, 0))
        downloaded_id_set = set(imported_by_album.get(release_id, set()))
        release_evidence = filesystem_evidence.get(release_id)
        release_manifest = manifest_tracks.get(release_id, {})
        if release_evidence is not None:
            downloaded_id_set.update(
                release_manifest[track_key]
                for track_key in release_evidence.track_keys
                if track_key in release_manifest
            )
        downloaded_ids = frozenset(downloaded_id_set)
        downloaded = (
            len(downloaded_ids)
            if release_manifest
            else max(
                len(downloaded_ids),
                release_evidence.file_count if release_evidence is not None else 0,
            )
        )
        progress[release_id] = ReleaseProgress(
            wanted_track_count=wanted,
            downloaded_track_count=min(downloaded, wanted),
            downloaded_catalog_track_ids=downloaded_ids,
        )
    return progress


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

    data_stmt = select(Track).options(selectinload(Track.import_plans))
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
    """Return watchlisted artists and every artist with a persisted library artifact."""
    per_page = _clamp_per_page(per_page)
    page = max(1, page)
    track_artist = func.lower(func.trim(_artist_expr()))
    catalog_artist = func.lower(func.trim(CatalogArtist.name))
    belongs_to_catalog_artist = or_(
        CatalogAlbum.artist_id == CatalogArtist.id,
        and_(Track.catalog_album_id.is_(None), track_artist == catalog_artist),
    )
    downloaded_count = (
        select(func.count(Track.id))
        .outerjoin(CatalogAlbum, CatalogAlbum.id == Track.catalog_album_id)
        .where(belongs_to_catalog_artist, _library_artifact_filter())
        .correlate(CatalogArtist)
        .scalar_subquery()
    )
    wanted_count = (
        select(func.count(CatalogAlbumProvider.id))
        .join(
            CatalogArtistIdentity,
            CatalogArtistIdentity.id == CatalogAlbumProvider.artist_identity_id,
        )
        .outerjoin(CatalogAlbum, CatalogAlbum.id == CatalogAlbumProvider.catalog_album_id)
        .where(
            CatalogArtistIdentity.artist_id == CatalogArtist.id,
            or_(
                CatalogAlbumProvider.catalog_album_id.is_(None),
                CatalogAlbum.in_library.is_(False),
            ),
        )
        .correlate(CatalogArtist)
        .scalar_subquery()
    )
    release_count = (
        select(func.count(CatalogAlbumProvider.id))
        .join(
            CatalogArtistIdentity,
            CatalogArtistIdentity.id == CatalogAlbumProvider.artist_identity_id,
        )
        .where(CatalogArtistIdentity.artist_id == CatalogArtist.id)
        .correlate(CatalogArtist)
        .scalar_subquery()
    )
    has_imported_file = exists(
        select(Track.id)
        .outerjoin(CatalogAlbum, CatalogAlbum.id == Track.catalog_album_id)
        .where(belongs_to_catalog_artist, _library_artifact_filter())
    )
    catalog_filters: list[Any] = [or_(CatalogArtist.monitored.is_(True), has_imported_file)]
    if q:
        catalog_filters.append(CatalogArtist.name.ilike(f"%{q}%"))
    catalog_rows = select(
        CatalogArtist.id.label("catalog_id"),
        CatalogArtist.name.label("name"),
        CatalogArtist.artwork_url.label("artwork_url"),
        CatalogArtist.monitored.label("monitored"),
        release_count.label("release_count"),
        downloaded_count.label("downloaded_file_count"),
        wanted_count.label("wanted_release_count"),
    ).where(*catalog_filters)

    matching_catalog_artist = exists(
        select(CatalogArtist.id).where(catalog_artist == track_artist).correlate(Track)
    )
    legacy_filters: list[Any] = [
        Track.catalog_album_id.is_(None),
        _library_artifact_filter(),
        ~matching_catalog_artist,
    ]
    if q:
        legacy_filters.append(_artist_expr().ilike(f"%{q}%"))
    legacy_rows = (
        select(
            literal(None).label("catalog_id"),
            _artist_expr().label("name"),
            literal(None).label("artwork_url"),
            literal(False).label("monitored"),
            literal(0).label("release_count"),
            func.count(Track.id).label("downloaded_file_count"),
            literal(0).label("wanted_release_count"),
        )
        .where(*legacy_filters)
        .group_by(_artist_expr())
    )
    combined = catalog_rows.union_all(legacy_rows).subquery()
    total = int((await db.scalar(select(func.count()).select_from(combined))) or 0)
    page = _clamp_page(page, total, per_page)
    stmt = select(combined)
    valid_sort = sort if sort in _VALID_WATCHLIST_SORTS else "name"
    if valid_sort == "downloaded":
        stmt = stmt.order_by(
            combined.c.downloaded_file_count.desc(),
            combined.c.name,
            combined.c.catalog_id,
        )
    elif valid_sort == "wanted":
        stmt = stmt.order_by(
            combined.c.wanted_release_count.desc(),
            combined.c.name,
            combined.c.catalog_id,
        )
    else:
        stmt = stmt.order_by(combined.c.name, combined.c.catalog_id)
    rows = (await db.execute(stmt.offset(_page_offset(page, per_page)).limit(per_page))).mappings()
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
                release_count=int(row["release_count"] or 0),
                downloaded_file_count=int(row["downloaded_file_count"] or 0),
                wanted_release_count=int(row["wanted_release_count"] or 0),
                watchlisted=bool(row["monitored"]),
            )
        )
    return Page(items=items, total=total, page=page, per_page=per_page)


async def get_missing_releases_page(
    db: AsyncSession,
    *,
    q: str = "",
    sort: str = "year",
    page: int = 1,
    per_page: int = _DEFAULT_PAGE_SIZE,
) -> Page[MissingReleaseRow]:
    per_page = _clamp_per_page(per_page)
    page = max(1, page)
    manifest_count = (
        select(func.count(CatalogAlbumTrack.id))
        .where(CatalogAlbumTrack.album_id == CatalogAlbum.id)
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    wanted_count = func.max(func.coalesce(CatalogAlbum.track_count, 0), manifest_count)
    downloaded_count = (
        select(func.count(func.distinct(Track.catalog_track_id)))
        .where(
            Track.catalog_album_id == CatalogAlbum.id,
            Track.catalog_track_id.is_not(None),
            _library_artifact_filter(),
        )
        .correlate(CatalogAlbum)
        .scalar_subquery()
    )
    filters: list[Any] = [
        CatalogArtist.monitored.is_(True),
        CatalogAlbum.monitored.is_(True),
        downloaded_count < wanted_count,
    ]
    if q:
        pattern = f"%{q}%"
        filters.append(or_(CatalogArtist.name.ilike(pattern), CatalogAlbum.title.ilike(pattern)))
    rows_query = (
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
    total = int((await db.scalar(select(func.count()).select_from(rows_query.subquery()))) or 0)
    page = _clamp_page(page, total, per_page)
    valid_sort = sort if sort in {"year", "artist", "title"} else "year"
    if valid_sort == "artist":
        rows_query = rows_query.order_by(CatalogArtist.name, CatalogAlbum.title, CatalogAlbum.id)
    elif valid_sort == "title":
        rows_query = rows_query.order_by(CatalogAlbum.title, CatalogArtist.name, CatalogAlbum.id)
    else:
        rows_query = rows_query.order_by(
            CatalogAlbum.year.desc(), CatalogArtist.name, CatalogAlbum.title, CatalogAlbum.id
        )
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
        .options(selectinload(CatalogArtist.albums))
    )
    if q:
        stmt = stmt.where(CatalogArtist.name.ilike(f"%{q}%"))
    catalog_artists = list((await db.execute(stmt)).scalars().all())
    items: list[WatchlistedArtistRow] = []
    for artist in catalog_artists:
        albums = singles_eps = compilations = 0
        for release in artist.albums:
            release_type = (release.release_type or "album").casefold()
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
        artist_expr == artist_name, _library_artifact_filter()
    )
    total_tracks = int((await db.scalar(count_stmt)) or 0)

    page = _clamp_page(page, total_tracks, per_page)

    stmt = (
        select(Track)
        .options(selectinload(Track.release), selectinload(Track.import_plans))
        .where(artist_expr == artist_name, _library_artifact_filter())
        .order_by(Track.year, Track.album, Track.disc, Track.track_no, Track.title, Track.id)
        .offset(_page_offset(page, per_page))
        .limit(per_page)
    )
    tracks = list((await db.execute(stmt)).scalars().all())

    total_duration_stmt = select(func.coalesce(func.sum(Track.duration_sec), 0)).where(
        artist_expr == artist_name, _library_artifact_filter()
    )
    total_duration = int((await db.scalar(total_duration_stmt)) or 0)

    album_count = await _count_album_groups(
        db, artist_expr == artist_name, _library_artifact_filter()
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
