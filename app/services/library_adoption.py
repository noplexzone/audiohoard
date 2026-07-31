from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.media_formats import IMPORTABLE_AUDIO_SUFFIXES
from app.metadata.audio_file import AudioFileMetadata, read_audio_file_metadata
from app.metadata.filename_parse import (
    normalize_for_catalog_match,
    parse_filename,
    parsed_position_evidence,
)
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import (
    CollisionState,
    ImportPlan,
    LibraryFileState,
    TagVerificationState,
)
from app.models.job import Job, JobStatus
from app.models.library_adoption import (
    AdoptionCandidateState,
    AdoptionScanState,
    AdoptionScopeKind,
    LibraryAdoptionCandidate,
    LibraryAdoptionScan,
)
from app.models.release import Release
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
)
from app.services.catalog import _has_symlink_component

_SCAN_LOCK = asyncio.Lock()
_STALE_AFTER = timedelta(minutes=15)
_MAX_ERROR_LENGTH = 500


@dataclass(frozen=True)
class AdoptionScope:
    kind: AdoptionScopeKind = AdoptionScopeKind.full
    scope_id: int | None = None
    artist_name: str | None = None
    album_title: str | None = None
    year: str | None = None

    def payload(self) -> dict[str, str]:
        return {
            key: value
            for key, value in {
                "artist_name": self.artist_name,
                "album_title": self.album_title,
                "year": self.year,
            }.items()
            if value
        }


@dataclass(frozen=True)
class FileSnapshot:
    path: Path
    device: int
    inode: int
    size: int
    mtime_ns: int
    sha256: str
    metadata: AudioFileMetadata
    folder_artist: str | None
    folder_album: str | None
    filename_title: str | None
    filename_disc: int | None
    filename_track: int | None

    @property
    def token(self) -> str:
        raw = f"{self.device}:{self.inode}:{self.size}:{self.mtime_ns}:{self.sha256}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def evidence(self) -> dict[str, Any]:
        return {
            "metadata": self.metadata.to_dict(),
            "folder_artist": self.folder_artist,
            "folder_album": self.folder_album,
            "filename_title": self.filename_title,
            "filename_disc": self.filename_disc,
            "filename_track": self.filename_track,
        }


@dataclass(frozen=True)
class MatchDecision:
    state: AdoptionCandidateState
    confidence: str
    reasons: tuple[str, ...]
    artist_id: int | None = None
    album_id: int | None = None
    catalog_track_id: int | None = None
    track_id: int | None = None


def _safe_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_LENGTH]


def _norm(value: str | None) -> str:
    return normalize_for_catalog_match(value or "")


def _duration_conflicts(actual: int | None, expected: int | None) -> bool:
    return actual is not None and expected is not None and abs(actual - expected) > 8


def _snapshot_file(root: Path, path: Path) -> FileSnapshot | None:
    root = root.resolve()
    if _has_symlink_component(root, path):
        return None
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        before = os.stat(resolved, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
            return None
        digest = hashlib.sha256()
        fd = os.open(resolved, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            with os.fdopen(fd, "rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        except BaseException:
            with contextlib.suppress(OSError):
                os.close(fd)
            raise
        metadata = read_audio_file_metadata(resolved)
        after = os.stat(resolved, follow_symlinks=False)
    except (OSError, ValueError, Exception) as exc:
        # Mutagen errors are intentionally treated as unsupported/corrupt evidence.
        if isinstance(exc, asyncio.CancelledError):
            raise
        return None
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after:
        return None
    relative = resolved.relative_to(root)
    parts = relative.parts
    parsed = parse_filename(resolved.name)
    position = parsed_position_evidence(resolved.name)
    return FileSnapshot(
        path=resolved,
        device=before.st_dev,
        inode=before.st_ino,
        size=before.st_size,
        mtime_ns=before.st_mtime_ns,
        sha256=digest.hexdigest(),
        metadata=metadata,
        folder_artist=parts[0] if len(parts) >= 2 else None,
        folder_album=parts[-2] if len(parts) >= 2 else None,
        filename_title=parsed.title,
        filename_disc=position.get("disc"),
        filename_track=position.get("track_no"),
    )


def discover_audio_files(
    root: Path, *, excluded_paths: set[str] | None = None
) -> list[FileSnapshot]:
    root = root.resolve()
    if not root.is_dir() or root.is_symlink():
        return []
    snapshots: list[FileSnapshot] = []
    for directory, dirs, files in os.walk(root, topdown=True, followlinks=False):
        base = Path(directory)
        dirs[:] = sorted(
            name
            for name in dirs
            if not (base / name).is_symlink() and not _has_symlink_component(root, base / name)
        )
        for name in sorted(files):
            path = base / name
            if path.suffix.casefold() not in IMPORTABLE_AUDIO_SUFFIXES or path.is_symlink():
                continue
            if excluded_paths and str(path.resolve(strict=False)) in excluded_paths:
                continue
            snapshot = _snapshot_file(root, path)
            if snapshot is not None:
                snapshots.append(snapshot)
    return snapshots


async def enqueue_library_adoption_scan(
    db: AsyncSession, *, library_root: Path, scope: AdoptionScope | None = None
) -> int:
    del library_root  # The configured root is supplied again by the persisted runner.
    scope = scope or AdoptionScope()
    if scope.kind == AdoptionScopeKind.full:
        if scope.scope_id is not None or scope.payload():
            raise ValueError("full scans cannot carry a scope identifier")
    elif scope.kind == AdoptionScopeKind.catalog_artist:
        if scope.scope_id is None or await db.get(CatalogArtist, scope.scope_id) is None:
            raise ValueError("catalog artist not found")
    elif scope.kind == AdoptionScopeKind.catalog_album:
        if scope.scope_id is None or await db.get(CatalogAlbum, scope.scope_id) is None:
            raise ValueError("catalog release not found")
    elif scope.kind in {AdoptionScopeKind.imported_artist, AdoptionScopeKind.imported_release}:
        if not scope.artist_name:
            raise ValueError("imported artist name is required")
        if scope.kind == AdoptionScopeKind.imported_release and not scope.album_title:
            raise ValueError("imported release title is required")
    else:
        raise ValueError("unsupported adoption scope")

    payload = json.dumps(scope.payload(), sort_keys=True) if scope.payload() else None
    active = await db.scalar(
        select(LibraryAdoptionScan.id)
        .where(
            LibraryAdoptionScan.state.in_([AdoptionScanState.queued, AdoptionScanState.running]),
            LibraryAdoptionScan.scope_kind == scope.kind,
            LibraryAdoptionScan.scope_id.is_(scope.scope_id)
            if scope.scope_id is None
            else LibraryAdoptionScan.scope_id == scope.scope_id,
            LibraryAdoptionScan.scope_json.is_(payload)
            if payload is None
            else LibraryAdoptionScan.scope_json == payload,
        )
        .order_by(LibraryAdoptionScan.id)
    )
    if active is not None:
        return int(active)
    scan = LibraryAdoptionScan(scope_kind=scope.kind, scope_id=scope.scope_id, scope_json=payload)
    db.add(scan)
    await db.flush()
    return scan.id


async def _catalog_albums(db: AsyncSession, scan: LibraryAdoptionScan) -> list[CatalogAlbum]:
    query = select(CatalogAlbum).options(
        selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks)
    )
    if scan.scope_kind == AdoptionScopeKind.catalog_artist:
        query = query.where(CatalogAlbum.artist_id == scan.scope_id)
    elif scan.scope_kind == AdoptionScopeKind.catalog_album:
        query = query.where(CatalogAlbum.id == scan.scope_id)
    return list((await db.scalars(query.order_by(CatalogAlbum.id))).all())


def _match_album(
    snapshot: FileSnapshot, albums: list[CatalogAlbum]
) -> tuple[CatalogAlbum | None, tuple[str, ...]]:
    metadata = snapshot.metadata
    observed_mbid = metadata.release_group_mbid or metadata.album_mbid
    if observed_mbid:
        matches = [
            album
            for album in albums
            if album.mbid and album.mbid.casefold() == observed_mbid.casefold()
        ]
        if len(matches) == 1:
            return matches[0], ("album_mbid",)
        if len(matches) > 1:
            return None, ("duplicate_album_mbid",)
        return None, ("album_mbid_contradiction",)
    if not metadata.album or not (metadata.album_artist or metadata.artist):
        folder_year: str | None = None
        folder_album = snapshot.folder_album or ""
        year_match = re.search(r"\s+\(((?:19|20)\d{2})\)$", folder_album)
        if year_match:
            folder_year = year_match.group(1)
            folder_album = folder_album[: year_match.start()]
        matches = [
            album
            for album in albums
            if snapshot.folder_artist
            and _norm(album.artist.name) == _norm(snapshot.folder_artist)
            and _norm(album.title) == _norm(folder_album)
            and not (folder_year and album.year and folder_year != album.year)
        ]
        if len(matches) == 1:
            return matches[0], ("canonical_library_folders",)
        if len(matches) > 1:
            return None, ("ambiguous_album_folder",)
        return None, ("insufficient_album_tags",)
    artist = metadata.album_artist or metadata.artist
    matches = [
        album
        for album in albums
        if _norm(album.title) == _norm(metadata.album)
        and _norm(album.artist.name) == _norm(artist)
        and not (metadata.year and album.year and metadata.year != album.year)
    ]
    if len(matches) == 1:
        return matches[0], ("artist_album_tags",)
    if len(matches) > 1:
        return None, ("ambiguous_album",)
    return None, ("album_identity_contradiction",)


def _match_catalog(snapshot: FileSnapshot, albums: list[CatalogAlbum]) -> MatchDecision:
    album, album_reasons = _match_album(snapshot, albums)
    if album is None:
        state = (
            AdoptionCandidateState.review
            if "ambiguous" in " ".join(album_reasons)
            or "contradiction" in " ".join(album_reasons)
            or "duplicate" in " ".join(album_reasons)
            else AdoptionCandidateState.unmatched
        )
        return MatchDecision(state, "none", album_reasons)
    metadata = snapshot.metadata
    if metadata.recording_mbid:
        recording = [
            track
            for track in album.tracks
            if track.recording_mbid
            and track.recording_mbid.casefold() == metadata.recording_mbid.casefold()
        ]
        if len(recording) == 1:
            track = recording[0]
            if metadata.title and _norm(metadata.title) != _norm(track.title):
                return MatchDecision(
                    AdoptionCandidateState.review,
                    "contradictory",
                    ("recording_title_contradiction",),
                    album.artist_id,
                    album.id,
                )
            return MatchDecision(
                AdoptionCandidateState.pending,
                "exact_mbid",
                album_reasons + ("recording_mbid",),
                album.artist_id,
                album.id,
                track.id,
            )
        return MatchDecision(
            AdoptionCandidateState.review,
            "contradictory",
            ("recording_mbid_not_unique",),
            album.artist_id,
            album.id,
        )
    title = metadata.title or snapshot.filename_title
    disc = metadata.disc or snapshot.filename_disc or 1
    position = metadata.track or snapshot.filename_track
    if title and position:
        matches = [
            track for track in album.tracks if track.disc == disc and track.position == position
        ]
        if len(matches) == 1:
            track = matches[0]
            if _norm(track.title) != _norm(title) or _duration_conflicts(
                metadata.duration_sec, track.duration_sec
            ):
                return MatchDecision(
                    AdoptionCandidateState.review,
                    "contradictory",
                    ("position_title_or_duration_contradiction",),
                    album.artist_id,
                    album.id,
                )
            return MatchDecision(
                AdoptionCandidateState.pending,
                "position_title",
                album_reasons + ("disc_position_title",),
                album.artist_id,
                album.id,
                track.id,
            )
        if matches:
            return MatchDecision(
                AdoptionCandidateState.review,
                "ambiguous",
                ("duplicate_position",),
                album.artist_id,
                album.id,
            )
    if title:
        matches = [track for track in album.tracks if _norm(track.title) == _norm(title)]
        compatible = [
            track
            for track in matches
            if not _duration_conflicts(metadata.duration_sec, track.duration_sec)
        ]
        if len(compatible) == 1 and metadata.duration_sec is not None:
            track = compatible[0]
            return MatchDecision(
                AdoptionCandidateState.pending,
                "title_duration",
                album_reasons + ("unique_title_duration",),
                album.artist_id,
                album.id,
                track.id,
            )
    if len(album.tracks) == 1 and album_reasons == ("artist_album_tags",):
        track = album.tracks[0]
        if title and _norm(title) != _norm(track.title):
            return MatchDecision(
                AdoptionCandidateState.review,
                "contradictory",
                ("sole_track_title_contradiction",),
                album.artist_id,
                album.id,
            )
        return MatchDecision(
            AdoptionCandidateState.pending,
            "scoped_sole_track",
            album_reasons + ("sole_track",),
            album.artist_id,
            album.id,
            track.id,
        )
    return MatchDecision(
        AdoptionCandidateState.review if title else AdoptionCandidateState.unmatched,
        "insufficient",
        ("track_identity_not_unique",),
        album.artist_id,
        album.id,
    )


async def _match_imported(
    db: AsyncSession, snapshot: FileSnapshot, scan: LibraryAdoptionScan
) -> MatchDecision:
    payload = json.loads(scan.scope_json or "{}")
    artist_name = str(payload.get("artist_name", ""))
    album_title = payload.get("album_title")
    year = payload.get("year")
    query = select(Track).where(
        func.lower(func.coalesce(Track.album_artist, Track.artist, "")) == artist_name.casefold(),
        Track.import_state == ImportWorkflowState.imported,
    )
    if album_title:
        query = query.where(
            func.lower(func.coalesce(Track.album, "")) == str(album_title).casefold()
        )
    if year:
        query = query.where(Track.year == str(year))
    tracks = list((await db.scalars(query.order_by(Track.id))).all())
    metadata = snapshot.metadata
    candidates = [
        track
        for track in tracks
        if (not metadata.album or _norm(track.album) == _norm(metadata.album))
        and (not metadata.title or _norm(track.title) == _norm(metadata.title))
        and (not metadata.track or track.track_no == metadata.track)
        and (not metadata.disc or (track.disc or 1) == metadata.disc)
        and (
            not metadata.recording_mbid
            or (track.mbid or "").casefold() == metadata.recording_mbid.casefold()
        )
        and not _duration_conflicts(metadata.duration_sec, track.duration_sec)
    ]
    if len(candidates) == 1 and metadata.title and (metadata.album or album_title):
        track = candidates[0]
        return MatchDecision(
            AdoptionCandidateState.pending,
            "existing_track",
            ("existing_import_identity",),
            track.catalog_album.artist_id if track.catalog_album else None,
            track.catalog_album_id,
            track.catalog_track_id,
            track.id,
        )
    if tracks and not candidates:
        return MatchDecision(
            AdoptionCandidateState.review, "contradictory", ("imported_identity_contradiction",)
        )
    if len(candidates) > 1:
        return MatchDecision(
            AdoptionCandidateState.review, "ambiguous", ("multiple_imported_tracks",)
        )
    return MatchDecision(
        AdoptionCandidateState.unmatched, "none", ("no_existing_import_identity",)
    )


def _candidate(
    scan_id: int, snapshot: FileSnapshot, decision: MatchDecision
) -> LibraryAdoptionCandidate:
    return LibraryAdoptionCandidate(
        scan_id=scan_id,
        path=str(snapshot.path),
        device=snapshot.device,
        inode=snapshot.inode,
        size_bytes=snapshot.size,
        mtime_ns=snapshot.mtime_ns,
        content_sha256=snapshot.sha256,
        snapshot_token=snapshot.token,
        evidence_json=json.dumps(snapshot.evidence(), sort_keys=True),
        proposed_artist_id=decision.artist_id,
        proposed_album_id=decision.album_id,
        proposed_catalog_track_id=decision.catalog_track_id,
        proposed_track_id=decision.track_id,
        confidence=decision.confidence,
        reason_codes_json=json.dumps(decision.reasons),
        state=decision.state,
    )


async def _path_is_claimed(db: AsyncSession, path: str) -> bool:
    count = await db.scalar(
        select(func.count(ImportPlan.id)).where(
            ImportPlan.destination_path == path,
            ImportPlan.status.in_(
                [
                    ImportWorkflowState.ready,
                    ImportWorkflowState.importing,
                    ImportWorkflowState.imported,
                ]
            ),
            ImportPlan.file_state != LibraryFileState.removed,
        )
    )
    return bool(count)


async def _track_has_other_file(db: AsyncSession, catalog_track_id: int, path: str) -> bool:
    count = await db.scalar(
        select(func.count(ImportPlan.id))
        .join(Track, ImportPlan.track_id == Track.id)
        .where(
            Track.catalog_track_id == catalog_track_id,
            ImportPlan.destination_path != path,
            ImportPlan.status == ImportWorkflowState.imported,
            ImportPlan.file_state == LibraryFileState.present,
        )
    )
    return bool(count)


def _snapshot_matches(candidate: LibraryAdoptionCandidate, root: Path) -> FileSnapshot | None:
    snapshot = _snapshot_file(root, Path(candidate.path))
    if snapshot is None or snapshot.token != candidate.snapshot_token:
        return None
    return snapshot


async def _adopt_candidate(
    db: AsyncSession,
    candidate: LibraryAdoptionCandidate,
    root: Path,
    batch_releases: dict[int, Release],
) -> None:
    snapshot = await asyncio.to_thread(_snapshot_matches, candidate, root)
    if snapshot is None:
        candidate.state = AdoptionCandidateState.stale
        return
    if await _path_is_claimed(db, candidate.path):
        candidate.state = AdoptionCandidateState.adopted
        candidate.reason_codes_json = json.dumps(["destination_already_owned"])
        return
    if candidate.proposed_catalog_track_id and await _track_has_other_file(
        db, candidate.proposed_catalog_track_id, candidate.path
    ):
        candidate.state = AdoptionCandidateState.review
        candidate.reason_codes_json = json.dumps(["catalog_track_already_owned"])
        return

    track: Track | None = None
    if candidate.proposed_track_id:
        track = await db.get(
            Track,
            candidate.proposed_track_id,
            options=(
                selectinload(Track.import_plans),
                selectinload(Track.release),
                selectinload(Track.catalog_album),
            ),
        )
    if track is None and candidate.proposed_catalog_track_id:
        existing = list(
            (
                await db.scalars(
                    select(Track)
                    .where(Track.catalog_track_id == candidate.proposed_catalog_track_id)
                    .options(selectinload(Track.import_plans), selectinload(Track.release))
                    .order_by(Track.id)
                )
            ).all()
        )
        repairable = [
            item
            for item in existing
            if not any(
                plan.status == ImportWorkflowState.imported
                and plan.file_state == LibraryFileState.present
                for plan in item.import_plans
            )
        ]
        if len(repairable) == 1:
            track = repairable[0]
        elif len(repairable) > 1:
            candidate.state = AdoptionCandidateState.review
            candidate.reason_codes_json = json.dumps(["multiple_lost_track_records"])
            return

    album: CatalogAlbum | None = None
    catalog_track: CatalogAlbumTrack | None = None
    if candidate.proposed_album_id:
        album = await db.get(
            CatalogAlbum,
            candidate.proposed_album_id,
            options=(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks)),
        )
    if candidate.proposed_catalog_track_id:
        catalog_track = await db.get(CatalogAlbumTrack, candidate.proposed_catalog_track_id)

    is_new_track = track is None
    release: Release | None
    if track is None:
        if album is None or catalog_track is None:
            candidate.state = AdoptionCandidateState.review
            candidate.reason_codes_json = json.dumps(["catalog_identity_disappeared"])
            return
        release = batch_releases.get(album.id)
        if release is None:
            job = Job(
                source="library_adoption",
                query=f"Adopt {album.artist.name} — {album.title}",
                status=JobStatus.done,
                queue_hidden=True,
                catalog_album_id=album.id,
            )
            release = Release(
                job=job,
                source="library_adoption",
                title=album.title,
                album_artist=album.artist.name,
                year=album.year,
                release_mbid=album.mbid,
                track_count=len(album.tracks),
                import_state=ImportWorkflowState.imported,
            )
            db.add(job)
            batch_releases[album.id] = release
        track = Track(job=release.job, release=release, source="library_adoption")
        db.add(track)
    else:
        release = track.release
        if release is None:
            candidate.state = AdoptionCandidateState.review
            candidate.reason_codes_json = json.dumps(["existing_track_has_no_release"])
            return

    assert release is not None

    if album is not None and catalog_track is not None:
        track.catalog_album_id = album.id
        track.catalog_track_id = catalog_track.id
        track.title = catalog_track.title
        track.artist = album.artist.name
        track.album_artist = album.artist.name
        track.album = album.title
        track.year = album.year
        track.disc = catalog_track.disc
        track.disc_total = max((item.disc for item in album.tracks), default=1)
        track.track_no = catalog_track.position
        track.duration_sec = catalog_track.duration_sec or snapshot.metadata.duration_sec
        track.mbid = catalog_track.recording_mbid
    track.source = (
        "library_adoption"
        if track.source == "library_adoption" or track.id is None
        else track.source
    )
    if is_new_track:
        track.source_path = None
        track.staging_path = None
    track.file_format = snapshot.metadata.file_format
    track.file_size_bytes = snapshot.size
    track.content_sha256 = snapshot.sha256
    track.file_metadata_checked_at = datetime.now(UTC)
    track.acquisition_state = AcquisitionState.downloaded
    track.import_state = ImportWorkflowState.imported
    track.identity_state = (
        IdentityResolutionState.resolved if track.mbid else IdentityResolutionState.unresolved
    )
    track.fingerprint_state = FingerprintState.skipped
    track.acoustid_verification_state = AcoustIDVerificationState.unavailable
    track.acquisition_provenance_json = json.dumps(
        {
            "kind": "library_adoption",
            "candidate_id": candidate.id,
            "evidence": snapshot.evidence(),
        },
        sort_keys=True,
    )
    await db.flush()
    plan = ImportPlan(
        release=release,
        track=track,
        source_path=candidate.path,
        staging_path=None,
        destination_path=candidate.path,
        planned_operations_json=json.dumps(
            {"operation": "adopt_in_place", "candidate_id": candidate.id}
        ),
        collision_state=CollisionState.clear,
        tag_verification_state=TagVerificationState.verified
        if snapshot.metadata.title and snapshot.metadata.album
        else TagVerificationState.skipped,
        status=ImportWorkflowState.imported,
        file_state=LibraryFileState.present,
        file_checked_at=datetime.now(UTC),
    )
    db.add(plan)
    await db.flush()
    candidate.state = AdoptionCandidateState.adopted
    candidate.resulting_track_id = track.id
    candidate.resulting_import_plan_id = plan.id


async def _refresh_album_truth(db: AsyncSession, album_ids: set[int]) -> None:
    for album_id in album_ids:
        album = await db.get(CatalogAlbum, album_id, options=(selectinload(CatalogAlbum.tracks),))
        if album is None:
            continue
        expected = {track.id for track in album.tracks}
        present = set(
            (
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
            ).all()
        )
        album.in_library = bool(expected) and expected <= present


async def run_library_adoption_scan(
    db: AsyncSession, *, scan_id: int, library_root: Path
) -> LibraryAdoptionScan:
    async with _SCAN_LOCK:
        scan = await db.get(LibraryAdoptionScan, scan_id)
        if scan is None:
            raise ValueError("adoption scan not found")
        if scan.state in {AdoptionScanState.completed, AdoptionScanState.cancelled}:
            return scan
        scan.state = AdoptionScanState.running
        scan.started_at = scan.started_at or datetime.now(UTC)
        scan.heartbeat_at = datetime.now(UTC)
        scan.error_detail = None
        await db.commit()
        try:
            claimed = set(
                (
                    await db.scalars(
                        select(ImportPlan.destination_path).where(
                            ImportPlan.status.in_(
                                [
                                    ImportWorkflowState.ready,
                                    ImportWorkflowState.importing,
                                    ImportWorkflowState.imported,
                                ]
                            ),
                            ImportPlan.file_state != LibraryFileState.removed,
                        )
                    )
                ).all()
            )
            snapshots = await asyncio.to_thread(
                discover_audio_files, library_root, excluded_paths=claimed
            )
            albums = (
                await _catalog_albums(db, scan)
                if scan.scope_kind
                in {
                    AdoptionScopeKind.full,
                    AdoptionScopeKind.catalog_artist,
                    AdoptionScopeKind.catalog_album,
                }
                else []
            )
            candidates: list[LibraryAdoptionCandidate] = []
            for snapshot in snapshots:
                decision = (
                    _match_catalog(snapshot, albums)
                    if albums
                    else await _match_imported(db, snapshot, scan)
                )
                candidates.append(_candidate(scan.id, snapshot, decision))
            by_target: dict[tuple[int | None, int | None], list[LibraryAdoptionCandidate]] = {}
            for candidate in candidates:
                if candidate.state == AdoptionCandidateState.pending:
                    by_target.setdefault(
                        (candidate.proposed_catalog_track_id, candidate.proposed_track_id), []
                    ).append(candidate)
            for group in by_target.values():
                if len(group) > 1:
                    for candidate in group:
                        candidate.state = AdoptionCandidateState.review
                        candidate.confidence = "ambiguous"
                        candidate.reason_codes_json = json.dumps(["multiple_files_for_one_track"])
            db.add_all(candidates)
            scan.scanned_count = len(snapshots)
            scan.heartbeat_at = datetime.now(UTC)
            await db.commit()
            album_ids: set[int] = set()
            batch_releases: dict[int, Release] = {}
            for candidate in candidates:
                if candidate.state != AdoptionCandidateState.pending:
                    continue
                await _adopt_candidate(db, candidate, library_root, batch_releases)
                if candidate.proposed_album_id:
                    album_ids.add(candidate.proposed_album_id)
                scan.heartbeat_at = datetime.now(UTC)
                await db.commit()
            await _refresh_album_truth(db, album_ids)
            states = list(
                (
                    await db.scalars(
                        select(LibraryAdoptionCandidate.state).where(
                            LibraryAdoptionCandidate.scan_id == scan.id
                        )
                    )
                ).all()
            )
            scan.adopted_count = states.count(AdoptionCandidateState.adopted)
            scan.review_count = states.count(AdoptionCandidateState.review)
            scan.unmatched_count = states.count(AdoptionCandidateState.unmatched)
            scan.stale_count = states.count(AdoptionCandidateState.stale)
            scan.error_count = states.count(AdoptionCandidateState.failed)
            scan.state = AdoptionScanState.completed
            scan.completed_at = datetime.now(UTC)
            scan.heartbeat_at = datetime.now(UTC)
            await db.commit()
            return scan
        except asyncio.CancelledError:
            await db.rollback()
            scan = await db.get(LibraryAdoptionScan, scan_id)
            assert scan is not None
            scan.state = AdoptionScanState.cancelled
            scan.completed_at = datetime.now(UTC)
            await db.commit()
            raise
        except Exception as exc:
            await db.rollback()
            scan = await db.get(LibraryAdoptionScan, scan_id)
            assert scan is not None
            scan.state = AdoptionScanState.failed
            scan.error_detail = _safe_error(exc)
            scan.error_count += 1
            scan.completed_at = datetime.now(UTC)
            await db.commit()
            return scan


async def recover_library_adoption_scans(db: AsyncSession) -> list[int]:
    cutoff = datetime.now(UTC) - _STALE_AFTER
    stale = list(
        (
            await db.scalars(
                select(LibraryAdoptionScan).where(
                    LibraryAdoptionScan.state == AdoptionScanState.running,
                    LibraryAdoptionScan.heartbeat_at < cutoff,
                )
            )
        ).all()
    )
    for scan in stale:
        scan.state = AdoptionScanState.queued
        scan.error_detail = "Recovered after interrupted scan"
    queued = list(
        (
            await db.scalars(
                select(LibraryAdoptionScan.id)
                .where(LibraryAdoptionScan.state == AdoptionScanState.queued)
                .order_by(LibraryAdoptionScan.id)
            )
        ).all()
    )
    await db.commit()
    return [int(scan_id) for scan_id in queued]


async def run_queued_library_adoption_scans(
    session_factory: async_sessionmaker[AsyncSession], *, library_root: Path
) -> None:
    async with session_factory() as db:
        scan_ids = await recover_library_adoption_scans(db)
    for scan_id in scan_ids:
        async with session_factory() as db:
            await run_library_adoption_scan(db, scan_id=scan_id, library_root=library_root)
