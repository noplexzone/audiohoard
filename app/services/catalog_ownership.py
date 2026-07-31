from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased, selectinload
from sqlalchemy.sql.elements import ColumnElement

from app.config import Settings
from app.metadata.content_rating import CONTENT_RATING_UNKNOWN, normalize_content_rating
from app.metadata.deezer import DeezerClient
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.models.import_plan import ImportPlan
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.catalog_metadata import _norm_title, release_bucket

logger = logging.getLogger(__name__)
_PROVENANCE_KEY = "catalog_ownership_rating_v1"


@dataclass(frozen=True)
class DeezerReleaseEvidence:
    track_id: str
    album_id: str | None
    content_rating: str


@dataclass(frozen=True)
class _OwnershipCandidate:
    track_id: int
    deezer_id: str
    destination_paths: tuple[str, ...]


class CatalogOwnershipEvidenceError(RuntimeError):
    """Strict reconciliation could not obtain authoritative provider evidence."""


def _provenance_payload(track: Track) -> dict[str, object] | None:
    if not track.acquisition_provenance_json:
        return {}
    try:
        value = json.loads(track.acquisition_provenance_json)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _rating_already_verified(track: Track) -> bool:
    current = track.catalog_album
    payload = _provenance_payload(track)
    if current is None or payload is None:
        return False
    marker = payload.get(_PROVENANCE_KEY)
    return bool(
        isinstance(marker, dict)
        and marker.get("deezer_id") == str(track.deezer_id)
        and marker.get("content_rating") == normalize_content_rating(current.content_rating)
    )


def _mark_rating_verified(track: Track, evidence: DeezerReleaseEvidence) -> None:
    payload = _provenance_payload(track)
    if payload is None:
        return
    payload[_PROVENANCE_KEY] = {
        "album_id": evidence.album_id,
        "content_rating": normalize_content_rating(evidence.content_rating),
        "deezer_id": evidence.track_id,
    }
    track.acquisition_provenance_json = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )


def _non_empty(column: Any) -> ColumnElement[bool]:
    return cast(ColumnElement[bool], column.is_not(None) & (func.length(func.trim(column)) > 0))


def _imported_plan_exists() -> ColumnElement[bool]:
    return exists(
        select(ImportPlan.id).where(
            ImportPlan.track_id == Track.id,
            ImportPlan.status == ImportWorkflowState.imported,
            _non_empty(ImportPlan.destination_path),
        )
    )


async def _collect_candidates(
    db: AsyncSession, *, artist_id: int | None
) -> list[_OwnershipCandidate]:
    sibling = aliased(CatalogAlbum)
    rated_sibling_exists = exists(
        select(sibling.id).where(
            sibling.artist_id == CatalogAlbum.artist_id,
            sibling.id != CatalogAlbum.id,
            func.lower(func.trim(sibling.title)) == func.lower(func.trim(CatalogAlbum.title)),
            (sibling.year == CatalogAlbum.year)
            | sibling.year.is_(None)
            | CatalogAlbum.year.is_(None),
            (sibling.release_type == CatalogAlbum.release_type)
            | sibling.release_type.is_(None)
            | CatalogAlbum.release_type.is_(None),
            (sibling.track_count == CatalogAlbum.track_count)
            | sibling.track_count.is_(None)
            | CatalogAlbum.track_count.is_(None),
            sibling.content_rating != CONTENT_RATING_UNKNOWN,
            CatalogAlbum.content_rating != CONTENT_RATING_UNKNOWN,
            sibling.content_rating != CatalogAlbum.content_rating,
        )
    )
    query = (
        select(Track)
        .join(CatalogAlbum, CatalogAlbum.id == Track.catalog_album_id)
        .where(
            Track.import_state == ImportWorkflowState.imported,
            Track.deezer_id.is_not(None),
            _imported_plan_exists(),
            rated_sibling_exists,
        )
        .options(selectinload(Track.catalog_album))
        .order_by(Track.id)
    )
    if artist_id is not None:
        query = query.where(CatalogAlbum.artist_id == artist_id)
    tracks = list((await db.scalars(query)).all())
    if not tracks:
        return []
    plans = (
        await db.execute(
            select(ImportPlan.track_id, ImportPlan.destination_path).where(
                ImportPlan.track_id.in_([track.id for track in tracks]),
                ImportPlan.status == ImportWorkflowState.imported,
                _non_empty(ImportPlan.destination_path),
            )
        )
    ).all()
    paths: dict[int, list[str]] = {}
    for track_id, destination_path in plans:
        if destination_path:
            paths.setdefault(int(track_id), []).append(str(destination_path))
    return [
        _OwnershipCandidate(track.id, str(track.deezer_id), tuple(paths.get(track.id, [])))
        for track in tracks
        if track.deezer_id and not _rating_already_verified(track)
    ]


async def _existing_candidate_ids(candidates: list[_OwnershipCandidate]) -> list[int]:
    def any_path_exists(values: tuple[str, ...]) -> bool:
        return any(Path(value).is_file() for value in values)

    checks = await asyncio.gather(
        *[
            asyncio.to_thread(any_path_exists, candidate.destination_paths)
            for candidate in candidates
        ]
    )
    return [
        candidate.track_id
        for candidate, exists_on_disk in zip(candidates, checks, strict=True)
        if exists_on_disk
    ]


async def _resolve_evidence(
    settings: Settings,
    candidates: list[_OwnershipCandidate],
    *,
    fail_fast: bool = False,
) -> dict[str, DeezerReleaseEvidence]:
    client = DeezerClient(settings.deezer_api_url)
    semaphore = asyncio.Semaphore(8)

    async def resolve(deezer_id: str) -> DeezerReleaseEvidence | None:
        try:
            async with semaphore:
                track = await client.get_track(deezer_id)
        except Exception as exc:
            if fail_fast:
                raise CatalogOwnershipEvidenceError(
                    f"Could not resolve Deezer ownership for track {deezer_id}"
                ) from exc
            logger.warning("Could not resolve Deezer ownership for track %s: %s", deezer_id, exc)
            return None
        if track is None:
            return None
        rating = normalize_content_rating(track.content_rating)
        if rating == CONTENT_RATING_UNKNOWN:
            return None
        return DeezerReleaseEvidence(track.deezer_id, track.album_id, rating)

    values = await asyncio.gather(*[resolve(candidate.deezer_id) for candidate in candidates])
    return {value.track_id: value for value in values if value is not None}


def _albums_compatible(current: CatalogAlbum, candidate: CatalogAlbum) -> bool:
    if current.artist_id != candidate.artist_id:
        return False
    if _norm_title(current.title) != _norm_title(candidate.title):
        return False
    if current.year and candidate.year and current.year != candidate.year:
        return False
    if (
        current.release_type
        and candidate.release_type
        and release_bucket(current.release_type) != release_bucket(candidate.release_type)
    ):
        return False
    return not (
        current.track_count
        and candidate.track_count
        and current.track_count != candidate.track_count
    )


def _matching_track(track: Track, album: CatalogAlbum) -> CatalogAlbumTrack | None:
    matches = [
        candidate
        for candidate in album.tracks
        if track.title and _norm_title(candidate.title) == _norm_title(track.title)
    ]
    if track.track_no is not None:
        positioned = [candidate for candidate in matches if candidate.position == track.track_no]
        if positioned:
            matches = positioned
    if track.disc is not None:
        on_disc = [candidate for candidate in matches if candidate.disc == track.disc]
        if on_disc:
            matches = on_disc
    if track.duration_sec is not None:
        matches = [
            candidate
            for candidate in matches
            if candidate.duration_sec is None
            or abs(candidate.duration_sec - track.duration_sec) <= 2
        ]
    return matches[0] if len(matches) == 1 else None


async def apply_catalog_ownership_evidence(
    db: AsyncSession,
    evidence_by_deezer_id: dict[str, DeezerReleaseEvidence],
    *,
    track_ids: list[int] | None = None,
) -> tuple[int, set[int], dict[int, DeezerReleaseEvidence]]:
    """Rebind imported tracks only when rating and sibling identity are unambiguous."""
    if not evidence_by_deezer_id:
        return 0, set(), {}
    query = (
        select(Track)
        .where(
            Track.import_state == ImportWorkflowState.imported,
            Track.deezer_id.in_(evidence_by_deezer_id),
            _imported_plan_exists(),
        )
        .options(selectinload(Track.catalog_album).selectinload(CatalogAlbum.tracks))
    )
    if track_ids is not None:
        if not track_ids:
            return 0, set(), {}
        query = query.where(Track.id.in_(track_ids))
    tracks = list((await db.scalars(query)).all())
    affected: set[int] = set()
    verified: dict[int, DeezerReleaseEvidence] = {}
    changed = 0
    for track in tracks:
        current = track.catalog_album
        evidence = evidence_by_deezer_id.get(str(track.deezer_id))
        if current is None or evidence is None:
            continue
        evidence_rating = normalize_content_rating(evidence.content_rating)
        if evidence_rating == CONTENT_RATING_UNKNOWN:
            continue
        if normalize_content_rating(current.content_rating) == evidence_rating:
            siblings = list(
                (
                    await db.scalars(
                        select(CatalogAlbum).where(
                            CatalogAlbum.artist_id == current.artist_id,
                            CatalogAlbum.id != current.id,
                            CatalogAlbum.content_rating != evidence_rating,
                        )
                    )
                ).all()
            )
            compatible_siblings = [
                sibling for sibling in siblings if _albums_compatible(current, sibling)
            ]
            if compatible_siblings:
                affected.add(current.id)
                affected.update(sibling.id for sibling in compatible_siblings)
                verified[track.id] = evidence
            continue
        candidates = list(
            (
                await db.scalars(
                    select(CatalogAlbum)
                    .where(
                        CatalogAlbum.artist_id == current.artist_id,
                        CatalogAlbum.id != current.id,
                        CatalogAlbum.content_rating == evidence_rating,
                    )
                    .options(selectinload(CatalogAlbum.tracks))
                )
            ).all()
        )
        compatible = [
            candidate for candidate in candidates if _albums_compatible(current, candidate)
        ]
        exact = [candidate for candidate in compatible if candidate.deezer_id == evidence.album_id]
        targets = exact or compatible
        if len(targets) != 1:
            continue
        target = targets[0]
        catalog_track: CatalogAlbumTrack | None
        if not target.tracks and target.track_count == 1 and track.title:
            catalog_track = CatalogAlbumTrack(
                album_id=target.id,
                position=track.track_no or 1,
                disc=track.disc or 1,
                title=track.title,
                duration_sec=track.duration_sec,
                content_rating=evidence_rating,
            )
            db.add(catalog_track)
            await db.flush()
        else:
            catalog_track = _matching_track(track, target)
        if catalog_track is None:
            continue
        affected.update((current.id, target.id))
        track.catalog_album_id = target.id
        track.catalog_track_id = catalog_track.id
        verified[track.id] = evidence
        changed += 1
    await db.flush()
    return changed, affected, verified


async def _mark_verified_tracks(
    session_factory: async_sessionmaker[AsyncSession],
    verified: dict[int, DeezerReleaseEvidence],
) -> None:
    if not verified:
        return
    async with session_factory() as db:
        tracks = list(
            (
                await db.scalars(
                    select(Track)
                    .where(Track.id.in_(verified))
                    .options(selectinload(Track.catalog_album))
                )
            ).all()
        )
        for track in tracks:
            evidence = verified[track.id]
            if (
                str(track.deezer_id) == evidence.track_id
                and track.catalog_album is not None
                and normalize_content_rating(track.catalog_album.content_rating)
                == normalize_content_rating(evidence.content_rating)
            ):
                _mark_rating_verified(track, evidence)
        await db.commit()


async def _collect_album_artifacts(
    db: AsyncSession, album_ids: set[int]
) -> tuple[dict[int, set[int]], list[tuple[int, int, str]]]:
    expected: dict[int, set[int]] = {album_id: set() for album_id in album_ids}
    for album_id, track_id in await db.execute(
        select(CatalogAlbumTrack.album_id, CatalogAlbumTrack.id).where(
            CatalogAlbumTrack.album_id.in_(album_ids)
        )
    ):
        expected[int(album_id)].add(int(track_id))
    rows = (
        await db.execute(
            select(Track.catalog_album_id, Track.catalog_track_id, ImportPlan.destination_path)
            .join(ImportPlan, ImportPlan.track_id == Track.id)
            .where(
                Track.catalog_album_id.in_(album_ids),
                Track.import_state == ImportWorkflowState.imported,
                ImportPlan.status == ImportWorkflowState.imported,
                _non_empty(ImportPlan.destination_path),
            )
        )
    ).all()
    artifacts = [
        (int(album_id), int(track_id), str(path))
        for album_id, track_id, path in rows
        if album_id is not None and track_id is not None and path
    ]
    return expected, artifacts


async def recompute_catalog_library_flags(
    session_factory: async_sessionmaker[AsyncSession], album_ids: set[int]
) -> None:
    if not album_ids:
        return
    async with session_factory() as db:
        expected, artifacts = await _collect_album_artifacts(db, album_ids)
    checks = await asyncio.gather(
        *[asyncio.to_thread(Path(path).is_file) for _album_id, _track_id, path in artifacts]
    )
    present: dict[int, set[int]] = {album_id: set() for album_id in album_ids}
    for (album_id, track_id, _path), exists_on_disk in zip(artifacts, checks, strict=True):
        if exists_on_disk:
            present[album_id].add(track_id)
    async with session_factory() as db:
        albums = list(
            (await db.scalars(select(CatalogAlbum).where(CatalogAlbum.id.in_(album_ids)))).all()
        )
        for album in albums:
            album.in_library = bool(expected[album.id]) and expected[album.id] <= present[album.id]
        await db.commit()


async def reconcile_deezer_catalog_ownership(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    artist_id: int | None = None,
    fail_on_provider_error: bool = False,
) -> int:
    """Resolve provider evidence outside transactions, then repair ownership idempotently."""
    async with session_factory() as db:
        candidates = await _collect_candidates(db, artist_id=artist_id)
    existing_ids = set(await _existing_candidate_ids(candidates))
    candidates = [candidate for candidate in candidates if candidate.track_id in existing_ids]
    if not candidates:
        return 0
    if fail_on_provider_error:
        evidence = await _resolve_evidence(settings, candidates, fail_fast=True)
    else:
        evidence = await _resolve_evidence(settings, candidates)
    async with session_factory() as db:
        changed, affected, verified = await apply_catalog_ownership_evidence(
            db, evidence, track_ids=[candidate.track_id for candidate in candidates]
        )
        await db.commit()
    await recompute_catalog_library_flags(session_factory, affected)
    await _mark_verified_tracks(session_factory, verified)
    return changed
