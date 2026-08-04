from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import re
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import ParseResult, urlparse

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings, get_settings
from app.database import get_session_factory
from app.fingerprint.acoustid import fingerprint_file
from app.media_formats import IMPORTABLE_AUDIO_EXTENSIONS, is_importable_audio
from app.metadata.deezer import DeezerClient
from app.metadata.filename_parse import (
    normalize_for_catalog_match,
    parse_filename,
    parsed_position_evidence,
    strip_non_identity_descriptors,
)
from app.metadata.musicbrainz import MusicBrainzClient
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.path_preview import PathPreview
from app.models.release import Release
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
)
from app.naming.convention import NamingError, render_path
from app.schemas.search import SearchRequest, SearchResult
from app.services.monitoring import map_slskd_transfer_state
from app.settings_service import (
    DEFAULT_FREE_TEXT_RESULT_LIMIT,
    build_effective_settings,
    get_runtime_settings,
)
from app.sources.base import SourceAdapter
from app.sources.prowlarr import ProwlarrAdapter
from app.sources.sabnzbd import SabnzbdAdapter
from app.sources.slskd import SlskdAdapter
from app.sources.tidal import TidalAdapter
from app.sources.youtube import ProviderError, YouTubeAdapter

logger = logging.getLogger(__name__)

# Compound disc-track prefix: "2-01 - Title", "cd2-01 - Title", "2.01. Title"
_DISC_TRACK_PREFIX_RE = re.compile(
    r"^(?:cd\s*)?(\d{1,2})\s*[-_.]\s*(\d{1,2})\s*(?:[-_.:]|\s-\s)\s*",
    re.IGNORECASE,
)
# Simple single-number track prefix: "02 Title", "02. Title", "02 - Title"
_SINGLE_TRACK_PREFIX_RE = re.compile(
    r"^(?:cd\s*\d+\s*[-_.]?\s*)?(\d{1,2})\s*[-_. ]",
)
_BRACKET_CONTENT_RE = re.compile(r"[\(\[\{]([^\)\]\}]+)[\)\]\}]")
_COLLABORATOR_SPLIT_RE = re.compile(
    r"\s*(?:,|&|/|;|\bfeat\.?\b|\bft\.?\b|\bfeaturing\b|\band\b)\s*",
    re.IGNORECASE,
)
_FEATURE_TEXT_RE = re.compile(
    r"(?:\(|\[)?\s*(?:feat\.?|ft\.?|featuring)\s+([^\)\]\-]+)",
    re.IGNORECASE,
)
_TECHNICAL_QUALIFIER_RE = re.compile(
    r"^(?:flac|mp3|aac|m4a|alac|lossless|hi-?res|web(?:rip)?|cd(?:rip)?|"
    r"(?:19|20)\d{2}|\d+(?:\.\d+)?\s*(?:khz|kbps|bit))$",
    re.IGNORECASE,
)


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _resolve_staged_path(raw_path: Path, staging_root: Path) -> Path:
    """Return resolved path, verify it is under staging_root. Raises ProviderError on traversal."""
    resolved = raw_path.resolve()
    root_resolved = staging_root.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError:
        raise ProviderError(
            "path_traversal", "artifact path escapes staging root", "acquire"
        ) from None
    return resolved


def _find_audio_files_sync(directory: Path) -> list[Path]:
    return sorted(
        p
        for p in directory.rglob("*")
        if p.is_file() and p.suffix.lower().lstrip(".") in IMPORTABLE_AUDIO_EXTENSIONS
    )


async def _find_audio_files(directory: Path, staging_root: Path) -> list[Path]:
    resolved_dir = _resolve_staged_path(directory, staging_root)
    return await asyncio.to_thread(_find_audio_files_sync, resolved_dir)


def _locate_slskd_artifact_sync(
    filename: str,
    transfer_details: dict[str, object],
    staging_root: Path,
) -> Path:
    root = staging_root.resolve()
    basename = Path(filename.replace("\\", "/")).name
    candidates: list[Path] = []
    for key in ("localPath", "localFilename", "downloadPath", "path"):
        value = transfer_details.get(key)
        if isinstance(value, str) and value.strip():
            candidate = Path(value)
            candidates.append(candidate if candidate.is_absolute() else root / candidate)
    candidates.append(root / basename)

    for candidate in candidates:
        try:
            resolved = _resolve_staged_path(candidate, root)
        except ProviderError:
            continue
        if resolved.is_file():
            return resolved

    matches: list[Path] = []
    for match in root.rglob(basename):
        try:
            resolved = _resolve_staged_path(match, root)
        except ProviderError:
            continue
        if resolved.is_file():
            matches.append(resolved)
    unique_matches = sorted(set(matches))
    if len(unique_matches) == 1:
        return unique_matches[0]
    if len(unique_matches) > 1:
        raise ProviderError(
            "artifact_ambiguous",
            f"multiple completed files match {basename} beneath the staging root",
            "acquire",
        )
    raise ProviderError(
        "artifact_missing",
        f"slskd transfer completed but file not found beneath the staging root: {basename}",
        "acquire",
    )


async def _locate_slskd_artifact(
    filename: str,
    transfer_details: dict[str, object],
    staging_root: Path,
) -> Path:
    return await asyncio.to_thread(
        _locate_slskd_artifact_sync,
        filename,
        transfer_details,
        staging_root,
    )


async def _poll_slskd_transfer(
    transfer_id: str,
    username: str,
    filename: str,
    adapter: SlskdAdapter,
    staging_root: Path,
    poll_interval: float,
    poll_timeout: float,
    on_provider_id: Callable[[str], Awaitable[None]] | None = None,
) -> tuple[Path, str]:
    """Poll slskd until terminal and return the staged path plus exact provider ID."""
    import time as _time

    deadline = _time.monotonic() + poll_timeout
    try:
        while True:
            remaining = deadline - _time.monotonic()
            if remaining <= 0:
                raise ProviderError(
                    "transfer_timeout", "slskd transfer timed out", "acquire", True
                )
            state = await adapter.status(transfer_id)

            provider_id = state.extra.get("id") or state.extra.get("transferId")
            if provider_id is not None and str(provider_id) != transfer_id:
                transfer_id = str(provider_id)
                if on_provider_id is not None:
                    await on_provider_id(transfer_id)

            acq_state = map_slskd_transfer_state(state)

            if acq_state == AcquisitionState.downloaded:
                staged = await _locate_slskd_artifact(filename, state.extra, staging_root)
                if not is_importable_audio(staged):
                    raise ProviderError(
                        "artifact_invalid",
                        "slskd transfer completed with a non-audio artifact",
                        "acquire",
                    )
                return staged, transfer_id
            if acq_state == AcquisitionState.failed:
                raise ProviderError(
                    "transfer_failed", f"slskd transfer failed: {state.reason}", "acquire", True
                )
            if acq_state == AcquisitionState.cancelled:
                raise ProviderError(
                    "transfer_failed", "slskd transfer was cancelled", "acquire", True
                )

            await asyncio.sleep(min(poll_interval, max(0.01, remaining)))
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await adapter.cancel(username, filename, transfer_id)
        raise


async def _poll_sab_job(
    nzo_id: str,
    adapter: SabnzbdAdapter,
    staging_root: Path,
    poll_interval: float,
    poll_timeout: float,
) -> Path:
    """Poll SABnzbd queue then history until the job reaches a terminal state."""
    import time as _time

    deadline = _time.monotonic() + poll_timeout
    while True:
        remaining = deadline - _time.monotonic()
        if remaining <= 0:
            raise ProviderError("transfer_timeout", "SABnzbd job timed out", "acquire", True)

        queue_state = await adapter.status(nzo_id)
        if queue_state.available:
            status = (queue_state.reason or "").casefold()
            if status == "failed":
                raise ProviderError(
                    "transfer_failed", "SABnzbd job failed in queue", "acquire", True
                )
            # Still active in queue — keep polling
        else:
            # Not in queue; check history for terminal state
            hist_state = await adapter.history_status(nzo_id)
            if not hist_state.available:
                raise ProviderError(
                    "transfer_lost",
                    "SABnzbd job disappeared from queue and history",
                    "acquire",
                    True,
                )
            hist_status = (hist_state.reason or "").casefold()
            if hist_status == "completed":
                storage = str(hist_state.extra.get("storage", ""))
                if not storage:
                    raise ProviderError(
                        "artifact_missing",
                        "SABnzbd completed job has no storage path",
                        "acquire",
                    )
                audio_files = await _find_audio_files(Path(storage), staging_root)
                if not audio_files:
                    raise ProviderError(
                        "artifact_missing",
                        f"SABnzbd completed but no audio files found in {storage}",
                        "acquire",
                    )
                return audio_files[0]
            raise ProviderError(
                "transfer_failed",
                f"SABnzbd job in terminal failure state: {hist_status}",
                "acquire",
                True,
            )

        await asyncio.sleep(min(poll_interval, max(0.01, remaining)))


async def run_job(
    job_id: int, db: AsyncSession | None = None, settings: Settings | None = None
) -> None:
    if db is not None:
        cfg = settings or await build_effective_settings(db, get_settings())
        await _run_job_in_session(job_id, db, cfg)
        return

    # Background path: short independent sessions with committed checkpoints.
    factory = get_session_factory()

    # Phase 1: guarded initial lookup and effective-settings load.
    cfg_built: Settings | None = None
    _phase1_error: str | None = None
    try:
        async with factory() as s:
            try:
                job = await s.get(Job, job_id)
            except Exception:
                _phase1_error = "init_error"
                raise
            if job is None:
                logger.error("Job %d not found", job_id)
                return
            try:
                cfg_built = settings or await build_effective_settings(s, get_settings())
            except Exception:
                _phase1_error = "settings_error"
                raise
    except Exception:
        logger.exception("Job %d phase-1 failed", job_id)
        if _phase1_error is not None:
            with contextlib.suppress(Exception):
                async with factory() as s:
                    j = await s.get(Job, job_id)
                    if j is not None:
                        j.status = JobStatus.failed
                        j.result_json = json.dumps(
                            {
                                "error": {
                                    "code": _phase1_error,
                                    "operation": "init",
                                    "retryable": False,
                                }
                            }
                        )
                        j.updated_at = _now()
                        await s.commit()
        return

    cfg = cfg_built

    # Phase 2: commit pending->running before any provider work so observers see it.
    try:
        async with factory() as s:
            job = await s.get(Job, job_id)
            if job is None:
                return
            job.status = JobStatus.running
            job.updated_at = _now()
            await s.commit()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Job %d phase-2 running transition failed", job_id)
        with contextlib.suppress(Exception):
            async with factory() as s:
                j = await s.get(Job, job_id)
                if j is not None:
                    j.status = JobStatus.failed
                    j.result_json = json.dumps(
                        {
                            "error": {
                                "code": "running_transition_error",
                                "operation": "init",
                                "retryable": False,
                            }
                        }
                    )
                    j.updated_at = _now()
                    await s.commit()
        return

    # Phase 3: execute with per-result progress commits; terminal state persisted on failure.
    try:
        async with factory() as s:
            await _run_job_in_session(job_id, s, cfg, commit_progress=True)
            await s.commit()
    except asyncio.CancelledError:
        async with factory() as s:
            job = await s.get(Job, job_id)
            if job is not None:
                job.status = JobStatus.cancelled
                tracks = (await s.execute(select(Track).where(Track.job_id == job_id))).scalars()
                for track in tracks:
                    if track.acquisition_state in {
                        AcquisitionState.queued,
                        AcquisitionState.searching,
                        AcquisitionState.acquiring,
                    }:
                        track.acquisition_state = AcquisitionState.cancelled
                job.result_json = json.dumps(
                    {"error": {"code": "cancelled", "operation": "job", "retryable": True}}
                )
                job.updated_at = _now()
                await s.commit()
        raise
    except Exception:
        logger.exception("Job %d failed", job_id)
        async with factory() as s:
            job = await s.get(Job, job_id)
            if job is not None:
                job.status = JobStatus.failed
                job.result_json = json.dumps(
                    {"error": {"code": "job_failed", "operation": "job", "retryable": True}}
                )
                job.updated_at = _now()
                await s.commit()
    finally:
        try:
            from app.services.acquisition_cleanup import cleanup_terminal_acquisitions

            await cleanup_terminal_acquisitions(
                factory,
                slskd_url=cfg.slskd_url,
                slskd_api_key=cfg.slskd_api_key,
                job_ids={job_id},
            )
        except Exception:
            logger.exception("Job %d terminal acquisition cleanup failed", job_id)


async def _run_job_in_session(
    job_id: int, db: AsyncSession, cfg: Settings, *, commit_progress: bool = False
) -> None:
    job = await db.get(Job, job_id)
    if job is None:
        logger.error("Job %d not found", job_id)
        return

    job.status = JobStatus.running
    job.updated_at = _now()
    await db.flush()
    # Bug 5: release the SQLite write lock before long provider HTTP/search/polling so
    # concurrent settings writes are not blocked during the acquisition wait.
    if commit_progress:
        await db.commit()

    try:
        runtime_settings = await get_runtime_settings(db)
        cfg = cfg.model_copy(
            update={"slskd_poll_timeout": float(runtime_settings.slskd_download_timeout_seconds)}
        )
        selected_results = _selected_result(job)
        if selected_results:
            selected_results = await _without_blocked_slskd_results(selected_results, db)
        results = selected_results or await _call_fetch_results(
            job,
            cfg,
            db,
            checkpoint=db.commit if commit_progress else None,
        )
        catalog_album = await _load_catalog_album(db, job.catalog_album_id)
        catalog_tracks = list(catalog_album.tracks) if catalog_album is not None else []

        declared_track_count = catalog_album.track_count if catalog_album is not None else None
        # Defensive hydration: an absent, partial, or structurally invalid catalog
        # manifest cannot be used to decide album completeness. Validate the full
        # album before narrowing a continuation job to its selected track.
        manifest_issue = _catalog_manifest_issue(catalog_tracks, declared_track_count)
        if catalog_album is not None and manifest_issue is not None:
            try:
                from app.services.catalog_metadata import (
                    fetch_and_store_album as _fetch_and_store_album,
                )

                catalog_album = await _fetch_and_store_album(db, cfg, catalog_album)
                if commit_progress:
                    await db.commit()
                catalog_album = await _load_catalog_album(db, job.catalog_album_id)
                if catalog_album is not None:
                    catalog_tracks = list(catalog_album.tracks)
            except Exception:
                logger.warning(
                    "Catalog track hydration failed for album %d (job %d)",
                    job.catalog_album_id,
                    job_id,
                )
            expected_tracks = (
                max(
                    declared_track_count or 0,
                    (catalog_album.track_count or 0) if catalog_album is not None else 0,
                )
                or None
            )
            if catalog_album is not None:
                catalog_album.track_count = expected_tracks
            manifest_issue = _catalog_manifest_issue(catalog_tracks, expected_tracks)
            if catalog_album is not None and manifest_issue is not None:
                job.status = JobStatus.failed
                job.result_json = json.dumps(
                    {
                        "error": {
                            "code": manifest_issue,
                            "operation": "hydrate",
                            "retryable": True,
                            "detail": (
                                f"album_id={job.catalog_album_id} "
                                f"track_count={expected_tracks} "
                                f"catalog_tracks={len(catalog_tracks)} "
                                f"issue={manifest_issue} after hydration attempt"
                            ),
                        }
                    },
                    sort_keys=True,
                )
                job.updated_at = _now()
                await db.flush()
                return

        if job.catalog_track_id is not None:
            catalog_tracks = [
                track for track in catalog_tracks if track.id == job.catalog_track_id
            ]

        if job.catalog_track_id is not None and not catalog_tracks:
            job.status = JobStatus.failed
            job.result_json = json.dumps(
                {
                    "error": {
                        "code": "catalog_track_missing",
                        "operation": "catalog",
                        "retryable": False,
                        "detail": (
                            f"catalog_track_id={job.catalog_track_id} does not belong to "
                            f"album_id={job.catalog_album_id}"
                        ),
                    }
                },
                sort_keys=True,
            )
            job.updated_at = _now()
            await db.flush()
            return

        if (
            catalog_album is not None
            and job.catalog_track_id is None
            and results
            and all(result.source == "prowlarr" for result in results)
        ):
            # Prowlarr results are alternative release candidates, not tracks.
            # Acquire one candidate and report manifest gaps rather than enqueueing
            # every NZB and assigning candidates to tracks by position.
            results = results[:1]
        tracks_created = 0
        failures: list[dict[str, object]] = []
        root_job = await _root_job(job, db)
        existing_releases = list(
            (await db.scalars(select(Release).where(Release.job_id == root_job.id))).all()
        )
        releases: dict[tuple[str | None, str | None], Release] = {
            (release.album_artist, release.title): release for release in existing_releases
        }
        release_ids = [release.id for release in existing_releases]
        track_query = select(Track).where(Track.job_id == job_id)
        if release_ids:
            track_query = select(Track).where(
                (Track.job_id == job_id) | Track.release_id.in_(release_ids)
            )
        existing_tracks = list((await db.scalars(track_query)).all())
        catalog_disc_total = _catalog_disc_total(catalog_tracks)
        selected_catalog_track = next(
            (track for track in catalog_tracks if track.id == job.catalog_track_id), None
        )
        selected_required_terms = (
            await _targeted_required_identity_terms(selected_catalog_track, catalog_album, cfg)
            if selected_catalog_track is not None
            else []
        )
        for result in results:
            if (
                result.source == "slskd"
                and selected_catalog_track is not None
                and not _targeted_catalog_result_matches(
                    result,
                    selected_catalog_track,
                    required_terms=selected_required_terms,
                )
            ):
                failures.append(
                    {
                        "code": "candidate_identity_mismatch",
                        "operation": "search",
                        "retryable": False,
                    }
                )
                continue
            catalog_track = _catalog_track_for_result(result, catalog_tracks, job.catalog_track_id)
            track_title = catalog_track.title if catalog_track is not None else result.title
            track_album = catalog_album.title if catalog_album is not None else result.album
            track = _existing_track_for_result(
                existing_tracks,
                track_title,
                track_album,
                catalog_track,
            )
            if track is not None and track.acquisition_state == AcquisitionState.downloaded:
                tracks_created += 1
                continue
            try:
                release_key = (
                    catalog_album.artist.name if catalog_album is not None else result.artist,
                    catalog_album.title
                    if catalog_album is not None
                    else result.album or result.title,
                )
                release = releases.get(release_key)
                if release is None and track is not None and track.release_id is not None:
                    release = await db.get(Release, track.release_id)
                if release is None:
                    release = Release(
                        job_id=root_job.id,
                        source=result.source,
                        title=catalog_album.title
                        if catalog_album is not None
                        else result.album or result.title,
                        album_artist=catalog_album.artist.name
                        if catalog_album is not None
                        else result.artist,
                        year=catalog_album.year if catalog_album is not None else None,
                        release_mbid=catalog_album.mbid if catalog_album is not None else None,
                        track_count=(
                            len(catalog_tracks)
                            if catalog_tracks
                            else catalog_album.track_count
                            if catalog_album is not None
                            else None
                        ),
                    )
                    db.add(release)
                    await db.flush()
                    releases[release_key] = release

                if track is None:
                    track = Track(
                        job_id=job_id,
                        release_id=release.id,
                        catalog_album_id=catalog_album.id if catalog_album is not None else None,
                        catalog_track_id=catalog_track.id if catalog_track is not None else None,
                        title=track_title,
                        artist=catalog_album.artist.name
                        if catalog_album is not None
                        else result.artist,
                        album_artist=catalog_album.artist.name
                        if catalog_album is not None
                        else result.artist,
                        album=track_album,
                        year=catalog_album.year if catalog_album is not None else None,
                        disc=catalog_track.disc if catalog_track is not None else None,
                        disc_total=catalog_disc_total if catalog_track is not None else None,
                        track_no=catalog_track.position if catalog_track is not None else None,
                        duration_sec=catalog_track.duration_sec
                        if catalog_track is not None
                        else None,
                        mbid=catalog_track.recording_mbid if catalog_track is not None else None,
                        identity_state=(
                            IdentityResolutionState.resolved
                            if catalog_track and catalog_track.recording_mbid
                            else IdentityResolutionState.unresolved
                        ),
                        source_path=None,
                        source=result.source,
                        acquisition_state=AcquisitionState.acquiring,
                        fingerprint_state=FingerprintState.pending,
                    )
                    db.add(track)
                    await db.flush()
                    existing_tracks.append(track)
                elif catalog_track is not None:
                    track.disc = catalog_track.disc
                    track.disc_total = catalog_disc_total
                    track.track_no = catalog_track.position
                if commit_progress:
                    job.updated_at = _now()
                    await db.commit()

                async def checkpoint() -> None:
                    job.updated_at = _now()
                    await db.commit()

                source_job_id, source_status = await _call_prepare_acquisition(
                    result,
                    job.source,
                    cfg,
                    track,
                    checkpoint=checkpoint if commit_progress else None,
                )
                track.source_job_id = source_job_id
                track.source_status = source_status

                if track.identity_state != IdentityResolutionState.resolved:
                    await _enrich_musicbrainz(track, cfg)
                await _enrich_deezer(track, cfg)
                await _run_fingerprint_and_verify(track, cfg, db)
                await _compute_path_preview(track, db, cfg)
                await _try_auto_import(release, db, cfg)

                tracks_created += 1
            except ProviderError as exc:
                if track is not None:
                    track.acquisition_state = AcquisitionState.failed
                    track.source_status = exc.code
                if exc.code == "transfer_timeout" and result.source == "slskd":
                    peer = str(result.metadata.get("username") or "")
                    filename = str(result.metadata.get("filename") or "")
                    if peer and filename:
                        existing_block = await db.scalar(
                            select(SourceCandidateBlock.id).where(
                                SourceCandidateBlock.provider == "slskd",
                                SourceCandidateBlock.peer == peer,
                                SourceCandidateBlock.filename == filename,
                            )
                        )
                        if existing_block is None:
                            db.add(
                                SourceCandidateBlock(
                                    provider="slskd",
                                    peer=peer,
                                    filename=filename,
                                    reason="timeout",
                                )
                            )
                        await db.flush()
                        if commit_progress:
                            await db.commit()
                logger.warning("Provider result processing failed with code %s", exc.code)
                failures.append(exc.details())
            except Exception:
                if track is not None:
                    track.acquisition_state = AcquisitionState.failed
                logger.warning("Result processing failed")
                failures.append({"code": "result_processing_failed"})
            if commit_progress:
                job.updated_at = _now()
                await db.commit()

        if catalog_tracks and catalog_album is not None:
            acquired_rows = (
                await db.execute(
                    select(Track.catalog_track_id, ImportPlan.destination_path)
                    .join(ImportPlan, ImportPlan.track_id == Track.id)
                    .where(
                        Track.catalog_album_id == catalog_album.id,
                        Track.catalog_track_id.is_not(None),
                        Track.import_state == ImportWorkflowState.imported,
                        ImportPlan.status == ImportWorkflowState.imported,
                        ImportPlan.destination_path != "",
                    )
                )
            ).all()
            root_release_ids = [r.id for r in releases.values() if r.id is not None]
            current_downloaded_ids = {
                track.catalog_track_id
                for track in (
                    await db.scalars(
                        select(Track).where(
                            Track.release_id.in_(root_release_ids),
                            Track.acquisition_state == AcquisitionState.downloaded,
                        )
                    )
                ).all()
                if track.catalog_track_id is not None
            }
            acquired_ids: set[int] = set(current_downloaded_ids)
            for track_id, destination_path in acquired_rows:
                if track_id is not None and destination_path:
                    exists = await asyncio.to_thread(Path(destination_path).is_file)
                    if exists:
                        acquired_ids.add(int(track_id))
            missing_catalog_ids = [
                track.id for track in catalog_tracks if track.id not in acquired_ids
            ]
            payload = _job_payload(job)
            payload.update(
                {
                    "tracks_created": tracks_created,
                    "missing_tracks": [
                        track.title for track in catalog_tracks if track.id not in acquired_ids
                    ],
                    "missing_catalog_track_ids": missing_catalog_ids,
                }
            )
            if failures:
                payload["errors"] = failures
            job.status = JobStatus.partial if missing_catalog_ids else JobStatus.done
            job.result_json = json.dumps(payload, sort_keys=True)
            runtime = await get_runtime_settings(db)
            if missing_catalog_ids and job.partial_attempt < runtime.max_partial_attempts:
                await _spawn_continuation_jobs(job, missing_catalog_ids, catalog_album, db)
            elif not missing_catalog_ids:
                await _reconcile_catalog_album_jobs(db, catalog_album.id, acquired_ids)
        elif failures and tracks_created:
            job.status = JobStatus.partial
            payload = _job_payload(job)
            payload.update({"tracks_created": tracks_created, "errors": failures})
            job.result_json = json.dumps(payload, sort_keys=True)
        elif failures:
            job.status = JobStatus.failed
            payload = _job_payload(job)
            payload.update({"tracks_created": tracks_created, "errors": failures})
            job.result_json = json.dumps(payload, sort_keys=True)
        else:
            job.status = JobStatus.done
            payload = _job_payload(job)
            payload.update({"tracks_created": tracks_created})
            job.result_json = json.dumps(payload, sort_keys=True)
        job.updated_at = _now()

        # Reconcile each release after the job closes. Per-track import already runs
        # above; this retry is idempotent and catches any persisted eligible rows.
        if job.status in {JobStatus.done, JobStatus.partial}:
            all_releases = list(
                (await db.scalars(select(Release).where(Release.job_id == root_job.id))).all()
            )
            for rel in all_releases:
                try:
                    await _try_auto_import(rel, db, cfg)
                except Exception:
                    logger.exception(
                        "auto_import attempt failed for release %d (job %d)", rel.id, job_id
                    )
    except ProviderError as exc:
        logger.warning("Job %d provider failure code %s", job_id, exc.code)
        job.status = JobStatus.failed
        payload = _job_payload(job)
        payload["error"] = exc.details()
        job.result_json = json.dumps(payload, sort_keys=True)
        job.updated_at = _now()
    except asyncio.CancelledError:
        job.status = JobStatus.cancelled
        tracks = (await db.execute(select(Track).where(Track.job_id == job.id))).scalars()
        for track in tracks:
            if track.acquisition_state in {
                AcquisitionState.queued,
                AcquisitionState.searching,
                AcquisitionState.acquiring,
            }:
                track.acquisition_state = AcquisitionState.cancelled
        job.result_json = json.dumps(
            {"error": {"code": "cancelled", "operation": "job", "retryable": True}}
        )
        job.updated_at = _now()
        await db.flush()
        raise
    except Exception:
        logger.exception("Job %d failed", job_id)
        job.status = JobStatus.failed
        job.result_json = json.dumps(
            {"error": {"code": "job_failed", "operation": "job", "retryable": True}}
        )
        job.updated_at = _now()

    await db.flush()


async def _load_catalog_album(db: AsyncSession, album_id: int | None) -> CatalogAlbum | None:
    if album_id is None:
        return None
    result = await db.execute(
        select(CatalogAlbum)
        .where(CatalogAlbum.id == album_id)
        .options(selectinload(CatalogAlbum.artist), selectinload(CatalogAlbum.tracks))
    )
    return result.scalar_one_or_none()


def _catalog_disc_total(tracks: list[CatalogAlbumTrack]) -> int | None:
    discs = [track.disc for track in tracks if track.disc and track.disc > 0]
    total = max(discs, default=1)
    return total if total > 1 else None


def _catalog_manifest_issue(
    tracks: list[CatalogAlbumTrack], expected_count: int | None
) -> str | None:
    if not tracks:
        return "catalog_tracks_empty"
    identities: set[tuple[int, int]] = set()
    for track in tracks:
        identity = (track.disc, track.position)
        if track.disc < 1 or track.position < 1 or identity in identities:
            return "catalog_tracks_invalid_positions"
        identities.add(identity)
    if expected_count and len(tracks) < expected_count:
        return "catalog_tracks_incomplete"
    if expected_count and len(tracks) > expected_count:
        return "catalog_tracks_overfull"
    return None


def _catalog_track_for_result(
    result: SearchResult,
    tracks: list[CatalogAlbumTrack],
    selected_track_id: int | None,
) -> CatalogAlbumTrack | None:
    if selected_track_id is not None:
        return next((track for track in tracks if track.id == selected_track_id), None)
    raw_title = result.title or ""
    if not raw_title:
        return None

    # 0. Disc+position match — evaluated before every title-only step so that
    #    duplicate-title rows on the same disc (or across discs) bind to the
    #    correct CatalogAlbumTrack.  Position is resolved from, in order:
    #      a. Explicit track_no [+ disc] in result.metadata
    #      b. Compound disc-track prefix: "2-01 - Title" → disc=2, pos=1
    #      c. Simple numbered prefix:     "02 Title"     → pos=2, disc defaults to 1
    #    Missing disc always defaults to 1 when a reliable position is available.
    _disc_meta = result.metadata.get("disc") if result.metadata else None
    _pos_meta = result.metadata.get("track_no") if result.metadata else None
    _disc_num: int | None = None
    _pos_num: int | None = None
    _compound_prefix_seen = False

    try:
        if isinstance(_pos_meta, (int, str, float)):
            _pos_num = int(_pos_meta)
        if isinstance(_disc_meta, (int, str, float)):
            _disc_num = int(_disc_meta)
    except (ValueError, TypeError):
        pass

    if _pos_num is None:
        _m_compound = _DISC_TRACK_PREFIX_RE.match(raw_title)
        if _m_compound:
            _disc_num = int(_m_compound.group(1))
            _pos_num = int(_m_compound.group(2))
            _compound_prefix_seen = True
        else:
            _m_single = _SINGLE_TRACK_PREFIX_RE.match(raw_title)
            if _m_single:
                _pos_num = int(_m_single.group(1))

    if _pos_num is not None and _disc_num is None:
        _disc_num = 1

    if _pos_num is not None:
        _matched0 = next(
            (t for t in tracks if t.position == _pos_num and t.disc == _disc_num),
            None,
        )
        if _matched0 is not None:
            return _matched0

    # 1. Exact casefolded match
    title_cf = raw_title.casefold().strip()
    for track in tracks:
        if track.title.casefold().strip() == title_cf:
            return track

    # 2. Normalized match (strip track-number prefix + Unicode/apostrophe normalize)
    title_norm = normalize_for_catalog_match(raw_title)
    for track in tracks:
        if normalize_for_catalog_match(track.title) == title_norm:
            return track

    # 3. Strip non-identity descriptors from catalog side and retry
    for track in tracks:
        catalog_stripped = normalize_for_catalog_match(strip_non_identity_descriptors(track.title))
        if catalog_stripped == title_norm:
            return track

    # 4. Also strip descriptors from result side (e.g. result has "(skit)" but catalog doesn't)
    title_stripped = normalize_for_catalog_match(strip_non_identity_descriptors(raw_title))
    if title_stripped and title_stripped != title_norm:
        for track in tracks:
            if normalize_for_catalog_match(track.title) == title_stripped:
                return track
        for track in tracks:
            catalog_stripped = normalize_for_catalog_match(
                strip_non_identity_descriptors(track.title)
            )
            if catalog_stripped == title_stripped:
                return track

    # 5. Single-track catalog releases: if the album context is already catalog-scoped,
    #    the only stored catalog track is the intended identity.  This repairs singles
    #    whose source filenames include extra artist/album/position tokens that defeat
    #    title parsing, while preserving the no-position-fallback rule for multi-track
    #    releases.
    if len(tracks) == 1:
        return tracks[0]

    return None


def _targeted_catalog_result_matches(
    result: SearchResult,
    target: CatalogAlbumTrack,
    *,
    required_terms: Sequence[str] = (),
) -> bool:
    """Require title and structured collaborator/version identity for a target."""
    filename = result.metadata.get("filename")
    observed_source = str(filename) if isinstance(filename, str) else result.title or ""
    if not _targeted_title_matches(result.title or "", observed_source, target.title):
        return False
    if not _targeted_duration_matches(result.duration_sec, target.duration_sec):
        return False
    if required_terms and not _observed_contains_required_terms(observed_source, required_terms):
        return False
    target_qualifiers = _identity_qualifiers(target.title)
    observed_qualifiers = _identity_qualifiers(observed_source, basename=True)
    if required_terms:
        # A MusicBrainz recording credit may carry the collaborator identity while
        # the catalog title is bare (e.g. Miami). In that case required artist
        # terms are the authoritative guard; do not reject a useful promo filename
        # merely because it adds harmless bracket tags like clean/dirty/year.
        return target_qualifiers <= observed_qualifiers
    if observed_qualifiers == target_qualifiers:
        return True
    if target_qualifiers:
        source_identity = _identity_text(observed_source, basename=True)
        return all(
            qualifier in observed_qualifiers
            or _contains_identity_phrase(source_identity, qualifier)
            for qualifier in target_qualifiers
        ) and not (observed_qualifiers - target_qualifiers)
    return False


def _targeted_duration_matches(observed_sec: int | None, expected_sec: int | None) -> bool:
    if observed_sec is None or expected_sec is None:
        return True
    return abs(observed_sec - expected_sec) <= 8


def _targeted_title_matches(observed_title: str, observed_source: str, target_title: str) -> bool:
    observed = normalize_for_catalog_match(strip_non_identity_descriptors(observed_title))
    expected_core = normalize_for_catalog_match(
        strip_non_identity_descriptors(parse_filename(target_title).title)
    )
    expected_full = normalize_for_catalog_match(strip_non_identity_descriptors(target_title))
    if observed and observed in (expected_core, expected_full):
        return True
    expected_identity = _identity_text(target_title)
    if not expected_identity:
        return False
    source_identity = _identity_text(observed_source, basename=True)
    return _contains_identity_phrase(source_identity, expected_identity)


def _identity_text(value: str, *, basename: bool = False) -> str:
    text = value.replace("\\", "/")
    if basename:
        text = text.rsplit("/", 1)[-1]
    text = re.sub(r"\.[A-Za-z0-9]{2,5}$", "", text)
    text = _provider_safe_text(text)
    text = re.sub(r"[\(\)\[\]{}_/.-]+", " ", text)
    text = normalize_for_catalog_match(text)
    return re.sub(r"\s+", " ", text).strip()


def _contains_identity_phrase(haystack: str, needle: str) -> bool:
    if not haystack or not needle:
        return False
    return re.search(rf"(?<!\w){re.escape(needle)}(?!\w)", haystack) is not None


def _observed_contains_required_terms(value: str, required_terms: Sequence[str]) -> bool:
    haystack = normalize_for_catalog_match(value)
    return all(normalize_for_catalog_match(term) in haystack for term in required_terms)


def _identity_qualifiers(value: str, *, basename: bool = False) -> set[str]:
    """Keep bracketed recording-version qualifiers while ignoring technical/non-identity tags."""
    qualifiers: set[str] = set()
    tail = value.replace("\\", "/")
    if basename:
        tail = tail.rsplit("/", 1)[-1]
    for match in _BRACKET_CONTENT_RE.finditer(tail):
        raw = match.group(1).strip()
        if not raw or _TECHNICAL_QUALIFIER_RE.fullmatch(raw):
            continue
        if strip_non_identity_descriptors(f"x ({raw})") == "x":
            continue
        normalized = _identity_text(raw)
        if normalized:
            qualifiers.add(normalized)
    return qualifiers


def _existing_track_for_result(
    tracks: list[Track],
    title: str | None,
    album: str | None,
    catalog_track: CatalogAlbumTrack | None,
) -> Track | None:
    if catalog_track is not None:
        return next(
            (track for track in tracks if track.catalog_track_id == catalog_track.id), None
        )
    return next(
        (
            track
            for track in tracks
            if (track.title or "").casefold().strip() == (title or "").casefold().strip()
            and (track.album or "").casefold().strip() == (album or "").casefold().strip()
        ),
        None,
    )


def _selected_result(job: Job) -> list[SearchResult] | None:
    if not job.selected_result_json:
        return None
    return [SearchResult.model_validate(json.loads(job.selected_result_json))]


def _slskd_identity_from_provenance(provenance_json: str | None) -> tuple[str, str] | None:
    if not provenance_json:
        return None
    try:
        provenance = json.loads(provenance_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(provenance, dict) or provenance.get("source") != "slskd":
        return None
    peer = str(provenance.get("username") or "")
    filename = str(provenance.get("filename") or "")
    return (peer, filename) if peer and filename else None


async def _blocked_slskd_identities(db: AsyncSession) -> set[tuple[str, str]]:
    block_rows = (
        await db.execute(
            select(SourceCandidateBlock.peer, SourceCandidateBlock.filename).where(
                SourceCandidateBlock.provider == "slskd"
            )
        )
    ).all()
    blocked = {(str(peer), str(filename)) for peer, filename in block_rows}
    denied_rows = (
        await db.scalars(
            select(Track.acquisition_provenance_json).where(
                Track.source == "slskd",
                Track.acoustid_verification_state == AcoustIDVerificationState.denied,
                Track.acquisition_provenance_json.is_not(None),
            )
        )
    ).all()
    for provenance_json in denied_rows:
        identity = _slskd_identity_from_provenance(provenance_json)
        if identity is not None:
            blocked.add(identity)
    return blocked


async def _without_blocked_slskd_results(
    results: list[SearchResult], db: AsyncSession
) -> list[SearchResult]:
    if not any(result.source == "slskd" for result in results):
        return results
    blocked = await _blocked_slskd_identities(db)
    return [
        result
        for result in results
        if result.source != "slskd"
        or (
            str(result.metadata.get("username") or ""),
            str(result.metadata.get("filename") or ""),
        )
        not in blocked
    ]


async def _call_fetch_results(
    job: Job,
    cfg: Settings,
    db: AsyncSession,
    *,
    checkpoint: Callable[[], Awaitable[None]] | None = None,
) -> list[SearchResult]:
    parameters = inspect.signature(_fetch_results).parameters
    values = tuple(parameters.values())
    accepts_keywords = any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in values)
    if "checkpoint" in parameters or accepts_keywords:
        return await _fetch_results(job, cfg, db, checkpoint=checkpoint)
    accepts_varargs = any(
        parameter.kind is inspect.Parameter.VAR_POSITIONAL for parameter in values
    )
    positional_count = sum(
        parameter.kind
        in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
        for parameter in values
    )
    if accepts_varargs or positional_count >= 3:
        return list(await _fetch_results(job, cfg, db))
    return list(await _fetch_results(job, cfg))  # type: ignore[call-arg]


def _slskd_search_timeout_seconds(runtime: object | None) -> float:
    if runtime is None:
        return 300.0
    configured_budget = float(getattr(runtime, "source_search_budget_seconds", 300) or 300)
    download_timeout = float(getattr(runtime, "slskd_download_timeout_seconds", 600) or 600)
    # slskd can keep valid searches active for several minutes during bulk dispatch.
    # Never let a legacy low UI value turn those in-flight searches into false
    # sources_exhausted failures, but cap search waiting at fifteen minutes so a
    # dead search cannot monopolize the queue forever.
    return min(900.0, max(configured_budget, min(download_timeout, 900.0)))


def _source_adapter(source: str, cfg: Settings, runtime: object | None = None) -> SourceAdapter:
    if source == "slskd":
        return SlskdAdapter(
            cfg.slskd_url,
            cfg.slskd_api_key,
            search_timeout_sec=_slskd_search_timeout_seconds(runtime),
        )
    if source == "prowlarr":
        return ProwlarrAdapter(cfg.prowlarr_url, cfg.prowlarr_api_key)
    if source == "youtube":
        return YouTubeAdapter(cfg.ytdlp_cookies_file)
    if source == "tidal":
        return TidalAdapter(cfg.tidal_config_path, cfg.tidal_session_path, cfg.tidal_quality)
    raise ValueError(f"Unknown source: {source}")


def _call_source_adapter(source: str, cfg: Settings, runtime: object | None) -> SourceAdapter:
    parameters = inspect.signature(_source_adapter).parameters
    values = tuple(parameters.values())
    accepts_keywords = any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in values)
    accepts_varargs = any(
        parameter.kind is inspect.Parameter.VAR_POSITIONAL for parameter in values
    )
    if "runtime" in parameters or accepts_keywords:
        return _source_adapter(source, cfg, runtime=runtime)
    positional_count = sum(
        parameter.kind
        in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
        for parameter in values
    )
    if accepts_varargs or positional_count >= 3:
        return _source_adapter(source, cfg, runtime)
    return _source_adapter(source, cfg)


def _job_payload(job: Job) -> dict[str, object]:
    if not job.result_json:
        return {}
    with contextlib.suppress(json.JSONDecodeError):
        payload = json.loads(job.result_json)
        if isinstance(payload, dict):
            return payload
    return {}


async def _reconcile_catalog_album_jobs(
    db: AsyncSession, album_id: int, acquired_ids: set[int]
) -> None:
    expected_ids = set(
        (
            await db.scalars(
                select(CatalogAlbumTrack.id).where(CatalogAlbumTrack.album_id == album_id)
            )
        ).all()
    )
    if not expected_ids or not expected_ids <= acquired_ids:
        return
    jobs = list(
        (
            await db.scalars(
                select(Job).where(
                    Job.catalog_album_id == album_id,
                    Job.status == JobStatus.partial,
                )
            )
        ).all()
    )
    for album_job in jobs:
        payload = _job_payload(album_job)
        payload["missing_catalog_track_ids"] = []
        payload["missing_tracks"] = []
        album_job.status = JobStatus.done
        album_job.result_json = json.dumps(payload, sort_keys=True)
        album_job.updated_at = _now()


_PROVIDER_PUNCTUATION = str.maketrans(
    {"‘": "'", "’": "'", "“": '"', "”": '"', "–": "-", "—": "-", "\u00a0": " "}
)
_EDITION_SUFFIX = re.compile(
    r"\s*\((?:acoustic|live(?:\s+from\s+[^)]*)?|deluxe|remaster(?:ed)?(?:\s+\d{4})?|radio\s+edit|single\s+version)\)\s*$",
    re.IGNORECASE,
)


def _provider_safe_text(value: str) -> str:
    return " ".join(value.translate(_PROVIDER_PUNCTUATION).split())


_GENERIC_ARTIST_TOKENS = frozenset({"the", "a", "an", "and", "&"})


def _first_significant_artist_token(artist: str) -> str:
    for raw_token in re.findall(r"[\w'’-]+", _provider_safe_text(artist)):
        token = str(raw_token)
        normalized = normalize_for_catalog_match(token)
        if len(normalized) >= 3 and normalized not in _GENERIC_ARTIST_TOKENS:
            return token
    return ""


def _targeted_query_variants(
    artist: str,
    album: str,
    track: str,
    *,
    required_terms: Sequence[str] = (),
) -> list[str]:
    safe_artist = _provider_safe_text(artist)
    safe_album = _provider_safe_text(album)
    safe_track = _provider_safe_text(track)
    title = safe_track or safe_album
    required_for_query = [
        term
        for term in required_terms
        if normalize_for_catalog_match(term) not in normalize_for_catalog_match(title)
    ]
    safe_required = _provider_safe_text(" ".join(required_for_query))
    first_artist_token = _first_significant_artist_token(safe_artist)
    titles = [title]
    simplified = _EDITION_SUFFIX.sub("", title).strip()
    if simplified and simplified.casefold() != title.casefold():
        titles.append(simplified)

    variants: list[str] = []
    for current_title in titles:
        variants.append(
            " ".join(part for part in (safe_artist, current_title, safe_required) if part)
        )
        if safe_required:
            variants.append(" ".join(part for part in (safe_artist, current_title) if part))
        # Soulseek/slskd can return zero results for full artist names while a
        # human-style title or first-name query returns the wanted files. Keep
        # these bounded variants after the precise query; downstream catalog
        # verification still decides whether any candidate is safe to import.
        if first_artist_token and first_artist_token != safe_artist:
            variants.append(
                " ".join(
                    part for part in (first_artist_token, current_title, safe_required) if part
                )
            )
            variants.append(
                " ".join(
                    part for part in (current_title, first_artist_token, safe_required) if part
                )
            )
        variants.append(" ".join(part for part in (current_title, safe_required) if part))
    return list(dict.fromkeys(variant for variant in variants if variant))


async def _queries_for_job(job: Job, album: CatalogAlbum | None, cfg: Settings) -> list[str]:
    if album is None or job.catalog_track_id is None:
        return [_provider_safe_text(job.query)]
    track = next((item for item in album.tracks if item.id == job.catalog_track_id), None)
    if track is None:
        return [_provider_safe_text(job.query)]
    artist = album.artist.name if album.artist is not None else ""
    required_terms = await _targeted_required_identity_terms(track, album, cfg)
    return _targeted_query_variants(
        artist, album.title, track.title, required_terms=required_terms
    )


async def _targeted_required_identity_terms(
    track: CatalogAlbumTrack,
    album: CatalogAlbum | None,
    cfg: Settings,
) -> list[str]:
    """Return collaborator/version terms required by the target recording identity."""
    primary_artist = album.artist.name if album is not None and album.artist is not None else ""
    terms = _required_identity_terms_from_text(track.title, primary_artist)
    if track.recording_mbid and cfg.musicbrainz_user_agent:
        try:
            meta = await MusicBrainzClient(cfg.musicbrainz_user_agent).lookup_recording(
                track.recording_mbid
            )
        except Exception as exc:
            logger.warning(
                "MusicBrainz target recording lookup failed for %s: %s",
                track.recording_mbid,
                exc,
            )
        else:
            if meta is not None:
                terms.extend(_required_identity_terms_from_text(meta.title, primary_artist))
                if meta.artist:
                    terms.extend(_collaborator_terms(meta.artist, primary_artist))
    return list(dict.fromkeys(term for term in terms if term))


def _required_identity_terms_from_text(value: str, primary_artist: str) -> list[str]:
    terms: list[str] = []
    for match in _FEATURE_TEXT_RE.finditer(value):
        terms.extend(_collaborator_terms(match.group(1), primary_artist))
    return terms


def _collaborator_terms(artist_credit: str, primary_artist: str) -> list[str]:
    primary = normalize_for_catalog_match(primary_artist)
    featured_match = re.search(
        r"\b(?:feat\.?|ft\.?|featuring)\b\s*(.+)", artist_credit, re.IGNORECASE
    )
    if featured_match:
        featured_source = featured_match.group(1)
        lead_source = artist_credit[: featured_match.start()]
        lead_pieces = [
            piece.strip(" .-_()[]")
            for piece in _COLLABORATOR_SPLIT_RE.split(lead_source)
            if piece.strip(" .-_()[]")
        ]
        # Preserve co-leads like "Thomas Wesley & Julia Michaels feat. Morgan Wallen"
        # without requiring the first lead alias when the watched artist is featured.
        pieces = lead_pieces[1:] + [
            piece.strip(" .-_()[]") for piece in _COLLABORATOR_SPLIT_RE.split(featured_source)
        ]
    else:
        pieces = [piece.strip(" .-_()[]") for piece in _COLLABORATOR_SPLIT_RE.split(artist_credit)]
    terms: list[str] = []
    for piece in pieces:
        normalized = normalize_for_catalog_match(piece)
        if not normalized or normalized == primary or normalized in primary:
            continue
        # Keep phrase-level terms; single generic words introduce too much noise.
        if len(normalized) < 3:
            continue
        terms.append(piece)
    return terms


def _set_acquisition_provenance(
    job: Job, attempted: list[dict[str, object]], served_source: str | None = None
) -> None:
    payload = _job_payload(job)
    payload["source_attempts"] = attempted
    payload["attempted_sources"] = [str(item.get("source", "")) for item in attempted]
    if served_source:
        payload["served_source"] = served_source
    job.result_json = json.dumps(payload, sort_keys=True)


async def _persist_provider_progress(
    db: AsyncSession, checkpoint: Callable[[], Awaitable[None]] | None
) -> None:
    if checkpoint is not None:
        await checkpoint()
    else:
        await db.flush()


async def _fetch_results(
    job: Job,
    cfg: Settings,
    db: AsyncSession,
    limit: int = DEFAULT_FREE_TEXT_RESULT_LIMIT,
    *,
    checkpoint: Callable[[], Awaitable[None]] | None = None,
) -> list[SearchResult]:
    runtime = await get_runtime_settings(db)
    configured = [
        s for s in runtime.enabled_sources if s in {"slskd", "prowlarr", "youtube", "tidal"}
    ]
    priority_job = job.source == "priority"
    if job.source in {"slskd", "prowlarr", "youtube", "tidal"}:
        priority = [job.source]
    else:
        priority = configured
    if not priority:
        priority = ["slskd"]

    attempted: list[dict[str, object]] = []
    # Load catalog album info for folder-based scoring when relevant.
    _scoring_album = (
        await _load_catalog_album(db, job.catalog_album_id) if job.catalog_album_id else None
    )
    query_variants = await _queries_for_job(job, _scoring_album, cfg)

    for source in priority:
        try:
            adapter = _call_source_adapter(source, cfg, runtime)
            if priority_job:
                cap = await adapter.health()
                if not cap.available:
                    attempted.append(
                        {
                            "source": source,
                            "status": "unhealthy",
                            "reason": cap.reason or "unavailable",
                        }
                    )
                    _set_acquisition_provenance(job, attempted)
                    await _persist_provider_progress(db, checkpoint)
                    continue

            # For slskd album downloads: group files by folder and select the best one.
            results: list[SearchResult] = []
            served_query: str | None = None
            if (
                source == "slskd"
                and _scoring_album is not None
                and job.catalog_track_id is None
                and isinstance(adapter, SlskdAdapter)
            ):
                req = SearchRequest(query=query_variants[0], sources=[source])
                results = await _fetch_slskd_album_results(
                    adapter, req, job, _scoring_album, runtime, db
                )
                results = (await _without_blocked_slskd_results(results, db))[:limit]
                served_query = query_variants[0]
                attempted.append(
                    {
                        "source": source,
                        "status": "served" if results else "empty",
                        "results": len(results),
                        "query": served_query,
                    }
                )
            else:
                for provider_query in query_variants:
                    req = SearchRequest(query=provider_query, sources=[source])
                    results = (
                        await _without_blocked_slskd_results(await adapter.search(req), db)
                    )[:limit]
                    served_query = provider_query
                    attempted.append(
                        {
                            "source": source,
                            "status": "served" if results else "empty",
                            "results": len(results),
                            "query": provider_query,
                        }
                    )
                    if results:
                        break

            if not results:
                _set_acquisition_provenance(job, attempted)
                await _persist_provider_progress(db, checkpoint)
                continue
            job.source = source
            _set_acquisition_provenance(job, attempted, source)
            await _persist_provider_progress(db, checkpoint)
            return results
        except ProviderError as exc:
            attempted.append(
                {"source": source, "status": "failed", "reason": exc.message, "code": exc.code}
            )
        except Exception as exc:
            attempted.append(
                {"source": source, "status": "failed", "reason": exc.__class__.__name__}
            )
        _set_acquisition_provenance(job, attempted)
        await _persist_provider_progress(db, checkpoint)
    raise ProviderError(
        "sources_exhausted",
        "All configured download sources were exhausted",
        "search",
        True,
    )


async def _fetch_slskd_album_results(
    adapter: SlskdAdapter,
    req: SearchRequest,
    job: Job,
    catalog_album: CatalogAlbum,
    runtime: object,
    db: AsyncSession,
) -> list[SearchResult]:
    """Fetch slskd results for an album job, grouped and scored by folder.

    Selects the best coherent folder (same peer, same directory, same format)
    and returns individual SearchResult items for each audio file in that folder.
    Never applies a global file count cap.
    """
    from app.services.slskd_scoring import select_best_folder

    folders, _raw = await adapter.search_album_folders(req)
    if not folders:
        return []
    blocked = await _blocked_slskd_identities(db)
    # Reject an entire folder if any member is blocked; selecting the remainder
    # would turn a coherent album into an incomplete candidate.
    folders = [
        folder
        for folder in folders
        if not any((folder.username, item.filename) in blocked for item in folder.files)
    ]
    if not folders:
        return []

    qp = getattr(runtime, "quality_profile", None)
    format_pref = list(qp.format_preference) if qp else ["flac", "mp3"]
    min_bitrate = int(qp.min_mp3_bitrate) if qp else 192
    allow_fallback = bool(qp.allow_lower_quality_fallback) if qp else True

    catalog_track_count = (
        len(catalog_album.tracks) if catalog_album.tracks else catalog_album.track_count
    )
    catalog_artist = catalog_album.artist.name if catalog_album.artist else None

    best = select_best_folder(
        folders,
        catalog_track_count=catalog_track_count,
        catalog_artist=catalog_artist,
        catalog_album=catalog_album.title,
        format_preference=format_pref,
        min_mp3_bitrate=min_bitrate,
        allow_lower_quality_fallback=allow_fallback,
    )
    if best is None:
        return []

    results: list[SearchResult] = []
    for slskd_file in best.files:
        filename = slskd_file.filename
        ext = best.audio_format
        guess = parse_filename(filename)
        position_evidence = parsed_position_evidence(filename)
        if slskd_file.disc is not None:
            position_evidence["disc"] = slskd_file.disc
        results.append(
            SearchResult(
                source="slskd",
                title=guess.title,
                artist=guess.artist,
                album=guess.album,
                duration_sec=slskd_file.duration_sec,
                size_bytes=slskd_file.size_bytes,
                format=ext,
                url=f"slskd://{best.username}/{filename}",
                metadata={
                    "username": best.username,
                    "filename": filename,
                    "parse_confidence": guess.confidence,
                    "parse_hints": list(guess.hints),
                    "bit_rate": slskd_file.bit_rate,
                    "sample_rate": slskd_file.sample_rate,
                    "folder_score": best.score,
                    "parent_dir": best.parent_dir,
                    **position_evidence,
                },
            )
        )
    return results


async def _call_prepare_acquisition(
    result: SearchResult,
    source: str,
    cfg: Settings,
    track: Track,
    *,
    checkpoint: Callable[[], Awaitable[None]] | None,
) -> tuple[str | None, str | None]:
    try:
        return await _prepare_acquisition(result, source, cfg, track, checkpoint=checkpoint)
    except TypeError as exc:
        if "checkpoint" not in str(exc) or "unexpected keyword" not in str(exc):
            raise
        return await _prepare_acquisition(result, source, cfg, track)


async def _prepare_acquisition(
    result: SearchResult,
    source: str,
    cfg: Settings,
    track: Track | None = None,
    *,
    checkpoint: Callable[[], Awaitable[None]] | None = None,
) -> tuple[str | None, str | None]:
    if source in {"youtube", "tidal"}:
        if not result.url:
            raise ProviderError("invalid_result", f"{source} result URL is missing", "acquire")
        if source == "youtube":
            acquired = await YouTubeAdapter(cfg.ytdlp_cookies_file).acquire(
                result.url, cfg.staging_root
            )
        else:
            acquired = await TidalAdapter(
                cfg.tidal_config_path,
                cfg.tidal_session_path,
                cfg.tidal_quality,
            ).acquire(result.url, cfg.staging_root)
        if track is not None:
            track.source_path = str(acquired.path)
            track.staging_path = str(acquired.path)
            track.acquisition_state = AcquisitionState.downloaded
            track.acquisition_provenance_json = json.dumps(acquired.provenance, sort_keys=True)
            with contextlib.suppress(OSError):
                st = await asyncio.to_thread(acquired.path.stat)
                track.file_size_bytes = st.st_size
                suffix = acquired.path.suffix.lower().lstrip(".")
                if suffix and len(suffix) <= 16 and suffix.isalnum():
                    track.file_format = suffix
        return None, "downloaded"
    if source == "slskd":
        username = str(result.metadata.get("username") or "")
        filename = str(result.metadata.get("filename") or "")
        if not is_importable_audio(filename):
            raise ProviderError(
                "invalid_result", "slskd result is not an importable audio file", "acquire"
            )
        adapter = SlskdAdapter(cfg.slskd_url, cfg.slskd_api_key)
        transfer_id: str | None = None
        if track is not None and track.source_job_id:
            if track.acquisition_state == AcquisitionState.acquiring:
                transfer_id = track.source_job_id
            else:
                existing = await adapter.status(track.source_job_id)
                existing_state = map_slskd_transfer_state(existing)
                if existing_state not in {AcquisitionState.failed, AcquisitionState.cancelled}:
                    transfer_id = track.source_job_id
        if not transfer_id:
            transfer_id = await adapter.enqueue(username, filename, result.size_bytes)
        if track is not None:
            track.source_job_id = transfer_id
            track.source_status = "acquiring"
            track.acquisition_state = AcquisitionState.acquiring
            track.acquisition_provenance_json = json.dumps(
                {"source": "slskd", "username": username, "filename": filename}, sort_keys=True
            )
        if checkpoint is not None:
            await checkpoint()

        async def persist_provider_id(provider_id: str) -> None:
            if track is not None:
                track.source_job_id = provider_id
            if checkpoint is not None:
                await checkpoint()

        staged, transfer_id = await _poll_slskd_transfer(
            transfer_id,
            username,
            filename,
            adapter,
            cfg.staging_root,
            cfg.slskd_poll_interval,
            cfg.slskd_poll_timeout,
            persist_provider_id,
        )
        if track is not None:
            # slskd queue responses may omit the provider UUID. Polling exposes it;
            # persist it before cleanup so deletion never falls back to peer/path.
            track.source_job_id = transfer_id
            track.source_path = str(staged)
            track.staging_path = str(staged)
            track.acquisition_state = AcquisitionState.downloaded
            with contextlib.suppress(OSError):
                st = await asyncio.to_thread(staged.stat)
                track.file_size_bytes = st.st_size
                suffix = staged.suffix.lower().lstrip(".")
                if suffix and len(suffix) <= 16 and suffix.isalnum():
                    track.file_format = suffix
        if checkpoint is not None:
            await checkpoint()
        return transfer_id, "downloaded"
    if source != "prowlarr":
        if track is not None:
            track.source_path = result.url
        return None, None
    nzb_url = _validated_nzb_url(result, cfg)

    sab = SabnzbdAdapter(cfg.sabnzbd_url, cfg.sabnzbd_api_key)
    sab_job_id = (
        track.source_job_id
        if track is not None and track.acquisition_state == AcquisitionState.acquiring
        else None
    )
    if not sab_job_id:
        sab_job_id = await sab.enqueue(nzb_url, name=result.title)
    if not sab_job_id:
        raise RuntimeError("SABnzbd enqueue returned no job id")
    if track is not None:
        track.source_job_id = sab_job_id
        track.source_status = "acquiring"
        track.acquisition_state = AcquisitionState.acquiring
        track.acquisition_provenance_json = json.dumps({"source": "sabnzbd"}, sort_keys=True)
    if checkpoint is not None:
        await checkpoint()
    staged = await _poll_sab_job(
        sab_job_id,
        sab,
        cfg.staging_root,
        cfg.sabnzbd_poll_interval,
        cfg.sabnzbd_poll_timeout,
    )
    if track is not None:
        track.source_path = str(staged)
        track.staging_path = str(staged)
        track.acquisition_state = AcquisitionState.downloaded
        with contextlib.suppress(OSError):
            st = await asyncio.to_thread(staged.stat)
            track.file_size_bytes = st.st_size
            suffix = staged.suffix.lower().lstrip(".")
            if suffix and len(suffix) <= 16 and suffix.isalnum():
                track.file_format = suffix
    return sab_job_id, "downloaded"


def _validated_nzb_url(result: SearchResult, cfg: Settings) -> str:
    if result.format != "nzb" or not result.url:
        raise RuntimeError("Prowlarr result is not a validated NZB URL")

    parsed = urlparse(result.url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise RuntimeError("Prowlarr result has invalid NZB URL")
    if not parsed.path.endswith(".nzb"):
        raise RuntimeError("Prowlarr result URL is not an NZB")
    trusted = _trusted_prowlarr_origin(cfg)
    result_origin = _origin_tuple(parsed)
    if trusted is None:
        raise RuntimeError("Prowlarr trusted NZB URL host is not configured")
    if result_origin != trusted:
        raise RuntimeError("Prowlarr result URL host is not trusted")
    return result.url


def _trusted_prowlarr_origin(cfg: Settings) -> tuple[str, str, int | None] | None:
    parsed = urlparse(cfg.prowlarr_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    return _origin_tuple(parsed)


def _origin_tuple(parsed: ParseResult) -> tuple[str, str, int | None]:
    if parsed.hostname is None:
        raise RuntimeError("Prowlarr result has invalid NZB URL")
    return (parsed.scheme.casefold(), parsed.hostname.casefold().rstrip("."), parsed.port)


async def _enrich_musicbrainz(track: Track, cfg: Settings) -> None:
    if not track.title:
        return
    started = _now()
    guess = parse_filename(str(track.title))
    title = guess.title
    artist = track.artist or (guess.artist if guess.confidence >= 0.65 else None)
    if guess.confidence < 0.25:
        track.identity_state = IdentityResolutionState.unresolved
        logger.info(
            "Skipping MusicBrainz enrichment for low-confidence parse on track %s", track.id
        )
        return
    try:
        client = MusicBrainzClient(cfg.musicbrainz_user_agent)
        results = await client.search_recording(
            title=title,
            artist=artist,
            album=track.album or (guess.album if guess.confidence >= 0.8 else None),
        )
        if results:
            meta = results[0]
            track.mbid = track.mbid or meta.mbid
            track.identity_state = IdentityResolutionState.resolved
            track.title = track.title or meta.title
            track.artist = track.artist or meta.artist
            track.album_artist = track.album_artist or meta.album_artist
            track.album = track.album or meta.album
            track.year = track.year or meta.year
            track.disc = track.disc or meta.disc
            track.disc_total = track.disc_total or meta.disc_total
            track.track_no = track.track_no or meta.track_no
            if meta.duration_ms and not track.duration_sec:
                track.duration_sec = meta.duration_ms // 1000
        else:
            track.identity_state = IdentityResolutionState.unresolved
        logger.info(
            "MusicBrainz enrichment for track %s took %.3fs",
            track.id,
            (_now() - started).total_seconds(),
        )
    except Exception as exc:
        track.identity_state = IdentityResolutionState.unresolved
        logger.warning("MusicBrainz enrichment failed for track %d: %s", track.id, exc)


async def _enrich_deezer(track: Track, cfg: Settings) -> None:
    if not track.title:
        return
    try:
        client = DeezerClient(cfg.deezer_api_url)
        results = await client.search_track(track.title or "", track.artist)
        if results:
            d = results[0]
            track.deezer_id = track.deezer_id or d.deezer_id
            if not track.duration_sec and d.duration_sec:
                track.duration_sec = d.duration_sec
    except Exception as exc:
        logger.warning("Deezer enrichment failed for track %d: %s", track.id, exc)


async def _run_fingerprint(track: Track, cfg: Settings) -> int | None:
    if not track.source_path:
        track.fingerprint_state = FingerprintState.skipped
        return None
    path = Path(track.source_path)
    if not await asyncio.to_thread(path.exists):
        track.fingerprint_state = FingerprintState.skipped
        return None

    result = await fingerprint_file(path)
    if result is None:
        import shutil

        if not shutil.which("fpcalc"):
            track.fingerprint_state = FingerprintState.skipped
        else:
            track.fingerprint_state = FingerprintState.failed
        return None

    duration, fingerprint = result
    track.acoustid = fingerprint
    if not track.duration_sec:
        track.duration_sec = duration
    track.fingerprint_state = FingerprintState.done
    return duration


async def _run_fingerprint_and_verify(track: Track, cfg: Settings, db: AsyncSession) -> None:
    """Fingerprint a download and require AcoustID confirmation or human review."""
    duration = await _run_fingerprint(track, cfg)
    fingerprint = track.acoustid
    raw_results: list[dict[str, object]] = []
    if (
        cfg.acoustid_api_key
        and track.fingerprint_state == FingerprintState.done
        and duration is not None
        and duration > 0
        and fingerprint
    ):
        raw_results = await _lookup_acoustid_raw(duration, fingerprint, cfg.acoustid_api_key)

    from app.services.acoustid_verification import run_acoustid_verification

    runtime = await get_runtime_settings(db)
    await run_acoustid_verification(
        track,
        acoustid_raw_results=raw_results,
        fingerprint_duration_sec=duration,
        db=db,
        acceptance_threshold=runtime.acoustid_acceptance_threshold,
    )


async def _lookup_acoustid_raw(
    duration: int, fingerprint: str, api_key: str
) -> list[dict[str, object]]:
    """Return raw AcoustID result list (not just MBIDs) for verification comparison."""
    import httpx

    _ACOUSTID_API = "https://api.acoustid.org/v2/lookup"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            resp = await client.post(
                _ACOUSTID_API,
                data={
                    "client": api_key,
                    "duration": str(duration),
                    "fingerprint": fingerprint,
                    "meta": "recordings",
                },
            )
            resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        return results if isinstance(results, list) else []
    except Exception as exc:
        logger.warning("AcoustID raw lookup failed: %s", exc)
        return []


async def _try_auto_import(release: Release, db: AsyncSession, cfg: Settings) -> None:
    """Import every currently eligible verified track for a release."""
    from app.services.auto_import import try_auto_import_release

    await try_auto_import_release(
        db,
        release,
        library_root=cfg.library_root,
        naming_template=cfg.naming_template,
    )


async def _root_job(job: Job, db: AsyncSession) -> Job:
    current = job
    seen = {job.id}
    while current.parent_job_id is not None and current.parent_job_id not in seen:
        parent = await db.get(Job, current.parent_job_id)
        if parent is None:
            break
        current = parent
        seen.add(current.id)
    return current


async def _spawn_continuation_jobs(
    parent_job: Job,
    missing_catalog_track_ids: list[int],
    catalog_album: CatalogAlbum,
    db: AsyncSession,
) -> None:
    """Spawn idempotent follow-up jobs, one targeted search per missing track."""
    from app.jobs.dispatcher import job_dispatcher

    artist = catalog_album.artist.name if catalog_album.artist else ""
    tracks_by_id = {track.id: track for track in catalog_album.tracks}
    continuation_ids: list[int] = []
    for track_id in missing_catalog_track_ids:
        duplicate = await db.scalar(
            select(Job.id).where(
                Job.catalog_album_id == catalog_album.id,
                Job.catalog_track_id == track_id,
                Job.status.in_([JobStatus.pending, JobStatus.running]),
            )
        )
        if duplicate is not None:
            continue
        catalog_track = tracks_by_id.get(track_id)
        if catalog_track is None:
            continue
        continuation = Job(
            source="priority",
            query=_targeted_query_variants(artist, catalog_album.title, catalog_track.title)[0],
            status=JobStatus.pending,
            catalog_album_id=catalog_album.id,
            catalog_track_id=track_id,
            parent_job_id=parent_job.id,
            partial_attempt=parent_job.partial_attempt + 1,
            result_json=json.dumps({"continuation_of": parent_job.id}, sort_keys=True),
        )
        db.add(continuation)
        await db.flush()
        continuation_ids.append(continuation.id)
    await db.commit()
    for continuation_id in continuation_ids:
        await job_dispatcher.dispatch(continuation_id)


async def _compute_path_preview(track: Track, db: AsyncSession, cfg: Settings) -> None:
    try:
        rendered = render_path(
            track,
            title=track.title or "Unknown",
            ext=_guess_ext(track.source_path),
            template=cfg.naming_template,
            library_root=cfg.library_root,
        )
        preview = PathPreview(
            track_id=track.id,
            rendered_path=rendered,
            naming_template=cfg.naming_template,
            computed_at=_now(),
        )
        db.add(preview)
        await db.flush()
    except NamingError as exc:
        logger.warning("Path preview failed for track %d: %s", track.id, exc)
        raise


def _guess_ext(source_path: str | None) -> str:
    if source_path and "." in source_path.rsplit("/", 1)[-1]:
        return source_path.rsplit(".", 1)[-1].lower()
    return "flac"
