from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import ParseResult, urlparse

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings, get_settings
from app.database import get_session_factory
from app.fingerprint.acoustid import fingerprint_file, lookup_acoustid
from app.media_formats import IMPORTABLE_AUDIO_EXTENSIONS
from app.metadata.deezer import DeezerClient
from app.metadata.filename_parse import parse_filename
from app.metadata.musicbrainz import MusicBrainzClient
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.models.job import Job, JobStatus
from app.models.path_preview import PathPreview
from app.models.release import Release
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import AcquisitionState
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
) -> Path:
    """Poll slskd until transfer reaches a terminal state. Returns verified staged file path."""
    import time as _time

    deadline = _time.monotonic() + poll_timeout
    while True:
        remaining = deadline - _time.monotonic()
        if remaining <= 0:
            with contextlib.suppress(Exception):
                await adapter.cancel(username, filename)
            raise ProviderError("transfer_timeout", "slskd transfer timed out", "acquire", True)
        try:
            state = await adapter.status(transfer_id)
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await adapter.cancel(username, filename)
            raise

        acq_state = map_slskd_transfer_state(state)
        if acq_state == AcquisitionState.downloaded:
            return await _locate_slskd_artifact(filename, state.extra, staging_root)
        if acq_state == AcquisitionState.failed:
            raise ProviderError(
                "transfer_failed", f"slskd transfer failed: {state.reason}", "acquire", True
            )
        if acq_state == AcquisitionState.cancelled:
            raise ProviderError("transfer_failed", "slskd transfer was cancelled", "acquire", True)

        await asyncio.sleep(min(poll_interval, max(0.01, remaining)))


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

    try:
        results = _selected_result(job) or await _call_fetch_results(job, cfg, db)
        catalog_album = await _load_catalog_album(db, job.catalog_album_id)
        catalog_tracks = list(catalog_album.tracks) if catalog_album is not None else []
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
        failures: list[str] = []
        existing_releases = list(
            (await db.scalars(select(Release).where(Release.job_id == job_id))).all()
        )
        releases: dict[tuple[str | None, str | None], Release] = {
            (release.album_artist, release.title): release for release in existing_releases
        }
        existing_tracks = list(
            (await db.scalars(select(Track).where(Track.job_id == job_id))).all()
        )
        for result in results:
            catalog_track = _catalog_track_for_result(result, catalog_tracks, job.catalog_track_id)
            track_title = catalog_track.title if catalog_track is not None else result.title
            track_album = catalog_album.title if catalog_album is not None else result.album
            track = _existing_track_for_result(
                existing_tracks,
                result.source,
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
                        job_id=job_id,
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
                await _run_fingerprint(track, cfg)
                await _compute_path_preview(track, db, cfg)

                tracks_created += 1
            except ProviderError as exc:
                if track is not None:
                    track.acquisition_state = AcquisitionState.failed
                    track.source_status = exc.code
                logger.warning("Provider result processing failed with code %s", exc.code)
                failures.append(json.dumps(exc.details(), sort_keys=True))
            except Exception:
                if track is not None:
                    track.acquisition_state = AcquisitionState.failed
                logger.warning("Result processing failed")
                failures.append("result_processing_failed")
            if commit_progress:
                job.updated_at = _now()
                await db.commit()

        if failures and tracks_created:
            job.status = JobStatus.partial
            payload = _job_payload(job)
            payload.update({"tracks_created": tracks_created, "errors": failures})
            job.result_json = json.dumps(payload, sort_keys=True)
        elif failures:
            job.status = JobStatus.failed
            payload = _job_payload(job)
            payload.update({"tracks_created": tracks_created, "errors": failures})
            job.result_json = json.dumps(payload, sort_keys=True)
        elif catalog_tracks and tracks_created < len(catalog_tracks):
            job.status = JobStatus.partial
            acquired_ids = {
                track.catalog_track_id
                for track in (await db.scalars(select(Track).where(Track.job_id == job.id))).all()
                if track.catalog_track_id is not None
            }
            missing = [track.title for track in catalog_tracks if track.id not in acquired_ids]
            payload = _job_payload(job)
            payload.update({"tracks_created": tracks_created, "missing_tracks": missing})
            job.result_json = json.dumps(payload, sort_keys=True)
        else:
            job.status = JobStatus.done
            payload = _job_payload(job)
            payload.update({"tracks_created": tracks_created})
            job.result_json = json.dumps(payload, sort_keys=True)
        job.updated_at = _now()
    except ProviderError as exc:
        logger.warning("Job %d provider failure code %s", job_id, exc.code)
        job.status = JobStatus.failed
        job.result_json = json.dumps({"error": exc.details()})
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


def _catalog_track_for_result(
    result: SearchResult,
    tracks: list[CatalogAlbumTrack],
    selected_track_id: int | None,
) -> CatalogAlbumTrack | None:
    if selected_track_id is not None:
        return next((track for track in tracks if track.id == selected_track_id), None)
    title = (result.title or "").casefold().strip()
    if title:
        for track in tracks:
            if track.title.casefold().strip() == title:
                return track
    return None


def _existing_track_for_result(
    tracks: list[Track],
    source: str,
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
            if track.source == source
            and (track.title or "").casefold().strip() == (title or "").casefold().strip()
            and (track.album or "").casefold().strip() == (album or "").casefold().strip()
        ),
        None,
    )


def _selected_result(job: Job) -> list[SearchResult] | None:
    if not job.selected_result_json:
        return None
    return [SearchResult.model_validate(json.loads(job.selected_result_json))]


async def _call_fetch_results(job: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
    try:
        return await _fetch_results(job, cfg, db)
    except TypeError as exc:
        # Backward-compatible test/plugin hook: older _fetch_results monkeypatches
        # accepted only (job, cfg). Do not swallow TypeError raised inside the hook.
        if "positional" not in str(exc) and "argument" not in str(exc):
            raise
        return list(await _fetch_results(job, cfg))  # type: ignore[call-arg]


def _source_adapter(source: str, cfg: Settings) -> SourceAdapter:
    if source == "slskd":
        return SlskdAdapter(cfg.slskd_url, cfg.slskd_api_key)
    if source == "prowlarr":
        return ProwlarrAdapter(cfg.prowlarr_url, cfg.prowlarr_api_key)
    if source == "youtube":
        return YouTubeAdapter(cfg.ytdlp_cookies_file)
    if source == "tidal":
        return TidalAdapter(cfg.tidal_config_path, cfg.tidal_session_path, cfg.tidal_quality)
    raise ValueError(f"Unknown source: {source}")


def _job_payload(job: Job) -> dict[str, object]:
    if not job.result_json:
        return {}
    with contextlib.suppress(json.JSONDecodeError):
        payload = json.loads(job.result_json)
        if isinstance(payload, dict):
            return payload
    return {}


def _set_acquisition_provenance(
    job: Job, attempted: list[dict[str, object]], served_source: str | None = None
) -> None:
    payload = _job_payload(job)
    payload["source_attempts"] = attempted
    payload["attempted_sources"] = [str(item.get("source", "")) for item in attempted]
    if served_source:
        payload["served_source"] = served_source
    job.result_json = json.dumps(payload, sort_keys=True)


async def _fetch_results(
    job: Job, cfg: Settings, db: AsyncSession, limit: int = DEFAULT_FREE_TEXT_RESULT_LIMIT
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
    for source in priority:
        req = SearchRequest(query=job.query, sources=[source])
        try:
            adapter = _source_adapter(source, cfg)
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
                    await db.flush()
                    continue
            results = (await adapter.search(req))[:limit]
            if not results:
                attempted.append({"source": source, "status": "empty", "reason": "zero results"})
                _set_acquisition_provenance(job, attempted)
                await db.flush()
                continue
            attempted.append({"source": source, "status": "served", "results": len(results)})
            job.source = source
            _set_acquisition_provenance(job, attempted, source)
            await db.flush()
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
        await db.flush()
    raise ProviderError(
        "sources_exhausted",
        "All configured download sources were exhausted",
        "search",
        True,
    )


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
        adapter = SlskdAdapter(cfg.slskd_url, cfg.slskd_api_key)
        transfer_id = (
            track.source_job_id
            if track is not None and track.acquisition_state == AcquisitionState.acquiring
            else None
        )
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
        staged = await _poll_slskd_transfer(
            transfer_id,
            username,
            filename,
            adapter,
            cfg.staging_root,
            cfg.slskd_poll_interval,
            cfg.slskd_poll_timeout,
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


async def _run_fingerprint(track: Track, cfg: Settings) -> None:
    if not track.source_path:
        track.fingerprint_state = FingerprintState.skipped
        return
    path = Path(track.source_path)
    if not await asyncio.to_thread(path.exists):
        track.fingerprint_state = FingerprintState.skipped
        return

    result = await fingerprint_file(path)
    if result is None:
        import shutil

        if not shutil.which("fpcalc"):
            track.fingerprint_state = FingerprintState.skipped
        else:
            track.fingerprint_state = FingerprintState.failed
        return

    duration, fingerprint = result
    track.acoustid = fingerprint
    if not track.duration_sec:
        track.duration_sec = duration
    track.fingerprint_state = FingerprintState.done

    if cfg.acoustid_api_key and not track.mbid:
        mbids = await lookup_acoustid(duration, fingerprint, cfg.acoustid_api_key)
        if mbids:
            track.mbid = mbids[0]
            track.identity_state = IdentityResolutionState.resolved


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
