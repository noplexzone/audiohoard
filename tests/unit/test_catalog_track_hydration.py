"""Regression tests for catalog album track hydration bugs.

Production scenario:
  - A CatalogAlbum has track_count=16 but zero CatalogAlbumTrack rows.
  - Direct-download and bulk-monitored-download dispatch never called
    fetch_and_store_album, so no tracks were persisted.
  - The runner saw an empty track list, couldn't bind catalog IDs, marked the
    job 'done', created no continuation jobs, and auto-import refused due to
    zero distinct catalog IDs.
  - A fully-approved 16-track legacy release also couldn't reconcile because
    the referenced CatalogAlbum had no CatalogAlbumTrack rows.
  - Long album searches/polling held a SQLite write transaction open, causing
    settings POST requests to fail with 'database is locked'.

Each test is labelled with the bug it covers.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.jobs import runner
from app.metadata.base import AlbumDetail, AlbumTrack
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import (
    AcoustIDVerificationState,
    AcquisitionState,
    ImportWorkflowState,
)
from app.schemas.search import SearchResult
from app.services import catalog_metadata
from app.services.acquisition_recovery import recover_approved_downloads

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_artist_album(
    *,
    track_count: int = 16,
    add_tracks: bool = False,
    deezer_id: str = "deezer-alb-1",
) -> tuple[CatalogArtist, CatalogAlbum]:
    artist = CatalogArtist(name="Test Artist", deezer_id="deezer-art-1")
    album = CatalogAlbum(
        title="Test Album",
        year="2024",
        track_count=track_count,
        deezer_id=deezer_id,
    )
    artist.albums.append(album)
    if add_tracks:
        for i in range(1, track_count + 1):
            album.tracks.append(
                CatalogAlbumTrack(
                    position=i,
                    disc=1,
                    title=f"Track {i:02d}",
                    recording_mbid=f"{i:08x}-0000-0000-0000-000000000000",
                )
            )
    return artist, album


async def _persist_artist_album(
    db: AsyncSession, *, track_count: int = 16, add_tracks: bool = False
) -> tuple[CatalogArtist, CatalogAlbum]:
    artist, album = _make_artist_album(track_count=track_count, add_tracks=add_tracks)
    db.add(artist)
    await db.flush()
    return artist, album


async def _make_album_job(db: AsyncSession, album: CatalogAlbum) -> Job:
    job = Job(
        source="priority",
        query=f"Test Artist {album.title}",
        status=JobStatus.pending,
        catalog_album_id=album.id,
    )
    db.add(job)
    await db.flush()
    return job


def _noop_noops(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch all side-effecting runner helpers to noops."""

    async def noop_enrich_mb(track: Track, cfg: Settings) -> None:
        pass

    async def noop_enrich_dz(track: Track, cfg: Settings) -> None:
        pass

    async def noop_verify(track: Track, cfg: Settings, db: AsyncSession) -> None:
        pass

    async def noop_preview(track: Track, db: AsyncSession, cfg: Settings) -> None:
        pass

    async def noop_auto_import(rel: Release, db: AsyncSession, cfg: Settings) -> None:
        pass

    monkeypatch.setattr(runner, "_enrich_musicbrainz", noop_enrich_mb)
    monkeypatch.setattr(runner, "_enrich_deezer", noop_enrich_dz)
    monkeypatch.setattr(runner, "_run_fingerprint_and_verify", noop_verify)
    monkeypatch.setattr(runner, "_compute_path_preview", noop_preview)
    monkeypatch.setattr(runner, "_try_auto_import", noop_auto_import)


def _make_slskd_results(count: int, *, first_track_no: int = 1) -> list[SearchResult]:
    """Return `count` slskd results for tracks first_track_no..first_track_no+count-1."""
    results = []
    for i in range(first_track_no, first_track_no + count):
        results.append(
            SearchResult(
                source="slskd",
                title=f"{i:02d} Track {i:02d}",
                url=f"slskd://peer/{i:02d} Track {i:02d}.flac",
                metadata={
                    "username": "peer",
                    "filename": f"{i:02d} Track {i:02d}.flac",
                    "track_no": i,
                },
            )
        )
    return results


def _acquiring_acquire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Patch _call_prepare_acquisition to mark tracks as downloaded."""

    async def fake_acquire(
        result: SearchResult,
        source: str,
        cfg: Settings,
        track: Track,
        *,
        checkpoint: object = None,
    ) -> tuple[None, str]:
        track.acquisition_state = AcquisitionState.downloaded
        track.source_path = f"/staging/{result.title}.flac"
        return None, "downloaded"

    monkeypatch.setattr(runner, "_call_prepare_acquisition", fake_acquire)


async def test_metadata_hydration_preserves_larger_known_track_count(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ten-track provider response cannot shrink a known sixteen-track album."""
    artist, album = await _persist_artist_album(db_session, track_count=16, add_tracks=False)

    class PartialProvider:
        async def get_album(self, provider_id: str) -> AlbumDetail:
            return AlbumDetail(
                provider="deezer",
                provider_id=provider_id,
                title=album.title,
                artist_name=artist.name,
                deezer_id=provider_id,
                track_count=10,
                tracks=[AlbumTrack(position=i, title=f"Track {i:02d}") for i in range(1, 11)],
            )

    monkeypatch.setattr(
        catalog_metadata,
        "build_metadata_provider",
        lambda provider_name, settings: PartialProvider(),
    )

    hydrated = await catalog_metadata.fetch_and_store_album(db_session, test_settings, album)

    assert hydrated.track_count == 16
    assert len(hydrated.tracks) == 10


# ---------------------------------------------------------------------------
# Bug 2 – Runner defensive hydration when catalog tracks absent
# ---------------------------------------------------------------------------


async def test_runner_fails_structured_when_hydration_fails_and_tracks_empty(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 2: When track_count>0 but no tracks exist and hydration fails, runner must fail
    with error code 'catalog_tracks_empty', not silently mark done."""
    artist, album = await _persist_artist_album(db_session, track_count=16, add_tracks=False)
    job = await _make_album_job(db_session, album)

    async def failing_hydrate(
        db: AsyncSession, settings: Settings, alb: CatalogAlbum
    ) -> CatalogAlbum:
        raise RuntimeError("provider unreachable")

    monkeypatch.setattr(catalog_metadata, "fetch_and_store_album", failing_hydrate)

    async def no_results(job: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        return []

    monkeypatch.setattr(runner, "_fetch_results", no_results)
    _noop_noops(monkeypatch)

    await runner._run_job_in_session(job.id, db_session, test_settings)

    assert job.status == JobStatus.failed, f"Expected failed, got {job.status}"
    payload = json.loads(job.result_json or "{}")
    assert payload.get("error", {}).get("code") == "catalog_tracks_empty", payload


async def test_runner_fails_structured_when_hydration_returns_no_tracks(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 2: Hydration that returns zero tracks (e.g. no provider IDs) → fail not done."""
    artist, album = await _persist_artist_album(db_session, track_count=16, add_tracks=False)
    job = await _make_album_job(db_session, album)

    async def noop_hydrate(
        db: AsyncSession, settings: Settings, alb: CatalogAlbum
    ) -> CatalogAlbum:
        return alb  # returns album unchanged, zero tracks persisted

    monkeypatch.setattr(catalog_metadata, "fetch_and_store_album", noop_hydrate)

    async def no_results(job: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        return []

    monkeypatch.setattr(runner, "_fetch_results", no_results)
    _noop_noops(monkeypatch)

    await runner._run_job_in_session(job.id, db_session, test_settings)

    assert job.status == JobStatus.failed
    payload = json.loads(job.result_json or "{}")
    assert payload.get("error", {}).get("code") == "catalog_tracks_empty"


async def test_runner_hydrates_then_proceeds_when_tracks_initially_absent(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 2: Runner calls fetch_and_store_album when tracks absent, then proceeds normally."""
    artist, album = await _persist_artist_album(db_session, track_count=4, add_tracks=False)
    job = await _make_album_job(db_session, album)

    hydration_calls: list[int] = []

    async def hydrate_with_tracks(
        db: AsyncSession, settings: Settings, alb: CatalogAlbum
    ) -> CatalogAlbum:
        hydration_calls.append(alb.id)
        for i in range(1, 5):
            db.add(
                CatalogAlbumTrack(
                    album_id=alb.id,
                    position=i,
                    disc=1,
                    title=f"Track {i:02d}",
                )
            )
        alb.track_count = 4
        await db.flush()
        await db.refresh(alb, ["tracks"])
        return alb

    monkeypatch.setattr(catalog_metadata, "fetch_and_store_album", hydrate_with_tracks)

    four_results = _make_slskd_results(4)

    async def fake_fetch(job: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        return four_results

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch)
    _noop_noops(monkeypatch)
    _acquiring_acquire(monkeypatch)

    await runner._run_job_in_session(job.id, db_session, test_settings)

    assert len(hydration_calls) == 1, "fetch_and_store_album should have been called once"
    assert job.status == JobStatus.done, f"Expected done, got {job.status}"

    tracks = list((await db_session.scalars(select(Track).where(Track.job_id == job.id))).all())
    assert len(tracks) == 4
    assert all(t.catalog_track_id is not None for t in tracks), "All tracks must be catalog-bound"


async def test_runner_rejects_partial_manifest_when_hydration_cannot_complete_it(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ten persisted catalog rows cannot redefine a known 16-track album as complete."""
    artist, album = await _persist_artist_album(db_session, track_count=16, add_tracks=False)
    for i in range(1, 11):
        db_session.add(
            CatalogAlbumTrack(
                album_id=album.id,
                position=i,
                disc=1,
                title=f"Track {i:02d}",
            )
        )
    await db_session.flush()
    await db_session.refresh(album, ["tracks"])
    job = await _make_album_job(db_session, album)

    async def incomplete_hydrate(
        db: AsyncSession, settings: Settings, alb: CatalogAlbum
    ) -> CatalogAlbum:
        alb.track_count = 10  # A partial provider response must not erase the known 16.
        return alb

    async def no_results(job: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        return []

    monkeypatch.setattr(catalog_metadata, "fetch_and_store_album", incomplete_hydrate)
    monkeypatch.setattr(runner, "_fetch_results", no_results)
    _noop_noops(monkeypatch)

    await runner._run_job_in_session(job.id, db_session, test_settings)

    assert job.status == JobStatus.failed
    payload = json.loads(job.result_json or "{}")
    assert payload.get("error", {}).get("code") == "catalog_tracks_incomplete"


# ---------------------------------------------------------------------------
# Bug 3 – 16-track album + 10-file slskd folder → partial + 6 continuations
# ---------------------------------------------------------------------------


async def test_runner_partial_16_expected_10_matched_spawns_6_continuations(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 3: 16 catalog tracks + 10 slskd results → partial status, 10 bound, 6 missing,
    6 continuation jobs dispatched (each targeting one missing catalog track id)."""
    artist, album = await _persist_artist_album(db_session, track_count=16, add_tracks=False)
    for i in range(1, 11):
        db_session.add(
            CatalogAlbumTrack(
                album_id=album.id,
                position=i,
                disc=1,
                title=f"Track {i:02d}",
            )
        )
    await db_session.flush()
    await db_session.refresh(album, ["tracks"])
    job = await _make_album_job(db_session, album)

    hydration_calls: list[int] = []

    async def complete_partial_manifest(
        db: AsyncSession, settings: Settings, alb: CatalogAlbum
    ) -> CatalogAlbum:
        hydration_calls.append(alb.id)
        for i in range(11, 17):
            db.add(
                CatalogAlbumTrack(
                    album_id=alb.id,
                    position=i,
                    disc=1,
                    title=f"Track {i:02d}",
                )
            )
        await db.flush()
        await db.refresh(alb, ["tracks"])
        return alb

    monkeypatch.setattr(catalog_metadata, "fetch_and_store_album", complete_partial_manifest)
    ten_results = _make_slskd_results(10)

    async def fake_fetch(job: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        return ten_results

    monkeypatch.setattr(runner, "_fetch_results", fake_fetch)
    _noop_noops(monkeypatch)
    _acquiring_acquire(monkeypatch)

    dispatched: list[int] = []
    from app.jobs.dispatcher import job_dispatcher

    async def fake_dispatch(job_id: int) -> None:
        dispatched.append(job_id)

    monkeypatch.setattr(job_dispatcher, "dispatch", fake_dispatch)

    await runner._run_job_in_session(job.id, db_session, test_settings)

    assert hydration_calls == [album.id]
    assert job.status == JobStatus.partial, f"Expected partial, got {job.status}"

    payload = json.loads(job.result_json or "{}")
    missing_ids = payload.get("missing_catalog_track_ids", [])
    assert len(missing_ids) == 6, (
        f"Expected 6 missing catalog ids, got {len(missing_ids)}: payload={payload}"
    )

    # 10 tracks persisted, all catalog-bound
    tracks = list((await db_session.scalars(select(Track).where(Track.job_id == job.id))).all())
    assert len(tracks) == 10
    assert all(t.catalog_track_id is not None for t in tracks), (
        "All 10 tracks must bind to catalog track IDs"
    )

    # 6 continuation jobs created, one per missing track
    cont_jobs = list(
        (
            await db_session.scalars(
                select(Job).where(
                    Job.parent_job_id == job.id,
                    Job.catalog_album_id == album.id,
                )
            )
        ).all()
    )
    assert len(cont_jobs) == 6, f"Expected 6 continuation jobs, got {len(cont_jobs)}"
    assert all(j.status == JobStatus.pending for j in cont_jobs)
    assert all(j.catalog_track_id is not None for j in cont_jobs)
    assert len(dispatched) == 6, f"Expected 6 dispatches, got {len(dispatched)}"

    # continuation job track IDs must be exactly the 6 missing ones
    cont_track_ids = {j.catalog_track_id for j in cont_jobs}
    assert cont_track_ids == set(missing_ids)


# ---------------------------------------------------------------------------
# Bug 4 – Legacy recovery hydrates missing catalog tracks before reconciling
# ---------------------------------------------------------------------------


async def test_legacy_recovery_hydrates_missing_catalog_tracks(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 4: recover_approved_downloads calls fetch_and_store_album when catalog
    tracks are absent, then reconciles and imports normally."""

    artist, album = await _persist_artist_album(db_session, track_count=4, add_tracks=False)
    album.track_count = None
    job_row = Job(
        source="slskd",
        query="Test Artist Test Album",
        status=JobStatus.done,
        catalog_album_id=album.id,
    )
    db_session.add(job_row)
    await db_session.flush()

    release = Release(
        job_id=job_row.id,
        source="slskd",
        title="Test Album",
        album_artist="Test Artist",
        year="2024",
        track_count=4,
    )
    db_session.add(release)
    await db_session.flush()

    staging = test_settings.staging_root
    staging.mkdir(parents=True, exist_ok=True)

    tracks: list[Track] = []
    for i in range(1, 5):
        audio = staging / f"{i:02d} Track {i:02d}.flac"
        audio.write_bytes(b"dummy")
        t = Track(
            job_id=job_row.id,
            release_id=release.id,
            source="slskd",
            title=f"Track {i:02d}",
            artist="Test Artist",
            album="Test Album",
            source_path=str(audio),
            staging_path=str(audio),
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.discovered,
            fingerprint_state=FingerprintState.done,
            identity_state=IdentityResolutionState.resolved,
            acoustid_verification_state=AcoustIDVerificationState.verified,
        )
        db_session.add(t)
        tracks.append(t)
    await db_session.flush()

    hydration_calls: list[int] = []

    async def hydrate_with_tracks(
        db: AsyncSession, settings: Settings, alb: CatalogAlbum
    ) -> CatalogAlbum:
        hydration_calls.append(alb.id)
        assert alb.artist.name == "Test Artist"
        for i in range(1, 5):
            db.add(
                CatalogAlbumTrack(
                    album_id=alb.id,
                    position=i,
                    disc=1,
                    title=f"Track {i:02d}",
                )
            )
        alb.track_count = 4
        await db.flush()
        await db.refresh(alb, ["tracks"])
        return alb

    monkeypatch.setattr(catalog_metadata, "fetch_and_store_album", hydrate_with_tracks)

    import_calls: list[int] = []

    async def fake_auto_import(
        db: AsyncSession,
        rel: Release,
        *,
        library_root: Path,
        naming_template: str,
    ) -> bool:
        import_calls.append(rel.id)
        return True

    from app.services import acquisition_recovery

    monkeypatch.setattr(acquisition_recovery, "try_auto_import_release", fake_auto_import)
    await db_session.commit()
    db_session.expunge_all()

    result = await recover_approved_downloads(db_session, test_settings)

    assert len(hydration_calls) == 1, "fetch_and_store_album must be called once for hydration"
    assert result >= 1, "At least one release should have been recovered"


async def test_legacy_recovery_skips_hydration_when_tracks_already_present(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 4 (idempotency): if tracks are already persisted, no hydration call is made."""
    artist, album = await _persist_artist_album(db_session, track_count=2, add_tracks=True)
    job_row = Job(
        source="slskd",
        query="Test Artist Test Album",
        status=JobStatus.done,
        catalog_album_id=album.id,
    )
    db_session.add(job_row)
    await db_session.flush()

    release = Release(
        job_id=job_row.id,
        source="slskd",
        title="Test Album",
        album_artist="Test Artist",
        year="2024",
        track_count=2,
    )
    db_session.add(release)
    await db_session.flush()

    staging = test_settings.staging_root
    staging.mkdir(parents=True, exist_ok=True)
    for i, cat_track in enumerate(album.tracks, start=1):
        audio = staging / f"{i:02d} Track {i:02d}.flac"
        audio.write_bytes(b"dummy")
        t = Track(
            job_id=job_row.id,
            release_id=release.id,
            source="slskd",
            title=cat_track.title,
            artist="Test Artist",
            album="Test Album",
            source_path=str(audio),
            staging_path=str(audio),
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.discovered,
            fingerprint_state=FingerprintState.done,
            identity_state=IdentityResolutionState.resolved,
            acoustid_verification_state=AcoustIDVerificationState.verified,
            catalog_track_id=cat_track.id,
        )
        db_session.add(t)
    await db_session.flush()

    hydration_calls: list[int] = []

    async def spy_hydrate(db: AsyncSession, settings: Settings, alb: CatalogAlbum) -> CatalogAlbum:
        hydration_calls.append(alb.id)
        return alb

    monkeypatch.setattr(catalog_metadata, "fetch_and_store_album", spy_hydrate)

    async def fake_auto_import(
        db: AsyncSession,
        rel: Release,
        *,
        library_root: Path,
        naming_template: str,
    ) -> bool:
        return True

    from app.services import acquisition_recovery

    monkeypatch.setattr(acquisition_recovery, "try_auto_import_release", fake_auto_import)

    await recover_approved_downloads(db_session, test_settings)

    assert len(hydration_calls) == 0, "Should not call fetch_and_store_album when tracks exist"


# ---------------------------------------------------------------------------
# Bug 5 – SQLite write lock released before long provider search/polling
# ---------------------------------------------------------------------------


async def test_runner_commits_running_state_before_provider_search(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 5: With commit_progress=True (background path), the running-state flush must
    be committed before _fetch_results is called so no write transaction is open during
    long provider HTTP/search/polling operations."""
    artist, album = await _persist_artist_album(db_session, track_count=4, add_tracks=True)
    job = await _make_album_job(db_session, album)
    await db_session.flush()

    call_order: list[str] = []

    original_commit = db_session.commit

    async def tracking_commit() -> None:
        call_order.append("commit")
        await original_commit()

    # Replace commit on the session instance so we can observe the call order
    db_session.commit = tracking_commit  # type: ignore[method-assign]

    from app.sources.youtube import ProviderError

    async def spy_fetch(job: Job, cfg: Settings, db: AsyncSession) -> list[SearchResult]:
        call_order.append("fetch")
        raise ProviderError("sources_exhausted", "no providers", "search", True)

    monkeypatch.setattr(runner, "_fetch_results", spy_fetch)
    _noop_noops(monkeypatch)

    await runner._run_job_in_session(job.id, db_session, test_settings, commit_progress=True)

    assert "commit" in call_order, "commit must have been called"
    assert "fetch" in call_order, "fetch must have been called"

    first_commit = next(i for i, x in enumerate(call_order) if x == "commit")
    first_fetch = next(i for i, x in enumerate(call_order) if x == "fetch")

    assert first_commit < first_fetch, (
        f"commit must precede fetch to release write lock; got order: {call_order}"
    )


# ---------------------------------------------------------------------------
# Bug 1 – Direct and bulk dispatch hydrate before creating jobs
# ---------------------------------------------------------------------------


async def test_dispatch_hydrates_catalog_tracks_before_creating_job(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 1 (direct dispatch): download_catalog_album must call fetch_and_store_album
    and persist tracks before dispatching the job, so the runner has catalog tracks available."""
    from app.routers import catalog as catalog_router

    artist, album = await _persist_artist_album(db_session, track_count=16, add_tracks=False)
    await db_session.commit()

    hydration_calls: list[int] = []

    async def hydrate_with_tracks(
        db: AsyncSession, settings: Settings, alb: CatalogAlbum
    ) -> CatalogAlbum:
        hydration_calls.append(alb.id)
        for i in range(1, 17):
            db.add(
                CatalogAlbumTrack(
                    album_id=alb.id,
                    position=i,
                    disc=1,
                    title=f"Track {i:02d}",
                )
            )
        alb.track_count = 16
        await db.flush()
        await db.refresh(alb, ["tracks"])
        return alb

    monkeypatch.setattr(catalog_router, "fetch_and_store_album", hydrate_with_tracks)

    dispatched: list[int] = []

    async def fake_dispatch(job_id: int) -> None:
        dispatched.append(job_id)

    from app.jobs import dispatcher as disp_module

    monkeypatch.setattr(disp_module.job_dispatcher, "dispatch", fake_dispatch)

    # Import settings dep & invoke the handler directly
    from app.settings_service import build_effective_settings

    settings = await build_effective_settings(db_session, test_settings)
    response = await catalog_router.download_catalog_album(
        album.id, db_session, settings, object()
    )
    assert response.status_code == 303
    assert len(dispatched) == 1

    # After hydration and dispatch, the album must have tracks persisted
    fresh = await db_session.scalar(select(CatalogAlbum).where(CatalogAlbum.id == album.id))
    await db_session.refresh(fresh, ["tracks"])
    assert len(fresh.tracks) == 16, f"Expected 16 tracks, got {len(fresh.tracks)}"
    assert len(hydration_calls) == 1


async def test_dispatch_raises_when_hydration_fails_and_tracks_remain_empty(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 1 (direct dispatch): hydration failure must raise so caller can return 502."""
    from app.routers import catalog as catalog_router

    artist, album = await _persist_artist_album(db_session, track_count=16, add_tracks=False)

    async def failing_hydrate(
        db: AsyncSession, settings: Settings, alb: CatalogAlbum
    ) -> CatalogAlbum:
        raise RuntimeError("provider offline")

    monkeypatch.setattr(catalog_router, "fetch_and_store_album", failing_hydrate)

    from app.settings_service import build_effective_settings

    settings = await build_effective_settings(db_session, test_settings)

    with pytest.raises(RuntimeError):
        await catalog_router._ensure_catalog_tracks(db_session, settings, album)


async def test_bulk_dispatch_hydrates_catalog_tracks_before_creating_jobs(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug 1 (bulk dispatch): download_monitored_catalog_albums must hydrate albums
    with zero tracks before dispatching jobs."""
    from app.routers import catalog as catalog_router

    artist = CatalogArtist(name="Bulk Artist", deezer_id="bulk-art-1", monitored=True)
    alb1 = CatalogAlbum(
        title="Album One",
        year="2023",
        track_count=10,
        deezer_id="bulk-alb-1",
        monitored=True,
    )
    alb2 = CatalogAlbum(
        title="Album Two",
        year="2024",
        track_count=8,
        deezer_id="bulk-alb-2",
        monitored=True,
    )
    artist.albums.extend([alb1, alb2])
    db_session.add(artist)
    await db_session.flush()

    hydrated: list[int] = []

    async def hydrate(db: AsyncSession, settings: Settings, alb: CatalogAlbum) -> CatalogAlbum:
        hydrated.append(alb.id)
        tc = alb.track_count or 2
        for i in range(1, tc + 1):
            db.add(CatalogAlbumTrack(album_id=alb.id, position=i, disc=1, title=f"T{i}"))
        await db.flush()
        await db.refresh(alb, ["tracks"])
        return alb

    monkeypatch.setattr(catalog_router, "fetch_and_store_album", hydrate)

    from app.settings_service import build_effective_settings

    settings = await build_effective_settings(db_session, test_settings)

    # Call the helper for each album that needs hydration
    for alb in [alb1, alb2]:
        await catalog_router._ensure_catalog_tracks(db_session, settings, alb)

    assert len(hydrated) == 2, f"Both albums must be hydrated, got: {hydrated}"


async def test_bulk_route_commits_each_album_before_next_hydration(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bulk dispatch must release each hydration write before the next provider wait."""
    from app.routers import catalog as catalog_router
    from app.settings_service import build_effective_settings

    artist = CatalogArtist(
        name="Bulk Route Artist",
        deezer_id="bulk-route-artist",
        monitored=True,
        watchlist_provider="deezer",
    )
    identity = CatalogArtistIdentity(
        provider="deezer",
        provider_artist_id="bulk-route-artist",
        name=artist.name,
    )
    artist.identities.append(identity)
    for index in range(1, 3):
        album = CatalogAlbum(
            title=f"Bulk Album {index}",
            deezer_id=f"bulk-route-album-{index}",
            track_count=10,
            monitored=True,
        )
        artist.albums.append(album)
        identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id=f"bulk-route-album-{index}",
                title=album.title,
                track_count=10,
                monitored=True,
                catalog_album=album,
            )
        )
    db_session.add(artist)
    await db_session.commit()

    call_order: list[str] = []

    async def fake_ensure(db: AsyncSession, settings: Settings, album: CatalogAlbum) -> None:
        call_order.append(f"hydrate:{album.id}")

    original_commit = db_session.commit

    async def tracking_commit() -> None:
        call_order.append("commit")
        await original_commit()

    async def fake_dispatch(job_id: int) -> None:
        return None

    monkeypatch.setattr(catalog_router, "_ensure_catalog_tracks", fake_ensure)
    monkeypatch.setattr(catalog_router.job_dispatcher, "dispatch", fake_dispatch)
    db_session.commit = tracking_commit  # type: ignore[method-assign]
    settings = await build_effective_settings(db_session, test_settings)

    response = await catalog_router.download_monitored_catalog_albums(
        artist.id, db_session, settings, object()
    )

    assert response.status_code == 303
    hydration_positions = [i for i, item in enumerate(call_order) if item.startswith("hydrate:")]
    assert len(hydration_positions) == 2
    assert "commit" in call_order[hydration_positions[0] + 1 : hydration_positions[1]]
