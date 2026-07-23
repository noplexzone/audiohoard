from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.jobs import runner
from app.models.job import Job, JobStatus
from app.models.track import IdentityResolutionState, Track
from app.models.workflow import AcquisitionState
from app.naming.convention import NamingError
from app.schemas.search import SearchResult
from app.sources.base import CapabilityState
from app.sources.youtube import ProviderError


async def _create_job(db_session: AsyncSession, source: str = "youtube") -> Job:
    job = Job(source=source, query="test query", status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()
    return job


async def test_run_job_marks_failed_when_result_processing_fails(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    job = await _create_job(db_session)

    async def fake_fetch_results(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        return [
            SearchResult(source="youtube", title="Song", artist="Artist", url="/tmp/song.flac")
        ]

    async def fail_musicbrainz(track: Track, cfg: Settings) -> None:
        raise RuntimeError("metadata boom")

    async def noop_acquisition(
        result: SearchResult, source: str, cfg: Settings, track: Track | None = None
    ) -> tuple[None, None]:
        return None, None

    mp.setattr(runner, "_fetch_results", fake_fetch_results)
    mp.setattr(runner, "_prepare_acquisition", noop_acquisition)
    mp.setattr(runner, "_enrich_musicbrainz", fail_musicbrainz)

    await runner.run_job(job.id, db_session, test_settings)

    assert job.status == JobStatus.failed
    assert job.result_json is not None
    assert "result_processing_failed" in job.result_json


async def test_provider_error_persists_typed_failure_without_secret(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object, caplog: object
) -> None:
    from pytest import LogCaptureFixture, MonkeyPatch

    assert isinstance(monkeypatch, MonkeyPatch)
    assert isinstance(caplog, LogCaptureFixture)
    job = await _create_job(db_session)

    async def fail(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        raise ProviderError("timeout", "secret URL https://x/?token=bad", "search", True)

    monkeypatch.setattr(runner, "_fetch_results", fail)
    await runner.run_job(job.id, db_session, test_settings)
    assert job.status == JobStatus.failed
    assert (
        job.result_json
        == '{"error": {"code": "timeout", "operation": "search", "retryable": true}}'
    )
    assert "secret" not in caplog.text


async def test_cancellation_persists_job_and_track_state(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    assert isinstance(monkeypatch, MonkeyPatch)
    job = await _create_job(db_session)
    track = Track(job_id=job.id, source="youtube", acquisition_state=AcquisitionState.acquiring)
    db_session.add(track)

    async def cancel(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        raise asyncio.CancelledError

    monkeypatch.setattr(runner, "_fetch_results", cancel)
    with pytest.raises(asyncio.CancelledError):
        await runner.run_job(job.id, db_session, test_settings)
    assert job.status == JobStatus.cancelled
    assert track.acquisition_state == AcquisitionState.cancelled


async def test_prowlarr_result_is_enqueued_to_sabnzbd(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object, tmp_path: object
) -> None:
    from pathlib import Path

    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    assert isinstance(tmp_path, Path)
    job = await _create_job(db_session, source="prowlarr")
    staging = tmp_path / "staging"
    staging.mkdir()
    audio_file = staging / "track.flac"
    audio_file.write_bytes(b"audio")
    test_settings = test_settings.model_copy(
        update={"prowlarr_url": "https://prowlarr.test", "staging_root": staging}
    )
    calls: list[str] = []

    async def fake_fetch_results(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        return [
            SearchResult(
                source="prowlarr",
                title="Artist - Album",
                url="https://prowlarr.test/download/file.nzb",
                format="nzb",
            )
        ]

    async def noop(track: Track, cfg: Settings) -> None:
        return None

    async def noop_preview(track: Track, db: AsyncSession, cfg: Settings) -> None:
        return None

    class FakeSabnzbdAdapter:
        def __init__(self, base_url: str, api_key: str) -> None:
            calls.append(f"configured:{base_url}:{api_key}")

        async def enqueue(self, nzb_url: str, name: str | None = None) -> str:
            calls.append(f"enqueue:{nzb_url}:{name}")
            return "SAB123"

    async def fake_poll_sab_job(
        nzo_id: str,
        adapter: object,
        staging_root: Path,
        poll_interval: float,
        poll_timeout: float,
    ) -> Path:
        calls.append(f"poll:{nzo_id}")
        return audio_file

    mp.setattr(runner, "_fetch_results", fake_fetch_results)
    mp.setattr(runner, "_enrich_musicbrainz", noop)
    mp.setattr(runner, "_enrich_deezer", noop)
    mp.setattr(runner, "_run_fingerprint", noop)
    mp.setattr(runner, "_compute_path_preview", noop_preview)
    mp.setattr(runner, "SabnzbdAdapter", FakeSabnzbdAdapter)
    mp.setattr(runner, "_poll_sab_job", fake_poll_sab_job)

    await runner.run_job(job.id, db_session, test_settings)

    track = (await db_session.execute(select(Track))).scalar_one()
    assert job.status == JobStatus.done
    assert track.source_job_id == "SAB123"
    assert track.source_status == "downloaded"
    assert "enqueue:https://prowlarr.test/download/file.nzb:Artist - Album" in calls
    assert "poll:SAB123" in calls


async def test_path_preview_naming_error_marks_job_failed(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    job = await _create_job(db_session)

    async def fake_fetch_results(job: Job, cfg: Settings) -> Sequence[SearchResult]:
        return [
            SearchResult(source="youtube", title="Song", artist="Artist", url="/tmp/song.flac")
        ]

    async def noop(track: Track, cfg: Settings) -> None:
        return None

    def fail_render_path(*args: object, **kwargs: object) -> str:
        raise NamingError("bad naming")

    async def noop_acquisition(
        result: SearchResult, source: str, cfg: Settings, track: Track | None = None
    ) -> tuple[None, None]:
        return None, None

    mp.setattr(runner, "_fetch_results", fake_fetch_results)
    mp.setattr(runner, "_prepare_acquisition", noop_acquisition)
    mp.setattr(runner, "_enrich_musicbrainz", noop)
    mp.setattr(runner, "_enrich_deezer", noop)
    mp.setattr(runner, "_run_fingerprint", noop)
    mp.setattr(runner, "render_path", fail_render_path)

    await runner.run_job(job.id, db_session, test_settings)

    assert job.status == JobStatus.failed
    assert job.result_json is not None
    assert "result_processing_failed" in job.result_json


async def test_prowlarr_rejects_non_nzb_and_loopback_urls(
    test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    test_settings = test_settings.model_copy(update={"prowlarr_url": "https://prowlarr.test"})
    calls: list[str] = []

    class FakeSabnzbdAdapter:
        def __init__(self, base_url: str, api_key: str) -> None:
            pass

        async def enqueue(self, nzb_url: str, name: str | None = None) -> str:
            calls.append(nzb_url)
            return "SAB123"

        async def status(self, nzo_id: str) -> CapabilityState:
            return CapabilityState(available=True, reason="Downloading")

    mp.setattr(runner, "SabnzbdAdapter", FakeSabnzbdAdapter)
    invalid_results = [
        SearchResult(
            source="prowlarr",
            title="Magnet",
            url="magnet:?xt=urn:btih:abc",
            format="nzb",
        ),
        SearchResult(
            source="prowlarr",
            title="Localhost",
            url="http://127.0.0.1/file.nzb",
            format="nzb",
        ),
        SearchResult(
            source="prowlarr",
            title="Html",
            url="https://indexer.local/file.html",
            format="html",
        ),
        SearchResult(
            source="prowlarr",
            title="Untrusted DNS host",
            url="https://attacker.example/file.nzb",
            format="nzb",
        ),
    ]

    for result in invalid_results:
        try:
            await runner._prepare_acquisition(result, "prowlarr", test_settings)
        except RuntimeError as exc:
            assert "NZB" in str(exc) or "URL" in str(exc)
        else:
            raise AssertionError(f"accepted invalid result: {result.url}")

    assert calls == []


async def test_musicbrainz_empty_result_marks_track_unresolved(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    job = await _create_job(db_session)
    track = Track(job_id=job.id, title="No Match", source="youtube")
    db_session.add(track)
    await db_session.flush()

    class EmptyMusicBrainzClient:
        def __init__(self, user_agent: str) -> None:
            pass

        async def search_recording(
            self, title: str, artist: str | None = None, album: str | None = None
        ) -> list[object]:
            return []

    mp.setattr(runner, "MusicBrainzClient", EmptyMusicBrainzClient)

    await runner._enrich_musicbrainz(track, test_settings)

    assert track.mbid is None
    assert track.identity_state == IdentityResolutionState.unresolved


async def test_run_job_uses_database_backed_effective_settings(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = await _create_job(db_session)
    expected = Settings(secret_key="effective-secret", slskd_url="http://db-slskd")
    observed: list[Settings] = []

    async def _effective(db: AsyncSession, env: Settings) -> Settings:
        assert db is db_session
        return expected

    async def _run(job_id: int, db: AsyncSession, cfg: Settings) -> None:
        observed.append(cfg)

    monkeypatch.setattr(runner, "build_effective_settings", _effective)
    monkeypatch.setattr(runner, "_run_job_in_session", _run)
    await runner.run_job(job.id, db_session)
    assert observed == [expected]


async def test_priority_job_falls_back_when_first_source_empty(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    from app.settings_service import save_runtime_settings

    assert isinstance(monkeypatch, MonkeyPatch)
    await save_runtime_settings(
        db_session,
        [{"name": "slskd", "enabled": True}, {"name": "youtube", "enabled": True}],
        10,
        metadata_providers=[{"name": "musicbrainz", "enabled": True}],
        primary_metadata_provider="musicbrainz",
    )
    job = Job(source="priority", query="artist album", status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()
    calls: list[str] = []

    class FakeAdapter:
        def __init__(self, source: str) -> None:
            self.source = source

        async def health(self) -> CapabilityState:
            return CapabilityState(available=True)

        async def search(self, request: object) -> Sequence[SearchResult]:
            calls.append(self.source)
            if self.source == "slskd":
                return []
            return [SearchResult(source="youtube", title="Served", url="https://youtu.be/1")]

    monkeypatch.setattr(runner, "_source_adapter", lambda source, cfg: FakeAdapter(source))

    results = await runner._fetch_results(job, test_settings, db_session)

    assert calls == ["slskd", "youtube"]
    assert job.source == "youtube"
    assert results[0].source == "youtube"
    assert '"attempted_sources": ["slskd", "youtube"]' in (job.result_json or "")
    assert '"served_source": "youtube"' in (job.result_json or "")


async def test_priority_job_falls_back_when_first_source_unhealthy(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    from app.settings_service import save_runtime_settings

    assert isinstance(monkeypatch, MonkeyPatch)
    await save_runtime_settings(
        db_session,
        [{"name": "slskd", "enabled": True}, {"name": "youtube", "enabled": True}],
        10,
        metadata_providers=[{"name": "musicbrainz", "enabled": True}],
        primary_metadata_provider="musicbrainz",
    )
    job = Job(source="priority", query="artist album", status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()

    class FakeAdapter:
        def __init__(self, source: str) -> None:
            self.source = source

        async def health(self) -> CapabilityState:
            if self.source == "slskd":
                return CapabilityState(available=False, reason="offline")
            return CapabilityState(available=True)

        async def search(self, request: object) -> Sequence[SearchResult]:
            return [SearchResult(source=self.source, title="Served", url="https://youtu.be/1")]

    monkeypatch.setattr(runner, "_source_adapter", lambda source, cfg: FakeAdapter(source))

    results = await runner._fetch_results(job, test_settings, db_session)

    assert job.source == "youtube"
    assert results[0].source == "youtube"
    assert '"status": "unhealthy"' in (job.result_json or "")


async def test_priority_job_records_clear_failure_when_all_sources_exhausted(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    from app.settings_service import save_runtime_settings

    assert isinstance(monkeypatch, MonkeyPatch)
    await save_runtime_settings(
        db_session,
        [{"name": "slskd", "enabled": True}, {"name": "youtube", "enabled": True}],
        10,
        metadata_providers=[{"name": "musicbrainz", "enabled": True}],
        primary_metadata_provider="musicbrainz",
    )
    job = Job(source="priority", query="artist album", status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()

    class FakeAdapter:
        def __init__(self, source: str) -> None:
            self.source = source

        async def health(self) -> CapabilityState:
            return CapabilityState(available=True)

        async def search(self, request: object) -> Sequence[SearchResult]:
            if self.source == "slskd":
                return []
            raise ProviderError("timeout", "provider timed out", "search", True)

    monkeypatch.setattr(runner, "_source_adapter", lambda source, cfg: FakeAdapter(source))

    with pytest.raises(ProviderError, match="All configured download sources were exhausted"):
        await runner._fetch_results(job, test_settings, db_session)

    assert job.source == "priority"
    assert '"status": "empty"' in (job.result_json or "")
    assert '"code": "timeout"' in (job.result_json or "")
