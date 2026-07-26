from __future__ import annotations

import base64
import json
from pathlib import Path

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.jobs import runner
from app.models.job import Job, JobStatus
from app.models.track import Track
from app.models.workflow import AcquisitionState
from app.schemas.search import SearchResult
from app.sources.base import CapabilityState
from app.sources.youtube import AcquiredMedia


async def test_tidal_health_search_job_and_settings_wiring(
    client: AsyncClient, tmp_path: Path, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    from app.sources.tidal import TidalAdapter

    assert isinstance(monkeypatch, MonkeyPatch)

    async def healthy(self: object) -> CapabilityState:
        return CapabilityState(
            True, extra={"code": "ok", "version": "2022.10.31.1", "auth": "unprobed"}
        )

    monkeypatch.setattr(TidalAdapter, "health", healthy)

    health = await client.get("/health/sources")
    assert health.status_code == 200 and "tidal" in health.json()
    found = await client.post(
        "/search", json={"query": "https://tidal.com/browse/track/123", "sources": ["tidal"]}
    )
    assert found.status_code == 200
    assert [item["source"] for item in found.json()["results"]] == ["tidal"]
    assert found.json()["source_states"]["tidal"]["available"] is True

    created = await client.post(
        "/jobs", json={"source": "tidal", "query": "https://tidal.com/browse/track/123"}
    )
    assert created.status_code == 201
    assert created.json()["source"] == "tidal" and created.json()["query"].endswith("/123")

    config = tmp_path / ".tidal-dl.json"
    config.write_text("{}")
    session = tmp_path / ".tidal-dl.token.json"
    session.write_bytes(base64.b64encode(b"{}"))
    tested = await client.post(
        "/api/settings/test",
        json={
            "provider": "tidal",
            "tidal_config_path": str(config),
            "tidal_session_path": str(session),
            "tidal_quality": "HiFi",
        },
    )
    assert tested.status_code == 200 and tested.json()["available"] is True

    settings_page = await client.get("/settings/download-clients")
    jobs_redirect = await client.get("/jobs/ui/list")
    assert jobs_redirect.status_code in {307, 308}
    jobs_page = await client.get(jobs_redirect.headers["location"])
    search_page = await client.get("/search")
    assert 'data-test-provider="tidal"' in settings_page.text
    assert ".tidal-dl.json" in settings_page.text and ".tidal-dl.token.json" in settings_page.text
    assert "direct TIDAL track URL" in settings_page.text
    assert '<option value="tidal">TIDAL' in jobs_page.text
    assert "TIDAL track URL" in search_page.text


async def test_tidal_runner_persists_downloaded_track_state(
    db_session: AsyncSession, test_settings: Settings, tmp_path: Path, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    from app.sources.tidal import TidalAdapter

    assert isinstance(monkeypatch, MonkeyPatch)
    url = "https://listen.tidal.com/track/456"
    job = Job(source="tidal", query=url, status=JobStatus.pending)
    db_session.add(job)
    await db_session.flush()
    artifact = tmp_path / "stage" / "tidal" / "456" / "song.flac"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"audio")

    async def search(self: object, request: object) -> list[SearchResult]:
        return [
            SearchResult(
                source="tidal", title="TIDAL track 456", url=url, metadata={"track_id": "456"}
            )
        ]

    async def acquire(self: object, result_url: str, staging_root: Path) -> AcquiredMedia:
        return AcquiredMedia(
            artifact,
            {"provider": "tidal", "track_id": "456", "quality": "HiFi", "extension": "flac"},
        )

    async def noop(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(TidalAdapter, "search", search)
    monkeypatch.setattr(TidalAdapter, "acquire", acquire)
    monkeypatch.setattr(runner, "_enrich_musicbrainz", noop)
    monkeypatch.setattr(runner, "_enrich_deezer", noop)
    monkeypatch.setattr(runner, "_run_fingerprint", noop)
    monkeypatch.setattr(runner, "_compute_path_preview", noop)
    cfg = test_settings.model_copy(
        update={
            "tidal_config_path": str(tmp_path / ".tidal-dl.json"),
            "tidal_session_path": str(tmp_path / ".tidal-dl.token.json"),
            "tidal_quality": "HiFi",
            "staging_root": tmp_path / "stage",
        }
    )

    await runner.run_job(job.id, db_session, cfg)

    track = (await db_session.execute(select(Track).where(Track.job_id == job.id))).scalar_one()
    assert job.status == JobStatus.done
    assert (
        track.source == "tidal"
        and track.source_path == str(artifact)
        and track.staging_path == str(artifact)
    )
    assert (
        track.acquisition_state == AcquisitionState.downloaded
        and track.source_status == "downloaded"
    )
    assert json.loads(track.acquisition_provenance_json or "{}")["track_id"] == "456"
