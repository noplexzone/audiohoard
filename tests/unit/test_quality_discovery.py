from __future__ import annotations

import json

from app.config import get_settings
from app.models.job import Job, JobStatus
from app.models.monitoring import MonitoringRecord
from app.models.release import Release
from app.schemas.search import SearchResult
from app.services.quality_discovery import build_upgrade_discovery


async def test_discovery_returns_candidates_for_higher_quality_source_result(
    db_session, monkeypatch
) -> None:
    job = Job(source="slskd", query="Artist Album", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    record = MonitoringRecord(release=release)
    db_session.add_all([job, release, record])
    await db_session.flush()

    async def fake_fetch(job_arg, cfg_arg, db_arg):
        assert job_arg.id == job.id
        return [
            SearchResult(
                source="slskd",
                title="Album",
                artist="Artist",
                album="Album",
                format="flac",
                url="slskd://peer/Album/01.flac",
                metadata={"bit_rate": 900_000, "sample_rate": 44100, "parse_confidence": 0.98},
            )
        ]

    monkeypatch.setattr("app.jobs.runner._call_fetch_results", fake_fetch)

    candidates = await build_upgrade_discovery(db_session, get_settings(), record)()

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.id is not None
    assert candidate.release_id == release.id
    assert candidate.track_id is None
    assert candidate.selected is True
    assert json.loads(candidate.quality_json or "{}") == {
        "bitrate_kbps": 900,
        "codec": "flac",
        "lossless": True,
        "reliability": 1.0,
        "sample_rate_hz": 44100,
    }


async def test_discovery_returns_empty_sequence_when_search_yields_nothing_better(
    db_session, monkeypatch
) -> None:
    job = Job(source="slskd", query="Artist Album", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    record = MonitoringRecord(release=release)
    db_session.add_all([job, release, record])
    await db_session.flush()

    async def fake_fetch(job_arg, cfg_arg, db_arg):
        return []

    monkeypatch.setattr("app.jobs.runner._call_fetch_results", fake_fetch)

    assert await build_upgrade_discovery(db_session, get_settings(), record)() == []
