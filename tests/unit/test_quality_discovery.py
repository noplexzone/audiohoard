from __future__ import annotations

import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.services.monitoring as monitoring_service
from app.config import get_settings
from app.database import Base
from app.models.job import Job, JobStatus
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release import Release
from app.schemas.search import SearchResult
from app.services.monitoring import run_monitoring_check
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


async def test_monitoring_check_commits_before_provider_discovery(db_session, monkeypatch) -> None:
    job = Job(source="slskd", query="Artist Album", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    record = MonitoringRecord(release=release)
    db_session.add_all([job, release, record])
    await db_session.flush()

    checkpoints: list[str] = []

    async def checkpoint() -> None:
        checkpoints.append("committed")
        await db_session.commit()

    async def fake_fetch(job_arg, cfg_arg, db_arg, *, checkpoint=None):
        assert checkpoints == ["committed"]
        assert checkpoint is not None
        return []

    monkeypatch.setattr("app.jobs.runner._call_fetch_results", fake_fetch)
    discover = build_upgrade_discovery(
        db_session,
        get_settings(),
        record,
        checkpoint=checkpoint,
    )

    await run_monitoring_check(
        db_session,
        record,
        {},
        discover,
        checkpoint=checkpoint,
    )

    assert checkpoints == ["committed", "committed"]


async def test_provider_discovery_allows_a_concurrent_sqlite_writer(tmp_path, monkeypatch) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'monitoring.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as setup:
            job = Job(source="slskd", query="Artist Album", status=JobStatus.done)
            release = Release(job=job, source="slskd", title="Album")
            record = MonitoringRecord(release=release)
            setup.add_all([job, release, record])
            await setup.commit()
            record_id = record.id

        async with factory() as scan:
            record = await scan.get(MonitoringRecord, record_id)
            assert record is not None

            async def fake_fetch(job_arg, cfg_arg, db_arg, *, checkpoint=None):
                async with factory() as writer:
                    writer.add(Job(source="slskd", query="concurrent UI write"))
                    await writer.commit()
                return []

            monkeypatch.setattr("app.jobs.runner._call_fetch_results", fake_fetch)
            discover = build_upgrade_discovery(
                scan, get_settings(), record, checkpoint=scan.commit
            )
            await run_monitoring_check(scan, record, {}, discover, checkpoint=scan.commit)
            await scan.commit()

        async with factory() as verify:
            concurrent = await verify.scalar(select(Job).where(Job.query == "concurrent UI write"))
            assert concurrent is not None
    finally:
        await engine.dispose()


async def test_provider_failure_persists_monitoring_terminal_state(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'failure.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as setup:
            job = Job(source="slskd", query="Artist Album", status=JobStatus.done)
            release = Release(job=job, source="slskd", title="Album")
            record = MonitoringRecord(release=release, status=MonitoringStatus.active)
            setup.add_all([job, release, record])
            await setup.commit()
            record_id = record.id

        async with factory() as scan:
            record = await scan.get(MonitoringRecord, record_id)
            assert record is not None

            async def failed_discovery():
                raise RuntimeError("provider failed")

            import pytest

            with pytest.raises(RuntimeError, match="provider failed"):
                await run_monitoring_check(
                    scan, record, {}, failed_discovery, checkpoint=scan.commit
                )

        async with factory() as verify:
            persisted = await verify.get(MonitoringRecord, record_id)
            assert persisted is not None
            assert persisted.status == MonitoringStatus.failed
    finally:
        await engine.dispose()


async def test_initial_checkpoint_failure_releases_in_process_monitoring_claim(db_session) -> None:
    job = Job(source="slskd", query="Artist Album", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album")
    record = MonitoringRecord(release=release, status=MonitoringStatus.active)
    db_session.add_all([job, release, record])
    await db_session.commit()
    record_id = record.id

    async def failed_checkpoint() -> None:
        raise RuntimeError("checkpoint failed")

    async def no_candidates():
        return []

    import pytest

    try:
        with pytest.raises(RuntimeError, match="checkpoint failed"):
            await run_monitoring_check(
                db_session, record, {}, no_candidates, checkpoint=failed_checkpoint
            )
        await db_session.rollback()
        record = await db_session.get(MonitoringRecord, record_id)
        assert record is not None
        await run_monitoring_check(db_session, record, {}, no_candidates)
    finally:
        monitoring_service._active_checks.discard(record_id)  # noqa: SLF001
