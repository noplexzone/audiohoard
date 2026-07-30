from __future__ import annotations

import pytest
from httpx import AsyncClient

import app.database as db_module
from app.models.job import Job, JobStatus
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release import Release
from app.services.maintenance_state import (
    DuplicateAlbumSummary,
    DuplicateScanSummary,
    empty_maintenance_state,
)
from app.services.monitoring import QualityProfile, run_quality_upgrade_scan
from app.services.quality_upgrade import QualityDuplicateResult


def _app(client: AsyncClient):
    return client._transport.app  # type: ignore[attr-defined,no-any-return]


@pytest.mark.asyncio
async def test_maintenance_page_renders_before_scan(client: AsyncClient) -> None:
    response = await client.get("/maintenance")

    assert response.status_code == 200
    assert "Library scan" in response.text
    assert "No upgrade candidates" in response.text


@pytest.mark.asyncio
async def test_upgrade_scan_redirects_and_queues_without_running_inline(
    client: AsyncClient, monkeypatch
) -> None:
    calls = 0
    queued_tasks = []

    async def unexpected_scan(*args, **kwargs):
        nonlocal calls
        calls += 1

    def capture_task(self, func, *args, **kwargs):
        queued_tasks.append((func, args, kwargs))

    monkeypatch.setattr("app.routers.maintenance.run_quality_upgrade_scan", unexpected_scan)
    monkeypatch.setattr("app.routers.maintenance.BackgroundTasks.add_task", capture_task)

    response = await client.post("/maintenance/upgrades/scan", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/maintenance"
    assert calls == 0
    assert len(queued_tasks) == 1
    assert queued_tasks[0][0].__name__ == "_run_upgrade_scan"


@pytest.mark.asyncio
async def test_run_quality_upgrade_scan_checks_active_records_once(
    client: AsyncClient, monkeypatch
) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        job = Job(source="slskd", query="Artist Album", status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
        record = MonitoringRecord(
            release=release,
            status=MonitoringStatus.active,
            desired_quality_json=QualityProfile(preferred_codecs=("flac", "mp3")).to_json(),
        )
        session.add_all([job, release, record])
        await session.commit()
        record_id = record.id

    calls: list[int] = []

    async def fake_check(db, record, current_quality, discover):
        calls.append(record.id)

    monkeypatch.setattr("app.services.monitoring.run_monitoring_check", fake_check)
    async with factory() as session:
        checked = await run_quality_upgrade_scan(session)

    assert checked == 1
    assert calls == [record_id]


@pytest.mark.asyncio
async def test_duplicate_scan_redirects_and_uses_dry_run(client: AsyncClient, monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_candidate_album_ids(db):
        return [42]

    async def fake_reconcile(db, album_id, **kwargs):
        calls.append({"album_id": album_id, **kwargs})
        return QualityDuplicateResult(deleted_files=1, would_delete_paths=("/music/a.flac",))

    monkeypatch.setattr(
        "app.services.maintenance_workflows.duplicate_candidate_album_ids",
        fake_candidate_album_ids,
    )
    monkeypatch.setattr(
        "app.services.maintenance_workflows.reconcile_album_quality_duplicates", fake_reconcile
    )

    response = await client.post("/maintenance/duplicates/scan", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/maintenance"
    assert calls and calls[0]["dry_run"] is True


@pytest.mark.asyncio
async def test_duplicate_clean_skips_albums_with_review_required(
    client: AsyncClient, monkeypatch
) -> None:
    state = empty_maintenance_state()
    state.store_duplicate_scan(
        DuplicateScanSummary(
            albums=(
                DuplicateAlbumSummary(
                    album_id=1, result=QualityDuplicateResult(deleted_files=1, review_required=2)
                ),
                DuplicateAlbumSummary(
                    album_id=2, result=QualityDuplicateResult(deleted_files=1, review_required=0)
                ),
            )
        )
    )
    _app(client).state.maintenance_state = state
    calls: list[int] = []

    async def fake_reconcile(db, album_id, **kwargs):
        calls.append(album_id)
        assert kwargs["defer_filesystem_delete"] is True
        assert "dry_run" not in kwargs or kwargs["dry_run"] is False
        return QualityDuplicateResult(deleted_files=1)

    monkeypatch.setattr(
        "app.services.maintenance_workflows.reconcile_album_quality_duplicates", fake_reconcile
    )

    response = await client.post("/maintenance/duplicates/clean", follow_redirects=False)

    assert response.status_code == 303
    assert calls == [2]


@pytest.mark.asyncio
async def test_maintenance_page_requires_auth(unauthenticated_client: AsyncClient) -> None:
    response = await unauthenticated_client.get(
        "/maintenance", headers={"accept": "text/html"}, follow_redirects=False
    )

    assert response.status_code in {303, 307}
    assert response.headers["location"].startswith("/login")
