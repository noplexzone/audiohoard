from __future__ import annotations

import json

from httpx import AsyncClient

from app.database import get_session_factory
from app.jobs.dispatcher import job_dispatcher
from app.models.job import Job, JobStatus


async def test_downloads_show_state_details_and_valid_actions(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        jobs = [
            Job(source="slskd", query="active", status=JobStatus.running),
            Job(
                source="prowlarr",
                query="broken",
                status=JobStatus.failed,
                result_json=json.dumps(
                    {"error": {"code": "client_failed", "detail": "Download failed"}}
                ),
            ),
            Job(source="priority", query="incomplete", status=JobStatus.partial),
        ]
        db.add_all(jobs)
        await db.commit()
        running_id, failed_id, partial_id = (job.id for job in jobs)

    response = await client.get("/downloads")

    assert response.status_code == 200
    assert "Download failed" in response.text
    assert "Active jobs refresh every 4 seconds" in response.text
    assert f'action="/downloads/{running_id}/cancel"' in response.text
    assert f'action="/downloads/{failed_id}/retry"' in response.text
    assert f'action="/downloads/{partial_id}/retry"' in response.text
    assert 'aria-live="polite"' in response.text


async def test_download_job_controls_redirect_with_feedback(
    client: AsyncClient, monkeypatch
) -> None:
    calls: list[tuple[str, int]] = []

    async def cancel(job_id: int) -> None:
        calls.append(("cancel", job_id))

    async def retry(job_id: int):
        calls.append(("retry", job_id))

    monkeypatch.setattr(job_dispatcher, "cancel_job", cancel)
    monkeypatch.setattr(job_dispatcher, "retry", retry)

    cancelled = await client.post("/downloads/41/cancel", follow_redirects=False)
    retried = await client.post("/downloads/42/retry", follow_redirects=False)

    assert cancelled.status_code == 303
    assert cancelled.headers["location"] == "/downloads?notice=cancelled"
    assert retried.status_code == 303
    assert retried.headers["location"] == "/downloads?notice=retried"
    assert calls == [("cancel", 41), ("retry", 42)]
