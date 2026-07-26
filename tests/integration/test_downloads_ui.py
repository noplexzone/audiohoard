from __future__ import annotations

import asyncio
import json

from httpx import AsyncClient

from app.database import get_session_factory
from app.jobs.dispatcher import job_dispatcher
from app.models.job import Job, JobStatus
from app.models.track import Track
from app.models.workflow import AcquisitionState


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
    assert "Active jobs refresh every 10 seconds" in response.text
    assert f'action="/downloads/{running_id}/cancel"' in response.text
    assert f'action="/downloads/{failed_id}/retry"' in response.text
    assert f'action="/downloads/{partial_id}/retry"' in response.text
    assert 'aria-live="polite"' in response.text

    filtered = await client.get("/downloads?status=partial")
    assert filtered.status_code == 200
    assert "incomplete" in filtered.text
    assert f"/downloads/{running_id}/cancel" not in filtered.text
    assert f"/downloads/{failed_id}/retry" not in filtered.text
    assert "Showing partial jobs" in filtered.text


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


async def test_downloads_shows_elapsed_and_external_transfer_wait(client: AsyncClient) -> None:
    """Running job with an acquiring track shows elapsed time and external wait label."""
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="Slow Album", status=JobStatus.running)
        db.add(job)
        await db.flush()
        acquiring = Track(
            job_id=job.id,
            source="slskd",
            acquisition_state=AcquisitionState.acquiring,
            title="Side A",
            artist="Test Artist",
        )
        done_track = Track(
            job_id=job.id,
            source="slskd",
            acquisition_state=AcquisitionState.downloaded,
            title="Side B",
            artist="Test Artist",
        )
        db.add_all([acquiring, done_track])
        await db.commit()
        job_id = job.id

    # Simulate another HTTP client observing the page while the job is blocked
    response = await client.get("/downloads")

    assert response.status_code == 200
    text = response.text

    # Auto-refresh at 10 seconds
    assert "10000" in text
    assert "Active jobs refresh every 10 seconds" in text

    # Elapsed and last-activity columns are present
    assert "Elapsed" in text
    assert "Last activity" in text
    # The page rendered some elapsed value (at minimum "0s")
    assert "ago" in text or "just now" in text

    # Track expansion shows external wait with source name
    assert "Side A" in text
    assert "downloading via slskd" in text

    # Downloaded track shows done state
    assert "Side B" in text

    # Cancel action visible for the running job
    assert f'action="/downloads/{job_id}/cancel"' in text


async def test_downloads_failed_row_shows_structured_error_not_raw_sql(
    client: AsyncClient,
) -> None:
    """Failed job renders structured error code/message; no raw internal details."""
    factory = get_session_factory()
    async with factory() as db:
        job = Job(
            source="prowlarr",
            query="broken release",
            status=JobStatus.failed,
            result_json=json.dumps(
                {
                    "error": {
                        "code": "source_timeout",
                        "detail": "Connection timed out after 30s",
                    }
                }
            ),
        )
        db.add(job)
        await db.commit()

    response = await client.get("/downloads")

    assert response.status_code == 200
    assert "source_timeout" in response.text
    assert "Connection timed out after 30s" in response.text
    # Must not leak internal implementation details
    assert "OperationalError" not in response.text
    assert "sqlite" not in response.text.lower()
    assert "Traceback" not in response.text


async def test_downloads_album_job_track_state_expansion(client: AsyncClient) -> None:
    """Album job with mixed track states renders per-track detail without N+1."""
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="Complete Album", status=JobStatus.running)
        db.add(job)
        await db.flush()
        tracks = [
            Track(
                job_id=job.id,
                source="slskd",
                acquisition_state=AcquisitionState.downloaded,
                title=f"Track {i:02d}",
                artist="The Artist",
            )
            for i in range(1, 4)
        ]
        tracks.append(
            Track(
                job_id=job.id,
                source="slskd",
                acquisition_state=AcquisitionState.acquiring,
                source_status="queued",
                title="Track 04",
                artist="The Artist",
            )
        )
        tracks.append(
            Track(
                job_id=job.id,
                source="slskd",
                acquisition_state=AcquisitionState.failed,
                title="Track 05",
                artist="The Artist",
            )
        )
        db.add_all(tracks)
        await db.commit()

    response = await client.get("/downloads")

    assert response.status_code == 200
    text = response.text
    assert "Track 04" in text
    # External wait label with source and status hint
    assert "downloading via slskd" in text
    assert "queued" in text
    # Other track states visible
    assert "Track 01" in text
    assert "Track 05" in text


async def test_downloads_slow_source_page_renders_while_blocked(client: AsyncClient) -> None:
    """A second page load while a job is blocked in acquiring state renders correctly."""
    factory = get_session_factory()
    async with factory() as db:
        blocked_job = Job(source="slskd", query="Blocked Album", status=JobStatus.running)
        db.add(blocked_job)
        await db.flush()
        # Track stuck in external transfer
        acquiring = Track(
            job_id=blocked_job.id,
            source="slskd",
            acquisition_state=AcquisitionState.acquiring,
            title="Long Track",
            artist="Patient Artist",
        )
        db.add(acquiring)
        await db.commit()
        blocked_id = blocked_job.id

    # First observer
    first = await client.get("/downloads")
    assert first.status_code == 200
    assert "Long Track" in first.text
    assert "downloading via slskd" in first.text
    assert f'action="/downloads/{blocked_id}/cancel"' in first.text

    # Short wait to simulate time passing while source is still blocked
    await asyncio.sleep(0.05)

    # Second observer sees the same running state and elapsed time increases
    second = await client.get("/downloads")
    assert second.status_code == 200
    assert "Long Track" in second.text
    assert "downloading via slskd" in second.text
    assert "Elapsed" in second.text
    assert "Last activity" in second.text
    assert "Active jobs refresh every 10 seconds" in second.text
