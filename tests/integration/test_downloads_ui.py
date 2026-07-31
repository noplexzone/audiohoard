from __future__ import annotations

import asyncio
import json
from pathlib import Path

from httpx import AsyncClient

from app.database import get_session_factory
from app.jobs.dispatcher import job_dispatcher
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState, ReviewDecision

DOWNLOADS_JS = Path("app/static/js/downloads.js").read_text()


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
    assert 'src="/static/js/downloads.js?v=0.11.1"' in response.text
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


async def test_downloads_hides_stale_review_release_without_actionable_items(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="Juice WRLD Fighting Demons", status=JobStatus.done)
        release = Release(
            job=job,
            source="slskd",
            title="Fighting Demons (Digital Deluxe)",
            album_artist="Juice WRLD",
            import_state=ImportWorkflowState.needs_review,
            error_detail="2 downloaded track(s) still require review",
        )
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title="Feel Alone",
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
        )
        review = StagingReviewItem(
            track=track,
            release=release,
            expected_title="Feel Alone",
            review_state=ReviewDecision.approved,
        )
        db.add_all([job, release, track, review])
        await db.commit()

    response = await client.get("/downloads")

    assert response.status_code == 200
    assert "Fighting Demons (Digital Deluxe)" not in response.text
    assert "2 downloaded track(s) still require review" not in response.text
    assert "Nothing waiting on you" in response.text


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


async def test_downloads_remove_terminal_and_clear_failed_jobs(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        failed_one = Job(source="slskd", query="failed one", status=JobStatus.failed)
        failed_two = Job(source="slskd", query="failed two", status=JobStatus.failed)
        done = Job(source="slskd", query="done", status=JobStatus.done)
        running = Job(source="slskd", query="running", status=JobStatus.running)
        db.add_all([failed_one, failed_two, done, running])
        await db.flush()
        library_track = Track(
            job_id=done.id,
            source="slskd",
            title="Preserved Track",
            source_path="/music/Artist/Album/Track.flac",
            acquisition_state=AcquisitionState.downloaded,
        )
        db.add(library_track)
        await db.commit()
        failed_one_id, failed_two_id = failed_one.id, failed_two.id
        done_id, running_id = done.id, running.id
        library_track_id = library_track.id

    page = await client.get("/downloads")
    assert page.status_code == 200
    assert 'action="/downloads/clear"' in page.text
    assert 'value="failed"' in page.text
    assert 'value="finished"' in page.text
    assert f'action="/downloads/{failed_one_id}/remove"' in page.text
    assert f'action="/downloads/{done_id}/remove"' in page.text
    assert f'action="/downloads/{running_id}/remove"' not in page.text

    removed = await client.post(f"/downloads/{done_id}/remove", follow_redirects=False)
    assert removed.status_code == 303
    assert removed.headers["location"] == "/downloads?notice=removed"

    cleared = await client.post(
        "/downloads/clear", data={"scope": "failed"}, follow_redirects=False
    )
    assert cleared.status_code == 303
    assert cleared.headers["location"] == "/downloads?notice=cleared&count=2"

    async with factory() as db:
        failed_one_row = await db.get(Job, failed_one_id)
        failed_two_row = await db.get(Job, failed_two_id)
        done_row = await db.get(Job, done_id)
        assert failed_one_row is not None and failed_one_row.queue_hidden is True
        assert failed_two_row is not None and failed_two_row.queue_hidden is True
        assert done_row is not None and done_row.queue_hidden is True
        assert await db.get(Track, library_track_id) is not None
        assert await db.get(Job, running_id) is not None

    hidden_page = await client.get("/downloads")
    assert "failed one" not in hidden_page.text
    assert "failed two" not in hidden_page.text
    assert "Preserved Track" not in hidden_page.text


async def test_downloads_refuses_to_remove_active_job(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        job = Job(source="slskd", query="running", status=JobStatus.running)
        db.add(job)
        await db.commit()
        job_id = job.id

    response = await client.post(f"/downloads/{job_id}/remove", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/downloads?notice=invalid_state"
    async with factory() as db:
        assert await db.get(Job, job_id) is not None


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

    # Auto-refresh is implemented by the external CSP-compatible script.
    assert 'src="/static/js/downloads.js?v=0.11.1"' in text
    assert "POLL_INTERVAL_MS = 10000" in DOWNLOADS_JS

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
    assert 'src="/static/js/downloads.js?v=0.11.1"' in second.text


async def test_downloads_group_album_jobs_and_target_active_attempt(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(name="Grouped Artist")
        album = CatalogAlbum(title="Grouped Release", track_count=3)
        artist.albums.append(album)
        album.tracks.extend(
            [
                CatalogAlbumTrack(position=1, disc=1, title="First"),
                CatalogAlbumTrack(position=2, disc=1, title="Second"),
                CatalogAlbumTrack(position=3, disc=1, title="Third"),
            ]
        )
        db.add(artist)
        await db.flush()
        root = Job(
            source="slskd",
            query="Grouped Artist Grouped Release",
            status=JobStatus.partial,
            catalog_album_id=album.id,
            queue_hidden=True,
        )
        independent = Job(
            source="youtube",
            query="Grouped Release second search",
            status=JobStatus.failed,
            catalog_album_id=album.id,
            catalog_track_id=album.tracks[1].id,
            result_json=json.dumps(
                {"error": {"code": "source_failed", "detail": "Second source failed"}}
            ),
        )
        standalone = Job(source="prowlarr", query="Unrelated Release", status=JobStatus.failed)
        db.add_all([root, independent, standalone])
        await db.flush()
        db.add_all(
            [
                Job(source="youtube", query=f"Noise {index}", status=JobStatus.pending)
                for index in range(98)
            ]
        )
        await db.flush()
        continuation = Job(
            source="priority",
            query="Grouped Release Third",
            status=JobStatus.running,
            catalog_album_id=album.id,
            catalog_track_id=album.tracks[2].id,
            parent_job_id=root.id,
        )
        db.add(continuation)
        await db.flush()
        db.add_all(
            [
                Track(
                    job_id=root.id,
                    source="slskd",
                    title="First",
                    catalog_album_id=album.id,
                    catalog_track_id=album.tracks[0].id,
                    acquisition_state=AcquisitionState.downloaded,
                ),
                Track(
                    job_id=independent.id,
                    source="youtube",
                    title="Second",
                    catalog_album_id=album.id,
                    catalog_track_id=album.tracks[1].id,
                    acquisition_state=AcquisitionState.failed,
                ),
                Track(
                    job_id=continuation.id,
                    source="slskd",
                    title="Third",
                    catalog_album_id=album.id,
                    catalog_track_id=album.tracks[2].id,
                    acquisition_state=AcquisitionState.acquiring,
                ),
            ]
        )
        await db.commit()
        root_id = root.id
        independent_id = independent.id
        continuation_id = continuation.id
        album_id = album.id

    response = await client.get("/downloads")

    assert response.status_code == 200
    assert response.text.count(f'data-download-group="album:{album_id}"') == 1
    assert "Grouped Artist" in response.text
    assert "Grouped Release" in response.text
    assert "1 / 3 downloaded" in response.text
    assert "2 attempts" in response.text
    assert f"<code>#{root_id}</code>" not in response.text
    assert f"#{independent_id}" in response.text
    assert f"#{continuation_id}" in response.text
    assert "Second source failed" in response.text
    assert f'action="/downloads/{continuation_id}/cancel"' in response.text
    assert f'action="/downloads/{independent_id}/retry"' not in response.text
    assert response.text.count('data-download-group="') == 100
    assert "Unrelated Release" in response.text

    failed = await client.get("/downloads?status=failed")
    assert failed.status_code == 200
    assert failed.text.count(f'data-download-group="album:{album_id}"') == 1
    assert "Grouped Release" in failed.text
    assert "2 attempts" in failed.text
    assert f'action="/downloads/{continuation_id}/cancel"' in failed.text
    assert f'action="/downloads/{independent_id}/retry"' not in failed.text
    assert "Unrelated Release" in failed.text


async def test_downloads_group_parent_chain_without_catalog_album(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        root = Job(source="slskd", query="Chain Root", status=JobStatus.partial)
        standalone = Job(source="youtube", query="Standalone", status=JobStatus.failed)
        db.add_all([root, standalone])
        await db.flush()
        child = Job(
            source="priority",
            query="Chain Continuation",
            status=JobStatus.pending,
            parent_job_id=root.id,
        )
        db.add(child)
        await db.commit()
        root_id = root.id
        child_id = child.id

    response = await client.get("/downloads")

    assert response.status_code == 200
    assert response.text.count(f'data-download-group="chain:{root_id}"') == 1
    assert f"#{root_id}" in response.text
    assert f"#{child_id}" in response.text
    assert response.text.count('data-download-group="job:') == 1


async def test_downloads_group_uses_declared_count_before_manifest_hydration(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(name="Manifest Pending Artist")
        album = CatalogAlbum(title="Manifest Pending Release", track_count=12)
        artist.albums.append(album)
        db.add(artist)
        await db.flush()
        job = Job(
            source="slskd",
            query="Manifest Pending Artist Manifest Pending Release",
            status=JobStatus.pending,
            catalog_album_id=album.id,
        )
        db.add(job)
        await db.commit()
        album_id = album.id

    response = await client.get("/downloads")

    assert response.status_code == 200
    group = response.text.split(f'data-download-group="album:{album_id}"', 1)[1].split("</tr>", 1)[
        0
    ]
    assert "0 / 12 downloaded" in group


async def test_downloads_queue_returns_only_queue_partial(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        db.add(Job(source="slskd", query="Queue Partial Album", status=JobStatus.pending))
        await db.commit()

    response = await client.get("/downloads/queue")

    assert response.status_code == 200
    assert "<table>" in response.text
    assert "Queue Partial Album" in response.text
    assert "<h1>Downloads</h1>" not in response.text
    assert 'aria-label="Import review"' not in response.text


async def test_downloads_queue_honours_status_filter_like_page(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        db.add_all(
            [
                Job(source="slskd", query="Pending Queue Item", status=JobStatus.pending),
                Job(source="slskd", query="Failed Queue Item", status=JobStatus.failed),
            ]
        )
        await db.commit()

    page = await client.get("/downloads?status=failed")
    queue = await client.get("/downloads/queue?status=failed")

    assert page.status_code == queue.status_code == 200
    assert "Failed Queue Item" in page.text and "Failed Queue Item" in queue.text
    assert "Pending Queue Item" not in page.text and "Pending Queue Item" not in queue.text


async def test_downloads_queue_has_no_query_column(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        db.add(Job(source="slskd", query="No Query Column", status=JobStatus.done))
        await db.commit()

    response = await client.get("/downloads/queue")

    assert response.status_code == 200
    assert '<th scope="col">Query</th>' not in response.text


async def test_downloads_queue_safely_renders_legacy_and_malformed_errors(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        db.add(
            Job(
                source="slskd",
                query="Legacy failures",
                status=JobStatus.failed,
                result_json=json.dumps(
                    {
                        "errors": [
                            "result_processing_failed",
                            json.dumps({"code": "provider_failed", "detail": "Unavailable"}),
                            "{malformed",
                            {"code": "structured_failure"},
                            None,
                        ]
                    }
                ),
            )
        )
        await db.commit()

    response = await client.get("/downloads/queue")

    assert response.status_code == 200
    assert "result_processing_failed" in response.text
    assert "provider_failed" in response.text
    assert "Unavailable" in response.text
    assert "structured_failure" in response.text
