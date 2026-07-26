from __future__ import annotations

import pytest_asyncio
from httpx import AsyncClient

import app.database as db_module
from app.models.job import Job, JobStatus
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import AcquisitionState, ImportWorkflowState


@pytest_asyncio.fixture
async def dashboard_client(client: AsyncClient) -> AsyncClient:
    factory = db_module.get_session_factory()
    async with factory() as session:
        jobs = [
            Job(source="slskd", query="completed album", status=JobStatus.done),
            Job(source="youtube", query="active single", status=JobStatus.running),
            Job(source="prowlarr", query="failed release", status=JobStatus.failed),
            Job(source="priority", query="partial album", status=JobStatus.partial),
            Job(source="slskd", query="cancelled request", status=JobStatus.cancelled),
        ]
        session.add_all(jobs)
        await session.flush()
        session.add_all(
            [
                Track(
                    job_id=jobs[0].id,
                    title="Real Track One",
                    artist="Artist One",
                    album="Album One",
                    source="slskd",
                    acquisition_state=AcquisitionState.downloaded,
                    import_state=ImportWorkflowState.imported,
                    fingerprint_state=FingerprintState.done,
                    identity_state=IdentityResolutionState.resolved,
                    duration_sec=180,
                    file_size_bytes=10_000,
                ),
                Track(
                    job_id=jobs[1].id,
                    title="Real Track Two",
                    artist="Artist Two",
                    album="Album Two",
                    source="youtube",
                    acquisition_state=AcquisitionState.downloaded,
                    import_state=ImportWorkflowState.staged,
                    fingerprint_state=FingerprintState.pending,
                    identity_state=IdentityResolutionState.pending,
                    duration_sec=240,
                    file_size_bytes=20_000,
                ),
            ]
        )
        await session.commit()
    return client


async def test_dashboard_requires_setup_or_auth(unauthenticated_client: AsyncClient) -> None:
    response = await unauthenticated_client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/setup"


async def test_dashboard_shows_real_aggregates_and_activity(
    dashboard_client: AsyncClient,
) -> None:
    response = await dashboard_client.get("/")
    assert response.status_code == 200
    body = response.text
    assert 'data-stat="tracks">2<' in body
    assert 'data-stat="artists">2<' in body
    assert 'data-stat="albums">2<' in body
    assert 'data-job-status="done">1<' in body
    assert 'data-job-status="running">1<' in body
    assert 'data-job-status="failed">1<' in body
    assert 'data-job-status="partial">1<' in body
    assert 'href="/downloads?status=partial"' in body
    assert 'data-job-status="cancelled">1<' in body
    assert "Real Track One" in body
    assert "Real Track Two" in body
    assert "partial album" in body
    assert "cancelled request" in body


async def test_dashboard_empty_state_is_truthful(client: AsyncClient) -> None:
    response = await client.get("/")
    assert response.status_code == 200
    body = response.text
    assert 'data-stat="tracks">0<' in body
    assert "No tracks in your library yet" in body
    assert "No acquisition jobs yet" in body
    assert "Recent activity" in body


async def test_shared_shell_has_accessible_active_navigation(client: AsyncClient) -> None:
    response = await client.get("/library")
    assert response.status_code == 200
    body = response.text
    assert 'href="#main-content"' in body
    assert '<main id="main-content"' in body
    assert 'aria-label="Primary navigation"' in body
    assert 'href="/library"' in body
    assert 'aria-current="page"' in body
    assert 'aria-label="Mobile navigation"' in body
    assert "<span>Artists</span>" in body
    assert "<span>Imports</span>" in body
    assert 'action="/logout"' in body
    assert "Sign out" in body
    assert "Signed in as <strong>test-owner</strong>" in body
    assert "v0.6.1" in body
    assert "fonts.googleapis.com" not in body
    assert "fonts.gstatic.com" not in body


async def test_dashboard_provider_readiness_uses_local_configuration(
    client: AsyncClient,
) -> None:
    response = await client.get("/")
    body = response.text
    assert "Provider readiness" in body
    assert "slskd" in body
    assert "Prowlarr" in body
    assert "YouTube" in body
    assert "TIDAL" in body
    assert "Setup needed" in body


async def test_dashboard_uses_local_provider_checks_without_live_youtube_probe(
    client: AsyncClient, monkeypatch
) -> None:
    from app.sources.youtube import YouTubeAdapter

    async def live_probe_must_not_run(self):
        raise AssertionError("dashboard invoked live YouTube probe")

    monkeypatch.setattr(YouTubeAdapter, "health", live_probe_must_not_run)
    response = await client.get("/")
    assert response.status_code == 200
    assert "Provider readiness" in response.text


async def test_provider_readiness_failure_does_not_break_dashboard(
    client: AsyncClient, monkeypatch
) -> None:
    from app.sources.youtube import YouTubeAdapter

    async def failed_local_check(self):
        raise OSError("simulated local spawn failure")

    monkeypatch.setattr(YouTubeAdapter, "local_health", failed_local_check)
    response = await client.get("/")
    assert response.status_code == 200
    assert "YouTube local readiness check unavailable" in response.text
