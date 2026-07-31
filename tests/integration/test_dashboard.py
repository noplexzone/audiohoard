from __future__ import annotations

from html.parser import HTMLParser

import pytest_asyncio
from httpx import AsyncClient

import app.database as db_module
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
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
        artist = CatalogArtist(name="Artist One")
        album = CatalogAlbum(
            artist=artist,
            title="Album One",
            track_count=2,
            in_library=True,
            artwork_url="https://example.test/cover.jpg",
        )
        first_catalog_track = CatalogAlbumTrack(
            album=album, position=1, disc=1, title="Real Track One"
        )
        second_catalog_track = CatalogAlbumTrack(
            album=album, position=2, disc=1, title="Real Track Three"
        )
        imported_release = Release(
            job=jobs[0], source="slskd", title="Album One", album_artist="Artist One"
        )
        imported_track = Track(
            job_id=jobs[0].id,
            release=imported_release,
            catalog_album=album,
            catalog_track=first_catalog_track,
            title="Real Track One",
            artist="Artist One",
            album="Album One",
            source="slskd",
            source_path="/staging/Real Track One.flac",
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
            fingerprint_state=FingerprintState.done,
            identity_state=IdentityResolutionState.resolved,
            duration_sec=180,
            file_format="flac",
            file_size_bytes=10_000,
        )
        imported_mp3_track = Track(
            job_id=jobs[0].id,
            release=imported_release,
            catalog_album=album,
            catalog_track=second_catalog_track,
            title="Real Track Three",
            artist="Artist One",
            album="Album One",
            source="slskd",
            source_path="/staging/Real Track Three.mp3",
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
            fingerprint_state=FingerprintState.done,
            identity_state=IdentityResolutionState.resolved,
            duration_sec=210,
            file_format="mp3 128kbps",
            file_size_bytes=11_000,
        )
        staged_track = Track(
            job_id=jobs[1].id,
            title="Real Track Two",
            artist="Artist Two",
            album="Album Two",
            source="youtube",
            source_path="/staging/Real Track Two.mp3",
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.staged,
            fingerprint_state=FingerprintState.pending,
            identity_state=IdentityResolutionState.pending,
            duration_sec=240,
            file_size_bytes=20_000,
        )
        session.add_all(
            [
                artist,
                album,
                first_catalog_track,
                second_catalog_track,
                imported_track,
                imported_mp3_track,
                staged_track,
            ]
        )
        session.add_all(
            [
                ImportPlan(
                    release=imported_release,
                    track=imported_track,
                    source_path="/staging/Real Track One.flac",
                    destination_path="/music/Artist One/Album One/Real Track One.flac",
                    status=ImportWorkflowState.imported,
                ),
                ImportPlan(
                    release=imported_release,
                    track=imported_mp3_track,
                    source_path="/staging/Real Track Three.mp3",
                    destination_path="/music/Artist One/Album One/Real Track Three.mp3",
                    status=ImportWorkflowState.imported,
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
    assert 'data-stat="artists">1<' in body
    assert 'data-stat="albums">1<' in body
    assert 'data-quality-tier="lossless"' in body
    assert 'data-quality-tier="lossy"' in body
    assert 'data-quality-upgrade-eligible="total">1<' in body
    assert 'data-job-status="done">1<' in body
    assert 'data-job-status="running">1<' in body
    assert 'data-job-status="failed">1<' in body
    assert 'data-job-status="partial">1<' in body
    assert 'href="/downloads?status=partial"' in body
    assert 'data-job-status="cancelled">1<' in body
    assert "Real Track One" in body
    assert "Real Track Three" in body
    assert "/artwork?url=https%3A//example.test/cover.jpg" in body
    assert "Real Track Two" not in body
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
    assert "Start quality upgrade scan" in body
    assert "Start duplicate cleanup scan" in body


async def test_dashboard_workflow_buttons_post_to_maintenance_scan_endpoints(
    client: AsyncClient, monkeypatch
) -> None:
    queued_tasks = []

    def capture_task(self, func, *args, **kwargs):
        queued_tasks.append((func, args, kwargs))

    monkeypatch.setattr("app.routers.maintenance.BackgroundTasks.add_task", capture_task)

    page = await client.get("/")
    assert page.status_code == 200
    assert 'action="/maintenance/upgrades/scan"' in page.text
    assert 'action="/maintenance/duplicates/scan"' in page.text

    upgrade = await client.post("/maintenance/upgrades/scan", follow_redirects=False)
    duplicate = await client.post("/maintenance/duplicates/scan", follow_redirects=False)

    assert upgrade.status_code == 303
    assert duplicate.status_code == 303
    assert [task[0].__name__ for task in queued_tasks] == [
        "_run_upgrade_scan",
        "_run_duplicate_scan",
    ]


class _NavAnchorCollector(HTMLParser):
    """Collects <a> elements found inside named <nav> elements."""

    def __init__(self) -> None:
        super().__init__()
        self._active: str | None = None
        self._depth: int = 0
        self.anchors: dict[str, list[dict[str, str | None]]] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        if tag == "nav":
            if self._active is None:
                label = attr.get("aria-label")
                if label in {"Primary navigation", "Mobile navigation"}:
                    self._active = label
                    self._depth = 1
                    self.anchors.setdefault(label, [])
            else:
                self._depth += 1
        elif tag == "a" and self._active is not None:
            self.anchors[self._active].append(attr)

    def handle_endtag(self, tag: str) -> None:
        if self._active is not None and tag == "nav":
            self._depth -= 1
            if self._depth == 0:
                self._active = None


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
    assert "<span>Artists</span>" not in body
    assert body.count("<span>Library</span>") == 2
    assert "<span>Imports</span>" not in body
    assert 'action="/logout"' in body
    assert "Sign out" in body
    assert "Signed in as <strong>test-owner</strong>" in body
    assert "v0.11.1" in body
    assert "fonts.googleapis.com" not in body
    assert "fonts.gstatic.com" not in body

    collector = _NavAnchorCollector()
    collector.feed(body)
    for nav_label in ("Primary navigation", "Mobile navigation"):
        assert nav_label in collector.anchors, f"{nav_label!r} nav not found in document"
        active_hrefs = [
            anchor.get("href")
            for anchor in collector.anchors[nav_label]
            if anchor.get("aria-current") == "page"
        ]
        assert active_hrefs == ["/library"], (
            f"{nav_label!r}: expected only /library to have aria-current='page', "
            f"found {active_hrefs}"
        )


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
