from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
import urllib.request
from collections.abc import Generator
from pathlib import Path

import pytest
import uvicorn
from playwright.sync_api import Page
from sqlalchemy.ext.asyncio import create_async_engine

import app.models  # noqa: F401
from app.config import Settings, get_settings, override_settings
from app.database import Base, get_session_factory, reset_engine
from app.main import create_app
from app.metadata.base import (
    ArtistDetail,
    ArtistHit,
    DiscoveryGenre,
    DiscoveryRelease,
    DiscoverySection,
)
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.source_candidate_block import SourceCandidateBlock
from app.models.staging_review import StagingReviewItem
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState, ReviewDecision
from app.schemas.health import SourceStatus
from app.services.catalog_metadata import ProviderOutcome
from app.services.health_status import CachedProviderStatus, get_health_status_service
from app.sources.base import CapabilityState

_USERNAME = "browser-owner"
_PASSWORD = "Browser-Owner-Password-42"


def _unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run_async_in_thread(coro: object) -> None:
    error: list[BaseException] = []

    def run() -> None:
        try:
            asyncio.run(coro)  # type: ignore[arg-type]
        except BaseException as exc:
            error.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()
    if error:
        raise error[0]


async def _create_schema(database_url: str) -> None:
    engine = create_async_engine(database_url)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    await engine.dispose()


async def _seed_browser_data(staging_root: Path) -> None:
    async with get_session_factory()() as db:
        wanted_artist = CatalogArtist(name="Browser Wanted Artist", monitored=True)
        wanted_album = CatalogAlbum(
            title="Browser Context Album", year="2026", track_count=2, monitored=True
        )
        wanted_album.tracks.extend(
            [
                CatalogAlbumTrack(position=1, disc=1, title="Browser Track One", duration_sec=181),
                CatalogAlbumTrack(position=2, disc=1, title="Browser Track Two", duration_sec=182),
            ]
        )
        wanted_artist.albums.append(wanted_album)
        block = SourceCandidateBlock(
            provider="slskd",
            peer="browser-peer",
            filename="Browser Rejected/track.flac",
            reason="denied",
        )
        db.add_all([wanted_artist, block])

        for index in range(1, 5):
            staged = staging_root / "slskd" / f"review-{index}" / f"0{index} Browser.flac"
            staged.parent.mkdir(parents=True, exist_ok=True)
            staged.write_bytes(f"browser-audio-{index}".encode())
            artist = CatalogArtist(name=f"Browser Review Artist {index}")
            album = CatalogAlbum(
                artist=artist,
                title=f"Browser Review Album {index}",
                year="2026",
                track_count=1,
            )
            catalog_track = CatalogAlbumTrack(
                album=album,
                position=1,
                disc=1,
                title=f"Browser Review Track {index}",
                duration_sec=200,
            )
            job = Job(source="slskd", query=f"browser review {index}", status=JobStatus.done)
            release = Release(
                job=job,
                source="slskd",
                title=album.title,
                album_artist=artist.name,
                track_count=1,
                import_state=ImportWorkflowState.needs_review,
            )
            track = Track(
                job=job,
                release=release,
                catalog_album=album,
                catalog_track=catalog_track,
                source="slskd",
                title=catalog_track.title,
                artist=artist.name,
                album=album.title,
                album_artist=artist.name,
                track_no=1,
                disc=1,
                duration_sec=200,
                staging_path=str(staged),
                source_path=str(staged),
                acquisition_state=AcquisitionState.downloaded,
                acquisition_provenance_json=json.dumps(
                    {
                        "source": "slskd",
                        "username": f"review-peer-{index}",
                        "filename": f"review-{index}/track.flac",
                    }
                ),
            )
            review = StagingReviewItem(
                track=track,
                release=release,
                expected_title=catalog_track.title,
                review_state=ReviewDecision.pending,
            )
            db.add_all([artist, album, catalog_track, job, release, track, review])
        await db.commit()


@pytest.fixture(scope="module")
def browser_base_url(tmp_path_factory: pytest.TempPathFactory) -> Generator[str, None, None]:
    root = tmp_path_factory.mktemp("audiohoard-browser")
    database_url = f"sqlite+aiosqlite:///{root / 'browser.db'}"
    staging_root = root / "staging"
    library_root = root / "library"
    staging_root.mkdir()
    library_root.mkdir()
    original_settings = get_settings()
    settings = Settings(
        database_url=database_url,
        secret_key="browser-test-secret",
        auth_cookie_secure=False,
        library_root=library_root,
        staging_root=staging_root,
        slskd_url="",
        slskd_api_key="",
        prowlarr_url="",
        prowlarr_api_key="",
        sabnzbd_url="",
        sabnzbd_api_key="",
    )
    override_settings(settings)
    reset_engine(database_url)
    _run_async_in_thread(_create_schema(database_url))

    monkeypatch = pytest.MonkeyPatch()

    async def search_artists(*_args: object, **_kwargs: object) -> list[ProviderOutcome]:
        hit = ArtistHit(
            provider="deezer",
            provider_id="browser-artist-42",
            name="Browser Search Artist",
            deezer_id="browser-artist-42",
            album_count=1,
        )
        return [
            ProviderOutcome(
                provider="deezer",
                artists=[hit],
                state=CapabilityState(available=True),
            )
        ]

    discovery_artists = {
        "browser-popular-1": "Browser Namesake",
        "browser-popular-2": "Browser Namesake",
        "browser-long-artist": (
            "Browser Artist With An Exceptionally Long Name That Must Wrap Without Overflow"
        ),
        "browser-new-artist": "Browser New Release Artist",
        "browser-trending-artist": "Browser Trending Artist",
        "browser-genre-artist": "Browser Jazz Artist",
    }

    async def artist_detail(_settings: object, provider: str, provider_id: str) -> ArtistDetail:
        if provider != "deezer" or provider_id not in {
            "browser-artist-42",
            *discovery_artists,
        }:
            raise ValueError("unknown deterministic browser artist identity")
        return ArtistDetail(
            provider=provider,
            provider_id=provider_id,
            name=(
                "Browser Search Artist"
                if provider_id == "browser-artist-42"
                else discovery_artists[provider_id]
            ),
            deezer_id=provider_id,
        )

    async def healthy_provider(*_args: object, **_kwargs: object) -> CachedProviderStatus:
        return CachedProviderStatus(SourceStatus(available=True, details={}), elapsed_ms=7)

    async def no_reference(*_args: object, **_kwargs: object) -> None:
        return None

    async def no_dispatch(*_args: object, **_kwargs: object) -> None:
        return None

    async def deterministic_discovery(
        feed: str,
        region: str,
        *,
        page: int = 1,
        limit: int = 12,
        genre_id: str | None = None,
    ) -> DiscoverySection:
        del limit
        titles = {
            "popular": "Popular artists",
            "genres": "Genres",
            "new": "Fresh chart releases",
            "trending": "Trending releases",
            "genre": "Browser Jazz",
        }
        if feed == "genre":
            if genre_id != "132":
                raise ValueError("unknown deterministic browser genre identity")
            items = (
                ArtistHit(
                    "deezer",
                    "browser-genre-artist",
                    discovery_artists["browser-genre-artist"],
                    artwork_url="https://images.browser.invalid/jazz-artist.jpg",
                ),
            )
        elif feed == "popular":
            items = (
                ArtistHit(
                    "deezer",
                    "browser-popular-1",
                    discovery_artists["browser-popular-1"],
                    disambiguation="North America",
                    artwork_url="https://images.browser.invalid/namesake.jpg",
                ),
                ArtistHit(
                    "deezer",
                    "browser-popular-2",
                    discovery_artists["browser-popular-2"],
                    disambiguation="Europe",
                ),
                ArtistHit(
                    "deezer",
                    "browser-long-artist",
                    discovery_artists["browser-long-artist"],
                ),
            )
        elif feed == "genres":
            items = (
                DiscoveryGenre(
                    "deezer",
                    "132",
                    "Browser Jazz",
                    artwork_url="https://images.browser.invalid/jazz.jpg",
                ),
                DiscoveryGenre("deezer", "116", "Browser Rap"),
            )
        elif feed == "new":
            items = (
                DiscoveryRelease(
                    "deezer",
                    "browser-new-release",
                    "Browser New Release",
                    discovery_artists["browser-new-artist"],
                    "browser-new-artist",
                    artwork_url="https://images.browser.invalid/new.jpg",
                    release_date="2026-08-25",
                ),
            )
        elif feed == "trending":
            items = (
                DiscoveryRelease(
                    "deezer",
                    "browser-trending-release",
                    "Browser Trending Release",
                    discovery_artists["browser-trending-artist"],
                    "browser-trending-artist",
                ),
            )
        else:
            raise ValueError("unknown deterministic browser discovery feed")
        return DiscoverySection(
            feed,
            titles[feed],
            region,
            "GLOBAL",
            True,
            items,
            has_next=page == 1 and feed in {"popular", "genre"},
        )

    monkeypatch.setattr("app.routers.search.search_catalog_artists", search_artists)
    monkeypatch.setattr("app.routers.search.discovery_service.get", deterministic_discovery)
    monkeypatch.setattr("app.routers.catalog.fetch_catalog_artist_detail", artist_detail)
    monkeypatch.setattr("app.routers.catalog._start_discography_task", lambda *_args: False)
    monkeypatch.setattr("app.routers.catalog.job_dispatcher.dispatch", no_dispatch)
    monkeypatch.setattr("app.services.staging.resolve_reference_audio", no_reference)
    monkeypatch.setattr(get_health_status_service(), "refresh_provider", healthy_provider)

    app = create_app()
    port = _unused_port()
    base_url = f"http://127.0.0.1:{port}"
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="off")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.02)
    if not server.started:
        server.should_exit = True
        thread.join(timeout=2)
        raise RuntimeError("browser test server did not start")

    setup_request = urllib.request.Request(
        f"{base_url}/api/auth/setup",
        data=json.dumps({"username": _USERNAME, "password": _PASSWORD}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(setup_request, timeout=5) as response:
        if response.status != 201:
            raise RuntimeError(f"browser setup failed: HTTP {response.status}")
    _run_async_in_thread(_seed_browser_data(staging_root))
    reset_engine(database_url)

    try:
        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        monkeypatch.undo()
        override_settings(original_settings)
        reset_engine(original_settings.database_url)


@pytest.fixture
def authenticated_page(page: Page, browser_base_url: str) -> Page:
    response = page.request.post(
        f"{browser_base_url}/api/auth/login",
        data={"username": _USERNAME, "password": _PASSWORD},
    )
    assert response.ok, response.text()
    return page
