from __future__ import annotations

from collections.abc import AsyncGenerator
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.database import Base
from app.models.catalog_entities import CatalogAlbum, CatalogArtist
from app.models.job import Job, JobStatus
from app.services import artist_monitoring
from app.services.artist_monitoring import (
    DiscographyRefreshScheduler,
    apply_monitor_policy,
    refresh_monitored_artist,
)


@pytest_asyncio.fixture
async def monitoring_factory() -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def _create_artist(
    factory: async_sessionmaker[AsyncSession], name: str, *, album: bool = True
) -> int:
    async with factory() as db:
        artist = CatalogArtist(name=name, monitored=True, monitor_policy="all")
        if album:
            artist.albums.append(
                CatalogAlbum(title=f"{name} Album", monitored=True, in_library=False)
            )
        db.add(artist)
        await db.commit()
        await db.refresh(artist)
        return artist.id


async def test_auto_download_deduplicates_active_album_jobs(
    monitoring_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artist_id = await _create_artist(monitoring_factory, "Duplicate")

    async def no_refresh(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(artist_monitoring, "fetch_and_store_discography", no_refresh)
    settings = Settings(secret_key="test-secret")
    async with monitoring_factory() as db:
        result = await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(selectinload(CatalogArtist.albums))
        )
        artist = result.scalar_one()
        _, first_ids = await refresh_monitored_artist(db, settings, artist, auto_download=True)
        await db.commit()
    assert len(first_ids) == 1

    async with monitoring_factory() as db:
        result = await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(selectinload(CatalogArtist.albums))
        )
        artist = result.scalar_one()
        _, second_ids = await refresh_monitored_artist(db, settings, artist, auto_download=True)
        await db.commit()
        jobs = list((await db.scalars(select(Job))).all())
    assert second_ids == []
    assert len(jobs) == 1


async def test_scheduler_isolates_artist_failures_and_dispatches_after_commit(
    monitoring_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failing_id = await _create_artist(monitoring_factory, "Failing", album=False)
    successful_id = await _create_artist(monitoring_factory, "Successful", album=False)
    processed: list[int] = []
    events: list[str] = []
    visible_when_dispatched: list[bool] = []

    monkeypatch.setattr(artist_monitoring, "get_session_factory", lambda: monitoring_factory)
    monkeypatch.setattr(
        artist_monitoring,
        "build_effective_settings",
        AsyncMock(return_value=Settings(secret_key="test-secret")),
    )
    monkeypatch.setattr(
        artist_monitoring,
        "get_runtime_settings",
        AsyncMock(
            return_value=SimpleNamespace(
                auto_download_wanted=True,
                discography_refresh_hours=1,
            )
        ),
    )

    async def fake_refresh(
        db: AsyncSession,
        settings: Settings,
        artist: CatalogArtist,
        *,
        auto_download: bool = False,
    ) -> tuple[list[CatalogAlbum], list[int]]:
        assert auto_download is False
        if artist.id == failing_id:
            raise RuntimeError("provider failure")
        processed.append(artist.id)
        events.append("refresh")
        return [], []

    async def fake_reconcile(*args: object, **kwargs: object) -> int:
        events.append("reconcile")
        return 0

    async def fake_queue(db: AsyncSession, artist: CatalogArtist) -> list[int]:
        events.append("queue")
        job = Job(source="priority", query=artist.name, status=JobStatus.pending)
        db.add(job)
        await db.flush()
        return [job.id]

    async def probe_dispatch(job_id: int) -> None:
        events.append("dispatch")
        async with monitoring_factory() as db:
            visible_when_dispatched.append(await db.get(Job, job_id) is not None)

    monkeypatch.setattr(artist_monitoring, "refresh_monitored_artist", fake_refresh)
    monkeypatch.setattr(artist_monitoring, "reconcile_deezer_catalog_ownership", fake_reconcile)
    monkeypatch.setattr(artist_monitoring, "queue_wanted_artist_releases", fake_queue)
    monkeypatch.setattr(artist_monitoring.job_dispatcher, "dispatch", probe_dispatch)

    delay = await DiscographyRefreshScheduler()._refresh_cycle()

    assert processed == [successful_id]
    assert events == ["refresh", "reconcile", "queue", "dispatch"]
    assert visible_when_dispatched == [True]
    assert delay == 3600


async def test_scheduler_does_not_queue_when_reconciliation_fails(
    monitoring_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _create_artist(monitoring_factory, "Unsafe", album=False)
    queue = AsyncMock(return_value=[])
    dispatch = AsyncMock()

    monkeypatch.setattr(artist_monitoring, "get_session_factory", lambda: monitoring_factory)
    monkeypatch.setattr(
        artist_monitoring,
        "build_effective_settings",
        AsyncMock(return_value=Settings(secret_key="test-secret")),
    )
    monkeypatch.setattr(
        artist_monitoring,
        "get_runtime_settings",
        AsyncMock(
            return_value=SimpleNamespace(
                auto_download_wanted=True,
                discography_refresh_hours=1,
            )
        ),
    )
    monkeypatch.setattr(
        artist_monitoring,
        "refresh_monitored_artist",
        AsyncMock(return_value=([], [])),
    )
    monkeypatch.setattr(
        artist_monitoring,
        "reconcile_deezer_catalog_ownership",
        AsyncMock(side_effect=RuntimeError("provider unavailable")),
    )
    monkeypatch.setattr(artist_monitoring, "queue_wanted_artist_releases", queue)
    monkeypatch.setattr(artist_monitoring.job_dispatcher, "dispatch", dispatch)

    assert await DiscographyRefreshScheduler()._refresh_cycle() == 3600
    queue.assert_not_awaited()
    dispatch.assert_not_awaited()


async def test_scheduler_can_restart_after_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = DiscographyRefreshScheduler()

    async def wait_until_cancelled() -> None:
        await scheduler._stop.wait()

    monkeypatch.setattr(scheduler, "_run", wait_until_cancelled)
    await scheduler.start()
    await scheduler.stop()
    await scheduler.start()
    assert scheduler._task is not None
    assert not scheduler._stop.is_set()
    await scheduler.stop()


def test_albums_only_policy_excludes_case_and_format_variants() -> None:
    artist = CatalogArtist(
        name="Variants",
        monitored=True,
        monitor_policy="albums_only",
        watchlist_provider="itunes",
    )
    albums = [
        CatalogAlbum(title="Album", release_type="ALBUM", providers_json='["itunes"]'),
        CatalogAlbum(title="Single", release_type="single / ep", providers_json='["itunes"]'),
        CatalogAlbum(title="EP", release_type="E.P.", providers_json='["itunes"]'),
        CatalogAlbum(title="Other provider", release_type="Album", providers_json='["deezer"]'),
    ]

    apply_monitor_policy(artist, albums)

    assert [album.monitored for album in albums] == [True, False, False, False]


async def test_refresh_uses_watchlist_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    artist = CatalogArtist(
        name="Selected",
        monitored=True,
        watchlist_provider="deezer",
        albums=[],
    )
    observed: list[str | None] = []

    async def fake_fetch(db, settings, artist, provider_name=None):
        observed.append(provider_name)
        return []

    monkeypatch.setattr(artist_monitoring, "fetch_and_store_discography", fake_fetch)
    db = AsyncMock()
    db.scalars = AsyncMock(return_value=SimpleNamespace(first=lambda: None))
    db.refresh = AsyncMock()
    db.flush = AsyncMock()

    await refresh_monitored_artist(db, Settings(secret_key="test"), artist)

    assert observed == ["deezer"]
