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
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.job import Job, JobStatus
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.models.release import Release
from app.services import artist_monitoring, upgrade_monitoring
from app.services.artist_monitoring import (
    DiscographyRefreshScheduler,
    apply_monitor_policy,
    queue_wanted_artist_releases,
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


@pytest.mark.parametrize("dispatch_jobs", [True, False])
async def test_scheduler_isolates_artist_failures_and_dispatches_after_commit(
    monitoring_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    dispatch_jobs: bool,
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
        assert kwargs["fail_on_provider_error"] is True
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

    delay = await DiscographyRefreshScheduler()._refresh_cycle(dispatch_jobs=dispatch_jobs)

    assert processed == [successful_id]
    expected = ["refresh", "reconcile", "queue"]
    if dispatch_jobs:
        expected.append("dispatch")
    assert events == expected
    assert visible_when_dispatched == ([True] if dispatch_jobs else [])
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

    async def wait_until_cancelled(**_kwargs) -> None:
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


async def _create_edition_artist(
    factory: async_sessionmaker[AsyncSession],
    *,
    manual_clean: bool = False,
    duplicate_explicit: bool = False,
) -> int:
    async with factory() as db:
        artist = CatalogArtist(
            name="Edition Artist",
            monitored=True,
            monitor_policy="all",
            watchlist_provider="deezer",
            watchlist_release_albums=True,
            watchlist_release_singles=False,
            watchlist_release_eps=False,
        )
        identity = CatalogArtistIdentity(
            artist=artist,
            provider="deezer",
            provider_artist_id="edition-dz",
            name=artist.name,
        )
        unknown_album = CatalogAlbum(
            artist=artist, title="Family Unknown", release_type="album", in_library=False
        )
        CatalogAlbumProvider(
            artist_identity=identity,
            catalog_album=unknown_album,
            provider_album_id="unknown",
            title="Family",
            year="2024",
            release_kind="album",
            content_rating="unknown",
            track_count=10,
            monitored=not manual_clean,
            monitor_override=False if manual_clean else None,
        )
        if manual_clean:
            clean_album = CatalogAlbum(
                artist=artist, title="Family Clean", release_type="album", in_library=False
            )
            CatalogAlbumProvider(
                artist_identity=identity,
                catalog_album=clean_album,
                provider_album_id="clean",
                title="Family",
                year="2024",
                release_kind="album",
                content_rating="clean",
                track_count=10,
                monitor_override=True,
            )
        if duplicate_explicit:
            for index, artwork in ((1, None), (2, "cover")):
                album = CatalogAlbum(
                    artist=artist,
                    title=f"Family Explicit {index}",
                    release_type="album",
                    in_library=False,
                )
                CatalogAlbumProvider(
                    artist_identity=identity,
                    catalog_album=album,
                    provider_album_id=f"explicit-{index}",
                    title="Family",
                    year="2024",
                    release_kind="album",
                    content_rating="explicit",
                    track_count=10,
                    artwork_url=artwork,
                )
        db.add(artist)
        await db.commit()
        return artist.id


async def _load_edition_artist(db: AsyncSession, artist_id: int) -> CatalogArtist:
    return (
        await db.execute(
            select(CatalogArtist)
            .where(CatalogArtist.id == artist_id)
            .options(
                selectinload(CatalogArtist.albums),
                selectinload(CatalogArtist.identities)
                .selectinload(CatalogArtistIdentity.releases)
                .selectinload(CatalogAlbumProvider.catalog_album),
            )
        )
    ).scalar_one()


async def test_refresh_applies_policy_to_complete_family_after_all_upserts(
    monitoring_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artist_id = await _create_edition_artist(monitoring_factory)

    async def add_explicit(db, settings, artist, provider_name=None):
        identity = (
            await db.scalars(
                select(CatalogArtistIdentity).where(
                    CatalogArtistIdentity.artist_id == artist.id,
                    CatalogArtistIdentity.provider == "deezer",
                )
            )
        ).one()
        album = CatalogAlbum(
            artist=artist, title="Family Explicit", release_type="album", in_library=False
        )
        db.add(
            CatalogAlbumProvider(
                artist_identity=identity,
                catalog_album=album,
                provider_album_id="explicit-new",
                title="Family",
                year="2024",
                release_kind="album",
                content_rating="explicit",
                track_count=10,
            )
        )
        await db.flush()
        return []

    monkeypatch.setattr(artist_monitoring, "fetch_and_store_discography", add_explicit)
    async with monitoring_factory() as db:
        artist = await _load_edition_artist(db, artist_id)
        await refresh_monitored_artist(db, Settings(secret_key="test"), artist)
        rows = list(
            (
                await db.scalars(select(CatalogAlbumProvider).order_by(CatalogAlbumProvider.id))
            ).all()
        )
    assert [(row.content_rating, row.monitored) for row in rows] == [
        ("unknown", False),
        ("explicit", True),
    ]


async def test_refresh_preserves_manual_clean_override(
    monitoring_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artist_id = await _create_edition_artist(monitoring_factory, manual_clean=True)
    monkeypatch.setattr(
        artist_monitoring, "fetch_and_store_discography", AsyncMock(return_value=[])
    )
    async with monitoring_factory() as db:
        artist = await _load_edition_artist(db, artist_id)
        await refresh_monitored_artist(db, Settings(secret_key="test"), artist)
        rows = list(
            (
                await db.scalars(select(CatalogAlbumProvider).order_by(CatalogAlbumProvider.id))
            ).all()
        )
    assert [(row.content_rating, row.monitored) for row in rows] == [
        ("unknown", False),
        ("clean", True),
    ]


async def test_refresh_never_double_monitors_same_rating_duplicates(
    monitoring_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artist_id = await _create_edition_artist(monitoring_factory, duplicate_explicit=True)
    monkeypatch.setattr(
        artist_monitoring, "fetch_and_store_discography", AsyncMock(return_value=[])
    )
    async with monitoring_factory() as db:
        artist = await _load_edition_artist(db, artist_id)
        await refresh_monitored_artist(db, Settings(secret_key="test"), artist)
        rows = list((await db.scalars(select(CatalogAlbumProvider))).all())
    assert sum(row.monitored for row in rows) == 1
    assert next(row for row in rows if row.monitored).provider_album_id == "explicit-2"


async def test_queue_supersedes_pending_unmonitored_sibling(
    monitoring_factory: async_sessionmaker[AsyncSession],
) -> None:
    artist_id = await _create_edition_artist(monitoring_factory, duplicate_explicit=True)
    async with monitoring_factory() as db:
        artist = await _load_edition_artist(db, artist_id)
        unknown = next(
            row
            for identity in artist.identities
            for row in identity.releases
            if row.content_rating == "unknown"
        )
        old_job = Job(
            source="priority",
            query="old edition",
            status=JobStatus.pending,
            catalog_album_id=unknown.catalog_album_id,
        )
        db.add(old_job)
        await db.flush()
        job_ids = await queue_wanted_artist_releases(db, artist)
        await db.flush()
        await db.refresh(old_job)
        new_job = await db.get(Job, job_ids[0])
    assert old_job.status == JobStatus.cancelled
    assert new_job is not None and new_job.catalog_album_id != unknown.catalog_album_id


async def test_queue_defers_preferred_edition_while_sibling_is_running(
    monitoring_factory: async_sessionmaker[AsyncSession],
) -> None:
    artist_id = await _create_edition_artist(monitoring_factory, duplicate_explicit=True)
    async with monitoring_factory() as db:
        artist = await _load_edition_artist(db, artist_id)
        unknown = next(
            row
            for identity in artist.identities
            for row in identity.releases
            if row.content_rating == "unknown"
        )
        db.add(
            Job(
                source="priority",
                query="running old edition",
                status=JobStatus.running,
                catalog_album_id=unknown.catalog_album_id,
            )
        )
        await db.flush()
        assert await queue_wanted_artist_releases(db, artist) == []


async def test_queue_respects_explicit_multi_rating_monitoring_choice(
    monitoring_factory: async_sessionmaker[AsyncSession],
) -> None:
    artist_id = await _create_edition_artist(monitoring_factory, manual_clean=True)
    async with monitoring_factory() as db:
        artist = await _load_edition_artist(db, artist_id)
        rows = [row for identity in artist.identities for row in identity.releases]
        for row in rows:
            row.monitor_override = True
        db.add(
            Job(
                source="priority",
                query="first chosen edition",
                status=JobStatus.pending,
                catalog_album_id=rows[0].catalog_album_id,
            )
        )
        await db.flush()
        job_ids = await queue_wanted_artist_releases(db, artist)
        jobs = list((await db.scalars(select(Job).order_by(Job.id))).all())
    assert len(job_ids) == 1
    assert [job.status for job in jobs] == [JobStatus.pending, JobStatus.pending]
    assert {job.catalog_album_id for job in jobs} == {row.catalog_album_id for row in rows}


async def test_upgrade_monitoring_uses_or_projection_for_shared_canonical_album(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artist = CatalogArtist(name="Upgrade OR", monitored=True, watchlist_monitor_upgrades=True)
    rows = [
        CatalogAlbumProvider(
            id=1,
            artist_identity_id=1,
            catalog_album_id=42,
            provider_album_id="monitored",
            title="Album",
            release_kind="album",
            monitored=True,
        ),
        CatalogAlbumProvider(
            id=2,
            artist_identity_id=1,
            catalog_album_id=42,
            provider_album_id="alternate",
            title="Album",
            release_kind="album",
            monitored=False,
        ),
    ]
    sync = AsyncMock()
    monkeypatch.setattr(upgrade_monitoring, "sync_album_upgrade_monitoring", sync)

    await upgrade_monitoring.sync_artist_upgrade_monitoring(AsyncMock(), artist, rows)

    sync.assert_awaited_once()
    assert sync.await_args.args[1:] == (42, True)


async def test_reenabling_upgrade_monitoring_preserves_history(
    monitoring_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with monitoring_factory() as db:
        job = Job(source="test", query="upgrade history", status=JobStatus.done)
        release = Release(job=job, source="test", title="Imported Album")
        record = MonitoringRecord(
            release=release,
            status=MonitoringStatus.active,
            desired_quality_json="{}",
            history_json='[{"outcome":"candidate_discovered"}]',
        )
        db.add(record)
        await db.flush()

        await upgrade_monitoring._set_release_upgrade_monitoring(db, release.id, True)

        assert record.status == MonitoringStatus.active
        assert record.history_json == '[{"outcome":"candidate_discovered"}]'


async def test_refresh_invokes_upgrade_projection_after_monitor_policy(
    monitoring_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artist_id = await _create_edition_artist(monitoring_factory, duplicate_explicit=True)
    monkeypatch.setattr(
        artist_monitoring, "fetch_and_store_discography", AsyncMock(return_value=[])
    )
    sync = AsyncMock()
    monkeypatch.setattr(artist_monitoring, "sync_artist_upgrade_monitoring", sync)
    async with monitoring_factory() as db:
        artist = await _load_edition_artist(db, artist_id)
        await refresh_monitored_artist(db, Settings(secret_key="test"), artist)

    sync.assert_awaited_once()
    releases = sync.await_args.args[2]
    assert sum(release.monitored for release in releases) == 1


async def test_scheduler_can_wait_for_initial_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = DiscographyRefreshScheduler()
    cycles: list[str] = []

    async def cycle(*, dispatch_jobs: bool = True) -> float:
        assert dispatch_jobs is True
        cycles.append("initial")
        return 3600.0

    monkeypatch.setattr(scheduler, "_refresh_cycle", cycle)
    await scheduler.start(wait_for_initial_cycle=True)
    try:
        assert cycles == ["initial"]
    finally:
        await scheduler.stop()
