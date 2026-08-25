from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings
from app.database import Base
from app.metadata.base import AlbumDetail, AlbumTrack
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyScopeKind,
)
from app.models.job import Job, JobStatus
from app.services import catalog_metadata, discography_batch_runner
from app.services.catalog_metadata import hydrate_discography_batch_item
from app.services.discography_batch_runner import DiscographyBatchRunner
from app.services.discography_batches import (
    cancel_discography_batch,
    pause_discography_batch,
    resume_discography_batch,
)
from app.settings_service import QualityProfile

PROFILE = QualityProfile(
    format_preference=["flac", "mp3", "m4a/aac", "ogg", "opus"],
    min_mp3_bitrate=320,
    allow_lower_quality_fallback=True,
)


async def _seed(factory: async_sessionmaker[AsyncSession], tracks: int = 1) -> tuple[int, int]:
    async with factory() as db:
        artist = CatalogArtist(name="Runner", monitored=True)
        album = CatalogAlbum(artist=artist, title="Album", monitored=True, track_count=tracks)
        album.tracks.extend(
            CatalogAlbumTrack(disc=1, position=index, title=f"Track {index}")
            for index in range(1, tracks + 1)
        )
        batch = DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json="{}",
            scope_hash="0" * 64,
            state=DiscographyBatchState.queued,
            matching_count=1,
        )
        item = DiscographyBatchItem(
            batch=batch,
            release_identity="catalog_album:pending",
            catalog_album=album,
            artist_name="Runner",
            release_title="Album",
            state=DiscographyBatchItemState.pending,
        )
        db.add(item)
        await db.commit()
        return batch.id, item.id


async def test_runner_commits_created_jobs_before_dispatch_and_bounds_tick(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runner.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    batch_id, item_id = await _seed(factory, tracks=30)
    observed: list[int] = []

    async def dispatch(job_id: int) -> None:
        async with factory() as observer:
            assert await observer.get(Job, job_id) is not None
            assert (
                await observer.scalar(
                    select(func.count(DiscographyBatchItemJob.id)).where(
                        DiscographyBatchItemJob.item_id == item_id,
                        DiscographyBatchItemJob.job_id == job_id,
                    )
                )
                == 1
            )
        observed.append(job_id)

    runner = DiscographyBatchRunner(factory, dispatcher=dispatch, quality_profile=PROFILE)
    assert await runner.run_once()
    assert len(observed) == 25
    async with factory() as db:
        batch = await db.get(DiscographyBatch, batch_id)
        item = await db.get(DiscographyBatchItem, item_id)
        assert batch is not None and batch.state == DiscographyBatchState.running
        assert item is not None and item.state == DiscographyBatchItemState.waiting
        assert await db.scalar(select(func.count(Job.id))) == 25
    await engine.dispose()


async def test_terminal_link_without_verified_artifact_fails_item(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'terminal.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    batch_id, item_id = await _seed(factory)

    async def dispatch(_job_id: int) -> None:
        return None

    runner = DiscographyBatchRunner(factory, dispatcher=dispatch, quality_profile=PROFILE)
    await runner.run_once()
    async with factory() as db:
        job = await db.scalar(select(Job))
        assert job is not None
        job.status = JobStatus.done
        await db.commit()
    await runner.run_once()
    async with factory() as db:
        batch = await db.get(DiscographyBatch, batch_id)
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None and item.state == DiscographyBatchItemState.failed
        assert batch is not None and batch.state == DiscographyBatchState.completed_with_failures
    await engine.dispose()


async def test_reconciliation_retains_snapshot_manifest_expectation(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'snapshot.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _batch_id, item_id = await _seed(factory)
    runner = DiscographyBatchRunner(
        factory, dispatcher=lambda _job_id: None, quality_profile=PROFILE
    )

    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None
        item.expected_track_count = 2
        item.state = DiscographyBatchItemState.expanding
        await db.commit()

    async with factory() as db:
        await runner._reconcile_item(db, item_id, attempted=False)
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None
        assert item.state == DiscographyBatchItemState.pending
        assert item.reason_code == "catalog_manifest_incomplete"
        assert await db.scalar(select(func.count(Job.id))) == 0

    await engine.dispose()


async def test_production_hydration_fetches_without_open_runner_transaction(
    tmp_path: Path, monkeypatch
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'hydrate.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as db:
        artist = CatalogArtist(name="Hydrate", monitored=True)
        identity = CatalogArtistIdentity(
            artist=artist, provider="deezer", provider_artist_id="artist-1", name=artist.name
        )
        album = CatalogAlbum(artist=artist, title="Hydrate", deezer_id="album-1", track_count=1)
        release = CatalogAlbumProvider(
            artist_identity=identity,
            catalog_album=album,
            provider_album_id="album-1",
            title=album.title,
            track_count=1,
        )
        batch = DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json="{}",
            scope_hash="0" * 64,
            state=DiscographyBatchState.queued,
        )
        item = DiscographyBatchItem(
            batch=batch,
            release_identity="provider:album-1",
            provider_release=release,
            catalog_album=album,
            artist_name=artist.name,
            release_title=album.title,
            state=DiscographyBatchItemState.pending,
            reason_code="catalog_manifest_missing",
            expected_track_count=1,
        )
        db.add(item)
        await db.commit()
        item_id, album_id = item.id, album.id

    class Provider:
        async def get_album(self, provider_id: str) -> AlbumDetail:
            async with factory() as observer:
                await observer.execute(text("BEGIN IMMEDIATE"))
                await observer.rollback()
            return AlbumDetail(
                provider="deezer",
                provider_id=provider_id,
                deezer_id=provider_id,
                title="Hydrate",
                track_count=1,
                tracks=[AlbumTrack(position=1, title="Stored")],
            )

    monkeypatch.setattr(catalog_metadata, "build_metadata_provider", lambda *_args: Provider())

    async def hydrate(claimed_item_id: int, token: str) -> None:
        await hydrate_discography_batch_item(
            factory, claimed_item_id, token, Settings(secret_key="test-secret")
        )

    dispatched: list[int] = []
    runner = DiscographyBatchRunner(
        factory,
        dispatcher=dispatched.append,
        hydration_callback=hydrate,
        quality_profile=PROFILE,
    )
    assert await runner.run_once()
    async with factory() as db:
        stored = await db.get(CatalogAlbum, album_id)
        assert stored is not None
        titles = list(
            await db.scalars(
                select(CatalogAlbumTrack.title).where(CatalogAlbumTrack.album_id == album_id)
            )
        )
        assert titles == ["Stored"]
        current = await db.get(DiscographyBatchItem, item_id)
        assert current is not None and current.state == DiscographyBatchItemState.waiting
    assert len(dispatched) == 1
    await engine.dispose()


async def test_stale_lease_token_cannot_transition_after_hydration(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'stale.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _batch_id, item_id = await _seed(factory)

    async def steal_lease(claimed_item_id: int, _token: str) -> None:
        async with factory() as db:
            item = await db.get(DiscographyBatchItem, claimed_item_id)
            assert item is not None
            item.lease_token = "new-owner"
            await db.commit()

    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None
        item.reason_code = "catalog_manifest_missing"
        await db.commit()
    dispatched: list[int] = []
    runner = DiscographyBatchRunner(
        factory,
        dispatcher=dispatched.append,
        hydration_callback=steal_lease,
        quality_profile=PROFILE,
    )
    assert await runner.run_once()
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None
        assert item.state == DiscographyBatchItemState.hydrating
        assert item.lease_token == "new-owner"
        assert await db.scalar(select(func.count(Job.id))) == 0
    assert dispatched == []
    await engine.dispose()


async def test_expired_lease_reclaim_fences_old_owner(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'reclaim.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _batch_id, item_id = await _seed(factory)
    old_token = "expired-owner"
    now = datetime.now(UTC)
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None
        item.state = DiscographyBatchItemState.expanding
        item.lease_token = old_token
        item.heartbeat_at = now - timedelta(minutes=10)
        await db.commit()
    runner = DiscographyBatchRunner(
        factory, dispatcher=lambda _job_id: None, quality_profile=PROFILE
    )
    await runner._recover_expired(now)
    claimed = await runner._claim_pending(now)
    assert claimed is not None and claimed[2] != old_token
    async with factory() as db:
        assert not await runner._reconcile_item(
            db, item_id, attempted=True, expected_lease_token=old_token
        )
        await db.commit()
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None
        assert item.state == DiscographyBatchItemState.expanding
        assert item.lease_token == claimed[2]
    await engine.dispose()


async def test_cancel_after_expansion_commit_prevents_dispatch(
    tmp_path: Path, monkeypatch
) -> None:
    await _assert_control_after_expansion(tmp_path, monkeypatch, cancel=True)


async def test_pause_after_expansion_commit_prevents_dispatch_and_is_resumable(
    tmp_path: Path, monkeypatch
) -> None:
    await _assert_control_after_expansion(tmp_path, monkeypatch, cancel=False)


async def _assert_control_after_expansion(tmp_path: Path, monkeypatch, *, cancel: bool) -> None:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / ('cancel.db' if cancel else 'pause.db')}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    batch_id, item_id = await _seed(factory, tracks=2)
    real_expand = discography_batch_runner.expand_catalog_album_missing_track_jobs

    async def expand_then_control(*args, **kwargs):
        outcome = await real_expand(*args, **kwargs)
        async with factory() as db:
            if cancel:
                await cancel_discography_batch(db, batch_id)
            else:
                await pause_discography_batch(db, batch_id)
        return outcome

    monkeypatch.setattr(
        discography_batch_runner, "expand_catalog_album_missing_track_jobs", expand_then_control
    )
    dispatched: list[int] = []
    runner = DiscographyBatchRunner(factory, dispatcher=dispatched.append, quality_profile=PROFILE)
    assert await runner.run_once()
    assert dispatched == []
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        batch = await db.get(DiscographyBatch, batch_id)
        jobs = list((await db.scalars(select(Job).order_by(Job.id))).all())
        assert item is not None and batch is not None and len(jobs) == 2
        if cancel:
            assert batch.state == DiscographyBatchState.cancelled
            assert item.state == DiscographyBatchItemState.cancelled
            assert all(job.status == JobStatus.cancelled for job in jobs)
        else:
            assert batch.state == DiscographyBatchState.paused
            assert item.state == DiscographyBatchItemState.pending
            assert item.lease_token is None
            assert all(job.status == JobStatus.cancelled for job in jobs)
            await resume_discography_batch(db, batch_id, quality_profile=PROFILE)

    if not cancel:
        monkeypatch.setattr(
            discography_batch_runner, "expand_catalog_album_missing_track_jobs", real_expand
        )
        assert await runner.run_once()
        assert len(dispatched) == 2
        async with factory() as db:
            active = list(
                await db.scalars(
                    select(Job).where(Job.status.in_((JobStatus.pending, JobStatus.running)))
                )
            )
            assert len(active) == 2
    await engine.dispose()
