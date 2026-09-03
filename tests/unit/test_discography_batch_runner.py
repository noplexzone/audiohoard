from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
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
    DiscographyBatchJobRole,
    DiscographyBatchState,
    DiscographyJobOwnership,
    DiscographyScopeKind,
)
from app.models.job import Job, JobStatus
from app.services import catalog_metadata, discography_batch_runner, discography_batches
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


async def test_runner_materializes_one_release_root_and_second_pass_is_idempotent(
    tmp_path: Path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runner.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    batch_id, item_id = await _seed(factory, tracks=25)
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
    assert len(observed) == 1
    async with factory() as db:
        batch = await db.get(DiscographyBatch, batch_id)
        item = await db.get(DiscographyBatchItem, item_id)
        assert batch is not None and batch.state == DiscographyBatchState.running
        assert item is not None and item.state == DiscographyBatchItemState.waiting
        root_job = await db.scalar(select(Job))
        assert root_job is not None
        assert root_job.catalog_album_id == item.catalog_album_id
        assert root_job.catalog_track_id is None
        link = await db.scalar(select(DiscographyBatchItemJob))
        assert link is not None
        assert link.role == DiscographyBatchJobRole.release_root
        assert link.catalog_track_id is None
        assert link.ownership == DiscographyJobOwnership.created
        assert await db.scalar(select(func.count(Job.id))) == 1
    assert not await runner.run_once()
    assert len(observed) == 1
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None and item.state == DiscographyBatchItemState.waiting
        assert await db.scalar(select(func.count(Job.id))) == 1
    await engine.dispose()


async def test_verified_projection_waits_for_active_release_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'active-root.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    batch_id, item_id = await _seed(factory)
    runner = DiscographyBatchRunner(
        factory,
        dispatcher=lambda _job_id: None,
        quality_profile=PROFILE,
    )
    assert await runner.run_once()

    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        root = await db.scalar(select(Job))
        assert item is not None and root is not None
        root.status = JobStatus.running
        await db.commit()

    async def project_complete(_db, album_ids, **_kwargs):
        return {album_id: SimpleNamespace(target_track_ids=()) for album_id in album_ids}

    monkeypatch.setattr(
        discography_batch_runner,
        "project_catalog_album_queue_targets",
        project_complete,
    )
    monkeypatch.setattr(
        discography_batches,
        "project_catalog_album_queue_targets",
        project_complete,
    )
    await runner.run_once()

    async with factory() as db:
        batch = await db.get(DiscographyBatch, batch_id)
        item = await db.get(DiscographyBatchItem, item_id)
        assert batch is not None and batch.state == DiscographyBatchState.running
        assert item is not None and item.state == DiscographyBatchItemState.waiting
        assert item.reason_code == "active_release_root"
        assert item.active_count == 1
        assert item.completed_at is None

        await pause_discography_batch(db, batch_id)
        await resume_discography_batch(db, batch_id, quality_profile=PROFILE)
        await db.refresh(item)
        assert item.state == DiscographyBatchItemState.waiting
        assert item.reason_code == "active_release_root"
        assert item.completed_at is None

        root = await db.scalar(select(Job))
        assert root is not None
        root.status = JobStatus.done
        await db.commit()

    assert await runner.run_once()
    async with factory() as db:
        batch = await db.get(DiscographyBatch, batch_id)
        item = await db.get(DiscographyBatchItem, item_id)
        assert batch is not None and batch.state == DiscographyBatchState.completed
        assert item is not None and item.state == DiscographyBatchItemState.complete
        assert item.reason_code == "verified_complete"
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


async def test_terminal_track_is_not_reacquired_while_sibling_remains_active(
    tmp_path: Path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'generation.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    batch_id, item_id = await _seed(factory, tracks=2)
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None and item.catalog_album_id is not None
        tracks = list(
            await db.scalars(
                select(CatalogAlbumTrack)
                .where(CatalogAlbumTrack.album_id == item.catalog_album_id)
                .order_by(CatalogAlbumTrack.id)
            )
        )
        jobs = [
            Job(
                source="priority",
                query=track.title,
                status=JobStatus.pending,
                catalog_album_id=item.catalog_album_id,
                catalog_track_id=track.id,
            )
            for track in tracks
        ]
        db.add_all(jobs)
        await db.flush()
        db.add_all(
            [
                DiscographyBatchItemJob(
                    item_id=item.id,
                    job_id=job.id,
                    generation=1,
                    catalog_track_id=job.catalog_track_id,
                    ownership=DiscographyJobOwnership.created,
                    role=DiscographyBatchJobRole.legacy_track,
                )
                for job in jobs
            ]
        )
        item.state = DiscographyBatchItemState.waiting
        jobs[0].status = JobStatus.failed
        jobs[1].status = JobStatus.running
        terminal_track_id = jobs[0].catalog_track_id
        await db.commit()

    dispatched: list[int] = []
    runner = DiscographyBatchRunner(factory, dispatcher=dispatched.append, quality_profile=PROFILE)

    for _ in range(3):
        await runner.run_once()
    async with factory() as db:
        assert (
            await db.scalar(
                select(func.count(Job.id)).where(Job.catalog_track_id == terminal_track_id)
            )
            == 1
        )
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None and item.state == DiscographyBatchItemState.waiting
        links = list(
            (
                await db.scalars(
                    select(DiscographyBatchItemJob).where(
                        DiscographyBatchItemJob.item_id == item_id
                    )
                )
            ).all()
        )
        assert {(link.catalog_track_id, link.generation) for link in links} == {
            (job.catalog_track_id, 1) for job in jobs
        }
        running_job = await db.scalar(select(Job).where(Job.status == JobStatus.running))
        assert running_job is not None
        running_job.status = JobStatus.done
        await db.commit()

    await runner.run_once()
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        batch = await db.get(DiscographyBatch, batch_id)
        assert item is not None and item.state == DiscographyBatchItemState.failed
        assert item.reason_code == "current_generation_attempts_exhausted"
        assert batch is not None and batch.state == DiscographyBatchState.completed_with_failures

    async with factory() as db:
        await discography_batches.retry_discography_batch_items(db, batch_id, [item_id])
    await runner.run_once()
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None and item.execution_generation == 2
        assert await db.scalar(select(func.count(Job.id))) == 3
        links = list(
            (
                await db.scalars(
                    select(DiscographyBatchItemJob).where(
                        DiscographyBatchItemJob.item_id == item_id
                    )
                )
            ).all()
        )
        assert len(links) == 3
        assert {link.generation for link in links} == {1, 2}
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
            release_identity="provider:deezer:album-1",
            provider_release=release,
            catalog_album=album,
            provider="deezer",
            provider_album_id="album-1",
            artist_name=artist.name,
            release_title=album.title,
            state=DiscographyBatchItemState.pending,
            reason_code="catalog_manifest_missing",
            expected_track_count=1,
        )
        db.add(item)
        await db.commit()
        item_id, album_id, release_id = item.id, album.id, release.id
    async with factory() as db:
        release = await db.get(CatalogAlbumProvider, release_id)
        assert release is not None
        await db.delete(release)
        await db.commit()

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


async def test_hydration_rejects_live_canonical_identity_change_during_provider_io(
    tmp_path: Path, monkeypatch
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'canonical-race.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as db:
        artist = CatalogArtist(name="Canonical race", monitored=True)
        album = CatalogAlbum(
            artist=artist, title="Canonical race", deezer_id="album-old", track_count=1
        )
        batch = DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json="{}",
            scope_hash="1" * 64,
            state=DiscographyBatchState.running,
        )
        item = DiscographyBatchItem(
            batch=batch,
            release_identity="provider:deezer:album-old",
            catalog_album=album,
            provider="deezer",
            provider_album_id="album-old",
            artist_name=artist.name,
            release_title=album.title,
            state=DiscographyBatchItemState.hydrating,
            lease_token="lease-owner",
            reason_code="catalog_manifest_missing",
            expected_track_count=1,
        )
        db.add(item)
        await db.commit()
        item_id, album_id = item.id, album.id

    class Provider:
        async def get_album(self, provider_id: str) -> AlbumDetail:
            async with factory() as db:
                album = await db.get(CatalogAlbum, album_id)
                assert album is not None
                album.deezer_id = "album-new"
                await db.commit()
            return AlbumDetail(
                provider="deezer",
                provider_id=provider_id,
                deezer_id=provider_id,
                title="Canonical race",
                track_count=1,
                tracks=[AlbumTrack(position=1, title="Must not store")],
            )

    monkeypatch.setattr(catalog_metadata, "build_metadata_provider", lambda *_args: Provider())
    with pytest.raises(RuntimeError, match="catalog identity changed"):
        await hydrate_discography_batch_item(
            factory, item_id, "lease-owner", Settings(secret_key="test-secret")
        )
    async with factory() as db:
        assert await db.scalar(select(func.count(CatalogAlbumTrack.id))) == 0
        assert await db.scalar(select(func.count(Job.id))) == 0
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


async def test_expired_hydration_lease_retries_hydration(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'hydrate-reclaim.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _batch_id, item_id = await _seed(factory)
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None
        item.state = DiscographyBatchItemState.hydrating
        item.reason_code = "catalog_manifest_missing"
        item.lease_token = "expired-hydrator"
        item.heartbeat_at = datetime.now(UTC) - timedelta(minutes=10)
        await db.commit()

    hydrated: list[tuple[int, str]] = []

    async def hydrate(claimed_item_id: int, lease_token: str) -> None:
        hydrated.append((claimed_item_id, lease_token))

    runner = DiscographyBatchRunner(
        factory,
        dispatcher=lambda _job_id: None,
        hydration_callback=hydrate,
        quality_profile=PROFILE,
    )
    assert await runner.run_once()
    assert len(hydrated) == 1
    assert hydrated[0][0] == item_id
    assert hydrated[0][1] != "expired-hydrator"
    async with factory() as db:
        current = await db.get(DiscographyBatchItem, item_id)
        assert current is not None
        assert current.reason_code == "active_release_root"
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
    real_expand = discography_batch_runner.materialize_batch_release_root_job

    async def expand_then_control(*args, **kwargs):
        outcome = await real_expand(*args, **kwargs)
        async with factory() as db:
            if cancel:
                await cancel_discography_batch(db, batch_id)
            else:
                await pause_discography_batch(db, batch_id)
        return outcome

    monkeypatch.setattr(
        discography_batch_runner, "materialize_batch_release_root_job", expand_then_control
    )
    dispatched: list[int] = []
    runner = DiscographyBatchRunner(factory, dispatcher=dispatched.append, quality_profile=PROFILE)
    assert await runner.run_once()
    assert dispatched == []
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        batch = await db.get(DiscographyBatch, batch_id)
        jobs = list((await db.scalars(select(Job).order_by(Job.id))).all())
        assert item is not None and batch is not None and len(jobs) == 1
        if cancel:
            assert batch.state == DiscographyBatchState.cancelled
            assert item.state == DiscographyBatchItemState.cancelled
            assert all(job.status == JobStatus.cancelled for job in jobs)
        else:
            assert batch.state == DiscographyBatchState.paused
            assert item.state == DiscographyBatchItemState.pending
            assert item.execution_generation == 1
            assert item.lease_token is None
            assert all(job.status == JobStatus.pending for job in jobs)
            await resume_discography_batch(db, batch_id, quality_profile=PROFILE)

    if not cancel:
        monkeypatch.setattr(
            discography_batch_runner, "materialize_batch_release_root_job", real_expand
        )
        assert await runner.run_once()
        assert len(dispatched) == 1
        async with factory() as db:
            item = await db.get(DiscographyBatchItem, item_id)
            assert item is not None and item.execution_generation == 1
            active = list(
                await db.scalars(
                    select(Job).where(Job.status.in_((JobStatus.pending, JobStatus.running)))
                )
            )
            assert len(active) == 1
    await engine.dispose()


async def test_waiting_item_does_not_starve_later_pending(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fair.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _batch_id, waiting_id = await _seed(factory)
    async with factory() as db:
        waiting = await db.get(DiscographyBatchItem, waiting_id)
        assert waiting is not None
        waiting.state = DiscographyBatchItemState.waiting
        source_album = await db.get(CatalogAlbum, waiting.catalog_album_id)
        assert source_album is not None
        album = CatalogAlbum(artist_id=source_album.artist_id, title="Later", track_count=1)
        album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Later"))
        later = DiscographyBatchItem(
            batch_id=waiting.batch_id,
            release_identity="later",
            catalog_album=album,
            artist_name="Runner",
            release_title="Later",
            state=DiscographyBatchItemState.pending,
        )
        db.add(later)
        await db.commit()
        later_id = later.id
    dispatched: list[int] = []
    runner = DiscographyBatchRunner(factory, dispatcher=dispatched.append, quality_profile=PROFILE)
    assert await runner.run_once()
    async with factory() as db:
        later = await db.get(DiscographyBatchItem, later_id)
        assert later is not None and later.state == DiscographyBatchItemState.waiting
    assert len(dispatched) == 1
    await engine.dispose()


async def test_unchanged_waiting_page_does_not_starve_later_terminal_job(
    tmp_path: Path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'waiting-fair.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    batch_id, first_id = await _seed(factory)
    async with factory() as db:
        first = await db.get(DiscographyBatchItem, first_id)
        assert first is not None
        source_album = await db.get(CatalogAlbum, first.catalog_album_id)
        assert source_album is not None
        items = [first]
        for index in range(2, 27):
            title = f"Album {index:02d}"
            album = CatalogAlbum(artist_id=source_album.artist_id, title=title, track_count=1)
            album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title=title))
            item = DiscographyBatchItem(
                batch_id=batch_id,
                release_identity=f"later-waiting-{index}",
                catalog_album=album,
                artist_name="Runner",
                release_title=title,
                state=DiscographyBatchItemState.pending,
            )
            db.add(item)
            items.append(item)
        await db.commit()
        waiting_ids = [item.id for item in items[:25]]
        later_id = items[25].id

    runner = DiscographyBatchRunner(
        factory, dispatcher=lambda _job_id: None, quality_profile=PROFILE
    )
    for _ in range(26):
        assert await runner.run_once()
    async with factory() as db:
        links = list(
            (
                await db.execute(
                    select(DiscographyBatchItemJob.item_id, Job)
                    .join(Job, Job.id == DiscographyBatchItemJob.job_id)
                    .order_by(DiscographyBatchItemJob.item_id)
                )
            ).all()
        )
        assert [item_id for item_id, _job in links] == waiting_ids + [later_id]
        for _item_id, job in links[:25]:
            job.status = JobStatus.running
        links[25][1].status = JobStatus.done
        old_timestamp = datetime(2026, 1, 1, tzinfo=UTC)
        later_timestamp = old_timestamp + timedelta(days=1)
        await db.execute(
            text("UPDATE discography_batch_items SET updated_at = :stamp WHERE id <= :last_id"),
            {"stamp": old_timestamp, "last_id": waiting_ids[-1]},
        )
        await db.execute(
            text("UPDATE discography_batch_items SET updated_at = :stamp WHERE id = :later_id"),
            {"stamp": later_timestamp, "later_id": later_id},
        )
        await db.commit()

    # The first bounded page contains only unchanged rows; its rotation markers
    # make the later terminal row visible to the next bounded pass.
    assert not await runner.run_once()
    assert await runner.run_once()
    async with factory() as db:
        earlier = list(
            await db.scalars(
                select(DiscographyBatchItem).where(DiscographyBatchItem.id.in_(waiting_ids))
            )
        )
        later = await db.get(DiscographyBatchItem, later_id)
        assert len(earlier) == 25
        assert all(item.state == DiscographyBatchItemState.waiting for item in earlier)
        assert later is not None and later.state == DiscographyBatchItemState.failed
        assert later.reason_code == "current_generation_attempts_exhausted"
    await engine.dispose()


async def test_partial_release_root_repairs_missing_fallback_materialization(
    tmp_path: Path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fallback-repair.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _batch_id, item_id = await _seed(factory, tracks=2)
    dispatched: list[int] = []
    runner = DiscographyBatchRunner(factory, dispatcher=dispatched.append, quality_profile=PROFILE)

    assert await runner.run_once()
    async with factory() as db:
        root = await db.scalar(select(Job))
        assert root is not None and root.catalog_track_id is None
        track_ids = list(
            await db.scalars(
                select(CatalogAlbumTrack.id)
                .where(CatalogAlbumTrack.album_id == root.catalog_album_id)
                .order_by(CatalogAlbumTrack.id)
            )
        )
        root.status = JobStatus.partial
        root.result_json = json.dumps({"missing_catalog_track_ids": track_ids})
        await db.commit()

    assert await runner.run_once()
    async with factory() as db:
        jobs = list((await db.scalars(select(Job).order_by(Job.id))).all())
        links = list(
            (
                await db.scalars(
                    select(DiscographyBatchItemJob)
                    .where(DiscographyBatchItemJob.item_id == item_id)
                    .order_by(DiscographyBatchItemJob.id)
                )
            ).all()
        )
        item = await db.get(DiscographyBatchItem, item_id)
        assert len(jobs) == 3
        assert [job.catalog_track_id for job in jobs[1:]] == track_ids
        assert [link.role for link in links] == [
            DiscographyBatchJobRole.release_root,
            DiscographyBatchJobRole.track_fallback,
            DiscographyBatchJobRole.track_fallback,
        ]
        assert item is not None and item.state == DiscographyBatchItemState.waiting
        assert item.active_count == 2
    assert dispatched == [jobs[0].id, jobs[1].id, jobs[2].id]

    await runner.run_once()
    async with factory() as db:
        assert await db.scalar(select(func.count(Job.id))) == 3
        assert await db.scalar(select(func.count(DiscographyBatchItemJob.id))) == 3
    assert dispatched == [jobs[0].id, jobs[1].id, jobs[2].id]
    await engine.dispose()


async def test_partial_release_repair_rejects_parent_retried_before_spawn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'stale-repair.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _batch_id, item_id = await _seed(factory)
    dispatched: list[int] = []
    runner = DiscographyBatchRunner(factory, dispatcher=dispatched.append, quality_profile=PROFILE)

    assert await runner.run_once()
    async with factory() as db:
        root = await db.scalar(select(Job))
        assert root is not None and root.catalog_album_id is not None
        track_id = await db.scalar(
            select(CatalogAlbumTrack.id).where(CatalogAlbumTrack.album_id == root.catalog_album_id)
        )
        assert track_id is not None
        root.status = JobStatus.partial
        root.result_json = json.dumps({"missing_catalog_track_ids": [track_id]})
        await db.commit()
        root_id = root.id

    from app.jobs import runner as job_runner

    original_spawn = job_runner._spawn_continuation_jobs

    async def retry_before_spawn(*args, **kwargs):
        async with factory() as retry:
            parent = await retry.get(Job, root_id)
            assert parent is not None
            parent.status = JobStatus.pending
            parent.result_json = None
            parent.updated_at = datetime.now(UTC)
            await retry.commit()
        return await original_spawn(*args, **kwargs)

    monkeypatch.setattr(job_runner, "_spawn_continuation_jobs", retry_before_spawn)

    assert not await runner.run_once()
    async with factory() as db:
        jobs = list((await db.scalars(select(Job).order_by(Job.id))).all())
        parent = await db.get(Job, root_id)
        assert parent is not None and parent.status == JobStatus.pending
        assert len(jobs) == 1
        assert await db.scalar(select(func.count(DiscographyBatchItemJob.id))) == 1
    assert dispatched == [root_id]
    await engine.dispose()


async def test_active_root_with_fallback_link_fails_closed(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'mixed-owners.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _batch_id, item_id = await _seed(factory)
    runner = DiscographyBatchRunner(
        factory, dispatcher=lambda _job_id: None, quality_profile=PROFILE
    )
    assert await runner.run_once()
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        root = await db.scalar(select(Job))
        assert item is not None and root is not None and item.catalog_album_id is not None
        track_id = await db.scalar(
            select(CatalogAlbumTrack.id).where(CatalogAlbumTrack.album_id == item.catalog_album_id)
        )
        assert track_id is not None
        fallback = Job(
            source="priority",
            query="fallback",
            status=JobStatus.pending,
            catalog_album_id=item.catalog_album_id,
            catalog_track_id=track_id,
            parent_job_id=root.id,
        )
        db.add(fallback)
        await db.flush()
        db.add(
            DiscographyBatchItemJob(
                item_id=item.id,
                job_id=fallback.id,
                generation=item.execution_generation,
                catalog_track_id=track_id,
                ownership=DiscographyJobOwnership.created,
                role=DiscographyBatchJobRole.track_fallback,
            )
        )
        await db.commit()

    assert await runner.run_once()
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        assert item is not None and item.state == DiscographyBatchItemState.failed
        assert item.reason_code == "malformed_batch_job_evidence"
    await engine.dispose()


async def test_resume_redispatches_pending_fallback_suppressed_while_paused(
    tmp_path: Path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'resume-fallback.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    batch_id, item_id = await _seed(factory)
    dispatched: list[int] = []
    runner = DiscographyBatchRunner(factory, dispatcher=dispatched.append, quality_profile=PROFILE)
    assert await runner.run_once()
    async with factory() as db:
        item = await db.get(DiscographyBatchItem, item_id)
        root = await db.scalar(select(Job))
        assert item is not None and root is not None and item.catalog_album_id is not None
        track_id = await db.scalar(
            select(CatalogAlbumTrack.id).where(CatalogAlbumTrack.album_id == item.catalog_album_id)
        )
        assert track_id is not None
        root.status = JobStatus.partial
        fallback = Job(
            source="priority",
            query="fallback",
            status=JobStatus.pending,
            catalog_album_id=item.catalog_album_id,
            catalog_track_id=track_id,
            parent_job_id=root.id,
        )
        db.add(fallback)
        await db.flush()
        db.add(
            DiscographyBatchItemJob(
                item_id=item.id,
                job_id=fallback.id,
                generation=item.execution_generation,
                catalog_track_id=track_id,
                ownership=DiscographyJobOwnership.created,
                role=DiscographyBatchJobRole.track_fallback,
            )
        )
        await db.commit()
        fallback_id = fallback.id
    async with factory() as db:
        await pause_discography_batch(db, batch_id)
        outcome = await resume_discography_batch(db, batch_id, quality_profile=PROFILE)
        assert outcome.reset_item_ids == (item_id,)

    assert await runner.run_once()
    assert dispatched == [1, fallback_id]
    await engine.dispose()
