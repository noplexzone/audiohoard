from __future__ import annotations

from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.discography_batch import (
    DiscographyBatch,
    DiscographyBatchItem,
    DiscographyBatchItemJob,
    DiscographyBatchItemState,
    DiscographyBatchState,
    DiscographyScopeKind,
)
from app.models.job import Job, JobStatus
from app.services.discography_batch_runner import DiscographyBatchRunner
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
