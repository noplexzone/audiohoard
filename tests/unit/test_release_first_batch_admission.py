from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.models.acquisition_claim import AcquisitionDispatchClaim, CatalogReleaseAcquisitionClaim
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
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
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.catalog import (
    DiscographyLeaseLostError,
    expand_catalog_album_missing_track_jobs,
)
from app.services.release_admission import (
    ReleaseRootAdmissionStatus,
    materialize_batch_release_root_job,
)
from app.settings_service import QualityProfile

PROFILE = QualityProfile(
    format_preference=["flac", "mp3", "m4a/aac", "ogg", "opus"],
    min_mp3_bitrate=320,
    allow_lower_quality_fallback=True,
)
LEASE = "release-admission-owner"


@asynccontextmanager
async def _database(path: Path) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


async def _seed(
    db: AsyncSession, *, item_count: int = 1, track_count: int = 2
) -> tuple[CatalogAlbum, tuple[DiscographyBatchItem, ...]]:
    artist = CatalogArtist(name="Release Artist", monitored=True)
    album = CatalogAlbum(
        artist=artist, title="Atomic Album", monitored=True, track_count=track_count
    )
    album.tracks.extend(
        CatalogAlbumTrack(disc=1, position=n, title=f"Song {n}") for n in range(1, track_count + 1)
    )
    db.add(artist)
    await db.flush()
    items = []
    for number in range(item_count):
        batch = DiscographyBatch(
            scope_kind=DiscographyScopeKind.wanted_selected,
            scope_json="{}",
            scope_hash=f"{number + 1:064x}",
            state=DiscographyBatchState.running,
        )
        item = DiscographyBatchItem(
            batch=batch,
            release_identity=f"catalog_album:{album.id}",
            catalog_album=album,
            artist_name=artist.name,
            release_title=album.title,
            expected_track_count=track_count,
            state=DiscographyBatchItemState.expanding,
            execution_generation=1,
            lease_token=LEASE,
        )
        db.add(item)
        items.append(item)
    await db.flush()
    return album, tuple(items)


async def _admit(
    db: AsyncSession,
    item_id: int,
    *,
    generation: int = 1,
    lease: str = LEASE,
    library_root: Path | None = None,
):
    return await materialize_batch_release_root_job(
        db,
        item_id,
        execution_generation=generation,
        batch_lease_token=lease,
        quality_profile=PROFILE,
        library_root=library_root,
    )


async def test_no_current_targets_creates_nothing(tmp_path: Path) -> None:
    destination = tmp_path / "library" / "owned.flac"
    destination.parent.mkdir()
    destination.write_bytes(b"owned")
    async with _database(tmp_path / "no-work.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed, track_count=1)
            source_job = Job(source="fixture", query="owned", status=JobStatus.done)
            release = Release(job=source_job, source="fixture", title=album.title)
            track = Track(
                job=source_job,
                release=release,
                source="fixture",
                catalog_album=album,
                catalog_track=album.tracks[0],
                import_state=ImportWorkflowState.imported,
                acquisition_state=AcquisitionState.downloaded,
                file_format="flac",
                file_size_bytes=5,
            )
            seed.add(
                ImportPlan(
                    release=release,
                    track=track,
                    source_path=str(destination),
                    destination_path=str(destination),
                    status=ImportWorkflowState.imported,
                    file_state=LibraryFileState.present,
                )
            )
            await seed.commit()
            item_id = item.id
        async with factory() as db:
            outcome = await _admit(db, item_id, library_root=destination.parent)
        assert outcome.status == ReleaseRootAdmissionStatus.no_work
        assert outcome.job_id is None
        assert outcome.target_track_ids == outcome.blocking_job_ids == ()
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 1
            assert (
                await observer.scalar(select(func.count(CatalogReleaseAcquisitionClaim.job_id)))
                == 0
            )
            assert await observer.scalar(select(func.count(DiscographyBatchItemJob.id))) == 0


async def test_existing_same_generation_root_is_idempotently_observed(tmp_path: Path) -> None:
    async with _database(tmp_path / "repeat.db") as factory:
        async with factory() as seed:
            _album, (item,) = await _seed(seed)
            await seed.commit()
            item_id = item.id
        async with factory() as db:
            first = await _admit(db, item_id)
            second = await _admit(db, item_id)
        assert first.status == ReleaseRootAdmissionStatus.created
        assert second.status == ReleaseRootAdmissionStatus.observed
        assert second.job_id == first.job_id
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 1
            link = await observer.scalar(select(DiscographyBatchItemJob))
            assert link is not None
            assert link.ownership == DiscographyJobOwnership.created
            assert link.role == DiscographyBatchJobRole.release_root


async def test_two_batches_concurrently_share_one_root_and_two_links(tmp_path: Path) -> None:
    async with _database(tmp_path / "concurrent.db") as factory:
        async with factory() as seed:
            _album, items = await _seed(seed, item_count=2)
            await seed.commit()
            item_ids = tuple(item.id for item in items)
        gate = asyncio.Event()
        mutex = asyncio.Lock()
        ready = 0

        async def materialize(item_id: int):
            nonlocal ready
            async with factory() as db:
                async with mutex:
                    ready += 1
                    if ready == 2:
                        gate.set()
                await gate.wait()
                return await _admit(db, item_id)

        outcomes = await asyncio.gather(*(materialize(item_id) for item_id in item_ids))
        assert {outcome.status for outcome in outcomes} == {
            ReleaseRootAdmissionStatus.created,
            ReleaseRootAdmissionStatus.observed,
        }
        assert len({outcome.job_id for outcome in outcomes}) == 1
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 1
            assert (
                await observer.scalar(select(func.count(CatalogReleaseAcquisitionClaim.job_id)))
                == 1
            )
            links = list((await observer.scalars(select(DiscographyBatchItemJob))).all())
            assert {link.item_id for link in links} == set(item_ids)
            assert {link.ownership for link in links} == {
                DiscographyJobOwnership.created,
                DiscographyJobOwnership.observed,
            }
            assert {link.role for link in links} == {DiscographyBatchJobRole.release_root}


async def test_active_other_root_is_observed(tmp_path: Path) -> None:
    async with _database(tmp_path / "other-root.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed)
            owner = Job(
                source="priority", query="existing", status=JobStatus.running, catalog_album=album
            )
            seed.add(owner)
            await seed.flush()
            seed.add(CatalogReleaseAcquisitionClaim(catalog_album_id=album.id, job_id=owner.id))
            await seed.commit()
            item_id, owner_id = item.id, owner.id
        async with factory() as db:
            outcome = await _admit(db, item_id)
        assert (outcome.status, outcome.job_id) == (
            ReleaseRootAdmissionStatus.observed,
            owner_id,
        )
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 1
            link = await observer.scalar(select(DiscographyBatchItemJob))
            assert link is not None and link.ownership == DiscographyJobOwnership.observed


@pytest.mark.parametrize("terminal", list(JobStatus)[2:])
async def test_terminal_release_claim_is_replaced(tmp_path: Path, terminal: JobStatus) -> None:
    async with _database(tmp_path / f"terminal-{terminal.value}.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed)
            old = Job(source="priority", query="old", status=terminal, catalog_album=album)
            seed.add(old)
            await seed.flush()
            seed.add(CatalogReleaseAcquisitionClaim(catalog_album_id=album.id, job_id=old.id))
            await seed.commit()
            album_id, item_id, old_id = album.id, item.id, old.id
        async with factory() as db:
            outcome = await _admit(db, item_id)
        assert outcome.status == ReleaseRootAdmissionStatus.created
        assert outcome.job_id is not None and outcome.job_id != old_id
        async with factory() as observer:
            claim = await observer.get(CatalogReleaseAcquisitionClaim, album_id)
            assert claim is not None and claim.job_id == outcome.job_id
            assert await observer.scalar(select(func.count(Job.id))) == 2


async def test_active_exact_track_claim_suppresses_root(tmp_path: Path) -> None:
    async with _database(tmp_path / "track-owner.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed)
            owner = Job(
                source="priority",
                query="track",
                status=JobStatus.pending,
                catalog_album=album,
                catalog_track=album.tracks[0],
            )
            seed.add(owner)
            await seed.flush()
            seed.add(
                AcquisitionDispatchClaim(
                    catalog_album_id=album.id,
                    catalog_track_id=album.tracks[0].id,
                    job_id=owner.id,
                )
            )
            await seed.commit()
            item_id, owner_id = item.id, owner.id
        async with factory() as db:
            outcome = await _admit(db, item_id)
        assert outcome.status == ReleaseRootAdmissionStatus.waiting_for_tracks
        assert outcome.job_id is None and outcome.blocking_job_ids == (owner_id,)
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 1
            assert (
                await observer.scalar(select(func.count(CatalogReleaseAcquisitionClaim.job_id)))
                == 0
            )
            assert await observer.scalar(select(func.count(DiscographyBatchItemJob.id))) == 0


@pytest.mark.parametrize(
    ("generation", "lease", "batch_state", "item_state"),
    [
        (2, LEASE, DiscographyBatchState.running, DiscographyBatchItemState.expanding),
        (1, "stale", DiscographyBatchState.running, DiscographyBatchItemState.expanding),
        (1, LEASE, DiscographyBatchState.paused, DiscographyBatchItemState.expanding),
        (1, LEASE, DiscographyBatchState.cancelled, DiscographyBatchItemState.cancelled),
    ],
)
async def test_stale_generation_lease_pause_and_cancel_are_rejected(
    tmp_path: Path,
    generation: int,
    lease: str,
    batch_state: DiscographyBatchState,
    item_state: DiscographyBatchItemState,
) -> None:
    async with _database(tmp_path / f"reject-{generation}-{batch_state.value}.db") as factory:
        async with factory() as seed:
            _album, (item,) = await _seed(seed)
            item.batch.state, item.state = batch_state, item_state
            await seed.commit()
            item_id = item.id
        async with factory() as db:
            with pytest.raises(DiscographyLeaseLostError, match="lease|generation|active"):
                await _admit(db, item_id, generation=generation, lease=lease)
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 0


@pytest.mark.parametrize("invalidity", ["empty", "underfull", "duplicate_position"])
async def test_structurally_invalid_manifest_is_rejected(tmp_path: Path, invalidity: str) -> None:
    async with _database(tmp_path / f"manifest-{invalidity}.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed)
            if invalidity == "empty":
                album.tracks.clear()
                album.track_count = item.expected_track_count = 0
            elif invalidity == "underfull":
                album.tracks.pop()
            else:
                album.tracks[1].position = album.tracks[0].position
            await seed.commit()
            item_id = item.id
        async with factory() as db:
            with pytest.raises(ValueError, match="catalog manifest"):
                await _admit(db, item_id)
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 0


async def test_locked_commit_retry_has_no_leaked_id_or_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with _database(tmp_path / "locked.db") as factory:
        async with factory() as seed:
            _album, (item,) = await _seed(seed)
            await seed.commit()
            item_id = item.id
        original_commit = AsyncSession.commit
        commit_calls = 0

        async def lock_first_reservation_commit(session: AsyncSession) -> None:
            nonlocal commit_calls
            commit_calls += 1
            if commit_calls == 2:
                raise OperationalError("COMMIT", {}, Exception("database is locked"))
            await original_commit(session)

        monkeypatch.setattr(AsyncSession, "commit", lock_first_reservation_commit)
        async with factory() as db:
            outcome = await _admit(db, item_id)
        assert commit_calls >= 3
        assert outcome.status == ReleaseRootAdmissionStatus.created and outcome.job_id is not None
        async with factory() as observer:
            jobs = list((await observer.scalars(select(Job))).all())
            assert [job.id for job in jobs] == [outcome.job_id]
            claim = await observer.scalar(select(CatalogReleaseAcquisitionClaim))
            link = await observer.scalar(select(DiscographyBatchItemJob))
            assert claim is not None and claim.job_id == outcome.job_id
            assert link is not None and link.job_id == outcome.job_id


async def test_active_root_claim_blocks_later_direct_track_expansion(tmp_path: Path) -> None:
    async with _database(tmp_path / "root-first.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed)
            await seed.commit()
            album_id, item_id = album.id, item.id
        async with factory() as root_db:
            root = await _admit(root_db, item_id)
        assert root.status == ReleaseRootAdmissionStatus.created
        async with factory() as track_db:
            current_album = await track_db.get(CatalogAlbum, album_id)
            assert current_album is not None
            track_outcome = await expand_catalog_album_missing_track_jobs(
                track_db,
                current_album,
                quality_profile=PROFILE,
            )
        assert track_outcome.created_job_ids == ()
        assert track_outcome.observed_job_ids == (root.job_id,)
        async with factory() as observer:
            assert await observer.scalar(select(func.count(AcquisitionDispatchClaim.job_id))) == 0
            assert await observer.scalar(select(func.count(Job.id))) == 1


async def test_root_and_direct_track_admission_race_never_overlap(tmp_path: Path) -> None:
    async with _database(tmp_path / "root-track-race.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed)
            await seed.commit()
            album_id, item_id = album.id, item.id
        gate = asyncio.Event()
        mutex = asyncio.Lock()
        ready = 0

        async def wait_for_peer() -> None:
            nonlocal ready
            async with mutex:
                ready += 1
                if ready == 2:
                    gate.set()
            await gate.wait()

        async def admit_root():
            async with factory() as db:
                await wait_for_peer()
                return await _admit(db, item_id)

        async def admit_tracks():
            async with factory() as db:
                current_album = await db.get(CatalogAlbum, album_id)
                assert current_album is not None
                await wait_for_peer()
                return await expand_catalog_album_missing_track_jobs(
                    db,
                    current_album,
                    quality_profile=PROFILE,
                )

        root, tracks = await asyncio.gather(admit_root(), admit_tracks())
        async with factory() as observer:
            release_claims = int(
                await observer.scalar(select(func.count(CatalogReleaseAcquisitionClaim.job_id)))
                or 0
            )
            track_claims = int(
                await observer.scalar(select(func.count(AcquisitionDispatchClaim.job_id))) or 0
            )
            active_roots = int(
                await observer.scalar(
                    select(func.count(Job.id)).where(
                        Job.status.in_((JobStatus.pending, JobStatus.running)),
                        Job.catalog_album_id == album_id,
                        Job.catalog_track_id.is_(None),
                    )
                )
                or 0
            )
            active_tracks = int(
                await observer.scalar(
                    select(func.count(Job.id)).where(
                        Job.status.in_((JobStatus.pending, JobStatus.running)),
                        Job.catalog_album_id == album_id,
                        Job.catalog_track_id.is_not(None),
                    )
                )
                or 0
            )
        assert not (active_roots and active_tracks)
        assert (release_claims, track_claims) in {(1, 0), (0, 2)}
        if active_roots:
            assert root.status == ReleaseRootAdmissionStatus.created
            assert tracks.created_job_ids == ()
            assert tracks.observed_job_ids == (root.job_id,)
        else:
            assert root.status == ReleaseRootAdmissionStatus.waiting_for_tracks
            assert len(tracks.created_job_ids) == 2


async def test_malformed_active_track_claim_fails_closed(tmp_path: Path) -> None:
    async with _database(tmp_path / "malformed-track-claim.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed)
            malformed_owner = Job(
                source="priority",
                query="wrong track",
                status=JobStatus.running,
                catalog_album=album,
                catalog_track=album.tracks[1],
            )
            seed.add(malformed_owner)
            await seed.flush()
            seed.add(
                AcquisitionDispatchClaim(
                    catalog_album_id=album.id,
                    catalog_track_id=album.tracks[0].id,
                    job_id=malformed_owner.id,
                )
            )
            await seed.commit()
            item_id = item.id
        async with factory() as db:
            with pytest.raises(ValueError, match="exact track"):
                await _admit(db, item_id)
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 1
            assert (
                await observer.scalar(select(func.count(CatalogReleaseAcquisitionClaim.job_id)))
                == 0
            )


async def test_malformed_terminal_release_claim_fails_closed(tmp_path: Path) -> None:
    async with _database(tmp_path / "malformed-release-claim.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed)
            other_artist = CatalogArtist(name="Other", monitored=True)
            other_album = CatalogAlbum(
                artist=other_artist, title="Other", monitored=True, track_count=1
            )
            other_album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Other"))
            malformed_owner = Job(
                source="priority",
                query="wrong release",
                status=JobStatus.failed,
                catalog_album=other_album,
            )
            seed.add_all([other_artist, malformed_owner])
            await seed.flush()
            seed.add(
                CatalogReleaseAcquisitionClaim(
                    catalog_album_id=album.id,
                    job_id=malformed_owner.id,
                )
            )
            await seed.commit()
            item_id, malformed_owner_id = item.id, malformed_owner.id
        async with factory() as db:
            with pytest.raises(ValueError, match="exact release root"):
                await _admit(db, item_id)
        async with factory() as observer:
            claim = await observer.get(CatalogReleaseAcquisitionClaim, album.id)
            assert claim is not None and claim.job_id == malformed_owner_id
            assert await observer.scalar(select(func.count(Job.id))) == 1


async def test_quality_projection_retries_when_db_state_changes_before_reservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "library" / "owned.mp3"
    destination.parent.mkdir()
    destination.write_bytes(b"owned")
    async with _database(tmp_path / "projection-race.db") as factory:
        async with factory() as seed:
            album, (item,) = await _seed(seed, track_count=1)
            source_job = Job(source="fixture", query="owned", status=JobStatus.done)
            release = Release(job=source_job, source="fixture", title=album.title)
            track = Track(
                job=source_job,
                release=release,
                source="fixture",
                catalog_album=album,
                catalog_track=album.tracks[0],
                import_state=ImportWorkflowState.imported,
                acquisition_state=AcquisitionState.downloaded,
                file_format="mp3 128 kbps",
                file_size_bytes=5,
            )
            seed.add(
                ImportPlan(
                    release=release,
                    track=track,
                    source_path=str(destination),
                    destination_path=str(destination),
                    status=ImportWorkflowState.imported,
                    file_state=LibraryFileState.present,
                )
            )
            await seed.commit()
            item_id, track_id = item.id, track.id

        original_commit = AsyncSession.commit
        upgraded = False

        async def upgrade_after_projection_commit(session: AsyncSession) -> None:
            nonlocal upgraded
            await original_commit(session)
            if upgraded:
                return
            upgraded = True
            async with factory() as concurrent:
                current = await concurrent.get(Track, track_id)
                assert current is not None
                current.file_format = "flac"
                await original_commit(concurrent)

        monkeypatch.setattr(AsyncSession, "commit", upgrade_after_projection_commit)
        async with factory() as db:
            outcome = await _admit(db, item_id, library_root=destination.parent)
        assert upgraded
        assert outcome.status == ReleaseRootAdmissionStatus.no_work
        assert outcome.target_track_ids == ()
        async with factory() as observer:
            assert await observer.scalar(select(func.count(Job.id))) == 1
            assert (
                await observer.scalar(select(func.count(CatalogReleaseAcquisitionClaim.job_id)))
                == 0
            )
