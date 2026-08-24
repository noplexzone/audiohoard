from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import (
    DeletionOperation,
    DeletionOperationState,
    ImportPlan,
    LibraryFileState,
)
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.settings import AppSetting
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.library_reconciliation import FileInspection, LibraryReconciliationService


async def _seed_plan(
    db: AsyncSession,
    root: Path,
    name: str,
    *,
    state: LibraryFileState = LibraryFileState.unknown,
    destination: Path | None = None,
    track: Track | None = None,
) -> tuple[ImportPlan, Track, Release, CatalogAlbum]:
    path = destination or root / "Artist" / "Album" / name
    if track is None:
        artist = CatalogArtist(name=f"Artist {name}")
        album = CatalogAlbum(artist=artist, title="Album", track_count=1, in_library=True)
        catalog_track = CatalogAlbumTrack(album=album, position=1, disc=1, title=name)
        job = Job(source="slskd", query=name, status=JobStatus.done, catalog_album=album)
        release = Release(
            job=job,
            source="slskd",
            title="Album",
            track_count=1,
            import_state=ImportWorkflowState.imported,
        )
        track = Track(
            job=job,
            release=release,
            catalog_album=album,
            catalog_track=catalog_track,
            source="slskd",
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
            file_size_bytes=10,
        )
        db.add_all([artist, album, job, release, track])
    else:
        assert track.release is not None and track.catalog_album is not None
        release = track.release
        album = track.catalog_album
    plan = ImportPlan(
        release=release,
        track=track,
        source_path=str(path),
        destination_path=str(path),
        status=ImportWorkflowState.imported,
        file_state=state,
    )
    db.add(plan)
    await db.commit()
    return plan, track, release, album


async def test_sweep_backfills_unknown_then_external_unlink_and_exact_restore_update_truth(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    path = root / "Artist" / "Album" / "song.mp3"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"audio")
    plan, track, release, album = await _seed_plan(db_session, root, "song.mp3")
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(factory, root, batch_size=8, startup_batches=1)

    assert await service.reconcile_sweep(max_batches=1) == 1
    async with factory() as check:
        assert (await check.get(ImportPlan, plan.id)).file_state == LibraryFileState.present

    path.unlink()
    assert await service.reconcile_paths({path}) == 1
    async with factory() as check:
        checked_plan = await check.get(ImportPlan, plan.id)
        checked_track = await check.get(Track, track.id)
        checked_release = await check.get(Release, release.id)
        checked_album = await check.get(CatalogAlbum, album.id)
        assert checked_plan is not None
        assert checked_plan.file_state == LibraryFileState.missing
        assert checked_plan.file_checked_at is not None
        assert checked_plan.file_removed_at is not None
        assert checked_plan.file_removal_reason == "external"
        assert (
            checked_track is not None
            and checked_track.import_state == ImportWorkflowState.needs_review
        )
        assert (
            checked_release is not None
            and checked_release.import_state == ImportWorkflowState.needs_review
        )
        assert checked_album is not None and checked_album.in_library is False

    path.write_bytes(b"restored")
    assert await service.reconcile_paths({path}) == 1
    async with factory() as check:
        checked_plan = await check.get(ImportPlan, plan.id)
        assert checked_plan is not None and checked_plan.file_state == LibraryFileState.present
        assert checked_plan.file_removed_at is None and checked_plan.file_removal_reason is None
        checked_track = await check.get(Track, track.id)
        assert checked_track is not None
        assert checked_track.import_state == ImportWorkflowState.imported
        assert checked_track.file_size_bytes == len(b"restored")
        assert (await check.get(Release, release.id)).import_state == ImportWorkflowState.imported
        assert (await check.get(CatalogAlbum, album.id)).in_library is True


async def test_active_deletion_journal_suppresses_watcher_missing_state(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    path = root / "Artist" / "Album" / "deleting.mp3"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"audio")
    plan, *_ = await _seed_plan(db_session, root, "deleting.mp3", state=LibraryFileState.present)
    metadata = path.stat()
    db_session.add(
        DeletionOperation(
            group_id="active-delete",
            import_plan_id=plan.id,
            original_path=str(path),
            temporary_path=str(path.with_name(".deleting.mp3.audiohoard-delete-active")),
            expected_device=metadata.st_dev,
            expected_inode=metadata.st_ino,
            state=DeletionOperationState.prepared,
        )
    )
    await db_session.commit()
    path.unlink()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(factory, root)

    assert await service.reconcile_paths({path}) == 0

    async with factory() as check:
        checked = await check.get(ImportPlan, plan.id)
        assert checked is not None and checked.file_state == LibraryFileState.present


async def test_missing_plan_keeps_track_imported_when_an_exact_present_plan_survives(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    first = root / "first.mp3"
    second = root / "second.mp3"
    root.mkdir()
    first.write_bytes(b"first")
    plan, track, _, _ = await _seed_plan(
        db_session, root, "first.mp3", state=LibraryFileState.present, destination=first
    )
    survivor, _, _, _ = await _seed_plan(
        db_session,
        root,
        "second.mp3",
        state=LibraryFileState.present,
        destination=second,
        track=track,
    )
    second.write_bytes(b"second")
    first.unlink()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(factory, root)

    await service.reconcile_paths({first})

    async with factory() as check:
        assert (await check.get(ImportPlan, plan.id)).file_state == LibraryFileState.missing
        assert (await check.get(ImportPlan, survivor.id)).file_state == LibraryFileState.present
        assert (await check.get(Track, track.id)).import_state == ImportWorkflowState.imported


async def test_sweep_rejects_escape_and_symlink_without_starving_later_rows(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_file = outside / "outside.mp3"
    outside_file.write_bytes(b"outside")
    unsafe, *_ = await _seed_plan(db_session, root, "unsafe.mp3", destination=outside_file)
    (root / "linked").symlink_to(outside, target_is_directory=True)
    symlinked, *_ = await _seed_plan(
        db_session, root, "linked.mp3", destination=root / "linked" / "outside.mp3"
    )
    safe_path = root / "safe.mp3"
    safe_path.write_bytes(b"safe")
    safe, *_ = await _seed_plan(db_session, root, "safe.mp3", destination=safe_path)
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(factory, root, batch_size=1)

    for _ in range(3):
        await service.reconcile_sweep(max_batches=1)

    async with factory() as check:
        assert (await check.get(ImportPlan, unsafe.id)).file_state == LibraryFileState.unknown
        assert (await check.get(ImportPlan, symlinked.id)).file_state == LibraryFileState.unknown
        assert (await check.get(ImportPlan, safe.id)).file_state == LibraryFileState.present
        cursor = await check.get(AppSetting, "library_reconciliation_cursor")
        assert cursor is not None and int(cursor.value) >= safe.id


async def test_reconcile_paths_coalesces_duplicates_and_ignores_temporary_paths(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "library"
    path = root / "song.mp3"
    root.mkdir()
    path.write_bytes(b"audio")
    plan, *_ = await _seed_plan(
        db_session, root, "song.mp3", state=LibraryFileState.present, destination=path
    )
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(factory, root)
    calls = 0
    original = service._inspect_paths

    async def counted(paths: list[Path]) -> dict[Path, FileInspection | None]:
        nonlocal calls
        calls += 1
        return await original(paths)

    monkeypatch.setattr(service, "_inspect_paths", counted)
    path.unlink()
    changed = await service.reconcile_paths(
        {path, Path(str(path)), path.with_name(".song.mp3.audiohoard-delete-token")}
    )

    assert changed == 1 and calls == 1
    async with factory() as check:
        assert (await check.get(ImportPlan, plan.id)).file_state == LibraryFileState.missing


async def test_reconciliation_never_reactivates_deliberately_removed_plan(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    path = root / "song.mp3"
    root.mkdir()
    path.write_bytes(b"replacement")
    plan, track, *_ = await _seed_plan(
        db_session, root, "song.mp3", state=LibraryFileState.removed, destination=path
    )
    plan.status = ImportWorkflowState.removed
    plan.file_removal_reason = "user"
    track.import_state = ImportWorkflowState.removed
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(factory, root)

    assert await service.reconcile_paths({path}) == 0

    async with factory() as check:
        current = await check.get(ImportPlan, plan.id)
        assert current is not None and current.file_state == LibraryFileState.removed
        assert current.status == ImportWorkflowState.removed
        assert current.file_removal_reason == "user"
        assert (await check.get(Track, track.id)).import_state == ImportWorkflowState.removed


async def test_event_reconciliation_applies_exact_path_matches_in_bounded_batches(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "library"
    path = root / "shared.mp3"
    root.mkdir()
    path.write_bytes(b"shared")
    for index in range(3):
        await _seed_plan(
            db_session,
            root,
            f"shared-{index}.mp3",
            state=LibraryFileState.present,
            destination=path,
        )
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(factory, root, batch_size=2)
    applied_batch_sizes: list[int] = []
    original = service._apply

    async def bounded_apply(*args, **kwargs):  # noqa: ANN002, ANN003
        applied_batch_sizes.append(len(args[0]))
        return await original(*args, **kwargs)

    monkeypatch.setattr(service, "_apply", bounded_apply)
    path.unlink()

    assert await service.reconcile_paths({path}) == 3
    assert applied_batch_sizes == [2, 1]


async def test_periodic_sweep_repairs_missed_event_and_stop_leaks_no_tasks(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    path = root / "song.mp3"
    root.mkdir()
    path.write_bytes(b"audio")
    plan, *_ = await _seed_plan(
        db_session, root, "song.mp3", state=LibraryFileState.present, destination=path
    )
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)

    async def quiet_watch(_root: Path) -> AsyncIterator[set[tuple[object, str]]]:
        await asyncio.Event().wait()
        yield set()

    service = LibraryReconciliationService(
        factory, root, periodic_interval=0.01, watch_changes=quiet_watch
    )
    path.unlink()
    await service.start()
    async with asyncio.timeout(1):
        while True:
            async with factory() as check:
                if (await check.get(ImportPlan, plan.id)).file_state == LibraryFileState.missing:
                    break
            await asyncio.sleep(0.01)
    tasks = service.tasks
    await service.stop()

    assert tasks and all(task.done() for task in tasks)


async def test_watcher_error_is_retried_and_cancellation_propagates(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    root.mkdir()
    attempts = 0
    retried = asyncio.Event()

    async def broken_watch(_root: Path) -> AsyncIterator[set[tuple[object, str]]]:
        nonlocal attempts
        attempts += 1
        if attempts >= 2:
            retried.set()
        raise OSError("watch failed")
        yield set()

    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(
        factory,
        root,
        periodic_interval=60,
        watcher_retry_interval=0.01,
        watch_changes=broken_watch,
    )
    await service.start()
    async with asyncio.timeout(1):
        await retried.wait()
    tasks = service.tasks
    await service.stop()

    assert attempts >= 2 and all(task.cancelled() or task.done() for task in tasks)


async def test_real_watcher_promptly_reconciles_external_unlink(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    path = root / "song.mp3"
    root.mkdir()
    path.write_bytes(b"audio")
    plan, *_ = await _seed_plan(
        db_session, root, "song.mp3", state=LibraryFileState.present, destination=path
    )
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(factory, root, periodic_interval=60)

    await service.start()
    try:
        # Let watchfiles establish its OS watch before producing the external event.
        await asyncio.sleep(0.2)
        path.unlink()
        async with asyncio.timeout(3):
            while True:  # noqa: ASYNC110 - polling the independently committed database state
                async with factory() as check:
                    current = await check.get(ImportPlan, plan.id)
                    if current is not None and current.file_state == LibraryFileState.missing:
                        break
                await asyncio.sleep(0.02)
    finally:
        await service.stop()


async def test_start_can_wait_for_initial_periodic_cycle(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "library"
    root.mkdir()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    service = LibraryReconciliationService(factory, root, periodic_interval=60)
    cycles: list[str] = []

    async def sweep(*, max_batches=None) -> int:
        cycles.append("initial")
        return 0

    monkeypatch.setattr(service, "reconcile_sweep", sweep)
    await service.start(wait_for_initial_cycle=True)
    try:
        assert cycles == ["initial"]
    finally:
        await service.stop()
