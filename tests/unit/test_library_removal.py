from __future__ import annotations

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
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services import library_removal
from app.services.acquisition_cleanup import prune_orphaned_terminal_records
from app.services.library_removal import (
    LibraryRemovalError,
    recover_deletion_operations,
    remove_catalog_album,
    remove_imported_release_group,
    remove_imported_track,
)


async def _seed_album(
    db: AsyncSession, root: Path, *, names: tuple[str, ...] = ("one.mp3", "two.mp3")
) -> tuple[CatalogAlbum, list[Track], list[ImportPlan], list[Path]]:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(artist=artist, title="Album", track_count=len(names), in_library=True)
    job = Job(source="slskd", query="album", status=JobStatus.done, catalog_album=album)
    release = Release(
        job=job,
        source="slskd",
        title="Album",
        album_artist="Artist",
        track_count=len(names),
        import_state=ImportWorkflowState.imported,
    )
    tracks, plans, paths = [], [], []
    for position, name in enumerate(names, 1):
        catalog_track = CatalogAlbumTrack(album=album, position=position, disc=1, title=name)
        path = root / "Artist" / "Album" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
        track = Track(
            job=job,
            release=release,
            catalog_album=album,
            catalog_track=catalog_track,
            source="slskd",
            title=name,
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
            file_size_bytes=len(name),
        )
        plan = ImportPlan(
            release=release,
            track=track,
            source_path=str(path),
            destination_path=str(path),
            status=ImportWorkflowState.imported,
            file_state=LibraryFileState.present,
        )
        tracks.append(track)
        plans.append(plan)
        paths.append(path)
    db.add_all([artist, album, job, release, *tracks, *plans])
    await db.commit()
    return album, tracks, plans, paths


async def test_single_removal_deletes_only_newest_canonical_plan_and_invalidates_cache(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    _, tracks, plans, paths = await _seed_album(db_session, root, names=("old.mp3",))
    newer_path = paths[0].with_name("new.mp3")
    newer_path.write_bytes(b"new")
    newer = ImportPlan(
        release_id=plans[0].release_id,
        track_id=tracks[0].id,
        source_path=str(newer_path),
        destination_path=str(newer_path),
        status=ImportWorkflowState.imported,
        file_state=LibraryFileState.present,
    )
    db_session.add(newer)
    await db_session.commit()
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / f"{tracks[0].id}-preview.mp3").write_bytes(b"cached")
    result = await remove_imported_track(
        db_session, tracks[0].id, library_root=root, cache_root=cache
    )
    assert (
        result.deleted_files == 1
        and paths[0].read_bytes() == b"old.mp3"
        and not newer_path.exists()
    )
    assert (
        plans[0].status == ImportWorkflowState.imported
        and newer.status == ImportWorkflowState.removed
    )
    assert tracks[0].import_state == ImportWorkflowState.imported and not list(
        cache.glob(f"{tracks[0].id}-*.mp3")
    )


async def test_album_removal_is_all_or_nothing_when_second_rename_fails(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "library"
    album, tracks, plans, paths = await _seed_album(db_session, root)
    real_stage = library_removal._stage_target
    calls = 0

    def fail_second(target):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected rename failure")
        return real_stage(target)

    monkeypatch.setattr(library_removal, "_stage_target", fail_second)
    with pytest.raises(LibraryRemovalError, match="could not be completed"):
        await remove_catalog_album(
            db_session, album.id, library_root=root, cache_root=tmp_path / "c"
        )
    assert [path.read_bytes() for path in paths] == [b"one.mp3", b"two.mp3"]
    for track in tracks:
        await db_session.refresh(track)
    for plan in plans:
        await db_session.refresh(plan)
    await db_session.refresh(album)
    assert album.in_library is True
    assert all(plan.status == ImportWorkflowState.imported for plan in plans)
    assert all(track.import_state == ImportWorkflowState.imported for track in tracks)


async def test_db_failure_restores_all_files_and_imported_truth(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "library"
    album, tracks, plans, paths = await _seed_album(db_session, root)
    original_commit = AsyncSession.commit
    calls = 0

    async def fail_state_commit(self: AsyncSession) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected DB failure")
        await original_commit(self)

    monkeypatch.setattr(AsyncSession, "commit", fail_state_commit)
    with pytest.raises(LibraryRemovalError, match="could not be completed"):
        await remove_catalog_album(
            db_session, album.id, library_root=root, cache_root=tmp_path / "c"
        )
    assert [path.read_bytes() for path in paths] == [b"one.mp3", b"two.mp3"]
    for track in tracks:
        await db_session.refresh(track)
    for plan in plans:
        await db_session.refresh(plan)
    await db_session.refresh(album)
    assert album.in_library is True
    assert all(plan.status == ImportWorkflowState.imported for plan in plans)
    assert all(track.import_state == ImportWorkflowState.imported for track in tracks)


async def test_post_commit_failure_never_restores_removed_file(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "library"
    _, tracks, plans, paths = await _seed_album(db_session, root, names=("committed.mp3",))

    def fail_cleanup(_target):
        raise RuntimeError("injected post-commit cleanup failure")

    monkeypatch.setattr(library_removal, "_unlink_target", fail_cleanup)
    with pytest.raises(LibraryRemovalError, match="recovery is required"):
        await remove_imported_track(
            db_session, tracks[0].id, library_root=root, cache_root=tmp_path / "c"
        )

    await db_session.refresh(plans[0])
    assert plans[0].file_state == LibraryFileState.removed
    assert not paths[0].exists()
    temporary = list(paths[0].parent.glob("*.audiohoard-delete-*"))
    assert len(temporary) == 1


async def test_committed_recovery_deletes_matching_file_restored_to_original_name(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    _, tracks, plans, paths = await _seed_album(db_session, root, names=("restored.mp3",))
    original = paths[0]
    metadata = original.stat()
    plans[0].file_state = LibraryFileState.removed
    plans[0].status = ImportWorkflowState.removed
    tracks[0].import_state = ImportWorkflowState.removed
    operation = DeletionOperation(
        group_id="committed-original-group",
        import_plan_id=plans[0].id,
        original_path=str(original),
        temporary_path=str(original.with_name(".restored.mp3.audiohoard-delete-recovery")),
        expected_device=metadata.st_dev,
        expected_inode=metadata.st_ino,
        state=DeletionOperationState.committed,
    )
    db_session.add(operation)
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)

    await recover_deletion_operations(factory, library_root=root, cache_root=tmp_path / "c")

    assert not original.exists()
    async with factory() as check:
        recovered = await check.get(DeletionOperation, operation.id)
        assert recovered is not None and recovered.state == DeletionOperationState.finalized


async def test_imported_release_group_removes_all_present_files(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    _, tracks, plans, paths = await _seed_album(db_session, root)

    result = await remove_imported_release_group(
        db_session,
        release_id=plans[0].release_id,
        artist_name="ignored",
        album_title="ignored",
        year="",
        library_root=root,
        cache_root=tmp_path / "c",
    )

    assert result.deleted_files == 2
    assert result.affected_track_ids == tuple(sorted(track.id for track in tracks))
    assert all(not path.exists() for path in paths)


async def test_imported_fallback_group_uses_exact_artist_album_and_year(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    _, tracks, _, paths = await _seed_album(db_session, root)
    for track in tracks:
        track.release = None
        track.album_artist = "Fallback Artist"
        track.album = "Fallback Album"
        track.year = "2024"
    await db_session.commit()

    result = await remove_imported_release_group(
        db_session,
        release_id=None,
        artist_name="Fallback Artist",
        album_title="Fallback Album",
        year="2024",
        library_root=root,
        cache_root=tmp_path / "c",
    )

    assert result.deleted_files == 2
    assert all(not path.exists() for path in paths)


async def test_album_rejects_unsafe_member_before_mutating_any_file(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    album, _, plans, paths = await _seed_album(db_session, root)
    outside = tmp_path / "outside.mp3"
    outside.write_bytes(b"outside")
    plans[1].destination_path = str(outside)
    await db_session.commit()
    with pytest.raises(LibraryRemovalError, match="not safe"):
        await remove_catalog_album(
            db_session, album.id, library_root=root, cache_root=tmp_path / "c"
        )
    assert (
        paths[0].exists()
        and outside.read_bytes() == b"outside"
        and not list(root.rglob("*.audiohoard-delete-*"))
    )


async def test_missing_file_becomes_truthfully_removed_and_repeated_delete_is_idempotent(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    _, tracks, plans, paths = await _seed_album(db_session, root, names=("gone.mp3",))
    paths[0].unlink()
    first = await remove_imported_track(
        db_session, tracks[0].id, library_root=root, cache_root=tmp_path / "c"
    )
    second = await remove_imported_track(
        db_session, tracks[0].id, library_root=root, cache_root=tmp_path / "c"
    )
    assert (
        first.deleted_files == 0 and second.deleted_files == 0 and second.already_removed is True
    )
    assert (
        plans[0].file_state == LibraryFileState.removed
        and plans[0].status == ImportWorkflowState.removed
        and tracks[0].import_state == ImportWorkflowState.removed
    )


async def test_prepared_and_committed_recovery_are_idempotent(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    _, tracks, plans, paths = await _seed_album(db_session, root, names=("recover.mp3",))
    original = paths[0]
    temporary = original.with_name(".recover.mp3.audiohoard-delete-test")
    original.replace(temporary)
    metadata = temporary.stat()
    operation = DeletionOperation(
        group_id="prepared-group",
        import_plan_id=plans[0].id,
        original_path=str(original),
        temporary_path=str(temporary),
        expected_device=metadata.st_dev,
        expected_inode=metadata.st_ino,
        state=DeletionOperationState.prepared,
    )
    db_session.add(operation)
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    await recover_deletion_operations(factory, library_root=root, cache_root=tmp_path / "c")
    await recover_deletion_operations(factory, library_root=root, cache_root=tmp_path / "c")
    assert original.read_bytes() == b"recover.mp3" and not temporary.exists()
    async with factory() as check:
        recovered = await check.get(DeletionOperation, operation.id)
        assert recovered is not None and recovered.state == DeletionOperationState.finalized
    original.replace(temporary)
    metadata = temporary.stat()
    async with factory() as db:
        plan = await db.get(ImportPlan, plans[0].id)
        track = await db.get(Track, tracks[0].id)
        assert plan and track
        plan.file_state = LibraryFileState.removed
        plan.status = ImportWorkflowState.removed
        track.import_state = ImportWorkflowState.removed
        db.add(
            DeletionOperation(
                group_id="committed-group",
                import_plan_id=plan.id,
                original_path=str(original),
                temporary_path=str(temporary),
                expected_device=metadata.st_dev,
                expected_inode=metadata.st_ino,
                state=DeletionOperationState.committed,
            )
        )
        await db.commit()
    await recover_deletion_operations(factory, library_root=root, cache_root=tmp_path / "c")
    await recover_deletion_operations(factory, library_root=root, cache_root=tmp_path / "c")
    assert not original.exists() and not temporary.exists()


async def test_symlink_ancestor_is_rejected_without_touching_outside(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    root.mkdir()
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    outside = outside_dir / "song.mp3"
    outside.write_bytes(b"outside")
    (root / "Artist").symlink_to(outside_dir, target_is_directory=True)
    _, tracks, plans, _ = await _seed_album(db_session, tmp_path / "safe", names=("seed.mp3",))
    plans[0].destination_path = str(root / "Artist" / "song.mp3")
    await db_session.commit()
    with pytest.raises(LibraryRemovalError, match="not safe"):
        await remove_imported_track(
            db_session, tracks[0].id, library_root=root, cache_root=tmp_path / "c"
        )
    assert outside.read_bytes() == b"outside"


async def test_terminal_prune_preserves_removed_library_history(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    root = tmp_path / "library"
    _, tracks, plans, _ = await _seed_album(db_session, root, names=("history.mp3",))
    track_id = tracks[0].id
    release_id = plans[0].release_id
    job_id = tracks[0].job_id

    await remove_imported_track(
        db_session,
        track_id,
        library_root=root,
        cache_root=tmp_path / "cache",
    )
    result = await prune_orphaned_terminal_records(db_session, batch_size=1)

    assert result.tracks == 0
    assert result.releases == 0
    assert result.jobs == 0
    assert await db_session.get(Track, track_id) is not None
    assert await db_session.get(Release, release_id) is not None
    assert await db_session.get(Job, job_id) is not None
