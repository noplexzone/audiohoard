from __future__ import annotations

from pathlib import Path

import pytest

from app.models.import_plan import ImportPlan
from app.models.job import Job
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.library_scan import scan_library_filesystem


def _import_graph(
    destination: Path, *, status: ImportWorkflowState = ImportWorkflowState.imported
) -> tuple[Job, Release, Track, ImportPlan]:
    job = Job(source="test", query="album")
    release = Release(job=job, source="test", title="Album")
    track = Track(
        job=job,
        release=release,
        source="test",
        title="Song",
        import_state=ImportWorkflowState.imported,
    )
    plan = ImportPlan(
        release=release,
        track=track,
        source_path="/source/song.flac",
        destination_path=str(destination),
        status=status,
    )
    return job, release, track, plan


@pytest.mark.asyncio
async def test_disk_file_without_import_plan_is_orphan(db_session, tmp_path: Path) -> None:
    library = tmp_path / "library"
    orphan = library / "Artist" / "Album" / "01 - Song.flac"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"audio")

    result = await scan_library_filesystem(db_session, library_root=library)

    assert result.scanned_files == 1
    assert result.matched == 0
    assert result.orphans == (str(orphan.resolve()),)


@pytest.mark.asyncio
async def test_imported_plan_missing_from_disk_is_missing(db_session, tmp_path: Path) -> None:
    library = tmp_path / "library"
    missing = library / "Artist" / "Album" / "01 - Song.flac"
    db_session.add(_import_graph(missing)[0])
    await db_session.commit()

    result = await scan_library_filesystem(db_session, library_root=library)

    assert result.scanned_files == 0
    assert result.matched == 0
    assert result.missing == (str(missing.resolve(strict=False)),)


@pytest.mark.asyncio
async def test_symlinked_directory_component_is_skipped(db_session, tmp_path: Path) -> None:
    library = tmp_path / "library"
    real_dir = tmp_path / "real"
    real_file = real_dir / "01 - Song.flac"
    real_file.parent.mkdir(parents=True)
    real_file.write_bytes(b"audio")
    library.mkdir()
    linked = library / "Linked"
    linked.symlink_to(real_dir, target_is_directory=True)
    destination = linked / real_file.name
    db_session.add(_import_graph(destination)[0])
    await db_session.commit()

    result = await scan_library_filesystem(db_session, library_root=library)

    assert result.scanned_files == 0
    assert result.matched == 0
    assert result.orphans == ()
    assert result.missing == ()
