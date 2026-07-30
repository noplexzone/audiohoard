from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.media_formats import IMPORTABLE_AUDIO_EXTENSIONS, is_importable_audio
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import CollisionState, ImportPlan, TagVerificationState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.library_import import (
    ImportExecutionError,
    MutagenTagWriter,
    execute_release_import,
    plan_release_import,
)
from app.services.pinned_destination import PinnedDestination


async def _release_with_staged_tracks(
    db_session: AsyncSession, tmp_path: Path, count: int = 1, suffix: str = ".mp3"
) -> tuple[Release, list[Track]]:
    staging = tmp_path / "staging"
    staging.mkdir()
    job = Job(source="slskd", query="artist album", status=JobStatus.done)
    db_session.add(job)
    await db_session.flush()
    release = Release(
        job_id=job.id,
        source="slskd",
        title="Album",
        album_artist="Artist",
        year="1999",
        release_mbid="release-mbid",
        track_count=count,
        staging_path=str(staging),
        import_state=ImportWorkflowState.ready,
    )
    db_session.add(release)
    await db_session.flush()
    tracks: list[Track] = []
    for index in range(1, count + 1):
        source = staging / f"{index:02d}{suffix}"
        source_bytes = _minimal_flac_bytes() if suffix == ".flac" else f"audio-{index}".encode()
        source.write_bytes(source_bytes)
        track = Track(
            job_id=job.id,
            release_id=release.id,
            title=f"Song {index}",
            artist="Artist",
            album_artist="Artist",
            album="Album",
            year="1999",
            disc=1,
            disc_total=1,
            track_no=index,
            mbid=f"recording-{index}",
            source="slskd",
            staging_path=str(source),
            source_path=str(source),
            import_state=ImportWorkflowState.ready,
        )
        db_session.add(track)
        tracks.append(track)
    await db_session.flush()
    return release, tracks


def _minimal_flac_bytes() -> bytes:
    min_block_size = (4096).to_bytes(2, "big")
    max_block_size = (4096).to_bytes(2, "big")
    min_frame_size = (0).to_bytes(3, "big")
    max_frame_size = (0).to_bytes(3, "big")
    stream_info = (
        min_block_size
        + max_block_size
        + min_frame_size
        + max_frame_size
        + ((44100 << 44) | (15 << 36)).to_bytes(8, "big")
        + bytes(16)
    )
    return b"fLaC" + bytes([0x80, 0, 0, 34]) + stream_info


async def test_plan_detects_same_path_conflict_and_same_bytes_duplicate(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    destination = library / "Artist" / "Album (1999)" / "01 - Song 1.mp3"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"different")

    plans = await plan_release_import(db_session, release, library_root=library)
    assert plans[0].collision_state == CollisionState.conflict
    assert plans[0].status == ImportWorkflowState.needs_review
    assert "different bytes" in (plans[0].error_detail or "")

    destination.write_bytes((Path(tracks[0].staging_path or "")).read_bytes())  # noqa: ASYNC240
    plans = await plan_release_import(db_session, release, library_root=library)
    assert plans[0].collision_state == CollisionState.duplicate
    assert plans[0].status == ImportWorkflowState.needs_review
    assert "same bytes" in (plans[0].error_detail or "")


async def test_plan_rejects_symlink_destination_parent(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, _tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    library.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (library / "Artist").symlink_to(outside, target_is_directory=True)

    plans = await plan_release_import(db_session, release, library_root=library)

    assert plans[0].collision_state == CollisionState.needs_review
    assert plans[0].status == ImportWorkflowState.needs_review
    assert "symlink" in (plans[0].error_detail or "")


async def test_execute_import_copies_to_destination_temp_writes_verified_tags_and_retains_source(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    plans = await plan_release_import(db_session, release, library_root=library)
    source = Path(tracks[0].staging_path or "")

    imported = await execute_release_import(
        db_session, release, library_root=library, tag_writer=MutagenTagWriter()
    )

    destination = Path(imported[0].destination_path)
    assert destination.exists()  # noqa: ASYNC240
    assert source.exists()  # noqa: ASYNC240
    assert imported[0].destination_temp_path is not None
    assert str(destination.parent) in imported[0].destination_temp_path
    assert imported[0].tag_verification_state == TagVerificationState.verified
    assert imported[0].status == ImportWorkflowState.imported
    assert release.import_state == ImportWorkflowState.imported
    readback = MutagenTagWriter().read_tags(destination)
    assert readback["title"] == "Song 1"
    assert readback["musicbrainz_trackid"] == "recording-1"
    assert plans[0].planned_operations_json is not None

    await db_session.commit()
    await asyncio.sleep(0)
    assert not source.exists()  # noqa: ASYNC240
    assert destination.exists()  # noqa: ASYNC240


async def test_execute_import_rejects_source_symlink_swap_after_planning(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    plans = await plan_release_import(db_session, release, library_root=library)
    source = Path(tracks[0].staging_path or "")
    original_bytes = source.read_bytes()  # noqa: ASYNC240
    outside = tmp_path / "outside.mp3"
    outside.write_bytes(original_bytes)
    source.unlink()  # noqa: ASYNC240
    source.symlink_to(outside)  # noqa: ASYNC240

    with pytest.raises(ImportExecutionError, match="regular non-symlink"):
        await execute_release_import(
            db_session, release, library_root=library, tag_writer=MutagenTagWriter()
        )

    destination = Path(plans[0].destination_path)
    assert not destination.exists()  # noqa: ASYNC240
    assert not list(library.rglob(f".{destination.name}.*"))
    assert release.import_state == ImportWorkflowState.rolled_back
    assert plans[0].status == ImportWorkflowState.failed


async def test_execute_import_rejects_source_ancestor_symlink_swap_after_planning(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    source = Path(tracks[0].staging_path or "")
    nested = source.parent / "nested"
    nested.mkdir()
    nested_source = nested / source.name
    source.rename(nested_source)  # noqa: ASYNC240
    tracks[0].staging_path = str(nested_source)
    tracks[0].source_path = str(nested_source)
    release.staging_path = str(source.parent)
    plans = await plan_release_import(db_session, release, library_root=library)
    original_bytes = nested_source.read_bytes()  # noqa: ASYNC240
    original_nested = source.parent / "nested-original"
    nested.rename(original_nested)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / source.name).write_bytes(original_bytes)
    nested.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ImportExecutionError, match="regular non-symlink"):
        await execute_release_import(
            db_session, release, library_root=library, tag_writer=MutagenTagWriter()
        )

    destination = Path(plans[0].destination_path)
    assert not destination.exists()  # noqa: ASYNC240
    assert not list(library.rglob(f".{destination.name}.*"))
    assert release.import_state == ImportWorkflowState.rolled_back
    assert plans[0].status == ImportWorkflowState.failed


async def test_execute_import_writes_and_verifies_flac_tags(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, _tracks = await _release_with_staged_tracks(
        db_session, tmp_path, count=1, suffix=".flac"
    )
    library = tmp_path / "library"
    await plan_release_import(db_session, release, library_root=library)

    imported = await execute_release_import(
        db_session, release, library_root=library, tag_writer=MutagenTagWriter()
    )

    destination = Path(imported[0].destination_path)
    assert destination.suffix == ".flac"
    assert imported[0].tag_verification_state == TagVerificationState.verified
    readback = MutagenTagWriter().read_tags(destination)
    assert readback["title"] == "Song 1"
    assert readback["release_date"] == "1999"
    assert readback["musicbrainz_albumid"] == "release-mbid"


async def test_plan_rejects_unsupported_formats_before_execution(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, _tracks = await _release_with_staged_tracks(
        db_session, tmp_path, count=1, suffix=".wav"
    )
    library = tmp_path / "library"

    plans = await plan_release_import(db_session, release, library_root=library)

    assert plans[0].status == ImportWorkflowState.needs_review
    assert plans[0].collision_state == CollisionState.needs_review
    assert "unsupported audio format '.wav'" in (plans[0].error_detail or "")
    with pytest.raises(ImportExecutionError, match="not ready"):
        await execute_release_import(db_session, release, library_root=library)


async def test_execute_import_rechecks_destination_race_and_rolls_back(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, _tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    await plan_release_import(db_session, release, library_root=library)

    def race(destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"racer")

    with pytest.raises(ImportExecutionError, match="destination appeared"):
        await execute_release_import(
            db_session,
            release,
            library_root=library,
            tag_writer=MutagenTagWriter(),
            before_commit=race,
        )

    assert release.import_state == ImportWorkflowState.rolled_back
    assert list(library.rglob("*.tmp")) == []


class FailingSecondTagWriter(MutagenTagWriter):
    def __init__(self) -> None:
        self.calls = 0

    def write_and_verify(self, path: Path, tags: dict[str, str]) -> bool:
        self.calls += 1
        return self.calls == 1 and super().write_and_verify(path, tags)


async def test_rollback_removes_prior_imported_tracks_after_later_tag_failure(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=2)
    library = tmp_path / "library"
    await plan_release_import(db_session, release, library_root=library)

    with pytest.raises(ImportExecutionError, match="tag readback failed"):
        await execute_release_import(
            db_session, release, library_root=library, tag_writer=FailingSecondTagWriter()
        )

    assert release.import_state == ImportWorkflowState.rolled_back
    assert not list(library.rglob("*.mp3"))
    assert Path(tracks[0].staging_path or "").exists()  # noqa: ASYNC240
    assert Path(tracks[1].staging_path or "").exists()  # noqa: ASYNC240


async def _assert_persisted_import_state_unchanged(
    db_session: AsyncSession, release_id: int, track_id: int
) -> None:
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    async with factory() as fresh:
        persisted_release = await fresh.get(Release, release_id)
        persisted_track = await fresh.get(Track, track_id)
        persisted_plan = (
            await fresh.execute(select(ImportPlan).where(ImportPlan.release_id == release_id))
        ).scalar_one()
        assert persisted_release is not None
        assert persisted_track is not None
        assert persisted_release.import_state == ImportWorkflowState.ready
        assert persisted_track.import_state == ImportWorkflowState.ready
        assert persisted_plan.status == ImportWorkflowState.ready


async def test_commit_failure_removes_new_destination_and_restores_persisted_state(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    plans = await plan_release_import(db_session, release, library_root=library)
    await db_session.commit()
    release_id = release.id
    track_id = tracks[0].id
    assert release_id is not None
    assert track_id is not None
    source = Path(tracks[0].staging_path or "")

    await execute_release_import(
        db_session, release, library_root=library, tag_writer=MutagenTagWriter()
    )
    destination = Path(plans[0].destination_path)
    assert destination.exists()  # noqa: ASYNC240

    original_commit = type(db_session.sync_session).commit

    def fail_commit(_session: object) -> None:
        raise RuntimeError("forced commit failure")

    monkeypatch.setattr(type(db_session.sync_session), "commit", fail_commit)
    with pytest.raises(RuntimeError, match="forced commit failure"):
        await db_session.commit()
    monkeypatch.setattr(type(db_session.sync_session), "commit", original_commit)
    await db_session.rollback()

    assert not destination.exists()  # noqa: ASYNC240
    assert source.exists()  # noqa: ASYNC240
    await _assert_persisted_import_state_unchanged(db_session, release_id, track_id)


async def test_commit_failure_restores_replaced_destination_and_persisted_state(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    plans = await plan_release_import(db_session, release, library_root=library)
    await db_session.commit()
    release_id = release.id
    track_id = tracks[0].id
    assert release_id is not None
    assert track_id is not None
    destination = Path(plans[0].destination_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    old_bytes = b"verified-old-library-bytes"
    destination.write_bytes(old_bytes)  # noqa: ASYNC240
    source = Path(tracks[0].staging_path or "")

    await execute_release_import(
        db_session,
        release,
        library_root=library,
        tag_writer=MutagenTagWriter(),
        replace_existing_verified=True,
    )
    assert destination.read_bytes() != old_bytes  # noqa: ASYNC240

    original_commit = type(db_session.sync_session).commit

    def fail_commit(_session: object) -> None:
        raise RuntimeError("forced commit failure")

    monkeypatch.setattr(type(db_session.sync_session), "commit", fail_commit)
    with pytest.raises(RuntimeError, match="forced commit failure"):
        await db_session.commit()
    monkeypatch.setattr(type(db_session.sync_session), "commit", original_commit)
    await db_session.rollback()

    assert destination.read_bytes() == old_bytes  # noqa: ASYNC240
    assert source.exists()  # noqa: ASYNC240
    assert not list(destination.parent.glob(f".{destination.name}.*.backup"))
    await _assert_persisted_import_state_unchanged(db_session, release_id, track_id)


async def test_destination_ancestor_swap_immediately_before_commit_cannot_escape_root(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, _tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    plans = await plan_release_import(db_session, release, library_root=library)
    destination = Path(plans[0].destination_path)
    moved_parent = tmp_path / "moved-original-parent"
    outside = tmp_path / "outside-target"
    outside.mkdir()

    def swap_parent(target: Path) -> None:
        target.parent.rename(moved_parent)
        target.parent.symlink_to(outside, target_is_directory=True)
        pinned_temp = next(moved_parent.glob(f".{target.name}.*{target.suffix}"))
        (outside / pinned_temp.name).write_bytes(b"attacker-controlled")

    with pytest.raises(ImportExecutionError, match="destination directory changed"):
        await execute_release_import(
            db_session,
            release,
            library_root=library,
            tag_writer=MutagenTagWriter(),
            before_commit=swap_parent,
        )

    assert not (outside / destination.name).exists()
    assert sorted(path.name for path in outside.iterdir()) == sorted(
        path.name for path in outside.iterdir() if path.name.startswith(f".{destination.name}.")
    )
    assert Path(plans[0].source_path).exists()  # noqa: ASYNC240


async def test_rollback_restores_backup_when_first_destination_unlink_fails(
    db_session: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    library = tmp_path / "library"
    plans = await plan_release_import(db_session, release, library_root=library)
    await db_session.commit()
    destination = Path(plans[0].destination_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    old_bytes = b"old-library-bytes"
    destination.write_bytes(old_bytes)  # noqa: ASYNC240
    staged_source = Path(tracks[0].staging_path or "")

    await execute_release_import(
        db_session,
        release,
        library_root=library,
        tag_writer=MutagenTagWriter(),
        replace_existing_verified=True,
    )
    assert destination.read_bytes() != old_bytes  # noqa: ASYNC240

    original_unlink = PinnedDestination.unlink
    failed_once = False

    def fail_first_destination_unlink(self: PinnedDestination, name: str) -> None:
        nonlocal failed_once
        if name == self.name and not failed_once:
            failed_once = True
            raise PermissionError("forced destination cleanup failure")
        original_unlink(self, name)

    monkeypatch.setattr(PinnedDestination, "unlink", fail_first_destination_unlink)
    await db_session.rollback()

    assert failed_once
    assert destination.read_bytes() == old_bytes  # noqa: ASYNC240
    assert staged_source.exists()  # noqa: ASYNC240


def test_audio_format_contract_excludes_unverifiable_formats() -> None:
    assert {"flac", "mp3", "m4a", "mp4", "ogg", "oga", "opus"} == set(IMPORTABLE_AUDIO_EXTENSIONS)
    for extension in IMPORTABLE_AUDIO_EXTENSIONS:
        assert is_importable_audio(f"track.{extension}")
    for extension in {"aac", "wav", "webm", "aiff", "wv"}:
        assert not is_importable_audio(f"track.{extension}")


async def _link_catalog_album(
    db: AsyncSession,
    release: Release,
    tracks: list[Track],
    *,
    expected_count: int,
) -> CatalogAlbum:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(title="Album", monitored=True, in_library=False)
    artist.albums.append(album)
    expected = [
        CatalogAlbumTrack(position=index, disc=1, title=f"Song {index}")
        for index in range(1, expected_count + 1)
    ]
    album.tracks.extend(expected)
    db.add(artist)
    await db.flush()
    for track, catalog_track in zip(tracks, expected, strict=False):
        track.catalog_album_id = album.id
        track.catalog_track_id = catalog_track.id
    await db.flush()
    return album


async def test_complete_import_marks_catalog_album_in_library(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=2)
    album = await _link_catalog_album(db_session, release, tracks, expected_count=2)
    library = tmp_path / "library"
    await plan_release_import(db_session, release, library_root=library)

    await execute_release_import(db_session, release, library_root=library)

    assert album.in_library is True


async def test_incomplete_import_keeps_catalog_album_wanted(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    album = await _link_catalog_album(db_session, release, tracks, expected_count=2)
    library = tmp_path / "library"
    await plan_release_import(db_session, release, library_root=library)

    await execute_release_import(db_session, release, library_root=library)

    assert album.in_library is False


async def test_failed_import_does_not_change_catalog_ownership(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=2)
    album = await _link_catalog_album(db_session, release, tracks, expected_count=2)
    library = tmp_path / "library"
    await plan_release_import(db_session, release, library_root=library)

    with pytest.raises(ImportExecutionError, match="tag readback failed"):
        await execute_release_import(
            db_session,
            release,
            library_root=library,
            tag_writer=FailingSecondTagWriter(),
        )

    assert album.in_library is False


async def test_partial_import_preserves_committed_plan_and_imports_later_track(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=2)
    library = tmp_path / "library"

    first_plans = await plan_release_import(
        db_session,
        release,
        library_root=library,
        track_ids={tracks[0].id},
    )
    await execute_release_import(db_session, release, library_root=library)

    assert first_plans[0].status == ImportWorkflowState.imported
    assert tracks[0].import_state == ImportWorkflowState.imported
    assert tracks[1].import_state != ImportWorkflowState.imported
    assert release.import_state != ImportWorkflowState.imported

    second_plans = await plan_release_import(
        db_session,
        release,
        library_root=library,
        track_ids={tracks[1].id},
    )
    persisted = list(
        (
            await db_session.scalars(
                select(ImportPlan)
                .where(ImportPlan.release_id == release.id)
                .order_by(ImportPlan.id)
            )
        ).all()
    )
    assert persisted == [first_plans[0], second_plans[0]]

    await execute_release_import(db_session, release, library_root=library)

    assert first_plans[0].status == ImportWorkflowState.imported
    assert second_plans[0].status == ImportWorkflowState.imported
    assert all(track.import_state == ImportWorkflowState.imported for track in tracks)
    assert release.import_state == ImportWorkflowState.imported
    assert all(
        Path(plan.destination_path).is_file()  # noqa: ASYNC240
        for plan in persisted
    )


async def test_execute_import_is_scoped_to_selected_ready_plan_ids(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=2)
    library = tmp_path / "library"
    plans = await plan_release_import(db_session, release, library_root=library)

    await execute_release_import(
        db_session,
        release,
        library_root=library,
        plan_ids={plans[0].id},
    )

    assert plans[0].status == ImportWorkflowState.imported
    assert tracks[0].import_state == ImportWorkflowState.imported
    assert plans[1].status == ImportWorkflowState.ready
    assert tracks[1].import_state != ImportWorkflowState.imported
    assert not Path(plans[1].destination_path).exists()  # noqa: ASYNC240

    await execute_release_import(
        db_session,
        release,
        library_root=library,
        plan_ids={plans[1].id},
    )
    assert plans[1].status == ImportWorkflowState.imported
    assert release.import_state == ImportWorkflowState.imported


async def test_catalog_scoped_import_without_catalog_track_id_stays_incomplete(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    release, tracks = await _release_with_staged_tracks(db_session, tmp_path, count=1)
    artist = CatalogArtist(name="Juice WRLD")
    album = CatalogAlbum(title="AGATS2 (Insecure)", track_count=1)
    catalog_track = CatalogAlbumTrack(position=1, disc=1, title="AGATS2 (Insecure)")
    album.tracks.append(catalog_track)
    artist.albums.append(album)
    db_session.add(artist)
    await db_session.flush()
    tracks[0].catalog_album_id = album.id
    tracks[0].catalog_track_id = None

    library = tmp_path / "library"
    await plan_release_import(db_session, release, library_root=library)
    await execute_release_import(db_session, release, library_root=library)

    assert tracks[0].import_state == ImportWorkflowState.imported
    assert release.import_state == ImportWorkflowState.needs_review
    assert "still require review" in (release.error_detail or "")


def test_catalog_tags_include_disc_total_for_multidisc_albums() -> None:
    from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
    from app.services.library_import import _catalog_tags

    artist = CatalogArtist(name="Morgan Wallen")
    album = CatalogAlbum(artist=artist, title="I’m The Problem", year="2025")
    album.tracks.extend(
        [
            CatalogAlbumTrack(position=1, disc=1, title="Disc One"),
            CatalogAlbumTrack(position=1, disc=2, title="Disc Two"),
            CatalogAlbumTrack(position=1, disc=3, title="Disc Three"),
        ]
    )

    tags = _catalog_tags(album, album.tracks[1], None)

    assert tags["discnumber"] == "2/3"


def test_import_tags_include_track_disc_total() -> None:
    from app.models.release import Release
    from app.models.track import Track
    from app.services.library_import import _tags_for

    release = Release(title="I’m The Problem", album_artist="Morgan Wallen", year="2025")
    track = Track(
        title="If You Were Mine",
        artist="Morgan Wallen",
        album_artist="Morgan Wallen",
        album="I’m The Problem",
        year="2025",
        disc=2,
        disc_total=3,
        track_no=6,
    )

    tags = _tags_for(release, track)

    assert tags["discnumber"] == "2/3"
