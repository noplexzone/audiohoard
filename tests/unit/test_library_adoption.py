from __future__ import annotations

from pathlib import Path

import pytest
from mutagen.id3 import ID3, TALB, TIT2, TPE1, TPE2, TPOS, TRCK, TXXX
from sqlalchemy import func, select

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.library_adoption import (
    AdoptionCandidateState,
    AdoptionScanState,
    AdoptionScopeKind,
    LibraryAdoptionCandidate,
)
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services.library_adoption import (
    AdoptionScope,
    enqueue_library_adoption_scan,
    run_library_adoption_scan,
)
from app.services.library_scan import scan_library_filesystem


def _tagged_mp3(
    path: Path,
    *,
    artist: str = "Artist",
    album: str = "Album",
    title: str = "Song",
    track: str = "1",
    disc: str = "1",
    recording_mbid: str | None = "recording-1",
    album_mbid: str | None = "album-1",
) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    tags = ID3()
    tags.add(TIT2(encoding=3, text=title))
    tags.add(TPE1(encoding=3, text=artist))
    tags.add(TPE2(encoding=3, text=artist))
    tags.add(TALB(encoding=3, text=album))
    tags.add(TRCK(encoding=3, text=track))
    tags.add(TPOS(encoding=3, text=disc))
    if recording_mbid:
        tags.add(TXXX(encoding=3, desc="MusicBrainz Track Id", text=recording_mbid))
    if album_mbid:
        tags.add(TXXX(encoding=3, desc="MusicBrainz Release Group Id", text=album_mbid))
    tags.save(path)
    return path.read_bytes()


async def _catalog(db_session, *, tracks: int = 1) -> tuple[CatalogArtist, CatalogAlbum]:
    artist = CatalogArtist(name="Artist")
    album = CatalogAlbum(
        artist=artist,
        title="Album",
        year="2024",
        mbid="album-1",
        track_count=tracks,
    )
    for position in range(1, tracks + 1):
        album.tracks.append(
            CatalogAlbumTrack(
                position=position,
                disc=1,
                title="Song" if position == 1 else f"Song {position}",
                recording_mbid=f"recording-{position}",
            )
        )
    db_session.add(artist)
    await db_session.commit()
    return artist, album


@pytest.mark.asyncio
async def test_exact_mbids_adopt_in_place_without_mutating_file(
    db_session, tmp_path: Path
) -> None:
    library = tmp_path / "library"
    path = library / "Artist" / "Album (2024)" / "01 - Song.mp3"
    before = _tagged_mp3(path)
    _artist, album = await _catalog(db_session)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=library)

    assert scan.state == AdoptionScanState.completed, scan.error_detail
    assert (scan.adopted_count, scan.review_count, scan.unmatched_count) == (1, 0, 0)
    plan = (await db_session.scalars(select(ImportPlan))).one()
    track = (await db_session.scalars(select(Track))).one()
    assert plan.destination_path == str(path.resolve())
    assert plan.source_path == str(path.resolve())
    assert plan.staging_path is None
    assert plan.status == ImportWorkflowState.imported
    assert plan.file_state == LibraryFileState.present
    assert track.catalog_track_id == album.tracks[0].id
    assert track.source_path is None and track.staging_path is None
    assert path.read_bytes() == before


@pytest.mark.asyncio
async def test_duplicate_files_for_one_track_persist_for_review_and_do_not_attach(
    db_session, tmp_path: Path
) -> None:
    library = tmp_path / "library"
    _tagged_mp3(library / "Artist" / "Album (2024)" / "01 - Song.mp3")
    _tagged_mp3(library / "Other" / "copy.mp3")
    _artist, album = await _catalog(db_session)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=library)

    assert scan.review_count == 2
    assert await db_session.scalar(select(func.count(ImportPlan.id))) == 0
    states = list((await db_session.scalars(select(LibraryAdoptionCandidate.state))).all())
    assert states == [AdoptionCandidateState.review, AdoptionCandidateState.review]


@pytest.mark.asyncio
async def test_contradictory_album_mbid_persists_without_ownership(
    db_session, tmp_path: Path
) -> None:
    library = tmp_path / "library"
    _tagged_mp3(library / "Artist" / "Album" / "01 - Song.mp3", album_mbid="wrong")
    _artist, album = await _catalog(db_session)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=library)

    assert scan.review_count == 1
    assert await db_session.scalar(select(func.count(ImportPlan.id))) == 0


@pytest.mark.asyncio
async def test_scan_is_idempotent_after_adoption(db_session, tmp_path: Path) -> None:
    library = tmp_path / "library"
    _tagged_mp3(library / "Artist" / "Album" / "01 - Song.mp3")
    _artist, album = await _catalog(db_session)
    first = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()
    await run_library_adoption_scan(db_session, scan_id=first, library_root=library)
    second = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()

    result = await run_library_adoption_scan(db_session, scan_id=second, library_root=library)

    assert result.scanned_count == 0
    assert await db_session.scalar(select(func.count(ImportPlan.id))) == 1


@pytest.mark.asyncio
async def test_unknown_artist_scope_never_broadens_to_library_root(
    db_session, tmp_path: Path
) -> None:
    library = tmp_path / "library"
    _tagged_mp3(library / "Artist" / "Album" / "01 - Song.mp3")

    with pytest.raises(ValueError, match="catalog artist not found"):
        await scan_library_filesystem(db_session, library_root=library, artist_id=999)
    with pytest.raises(ValueError, match="catalog artist not found"):
        await enqueue_library_adoption_scan(
            db_session,
            library_root=library,
            scope=AdoptionScope(AdoptionScopeKind.catalog_artist, 999),
        )


@pytest.mark.asyncio
async def test_canonical_folders_can_supply_missing_album_tags(db_session, tmp_path: Path) -> None:
    library = tmp_path / "library"
    path = library / "Artist" / "Album (2024)" / "01 - Song.mp3"
    _tagged_mp3(path, artist="", album="", album_mbid=None, recording_mbid=None)
    _artist, album = await _catalog(db_session)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=library)

    assert scan.adopted_count == 1
    candidate = (await db_session.scalars(select(LibraryAdoptionCandidate))).one()
    assert "canonical_library_folders" in candidate.reason_codes_json


@pytest.mark.asyncio
async def test_lost_plan_is_repaired_without_erasing_provenance(
    db_session, tmp_path: Path
) -> None:
    library = tmp_path / "library"
    path = library / "Artist" / "Album (2024)" / "01 - Song.mp3"
    _tagged_mp3(path)
    _artist, album = await _catalog(db_session)
    job = Job(source="slskd", query="lost plan", status=JobStatus.done)
    release = Release(job=job, source="slskd", title="Album", album_artist="Artist")
    track = Track(
        job=job,
        release=release,
        source="slskd",
        source_path="/staging/original.mp3",
        staging_path="/staging/original.mp3",
        title="Song",
        artist="Artist",
        album="Album",
        catalog_album_id=album.id,
        catalog_track_id=album.tracks[0].id,
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.imported,
    )
    db_session.add(job)
    await db_session.commit()
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=library)

    assert scan.adopted_count == 1
    await db_session.refresh(track)
    assert track.source_path == "/staging/original.mp3"
    assert track.staging_path == "/staging/original.mp3"
    plan = (await db_session.scalars(select(ImportPlan))).one()
    assert plan.track_id == track.id
    assert plan.destination_path == str(path.resolve())


@pytest.mark.asyncio
async def test_album_batch_reuses_release_and_sets_complete_truth(
    db_session, tmp_path: Path
) -> None:
    library = tmp_path / "library"
    _tagged_mp3(library / "Artist" / "Album (2024)" / "01 - Song.mp3")
    _tagged_mp3(
        library / "Artist" / "Album (2024)" / "02 - Song 2.mp3",
        title="Song 2",
        track="2",
        recording_mbid="recording-2",
    )
    _artist, album = await _catalog(db_session, tracks=2)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=library)

    assert scan.adopted_count == 2
    assert await db_session.scalar(select(func.count(Release.id))) == 1
    assert await db_session.scalar(select(func.count(Job.id))) == 1
    await db_session.refresh(album)
    assert album.in_library is True


@pytest.mark.asyncio
async def test_symlink_and_zero_byte_files_never_create_candidates(
    db_session, tmp_path: Path
) -> None:
    library = tmp_path / "library"
    real = tmp_path / "outside.mp3"
    _tagged_mp3(real)
    linked = library / "Artist" / "Album" / "linked.mp3"
    linked.parent.mkdir(parents=True)
    linked.symlink_to(real)
    (linked.parent / "empty.mp3").touch()
    _artist, album = await _catalog(db_session)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=library)

    assert scan.scanned_count == 0
    assert await db_session.scalar(select(func.count(LibraryAdoptionCandidate.id))) == 0
