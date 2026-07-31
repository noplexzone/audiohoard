from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest
from mutagen.id3 import ID3, TALB, TDRC, TIT2, TPE1, TPE2, TPOS, TRCK, TXXX
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

import app.services.library_adoption as adoption_service
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.library_adoption import (
    AdoptionCandidateState,
    AdoptionScanState,
    AdoptionScopeKind,
    LibraryAdoptionCandidate,
    LibraryAdoptionScan,
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
    release_mbid: str | None = None,
    year: str | None = None,
) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    tags = ID3()
    tags.add(TIT2(encoding=3, text=title))
    tags.add(TPE1(encoding=3, text=artist))
    tags.add(TPE2(encoding=3, text=artist))
    tags.add(TALB(encoding=3, text=album))
    tags.add(TRCK(encoding=3, text=track))
    tags.add(TPOS(encoding=3, text=disc))
    if year:
        tags.add(TDRC(encoding=3, text=year))
    if recording_mbid:
        tags.add(TXXX(encoding=3, desc="MusicBrainz Track Id", text=recording_mbid))
    if album_mbid:
        tags.add(TXXX(encoding=3, desc="MusicBrainz Release Group Id", text=album_mbid))
    if release_mbid:
        tags.add(TXXX(encoding=3, desc="MusicBrainz Album Id", text=release_mbid))
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
    release = (await db_session.scalars(select(Release))).one()
    assert plan.destination_path == str(path.resolve())
    assert plan.source_path == str(path.resolve())
    assert plan.staging_path is None
    assert plan.status == ImportWorkflowState.imported
    assert plan.file_state == LibraryFileState.present
    assert track.catalog_track_id == album.tracks[0].id
    assert release.release_mbid is None
    assert track.catalog_album is not None and track.catalog_album.mbid == album.mbid
    assert track.source_path is None and track.staging_path is None
    assert path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("artist", "album", "year", "disc", "track_no"),
    [
        ("Wrong Artist", "Wrong Album", "1999", "1", "1"),
        ("Artist", "Album", "2024", "9", "9"),
    ],
)
async def test_exact_mbids_with_contradictory_metadata_require_review(
    db_session,
    tmp_path: Path,
    artist: str,
    album: str,
    year: str,
    disc: str,
    track_no: str,
) -> None:
    library = tmp_path / "library"
    _tagged_mp3(
        library / "Artist" / "Album (2024)" / "01 - Song.mp3",
        artist=artist,
        album=album,
        year=year,
        disc=disc,
        track=track_no,
    )
    _artist, catalog_album = await _catalog(db_session)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, catalog_album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=library)

    assert scan.adopted_count == 0
    assert scan.review_count == 1
    assert await db_session.scalar(select(func.count(ImportPlan.id))) == 0


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

    assert scan.state == AdoptionScanState.completed, scan.error_detail
    assert scan.review_count == 2, (
        scan.scanned_count,
        scan.unmatched_count,
        scan.error_count,
    )
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

    assert scan.state == AdoptionScanState.completed, scan.error_detail
    assert scan.review_count == 1, (
        scan.scanned_count,
        scan.unmatched_count,
        scan.error_count,
    )
    assert await db_session.scalar(select(func.count(ImportPlan.id))) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("artist", "album"),
    [("Wrong Artist", ""), ("", "Wrong Album")],
)
async def test_incomplete_contradictory_tags_do_not_override_canonical_folders(
    db_session, tmp_path: Path, artist: str, album: str
) -> None:
    library = tmp_path / "library"
    _tagged_mp3(
        library / "Artist" / "Album (2024)" / "01 - Song.mp3",
        artist=artist,
        album=album,
        album_mbid=None,
        recording_mbid=None,
    )
    _artist, catalog_album = await _catalog(db_session)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=library,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, catalog_album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=library)

    assert scan.adopted_count == 0
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
async def test_interrupted_scan_resumes_persisted_candidates(
    db_session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "music"
    path = root / "Example Artist" / "Example Album (2024)" / "01 - Song 1.mp3"
    _tagged_mp3(path)
    _, album = await _catalog(db_session)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=root,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, scope_id=album.id),
    )
    await db_session.commit()

    original = adoption_service._adopt_candidate

    async def interrupt(*args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(adoption_service, "_adopt_candidate", interrupt)
    with pytest.raises(asyncio.CancelledError):
        await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=root)
    scan = await db_session.get(LibraryAdoptionScan, scan_id)
    assert scan is not None and scan.state == AdoptionScanState.queued
    candidate_count = await db_session.scalar(
        select(func.count(LibraryAdoptionCandidate.id)).where(
            LibraryAdoptionCandidate.scan_id == scan_id
        )
    )
    assert candidate_count == 1

    monkeypatch.setattr(adoption_service, "_adopt_candidate", original)
    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=root)
    assert scan.state == AdoptionScanState.completed, scan.error_detail
    assert scan.adopted_count == 1
    candidate_count = await db_session.scalar(
        select(func.count(LibraryAdoptionCandidate.id)).where(
            LibraryAdoptionCandidate.scan_id == scan_id
        )
    )
    assert candidate_count == 1


@pytest.mark.asyncio
async def test_resume_refreshes_catalog_truth_for_previously_committed_adoption(
    db_session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "music"
    _tagged_mp3(root / "Artist" / "Album (2024)" / "01 - Song.mp3")
    _, album = await _catalog(db_session)
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=root,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, scope_id=album.id),
    )
    await db_session.commit()

    original_refresh = adoption_service._refresh_album_truth

    async def interrupt_after_candidate_commit(*args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(
        adoption_service,
        "_refresh_album_truth",
        interrupt_after_candidate_commit,
    )
    with pytest.raises(asyncio.CancelledError):
        await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=root)

    candidate = await db_session.scalar(
        select(LibraryAdoptionCandidate).where(LibraryAdoptionCandidate.scan_id == scan_id)
    )
    assert candidate is not None and candidate.state == AdoptionCandidateState.adopted
    await db_session.refresh(album)
    assert album.in_library is False

    monkeypatch.setattr(adoption_service, "_refresh_album_truth", original_refresh)
    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=root)

    assert scan.state == AdoptionScanState.completed
    await db_session.refresh(album)
    assert album.in_library is True


@pytest.mark.asyncio
async def test_full_scan_falls_back_to_imported_only_identity(db_session, tmp_path: Path) -> None:
    root = tmp_path / "music"
    path = root / "Legacy Artist" / "Legacy Album (2020)" / "01 - Legacy Song.mp3"
    _tagged_mp3(
        path,
        artist="Legacy Artist",
        album="Legacy Album",
        title="Legacy Song",
        album_mbid=None,
        recording_mbid=None,
    )
    await _catalog(db_session)  # Keep an unrelated catalog identity present.
    job = Job(source="legacy", query="legacy import", status=JobStatus.done)
    release = Release(
        job=job,
        source="legacy",
        title="Legacy Album",
        album_artist="Legacy Artist",
        year="2020",
    )
    track = Track(
        job=job,
        release=release,
        source="legacy",
        artist="Legacy Artist",
        album_artist="Legacy Artist",
        album="Legacy Album",
        title="Legacy Song",
        year="2020",
        track_no=1,
        disc=1,
        import_state=ImportWorkflowState.imported,
    )
    db_session.add(track)
    await db_session.flush()
    track_id = track.id
    scan_id = await enqueue_library_adoption_scan(db_session, library_root=root)
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=root)
    assert scan.state == AdoptionScanState.completed, scan.error_detail
    assert scan.adopted_count == 1
    plan = await db_session.scalar(select(ImportPlan).where(ImportPlan.track_id == track_id))
    assert plan is not None and plan.destination_path == str(path)


@pytest.mark.asyncio
async def test_full_scan_never_overrides_catalog_mbid_conflict_with_imported_fallback(
    db_session, tmp_path: Path
) -> None:
    root = tmp_path / "music"
    path = root / "Legacy Artist" / "Legacy Album" / "01 - Legacy Song.mp3"
    _tagged_mp3(
        path,
        artist="Legacy Artist",
        album="Legacy Album",
        title="Legacy Song",
        album_mbid=None,
        release_mbid="contradictory-release",
        recording_mbid=None,
    )
    await _catalog(db_session)
    job = Job(source="legacy", query="legacy import", status=JobStatus.done)
    release = Release(
        job=job,
        source="legacy",
        title="Legacy Album",
        album_artist="Legacy Artist",
        release_mbid="different-release",
    )
    track = Track(
        job=job,
        release=release,
        source="legacy",
        artist="Legacy Artist",
        album_artist="Legacy Artist",
        album="Legacy Album",
        title="Legacy Song",
        track_no=1,
        disc=1,
        import_state=ImportWorkflowState.imported,
    )
    db_session.add(track)
    await db_session.flush()
    scan_id = await enqueue_library_adoption_scan(db_session, library_root=root)
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=root)

    assert scan.adopted_count == 0
    assert scan.review_count == 1
    assert await db_session.scalar(select(func.count(ImportPlan.id))) == 0


@pytest.mark.asyncio
async def test_release_mbid_is_imported_evidence_not_catalog_group_contradiction(
    db_session, tmp_path: Path
) -> None:
    root = tmp_path / "music"
    path = root / "Legacy Artist" / "Legacy Album" / "01 - Legacy Song.mp3"
    payload = _tagged_mp3(
        path,
        artist="Legacy Artist",
        album="Legacy Album",
        title="Legacy Song",
        album_mbid=None,
        release_mbid="release-legacy",
        recording_mbid=None,
    )
    await _catalog(db_session)
    job = Job(source="legacy", query="legacy import", status=JobStatus.done)
    release = Release(
        job=job,
        source="legacy",
        title="Legacy Album",
        album_artist="Legacy Artist",
        year="2020",
        release_mbid="release-legacy",
        import_state=ImportWorkflowState.imported,
    )
    track = Track(
        job=job,
        release=release,
        source="legacy",
        title="Legacy Song",
        artist="Legacy Artist",
        album_artist="Legacy Artist",
        album="Legacy Album",
        year="2020",
        disc=1,
        track_no=1,
        content_sha256=hashlib.sha256(payload).hexdigest(),
        import_state=ImportWorkflowState.imported,
    )
    db_session.add_all([job, release, track])
    await db_session.commit()

    scan_id = await enqueue_library_adoption_scan(db_session, library_root=root)
    await db_session.commit()
    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=root)

    candidate = await db_session.scalar(select(LibraryAdoptionCandidate))
    assert scan.adopted_count == 1
    assert candidate is not None
    assert candidate.state == AdoptionCandidateState.adopted
    assert candidate.resulting_track_id == track.id


@pytest.mark.asyncio
async def test_queued_scan_fails_closed_when_library_root_changes(
    db_session, tmp_path: Path
) -> None:
    original_root = tmp_path / "music-a"
    changed_root = tmp_path / "music-b"
    original_root.mkdir()
    changed_root.mkdir()
    scan_id = await enqueue_library_adoption_scan(db_session, library_root=original_root)
    await db_session.commit()
    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=changed_root)
    assert scan.state == AdoptionScanState.failed
    assert "root changed" in (scan.error_detail or "")


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


@pytest.mark.asyncio
async def test_imported_identity_with_present_file_sends_duplicate_to_review(
    db_session, tmp_path: Path
) -> None:
    root = tmp_path / "music"
    owned = root / "Legacy Artist" / "Legacy Album (2020)" / "01 - Legacy Song.mp3"
    duplicate = root / "Loose" / "copy.mp3"
    _tagged_mp3(
        owned,
        artist="Legacy Artist",
        album="Legacy Album",
        title="Legacy Song",
        album_mbid=None,
        recording_mbid=None,
    )
    _tagged_mp3(
        duplicate,
        artist="Legacy Artist",
        album="Legacy Album",
        title="Legacy Song",
        album_mbid=None,
        recording_mbid=None,
    )
    job = Job(source="legacy", query="legacy import", status=JobStatus.done)
    release = Release(
        job=job,
        source="legacy",
        title="Legacy Album",
        album_artist="Legacy Artist",
        year="2020",
    )
    track = Track(
        job=job,
        release=release,
        source="legacy",
        artist="Legacy Artist",
        album_artist="Legacy Artist",
        album="Legacy Album",
        title="Legacy Song",
        year="2020",
        track_no=1,
        disc=1,
        import_state=ImportWorkflowState.imported,
    )
    plan = ImportPlan(
        release=release,
        track=track,
        source_path=str(owned),
        destination_path=str(owned),
        status=ImportWorkflowState.imported,
        file_state=LibraryFileState.present,
    )
    db_session.add(plan)
    await db_session.flush()
    scan_id = await enqueue_library_adoption_scan(db_session, library_root=root)
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=root)

    assert scan.adopted_count == 0
    assert scan.review_count == 1
    assert (
        await db_session.scalar(
            select(func.count(ImportPlan.id)).where(ImportPlan.track_id == track.id)
        )
        == 1
    )


@pytest.mark.asyncio
async def test_catalog_track_with_active_normal_plan_sends_adoption_to_review(
    db_session, tmp_path: Path
) -> None:
    root = tmp_path / "music"
    candidate_path = root / "Artist" / "Album (2024)" / "01 - Song.mp3"
    _tagged_mp3(candidate_path)
    _artist, album = await _catalog(db_session)
    job = Job(source="legacy", query="planned import", status=JobStatus.done)
    release = Release(
        job=job,
        source="legacy",
        title="Album",
        album_artist="Artist",
        year="2024",
    )
    track = Track(
        job=job,
        release=release,
        catalog_track_id=album.tracks[0].id,
        source="legacy",
        artist="Artist",
        album_artist="Artist",
        album="Album",
        title="Song",
        year="2024",
        track_no=1,
        disc=1,
        import_state=ImportWorkflowState.ready,
    )
    existing_plan = ImportPlan(
        release=release,
        track=track,
        source_path="/staging/planned.mp3",
        destination_path=str(root / "Artist" / "Album (2024)" / "planned.mp3"),
        status=ImportWorkflowState.ready,
        file_state=LibraryFileState.unknown,
    )
    db_session.add(existing_plan)
    await db_session.flush()
    scan_id = await enqueue_library_adoption_scan(
        db_session,
        library_root=root,
        scope=AdoptionScope(AdoptionScopeKind.catalog_album, album.id),
    )
    await db_session.commit()

    scan = await run_library_adoption_scan(db_session, scan_id=scan_id, library_root=root)

    assert scan.adopted_count == 0
    assert scan.review_count == 1
    assert (
        await db_session.scalar(
            select(func.count(ImportPlan.id)).where(ImportPlan.track_id == track.id)
        )
        == 1
    )


@pytest.mark.asyncio
async def test_active_destination_claim_is_database_unique(db_session) -> None:
    job = Job(source="legacy", query="legacy import", status=JobStatus.done)
    release = Release(
        job=job,
        source="legacy",
        title="Album",
        album_artist="Artist",
    )
    db_session.add_all(
        [
            ImportPlan(
                release=release,
                source_path="/music/a.mp3",
                destination_path="/music/a.mp3",
                planned_operations_json='{"operation": "adopt_in_place"}',
                status=ImportWorkflowState.ready,
                file_state=LibraryFileState.unknown,
            ),
            ImportPlan(
                release=release,
                source_path="/staging/b.mp3",
                destination_path="/music/a.mp3",
                status=ImportWorkflowState.imported,
                file_state=LibraryFileState.present,
            ),
        ]
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()
