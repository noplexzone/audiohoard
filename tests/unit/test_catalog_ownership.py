from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import AcquisitionState, ImportWorkflowState
from app.services import catalog_ownership
from app.services.catalog_ownership import DeezerReleaseEvidence


async def _add_imported_single(
    db: AsyncSession,
    destination: Path,
    *,
    title: str = "We Don’t Get Along",
    year: str = "2026",
    deezer_id: str = "3866476251",
) -> tuple[CatalogAlbum, CatalogAlbum, Track]:
    artist = CatalogArtist(name="Juice WRLD")
    clean = CatalogAlbum(
        title=title,
        year=year,
        release_type="single",
        track_count=1,
        content_rating="clean",
        deezer_id="clean-album",
        in_library=True,
    )
    explicit = CatalogAlbum(
        title=title,
        year=year,
        release_type="single",
        track_count=1,
        content_rating="explicit",
        deezer_id="explicit-album",
    )
    clean_track = CatalogAlbumTrack(
        position=1, disc=1, title=title, duration_sec=149, content_rating="clean"
    )
    explicit_track = CatalogAlbumTrack(
        position=1, disc=1, title=title, duration_sec=149, content_rating="explicit"
    )
    clean.tracks.append(clean_track)
    explicit.tracks.append(explicit_track)
    artist.albums.extend((clean, explicit))
    job = Job(source="slskd", query=title, status=JobStatus.done, catalog_album=clean)
    release = Release(
        job=job,
        source="slskd",
        title=title,
        album_artist=artist.name,
        year=year,
        import_state=ImportWorkflowState.imported,
    )
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title=title,
        artist=artist.name,
        album_artist=artist.name,
        album=title,
        year=year,
        disc=1,
        track_no=1,
        duration_sec=149,
        deezer_id=deezer_id,
        acquisition_provenance_json='{"source":"slskd"}',
        catalog_album=clean,
        catalog_track=clean_track,
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.imported,
    )
    plan = ImportPlan(
        release=release,
        track=track,
        source_path=str(destination),
        destination_path=str(destination),
        status=ImportWorkflowState.imported,
    )
    db.add_all((artist, job, release, track, plan))
    await db.commit()
    return clean, explicit, track


async def test_reconcile_moves_explicit_import_and_recomputes_library_flags(
    db_session: AsyncSession,
    test_settings: Settings,
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "track.flac"
    destination.write_bytes(b"audio")
    clean, explicit, track = await _add_imported_single(db_session, destination)
    clean_id, explicit_id, track_id = clean.id, explicit.id, track.id
    await db_session.delete(explicit.tracks[0])
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    evidence_calls = 0

    async def evidence(_settings, _candidates):
        nonlocal evidence_calls
        evidence_calls += 1
        return {"3866476251": DeezerReleaseEvidence("3866476251", "explicit-album", "explicit")}

    monkeypatch.setattr(catalog_ownership, "_resolve_evidence", evidence)
    assert await catalog_ownership.reconcile_deezer_catalog_ownership(factory, test_settings) == 1

    db_session.expire_all()
    moved = await db_session.get(Track, track_id)
    old_album = await db_session.get(CatalogAlbum, clean_id)
    new_album = await db_session.get(CatalogAlbum, explicit_id)
    assert moved is not None and moved.catalog_album_id == explicit_id
    assert moved.catalog_track_id is not None
    created_track = await db_session.get(CatalogAlbumTrack, moved.catalog_track_id)
    assert created_track is not None and created_track.content_rating == "explicit"
    assert json.loads(moved.acquisition_provenance_json or "{}")["source"] == "slskd"
    assert old_album is not None and old_album.in_library is False
    assert new_album is not None and new_album.in_library is True

    assert await catalog_ownership.reconcile_deezer_catalog_ownership(factory, test_settings) == 0
    assert evidence_calls == 1


async def test_reconcile_uses_unique_rating_sibling_when_track_album_differs(
    db_session: AsyncSession,
    test_settings: Settings,
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "agats2.flac"
    destination.write_bytes(b"audio")
    _clean, explicit, track = await _add_imported_single(
        db_session,
        destination,
        title="AGATS2 (Insecure)",
        year="2024",
        deezer_id="3115125011",
    )
    explicit_id, track_id = explicit.id, track.id
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)

    async def evidence(_settings, _candidates):
        return {
            "3115125011": DeezerReleaseEvidence(
                "3115125011", "different-explicit-album", "explicit"
            )
        }

    monkeypatch.setattr(catalog_ownership, "_resolve_evidence", evidence)
    assert await catalog_ownership.reconcile_deezer_catalog_ownership(factory, test_settings) == 1
    db_session.expire_all()
    moved = await db_session.get(Track, track_id)
    assert moved is not None and moved.catalog_album_id == explicit_id


async def test_reconcile_recovers_projection_after_interrupted_ownership_commit(
    db_session: AsyncSession,
    test_settings: Settings,
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "track.flac"
    destination.write_bytes(b"audio")
    clean, explicit, track = await _add_imported_single(db_session, destination)
    clean_id, explicit_id, track_id = clean.id, explicit.id, track.id
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    evidence_value = DeezerReleaseEvidence("3866476251", "explicit-album", "explicit")

    async with factory() as interrupted:
        changed, affected, verified = await catalog_ownership.apply_catalog_ownership_evidence(
            interrupted, {"3866476251": evidence_value}, track_ids=[track_id]
        )
        assert changed == 1 and affected == {clean_id, explicit_id}
        assert verified == {track_id: evidence_value}
        await interrupted.commit()

    async def evidence(_settings, _candidates):
        return {"3866476251": evidence_value}

    monkeypatch.setattr(catalog_ownership, "_resolve_evidence", evidence)
    assert await catalog_ownership.reconcile_deezer_catalog_ownership(factory, test_settings) == 0

    db_session.expire_all()
    old_album = await db_session.get(CatalogAlbum, clean_id)
    new_album = await db_session.get(CatalogAlbum, explicit_id)
    assert old_album is not None and old_album.in_library is False
    assert new_album is not None and new_album.in_library is True


async def test_reconcile_fails_closed_for_ambiguous_rating_siblings(
    db_session: AsyncSession,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "track.flac"
    destination.write_bytes(b"audio")
    clean, _explicit, track = await _add_imported_single(db_session, destination)
    artist = await db_session.get(CatalogArtist, clean.artist_id)
    assert artist is not None
    duplicate = CatalogAlbum(
        artist_id=artist.id,
        title=clean.title,
        year=clean.year,
        release_type="single",
        track_count=1,
        content_rating="explicit",
        deezer_id="other-explicit",
    )
    duplicate.tracks.append(
        CatalogAlbumTrack(
            position=1,
            disc=1,
            title=clean.title,
            duration_sec=149,
            content_rating="explicit",
        )
    )
    db_session.add(duplicate)
    await db_session.commit()

    changed, affected, verified = await catalog_ownership.apply_catalog_ownership_evidence(
        db_session,
        {str(track.deezer_id): DeezerReleaseEvidence(str(track.deezer_id), None, "explicit")},
    )
    assert changed == 0 and affected == set() and verified == {}
    assert track.catalog_album_id == clean.id


async def test_reconcile_rejects_different_release_version(
    db_session: AsyncSession,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "track.flac"
    destination.write_bytes(b"audio")
    clean, explicit, track = await _add_imported_single(db_session, destination)
    explicit.title = f"{clean.title} (Alternate Versions)"
    await db_session.commit()

    changed, affected, verified = await catalog_ownership.apply_catalog_ownership_evidence(
        db_session,
        {
            str(track.deezer_id): DeezerReleaseEvidence(
                str(track.deezer_id), "explicit-album", "explicit"
            )
        },
    )
    assert changed == 0 and affected == set() and verified == {}
    assert track.catalog_album_id == clean.id


async def test_reconcile_does_not_query_provider_when_imported_file_is_missing(
    db_session: AsyncSession,
    test_settings: Settings,
    tmp_path: Path,
    monkeypatch,
) -> None:
    destination = tmp_path / "missing.flac"
    await _add_imported_single(db_session, destination)
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)

    async def evidence(_settings, _candidates):
        raise AssertionError("provider should not be queried without an imported artifact")

    monkeypatch.setattr(catalog_ownership, "_resolve_evidence", evidence)
    assert await catalog_ownership.reconcile_deezer_catalog_ownership(factory, test_settings) == 0


async def test_reconcile_ignores_unknown_rating_and_non_imported_track(
    db_session: AsyncSession,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "track.flac"
    destination.write_bytes(b"audio")
    clean, _explicit, track = await _add_imported_single(db_session, destination)

    assert await catalog_ownership.apply_catalog_ownership_evidence(
        db_session,
        {str(track.deezer_id): DeezerReleaseEvidence(str(track.deezer_id), None, "unknown")},
    ) == (0, set(), {})
    track.import_state = ImportWorkflowState.discovered
    await db_session.flush()
    assert await catalog_ownership.apply_catalog_ownership_evidence(
        db_session,
        {str(track.deezer_id): DeezerReleaseEvidence(str(track.deezer_id), None, "explicit")},
    ) == (0, set(), {})
    assert track.catalog_album_id == clean.id
