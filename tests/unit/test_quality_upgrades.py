from __future__ import annotations

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import app.services.quality_upgrade as quality_upgrade_module
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.quality_upgrade import reconcile_album_quality_duplicates
from app.settings_service import QualityProfile


async def _seed_duplicate_album(
    db: AsyncSession,
    library_root: Path,
    *,
    duplicate_in_same_folder: bool = True,
) -> tuple[CatalogAlbum, Path, Path]:
    artist = CatalogArtist(name="Tyler Childers", mbid="9947e874-ddd0-4e1a-b0d4-a76f6532fe45")
    album = CatalogAlbum(
        title="Bottles and Bibles",
        year="2011",
        release_type="album",
        track_count=1,
        in_library=True,
    )
    catalog_track = CatalogAlbumTrack(
        disc=1,
        position=1,
        title="Hard Times",
        recording_mbid="2b9b81a1-eeac-401d-83eb-12c2354cac8d",
    )
    album.tracks.append(catalog_track)
    artist.albums.append(album)
    db.add(artist)
    await db.flush()

    album_folder = library_root / artist.name / f"{album.title} ({album.year})"
    other_folder = library_root / artist.name / "Hard Times (Single) (2011)"
    album_folder.mkdir(parents=True)
    other_folder.mkdir(parents=True)
    flac_path = album_folder / "01 - Hard Times.flac"
    mp3_path = (album_folder if duplicate_in_same_folder else other_folder) / "01 - Hard Times.mp3"
    flac_path.write_bytes(b"flac-quality")
    mp3_path.write_bytes(b"mp3-quality")

    for file_format, destination, size in (
        ("mp3", mp3_path, 11),
        ("flac", flac_path, 12),
    ):
        job = Job(source="slskd", query="Hard Times", status=JobStatus.done)
        release = Release(
            job=job,
            source="slskd",
            title=album.title,
            album_artist=artist.name,
            year=album.year,
            track_count=1,
            import_state=ImportWorkflowState.imported,
        )
        track = Track(
            job=job,
            release=release,
            catalog_album_id=album.id,
            catalog_track_id=catalog_track.id,
            title=catalog_track.title,
            artist=artist.name,
            album_artist=artist.name,
            album=album.title,
            year=album.year,
            disc=1,
            track_no=1,
            mbid=catalog_track.recording_mbid,
            source="slskd",
            import_state=ImportWorkflowState.imported,
            file_format=file_format,
            file_size_bytes=size,
            content_sha256=f"sha-{file_format}",
        )
        track.import_plans.append(
            ImportPlan(
                release=release,
                source_path=f"/staging/{destination.name}",
                destination_path=str(destination),
                status=ImportWorkflowState.imported,
            )
        )
        db.add(track)
    await db.flush()
    return album, flac_path, mp3_path


async def test_reconcile_album_quality_duplicates_permanently_deletes_lower_quality_same_folder(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    album, flac_path, mp3_path = await _seed_duplicate_album(db_session, tmp_path / "library")

    result = await reconcile_album_quality_duplicates(
        db_session,
        album.id,
        library_root=tmp_path / "library",
        quality_profile=QualityProfile(
            format_preference=["flac", "mp3", "m4a/aac", "ogg", "opus"],
            min_mp3_bitrate=320,
            allow_lower_quality_fallback=True,
        ),
    )

    assert result.deleted_files == 1
    assert result.review_required == 0
    assert flac_path.exists()
    assert not mp3_path.exists()

    rows = await db_session.execute(
        Track.__table__.select()
        .where(Track.catalog_album_id == album.id)
        .order_by(Track.file_format)
    )
    states = {row.file_format: row.import_state for row in rows}
    assert states == {"flac": ImportWorkflowState.imported, "mp3": ImportWorkflowState.rolled_back}


async def test_reconcile_album_quality_duplicates_ignores_same_song_outside_album_folder(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    album, flac_path, mp3_path = await _seed_duplicate_album(
        db_session, tmp_path / "library", duplicate_in_same_folder=False
    )

    result = await reconcile_album_quality_duplicates(
        db_session,
        album.id,
        library_root=tmp_path / "library",
        quality_profile=QualityProfile(
            format_preference=["flac", "mp3", "m4a/aac", "ogg", "opus"],
            min_mp3_bitrate=320,
            allow_lower_quality_fallback=True,
        ),
    )

    assert result.deleted_files == 0
    assert result.review_required == 0
    assert flac_path.exists()
    assert mp3_path.exists()


async def test_reconcile_album_quality_duplicates_uses_mp3_bitrate_preference(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    album, flac_path, mp3_path = await _seed_duplicate_album(db_session, tmp_path / "library")
    high_mp3 = flac_path.with_name("01 - Hard Times (320).mp3")
    flac_path.rename(high_mp3)

    imported_tracks = list(
        (await db_session.scalars(select(Track).where(Track.catalog_album_id == album.id))).all()
    )
    for track in imported_tracks:
        track.file_format = "mp3"
        for plan in track.import_plans:
            if plan.destination_path == str(flac_path):
                plan.destination_path = str(high_mp3)
    await db_session.flush()

    def fake_bitrate(path: Path) -> int | None:
        return 320 if path == high_mp3 else 128

    monkeypatch.setattr(quality_upgrade_module, "_audio_bitrate_kbps", fake_bitrate)

    result = await reconcile_album_quality_duplicates(
        db_session,
        album.id,
        library_root=tmp_path / "library",
        quality_profile=QualityProfile(
            format_preference=["mp3", "flac", "m4a/aac", "ogg", "opus"],
            min_mp3_bitrate=320,
            allow_lower_quality_fallback=True,
        ),
    )

    assert result.deleted_files == 1
    assert high_mp3.exists()
    assert not mp3_path.exists()
