from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.catalog_artist_credits import project_catalog_artist_credits
from app.services.library_import import MutagenTagWriter, _catalog_tags, plan_release_import


async def test_plan_release_import_preserves_distinct_compilation_credits(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    owner = CatalogArtist(name="Olivia Rodrigo")
    album = CatalogAlbum(
        artist=owner,
        title="The Hunger Games: The Ballad of Songbirds & Snakes",
        year="2023",
        release_type="compilation",
        is_compilation=True,
        album_artist_name="Various Artists",
        track_count=3,
    )
    credits = [
        ("Can't Catch Me Now", "Olivia Rodrigo"),
        ("The Hanging Tree", "Rachel Zegler"),
        ("Pure as the Driven Snow", "Tom Blyth"),
    ]
    job = Job(source="slskd", query="soundtrack", status=JobStatus.done)
    release = Release(
        job=job,
        source="slskd",
        title=album.title,
        album_artist=owner.name,
        year=album.year,
        track_count=len(credits),
        staging_path=str(staging),
        import_state=ImportWorkflowState.ready,
    )
    db_session.add_all([owner, job])
    for position, (title, artist_name) in enumerate(credits, start=1):
        catalog_track = CatalogAlbumTrack(
            disc=1,
            position=position,
            title=title,
            artist_name=artist_name,
        )
        album.tracks.append(catalog_track)
        source = staging / f"{position:02d}.mp3"
        source.write_bytes(f"audio-{position}".encode())
        db_session.add(
            Track(
                job=job,
                release=release,
                catalog_album=album,
                catalog_track=catalog_track,
                title=title,
                artist=artist_name,
                album_artist="Various Artists",
                album=album.title,
                year=album.year,
                disc=1,
                track_no=position,
                source="slskd",
                staging_path=str(source),
                source_path=str(source),
                import_state=ImportWorkflowState.ready,
            )
        )
    await db_session.flush()

    plans = await plan_release_import(db_session, release, library_root=tmp_path / "library")

    assert len(plans) == 3
    assert [(plan.track.artist, plan.track.album_artist) for plan in plans] == [
        (artist_name, "Various Artists") for _title, artist_name in credits
    ]
    assert {Path(plan.destination_path).parts[-3] for plan in plans} == {"Various Artists"}


def test_missing_compilation_album_artist_uses_conservative_fallback() -> None:
    owner = CatalogArtist(name="Olivia Rodrigo")
    compilation = CatalogAlbum(
        artist=owner,
        title="Soundtrack",
        release_type="compilation",
        is_compilation=True,
    )
    ordinary = CatalogAlbum(artist=owner, title="SOUR", release_type="album")

    assert project_catalog_artist_credits(compilation).album_artist == "Various Artists"
    assert project_catalog_artist_credits(ordinary).album_artist == owner.name


async def test_missing_compilation_child_artist_ignores_mutable_source_track(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    owner = CatalogArtist(name="Olivia Rodrigo")
    album = CatalogAlbum(
        artist=owner,
        title="Soundtrack",
        year="2023",
        release_type="compilation",
        is_compilation=True,
        album_artist_name="Various Artists",
        track_count=1,
    )
    catalog_track = CatalogAlbumTrack(
        disc=1, position=1, title="Unknown Performer", artist_name=None
    )
    album.tracks.append(catalog_track)
    job = Job(source="slskd", query="soundtrack", status=JobStatus.done)
    release = Release(
        job=job,
        source="slskd",
        title=album.title,
        album_artist=owner.name,
        year=album.year,
        track_count=1,
        staging_path=str(staging),
        import_state=ImportWorkflowState.ready,
    )
    source = staging / "01.mp3"
    source.write_bytes(b"planning-only fixture")
    track = Track(
        job=job,
        release=release,
        catalog_album=album,
        catalog_track=catalog_track,
        title=catalog_track.title,
        artist="Untrusted Source Artist",
        album_artist="Untrusted Source Artist",
        album=album.title,
        year=album.year,
        disc=1,
        track_no=1,
        source="slskd",
        staging_path=str(source),
        source_path=str(source),
        import_state=ImportWorkflowState.ready,
    )
    db_session.add_all([owner, job, track])
    await db_session.flush()

    plans = await plan_release_import(db_session, release, library_root=tmp_path / "library")
    tags = _catalog_tags(album, catalog_track, track)

    assert len(plans) == 1
    assert plans[0].track.artist == owner.name
    assert plans[0].track.album_artist == "Various Artists"
    assert tags["artist"] == owner.name
    assert tags["album_artist"] == "Various Artists"
    assert Path(plans[0].destination_path).parts[-3] == "Various Artists"


def _generate_decodable_audio(path: Path, suffix: str) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        pytest.skip("ffmpeg is required for real compilation audio fixtures")
    codecs = {".flac": "flac", ".mp3": "libmp3lame", ".ogg": "libvorbis"}
    subprocess.run(
        [
            ffmpeg,
            "-nostdin",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=0.2",
            "-c:a",
            codecs[suffix],
            str(path),
        ],
        check=True,
        timeout=30,
    )


def _probe_duration(path: Path) -> float:
    ffprobe = shutil.which("ffprobe")
    assert ffprobe is not None, "ffprobe is required to independently verify generated audio"
    result = subprocess.run(
        [
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "json",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return float(json.loads(result.stdout)["format"]["duration"])


@pytest.mark.parametrize(
    ("suffix", "performer", "position"),
    [(".flac", "Olivia Rodrigo", 1), (".mp3", "Rachel Zegler", 2), (".ogg", "Tom Blyth", 3)],
)
def test_compilation_credit_tags_round_trip_real_media(
    tmp_path: Path, suffix: str, performer: str, position: int
) -> None:
    owner = CatalogArtist(name="Olivia Rodrigo")
    album = CatalogAlbum(
        artist=owner,
        title="Soundtrack",
        year="2023",
        release_type="compilation",
        is_compilation=True,
        album_artist_name="Various Artists",
    )
    catalog_track = CatalogAlbumTrack(
        disc=1, position=position, title=f"Song {position}", artist_name=performer
    )
    album.tracks.append(catalog_track)
    track = Track(
        title=catalog_track.title,
        artist=owner.name,
        album_artist=owner.name,
        album=album.title,
        source="slskd",
    )
    path = tmp_path / f"track{suffix}"
    _generate_decodable_audio(path, suffix)
    assert _probe_duration(path) > 0

    writer = MutagenTagWriter()
    assert writer.write_and_verify(path, _catalog_tags(album, catalog_track, track))
    assert _probe_duration(path) > 0

    readback = writer.read_tags(path)
    assert readback["artist"] == performer
    assert readback["album_artist"] == "Various Artists"
