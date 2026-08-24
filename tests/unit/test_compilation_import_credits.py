from __future__ import annotations

from pathlib import Path

import pytest
from mutagen.flac import FLAC
from mutagen.id3 import ID3
from mutagen.oggvorbis import OggVorbis
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
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


def _minimal_flac_bytes() -> bytes:
    stream_info = (
        (4096).to_bytes(2, "big")
        + (4096).to_bytes(2, "big")
        + (0).to_bytes(3, "big")
        + (0).to_bytes(3, "big")
        + ((44100 << 44) | (15 << 36)).to_bytes(8, "big")
        + bytes(16)
    )
    return b"fLaC" + bytes([0x80, 0, 0, 34]) + stream_info


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
    if suffix == ".flac":
        path.write_bytes(_minimal_flac_bytes())
        FLAC(path).save()
    elif suffix == ".mp3":
        ID3().save(path)
    else:
        fixture = Path(__file__).parents[1] / "fixtures" / "audio" / "minimal.ogg"
        path.write_bytes(fixture.read_bytes())
        OggVorbis(path).save()

    writer = MutagenTagWriter()
    assert writer.write_and_verify(path, _catalog_tags(album, catalog_track, track))

    readback = writer.read_tags(path)
    assert readback["artist"] == performer
    assert readback["album_artist"] == "Various Artists"
