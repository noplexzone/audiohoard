from __future__ import annotations

from pathlib import Path

import pytest
from mutagen.flac import FLAC
from mutagen.id3 import APIC, ID3, TXXX
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.library_import import ImportExecutionError, MutagenTagWriter, retag_catalog_album


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


async def _seed_imported_album(
    db: AsyncSession, library_root: Path
) -> tuple[CatalogAlbum, list[Path], list[Track]]:
    artist = CatalogArtist(name="Juice WRLD", mbid="4e4ebde4-0c56-4dec-844b-6c73adcdd92d")
    album = CatalogAlbum(
        title="Death Race For Love (Bonus Track Version)",
        year="2022",
        release_type="album",
        mbid="9c8f6278-1fcb-4299-939d-c00ce290730a",
        track_count=2,
        in_library=True,
    )
    artist.albums.append(album)
    album.tracks.extend(
        [
            CatalogAlbumTrack(
                disc=1,
                position=17,
                title="The Bees Knees",
                recording_mbid="9370ac3a-4911-470d-a7d9-66f41b66bf78",
            ),
            CatalogAlbumTrack(
                disc=1,
                position=19,
                title="10 Feet",
                recording_mbid="636bd1e8-23a7-4a2c-8fc5-2d671b7e4710",
            ),
        ]
    )
    db.add(artist)
    await db.flush()
    folder = library_root / artist.name / f"{album.title} ({album.year})"
    folder.mkdir(parents=True)
    paths: list[Path] = []
    imported_tracks: list[Track] = []
    for index, catalog_track in enumerate(album.tracks, start=1):
        job = Job(source="slskd", query=catalog_track.title, status=JobStatus.done)
        release = Release(
            job=job,
            source="slskd",
            title=album.title,
            album_artist="Juice Wrld" if index == 1 else "Juice WRLD",
            year="2019" if index == 1 else "2022",
        )
        path = folder / f"{catalog_track.position:02d} - {catalog_track.title}.flac"
        path.write_bytes(_minimal_flac_bytes())
        flac = FLAC(path)
        for key, value in {
            "title": catalog_track.title,
            "artist": "Juice WRLD",
            "album": album.title,
            "albumartist": "Juice Wrld",
            "albumartists": "Juice Wrld",
            "date": "2019",
            "musicbrainz_albumid": f"wrong-release-{index}",
            "musicbrainz_albumartistid": f"wrong-artist-{index}",
            "musicbrainz_releasegroupid": f"stale-group-{index}",
        }.items():
            flac[key] = value
        flac.save()
        track = Track(
            job=job,
            release=release,
            catalog_album_id=album.id,
            catalog_track_id=catalog_track.id,
            title=catalog_track.title,
            artist="Juice WRLD",
            album_artist=release.album_artist,
            album=album.title,
            year=release.year,
            disc=1,
            track_no=catalog_track.position,
            mbid=catalog_track.recording_mbid,
            source="slskd",
            source_path=f"/staging/{path.name}",
            import_state=ImportWorkflowState.imported,
        )
        track.import_plans.append(
            ImportPlan(
                release=release,
                source_path=f"/staging/{path.name}",
                destination_path=str(path),
                status=ImportWorkflowState.imported,
            )
        )
        db.add(track)
        paths.append(path)
        imported_tracks.append(track)
    await db.flush()
    return album, paths, imported_tracks


async def test_retag_catalog_album_synchronizes_release_tags_without_changing_database(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library_root = tmp_path / "library"
    album, paths, tracks = await _seed_imported_album(db_session, library_root)
    original_db_values = [
        (track.album_artist, track.year, track.mbid, track.import_plans[0].destination_path)
        for track in tracks
    ]
    result = await retag_catalog_album(db_session, album.id, library_root=library_root)
    assert result.files_retagged == 2
    assert result.folder == paths[0].parent
    for path, catalog_track in zip(paths, album.tracks, strict=True):
        tags = {key.casefold(): values for key, values in FLAC(path).tags.items()}
        assert tags["album"] == [album.title]
        assert tags["albumartist"] == [album.artist.name]
        assert tags["albumartists"] == [album.artist.name]
        assert tags["date"] == [album.year]
        assert tags["musicbrainz_releasegroupid"] == [album.mbid]
        assert tags["musicbrainz_albumartistid"] == [album.artist.mbid]
        assert tags["musicbrainz_trackid"] == [catalog_track.recording_mbid]
        assert tags["tracknumber"] == [str(catalog_track.position)]
        assert tags["discnumber"] == [str(catalog_track.disc)]
        assert "musicbrainz_albumid" not in tags
    assert [
        (track.album_artist, track.year, track.mbid, track.import_plans[0].destination_path)
        for track in tracks
    ] == original_db_values


class _FailingSecondWriter(MutagenTagWriter):
    def __init__(self) -> None:
        self.calls = 0

    def write_and_verify(self, path: Path, tags: dict[str, str]) -> bool:
        self.calls += 1
        return False if self.calls == 2 else super().write_and_verify(path, tags)


async def test_retag_catalog_album_leaves_every_original_untouched_when_preparation_fails(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library_root = tmp_path / "library"
    album, paths, _tracks = await _seed_imported_album(db_session, library_root)
    original_bytes = [path.read_bytes() for path in paths]
    with pytest.raises(ImportExecutionError, match="tag readback failed"):
        await retag_catalog_album(
            db_session, album.id, library_root=library_root, tag_writer=_FailingSecondWriter()
        )
    assert [path.read_bytes() for path in paths] == original_bytes
    assert not list(paths[0].parent.glob(".*.retag-*"))


async def test_retag_catalog_album_rejects_untracked_audio_in_release_folder(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library_root = tmp_path / "library"
    album, paths, _tracks = await _seed_imported_album(db_session, library_root)
    extra = paths[0].parent / "18 - Unknown.flac"
    extra.write_bytes(_minimal_flac_bytes())
    original_bytes = [path.read_bytes() for path in paths]
    with pytest.raises(ImportExecutionError, match="not linked to stored track metadata"):
        await retag_catalog_album(db_session, album.id, library_root=library_root)
    assert [path.read_bytes() for path in paths] == original_bytes


def test_tag_writer_preserves_mp3_artwork_while_replacing_grouping_tags(tmp_path: Path) -> None:
    path = tmp_path / "track.mp3"
    original = ID3()
    original.add(APIC(encoding=3, mime="image/jpeg", type=3, desc="Cover", data=b"cover"))
    original.add(TXXX(encoding=3, desc="MusicBrainz Release Group Id", text="wrong-release-group"))
    original.save(path)

    assert MutagenTagWriter().write_and_verify(
        path,
        {
            "title": "Track",
            "artist": "Artist",
            "album": "Album",
            "album_artist": "Artist",
            "tracknumber": "1",
            "discnumber": "1",
            "musicbrainz_releasegroupid": "canonical-release-group",
        },
    )

    repaired = ID3(path)
    assert repaired.getall("APIC")[0].data == b"cover"
    release_groups = [
        str(frame.text[0])
        for frame in repaired.getall("TXXX")
        if frame.desc.casefold() == "musicbrainz release group id"
    ]
    assert release_groups == ["canonical-release-group"]
