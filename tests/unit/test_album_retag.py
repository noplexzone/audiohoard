from __future__ import annotations

import base64
import os
import shutil
import stat
import subprocess
import threading
from pathlib import Path

import httpx
import pytest
from mutagen.flac import FLAC, Picture
from mutagen.id3 import APIC, ID3, TXXX
from mutagen.oggvorbis import OggVorbis
from sqlalchemy.ext.asyncio import AsyncSession

import app.services.library_import as library_import_module
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState
from app.services.library_import import (
    CanonicalArtwork,
    ImportExecutionError,
    MutagenTagWriter,
    _discover_legacy_album_files,
    retag_catalog_album,
)
from app.services.pinned_destination import PinnedDestination


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


def test_canonical_artwork_fetch_rejects_untrusted_hosts() -> None:
    assert not library_import_module._artwork_url_allowed("http://coverartarchive.org/release/x")
    assert not library_import_module._artwork_url_allowed("https://example.test/cover.jpg")
    assert library_import_module._artwork_url_allowed(
        "https://coverartarchive.org/release/x/front"
    )


class _FakeArtworkResponse:
    def __init__(
        self, chunks: list[bytes], headers: dict[str, str], status_code: int = 200
    ) -> None:
        self.status_code = status_code
        self.headers = headers
        self._chunks = chunks
        self.closed = False

    async def aiter_bytes(self):
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


async def test_canonical_artwork_fetch_enforces_size_cap(monkeypatch) -> None:
    response = _FakeArtworkResponse(
        [b"x"], {"content-type": "image/jpeg", "content-length": "5242881"}
    )

    async def fake_stream_with_retry(client, method: str, url: str):
        return response

    monkeypatch.setattr(library_import_module, "stream_with_retry", fake_stream_with_retry)

    artwork = await library_import_module._fetch_canonical_artwork(
        "https://coverartarchive.org/release/x/front"
    )

    assert artwork is None
    assert response.closed


async def test_canonical_artwork_fetch_streams_allowed_jpeg(monkeypatch) -> None:
    response = _FakeArtworkResponse(
        [b"\xff\xd8", b"jpeg"], {"content-type": "application/octet-stream"}
    )

    async def fake_stream_with_retry(client, method: str, url: str):
        assert isinstance(client, httpx.AsyncClient)
        assert method == "GET"
        return response

    monkeypatch.setattr(library_import_module, "stream_with_retry", fake_stream_with_retry)

    artwork = await library_import_module._fetch_canonical_artwork(
        "https://coverartarchive.org/release/x/front"
    )

    assert artwork == CanonicalArtwork(data=b"\xff\xd8jpeg", mime="image/jpeg")
    assert response.closed


async def test_canonical_artwork_fetch_follows_cover_art_archive_redirects(monkeypatch) -> None:
    archive_url = "https://archive.org/download/mbid-release/cover_thumb250.jpg"
    cdn_url = "https://dn721704.ca.archive.org/0/items/mbid-release/cover_thumb250.jpg"
    responses = {
        "https://coverartarchive.org/release-group/rg/front-250": _FakeArtworkResponse(
            [],
            {
                "location": archive_url,
            },
            status_code=307,
        ),
        archive_url: _FakeArtworkResponse(
            [],
            {"location": cdn_url},
            status_code=302,
        ),
        cdn_url: _FakeArtworkResponse(
            [b"\xff\xd8", b"jpeg"],
            {"content-type": "image/jpeg"},
        ),
    }
    requested: list[str] = []

    async def fake_stream_with_retry(client, method: str, url: str):
        requested.append(url)
        return responses[url]

    monkeypatch.setattr(library_import_module, "stream_with_retry", fake_stream_with_retry)

    artwork = await library_import_module._fetch_canonical_artwork(
        "https://coverartarchive.org/release-group/rg/front-250"
    )

    assert artwork == CanonicalArtwork(data=b"\xff\xd8jpeg", mime="image/jpeg")
    assert requested == list(responses)
    assert all(response.closed for response in responses.values())


async def test_canonical_artwork_fetch_rejects_untrusted_redirect(monkeypatch) -> None:
    response = _FakeArtworkResponse(
        [], {"location": "https://example.test/cover.jpg"}, status_code=302
    )

    async def fake_stream_with_retry(client, method: str, url: str):
        return response

    monkeypatch.setattr(library_import_module, "stream_with_retry", fake_stream_with_retry)

    artwork = await library_import_module._fetch_canonical_artwork(
        "https://coverartarchive.org/release-group/rg/front-250"
    )

    assert artwork is None
    assert response.closed


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
            "album artist": "Future & Lil Uzi Vert",
            "album_artists": "Future; Lil Uzi Vert",
            "albumartist_credit": "Future & Lil Uzi Vert",
            "albumartists_credit": "Future; Lil Uzi Vert",
            "albumartist": "Juice Wrld",
            "albumartists": "Juice Wrld",
            "albumartistsort": "Future & Lil Uzi Vert",
            "albumartists_sort": "Future; Lil Uzi Vert",
            "date": "2019",
            "musicbrainz_albumid": f"wrong-release-{index}",
            "musicbrainz_albumartistid": f"wrong-artist-{index}",
            "musicbrainz_releasegroupid": f"stale-group-{index}",
            "release_date": "",
            "genre": "Hip Hop",
            "organization": "Grade A Productions/Interscope Records",
            "label": "Grade A Productions/Interscope Records",
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
        assert "album artist" not in tags
        assert tags["album_artist"] == [album.artist.name]
        assert tags["albumartist"] == [album.artist.name]
        assert tags["albumartists"] == [album.artist.name]
        assert "album_artists" not in tags
        assert "albumartist_credit" not in tags
        assert "albumartists_credit" not in tags
        assert "albumartistsort" not in tags
        assert "albumartists_sort" not in tags
        assert tags["date"] == [album.year]
        assert tags["releasedate"] == [album.year]
        assert tags["release_date"] == [album.year]
        assert "genre" not in tags
        assert "organization" not in tags
        assert "label" not in tags
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


def test_mutagen_tag_writer_embeds_ogg_cover_art_and_clears_album_artist_aliases(
    tmp_path: Path,
) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        pytest.skip("ffmpeg is required for a real Ogg/Vorbis fixture")
    path = tmp_path / "track.ogg"
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
            "anullsrc=r=44100:cl=mono",
            "-t",
            "0.1",
            "-c:a",
            "libvorbis",
            str(path),
        ],
        check=True,
    )
    original = OggVorbis(path)
    original["album artist"] = "Future & Lil Uzi Vert"
    original["albumartist_credit"] = "Future & Lil Uzi Vert"
    original["albumartists_sort"] = "Future/Lil Uzi Vert"
    original.save()

    ok = MutagenTagWriter().write_and_verify(
        path,
        {
            "title": "That Way",
            "artist": "Lil Uzi Vert",
            "album": "That Way",
            "album_artist": "Lil Uzi Vert",
            "date": "2020",
            "releasedate": "2020",
            "release_date": "2020",
            "tracknumber": "1",
            "discnumber": "1",
        },
        CanonicalArtwork(data=b"\xff\xd8canonical-cover", mime="image/jpeg"),
    )

    assert ok is True
    repaired = OggVorbis(path)
    tags = {key.casefold(): values for key, values in repaired.tags.items()}
    assert tags["albumartist"] == ["Lil Uzi Vert"]
    assert tags["albumartists"] == ["Lil Uzi Vert"]
    assert tags["album_artist"] == ["Lil Uzi Vert"]
    assert "album artist" not in tags
    assert "albumartist_credit" not in tags
    assert "albumartists_sort" not in tags
    pictures = tags.get("metadata_block_picture", [])
    assert len(pictures) == 1
    picture = Picture(base64.b64decode(pictures[0]))
    assert picture.mime == "image/jpeg"
    assert picture.data == b"\xff\xd8canonical-cover"


async def test_retag_catalog_album_overwrites_flac_cover_art(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    album, paths, _tracks = await _seed_imported_album(db_session, library_root)
    album.artwork_url = "https://example.test/cover.jpg"
    original = FLAC(paths[0])
    original.clear_pictures()
    original.save()

    async def fake_fetch(url: str | None) -> CanonicalArtwork | None:
        assert url == album.artwork_url
        return CanonicalArtwork(data=b"\xff\xd8canonical-cover", mime="image/jpeg")

    monkeypatch.setattr(library_import_module, "_fetch_canonical_artwork", fake_fetch)

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 2
    for path in paths:
        repaired = FLAC(path)
        assert len(repaired.pictures) == 1
        assert repaired.pictures[0].mime == "image/jpeg"
        assert repaired.pictures[0].data == b"\xff\xd8canonical-cover"


async def test_retag_catalog_album_discovers_legacy_library_files_without_import_rows(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    artist = CatalogArtist(name="Juice WRLD", mbid="artist-mbid")
    album = CatalogAlbum(
        title="Goodbye & Good Riddance",
        year="2018",
        release_type="album",
        mbid="release-group",
        track_count=2,
    )
    artist.albums.append(album)
    album.tracks.extend(
        [
            CatalogAlbumTrack(disc=1, position=1, title="Intro", recording_mbid="intro-mbid"),
            CatalogAlbumTrack(
                disc=1, position=2, title="All Girls Are the Same", recording_mbid="agats-mbid"
            ),
        ]
    )
    db_session.add(artist)
    await db_session.flush()
    folder = library_root / artist.name / f"{album.title} ({album.year})"
    folder.mkdir(parents=True)
    paths = [folder / "01 - Intro.flac", folder / "02 - All Girls Are the Same.flac"]
    for path in paths:
        path.write_bytes(_minimal_flac_bytes())
        flac = FLAC(path)
        flac["albumartist"] = "Juice Wrld"
        flac["date"] = "2024"
        flac.save()

    async def no_artwork(url: str | None) -> CanonicalArtwork | None:
        return None

    monkeypatch.setattr(library_import_module, "_fetch_canonical_artwork", no_artwork)

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 2
    assert FLAC(paths[0])["albumartist"] == [artist.name]
    assert FLAC(paths[0])["date"] == [album.year]
    assert FLAC(paths[1])["musicbrainz_trackid"] == ["agats-mbid"]


async def test_retag_catalog_album_renames_multidisc_files_to_disc_track_template(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    artist = CatalogArtist(name="Morgan Wallen", mbid="artist-mbid")
    album = CatalogAlbum(title="I’m The Problem", year="2025", release_type="album", track_count=2)
    artist.albums.append(album)
    album.tracks.extend(
        [
            CatalogAlbumTrack(disc=1, position=1, title="I'm the Problem", recording_mbid="one"),
            CatalogAlbumTrack(disc=3, position=9, title="LA Night", recording_mbid="la"),
        ]
    )
    job = Job(source="slskd", query="morgan", status=JobStatus.done)
    release = Release(job=job, source="slskd", title=album.title, album_artist=artist.name)
    db_session.add_all([artist, job, release])
    await db_session.flush()
    folder = library_root / artist.name / f"{album.title} ({album.year})"
    folder.mkdir(parents=True)
    old_path = folder / "09 - LA Night.flac"
    old_path.write_bytes(_minimal_flac_bytes())
    track = Track(
        job=job,
        release=release,
        source="slskd",
        title="LA Night",
        album=album.title,
        album_artist=artist.name,
        catalog_album_id=album.id,
        catalog_track_id=album.tracks[1].id,
        disc=3,
        disc_total=None,
        track_no=9,
        import_state=ImportWorkflowState.imported,
    )
    plan = ImportPlan(
        release=release,
        track=track,
        source_path=str(old_path),
        destination_path=str(old_path),
        status=ImportWorkflowState.imported,
    )
    db_session.add_all([track, plan])
    await db_session.flush()

    async def no_artwork(url: str | None) -> CanonicalArtwork | None:
        return None

    monkeypatch.setattr(library_import_module, "_fetch_canonical_artwork", no_artwork)
    db_session.expire(track, ["import_plans"])

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    new_path = folder / "3-09 - LA Night.flac"
    assert result.files_retagged == 1
    assert result.files_renamed == 1
    assert not old_path.exists()
    assert new_path.exists()
    assert plan.destination_path == str(new_path)
    assert track.disc_total == 3
    repaired = FLAC(new_path)
    assert repaired["discnumber"] == ["3"]
    assert repaired["disctotal"] == ["3"]
    assert repaired["tracktotal"] == ["1"]


async def test_retag_catalog_album_matches_flat_multidisc_files_by_title(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    artist = CatalogArtist(name="Ty Myers", mbid="artist-mbid")
    album = CatalogAlbum(
        title="The Select (Deluxe)", year="2025", release_type="album", track_count=3
    )
    artist.albums.append(album)
    album.tracks.extend(
        [
            CatalogAlbumTrack(
                disc=1,
                position=5,
                title="Worry is a Sickness (Acoustic)",
                recording_mbid="acoustic-mbid",
            ),
            CatalogAlbumTrack(
                disc=2, position=1, title="Ends of the Earth", recording_mbid="ends-mbid"
            ),
            CatalogAlbumTrack(
                disc=2,
                position=5,
                title="Love Is Two Faced",
                recording_mbid="love-mbid",
            ),
        ]
    )
    db_session.add(artist)
    await db_session.flush()
    folder = library_root / artist.name / f"{album.title} ({album.year})"
    folder.mkdir(parents=True)
    acoustic = folder / "05 - Worry is a Sickness (Acoustic).flac"
    ends = folder / "01 - Ends of the Earth.flac"
    love = folder / "05 - Love Is Two Faced.flac"
    for path in (acoustic, ends, love):
        path.write_bytes(_minimal_flac_bytes())

    async def no_artwork(url: str | None) -> CanonicalArtwork | None:
        return None

    monkeypatch.setattr(library_import_module, "_fetch_canonical_artwork", no_artwork)

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 3
    acoustic_new = folder / "1-05 - Worry is a Sickness (Acoustic).flac"
    ends_new = folder / "2-01 - Ends of the Earth.flac"
    love_new = folder / "2-05 - Love Is Two Faced.flac"
    assert not acoustic.exists()
    assert not ends.exists()
    assert not love.exists()
    assert FLAC(acoustic_new)["discnumber"] == ["1"]
    assert FLAC(acoustic_new)["musicbrainz_trackid"] == ["acoustic-mbid"]
    assert FLAC(ends_new)["discnumber"] == ["2"]
    assert FLAC(ends_new)["musicbrainz_trackid"] == ["ends-mbid"]
    assert FLAC(love_new)["discnumber"] == ["2"]
    assert FLAC(love_new)["musicbrainz_trackid"] == ["love-mbid"]


async def test_retag_catalog_album_maps_multidisc_legacy_filenames(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    artist = CatalogArtist(name="Various Artist", mbid="artist-mbid")
    album = CatalogAlbum(title="Double Album", year="2020", release_type="album", track_count=2)
    artist.albums.append(album)
    album.tracks.extend(
        [
            CatalogAlbumTrack(disc=1, position=1, title="Disc One", recording_mbid="disc-one"),
            CatalogAlbumTrack(disc=2, position=1, title="Disc Two", recording_mbid="disc-two"),
        ]
    )
    db_session.add(artist)
    await db_session.flush()
    folder = library_root / artist.name / f"{album.title} ({album.year})"
    folder.mkdir(parents=True)
    disc_one = folder / "1-01 - Disc One.flac"
    disc_two = folder / "2-01 - Disc Two.flac"
    for path in (disc_one, disc_two):
        path.write_bytes(_minimal_flac_bytes())

    async def no_artwork(url: str | None) -> CanonicalArtwork | None:
        return None

    monkeypatch.setattr(library_import_module, "_fetch_canonical_artwork", no_artwork)

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 2
    assert FLAC(disc_one)["musicbrainz_trackid"] == ["disc-one"]
    assert FLAC(disc_one)["discnumber"] == ["1"]
    assert FLAC(disc_two)["musicbrainz_trackid"] == ["disc-two"]
    assert FLAC(disc_two)["discnumber"] == ["2"]


class _FailingSecondWriter(MutagenTagWriter):
    def __init__(self) -> None:
        self.calls = 0

    def write_and_verify(self, path: Path, tags: dict[str, str], **kwargs) -> bool:
        self.calls += 1
        return False if self.calls == 2 else super().write_and_verify(path, tags, **kwargs)


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


def test_tag_writer_clears_nav_grouping_txxx_fields_that_split_mp3_albums(
    tmp_path: Path,
) -> None:
    path = tmp_path / "track.mp3"
    original = ID3()
    original.add(TXXX(encoding=3, desc="BARCODE", text="602445694884"))
    original.add(TXXX(encoding=3, desc="ALBUMVERSION", text="explicit, bonus"))
    original.add(TXXX(encoding=3, desc="MusicBrainz Album Comment", text="explicit, bonus"))
    original.add(TXXX(encoding=3, desc="DISC", text="1"))
    original.add(TXXX(encoding=3, desc="DISCC", text="1"))
    original.add(TXXX(encoding=3, desc="TRACK", text="16"))
    original.add(TXXX(encoding=3, desc="TRACKC", text="37"))
    original.add(TXXX(encoding=3, desc="MEDIA", text="Digital Media"))
    original.add(TXXX(encoding=3, desc="MusicBrainz Album Status", text="official"))
    original.add(TXXX(encoding=3, desc="MusicBrainz Album Type", text="album"))
    original.add(TXXX(encoding=3, desc="MusicBrainz Album Release Country", text="US"))
    original.save(path)

    assert MutagenTagWriter().write_and_verify(
        path,
        {
            "title": "Lean Wit Me",
            "artist": "Juice WRLD",
            "album": "Goodbye & Good Riddance",
            "album_artist": "Juice WRLD",
            "date": "2018",
            "releasedate": "2018",
            "release_date": "2018",
            "tracknumber": "4",
            "discnumber": "1",
        },
    )

    stale_descriptions = {
        frame.desc.casefold() for frame in ID3(path).getall("TXXX") if frame.text
    }
    assert stale_descriptions.isdisjoint(
        {
            "barcode",
            "albumversion",
            "musicbrainz album comment",
            "disc",
            "discc",
            "track",
            "trackc",
            "media",
            "musicbrainz album status",
            "musicbrainz album type",
            "musicbrainz album release country",
        }
    )


def test_tag_writer_clears_nav_grouping_fields_that_split_flac_albums(tmp_path: Path) -> None:
    path = tmp_path / "track.flac"
    path.write_bytes(_minimal_flac_bytes())
    original = FLAC(path)
    original["title"] = "Feel Alone"
    original["album"] = "Fighting Demons (Digital Deluxe)"
    original["albumartist"] = "Juice WRLD"
    original["year"] = "2021"
    original["releasedate"] = "2021"
    original["release_date"] = "2021"
    original["originaldate"] = "2021"
    original["originalyear"] = "2021"
    original["recordlabel"] = "Grade A Productions/Interscope Records"
    original["albumversion"] = "explicit, bonus"
    original["musicbrainz_albumcomment"] = "explicit, bonus"
    original["disc"] = "1"
    original["discc"] = "1"
    original["track"] = "16"
    original["trackc"] = "37"
    original["musicbrainz_albumstatus"] = "official"
    original["musicbrainz_albumtype"] = "album"
    original["musicbrainz_artistid"] = "stale-artist"
    original["musicbrainz_releasetrackid"] = "stale-release-track"
    original["barcode"] = "602445694884"
    original["isrc"] = "USUG12106076"
    original["media"] = "Digital Media"
    original["releasecountry"] = "US"
    original["releasestatus"] = "official"
    original["releasetype"] = "album"
    original["tracktotal"] = "23"
    original["disctotal"] = "1"
    original.save()

    assert MutagenTagWriter().write_and_verify(
        path,
        {
            "title": "Feel Alone",
            "artist": "Juice WRLD",
            "album": "Fighting Demons (Digital Deluxe)",
            "album_artist": "Juice WRLD",
            "date": "2022",
            "releasedate": "2022",
            "release_date": "2022",
            "tracknumber": "19",
            "discnumber": "1",
        },
    )

    tags = {key.casefold(): values for key, values in FLAC(path).tags.items()}
    for key in {
        "year",
        "originaldate",
        "originalyear",
        "recordlabel",
        "albumversion",
        "musicbrainz_albumcomment",
        "disc",
        "discc",
        "track",
        "trackc",
        "musicbrainz_albumstatus",
        "musicbrainz_albumtype",
        "musicbrainz_artistid",
        "musicbrainz_releasetrackid",
        "barcode",
        "isrc",
        "media",
        "releasecountry",
        "releasestatus",
        "releasetype",
        "tracktotal",
        "disctotal",
    }:
        assert key not in tags
    assert tags["releasedate"] == ["2022"]


def test_tag_writer_clears_nav_grouping_fields_from_ogg_vorbis(tmp_path: Path) -> None:
    fixture = Path(__file__).parents[1] / "fixtures" / "audio" / "minimal.ogg"
    path = tmp_path / "track.ogg"
    path.write_bytes(fixture.read_bytes())
    original = OggVorbis(path)
    original["albumversion"] = "explicit, bonus"
    original["musicbrainz_albumcomment"] = "explicit, bonus"
    original["disc"] = "1"
    original["discc"] = "1"
    original["track"] = "16"
    original["trackc"] = "37"
    original.save()

    assert MutagenTagWriter().write_and_verify(
        path,
        {
            "title": "Revelation",
            "artist": "Morgan Wallen",
            "album": "I’m The Problem",
            "album_artist": "Morgan Wallen",
            "date": "2025",
            "tracknumber": "3",
            "discnumber": "2",
            "tracktotal": "12",
            "disctotal": "3",
        },
    )

    tags = {key.casefold(): values for key, values in OggVorbis(path).tags.items()}
    for key in {
        "albumversion",
        "musicbrainz_albumcomment",
        "disc",
        "discc",
        "track",
        "trackc",
    }:
        assert key not in tags
    assert tags["discnumber"] == ["2"]
    assert tags["tracknumber"] == ["3"]


async def test_retag_catalog_album_repairs_mixed_imported_and_legacy_files(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library_root = tmp_path / "library"
    album, paths, tracks = await _seed_imported_album(db_session, library_root)
    legacy_catalog = CatalogAlbumTrack(
        disc=1,
        position=12,
        title="Syphilis",
        recording_mbid="a274f59b-78b6-4ee4-91f7-153befea1600",
    )
    album.tracks.append(legacy_catalog)
    legacy_path = paths[0].parent / "12 - Syphilis.flac"
    legacy_path.write_bytes(_minimal_flac_bytes())
    legacy = FLAC(legacy_path)
    legacy["title"] = "Syphilis"
    legacy["album"] = album.title
    legacy["albumartist"] = "Juice WRLD"
    legacy["date"] = "2019"
    legacy["originalyear"] = "2019"
    legacy["musicbrainz_albumtype"] = "album"
    legacy.save()
    await db_session.flush()

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 3
    repaired = {key.casefold(): values for key, values in FLAC(legacy_path).tags.items()}
    assert repaired["date"] == [album.year]
    assert repaired["releasedate"] == [album.year]
    assert repaired["release_date"] == [album.year]
    assert repaired["musicbrainz_trackid"] == [legacy_catalog.recording_mbid]
    assert "originalyear" not in repaired
    assert "musicbrainz_albumtype" not in repaired
    assert all(path.exists() for path in paths)


async def test_retag_catalog_album_emits_final_scanner_notification(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    album, _paths, _tracks = await _seed_imported_album(db_session, library_root)
    notifications: list[str] = []

    def notify_changed(self: PinnedDestination, name: str | None = None) -> None:
        notifications.append(name or self.name)

    monkeypatch.setattr(PinnedDestination, "notify_changed", notify_changed, raising=False)

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 2
    assert len(notifications) == 1


async def test_retag_catalog_album_supports_legacy_unmapped_imports(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library_root = tmp_path / "library"
    album, paths, tracks = await _seed_imported_album(db_session, library_root)
    album.title = "Édition"
    album.artist.name = "Beyoncé"
    for track in tracks:
        track.catalog_album_id = None
        track.catalog_track_id = None
        track.album = "ÉDITION"
        track.album_artist = "BEYONCÉ"
    await db_session.flush()

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 2
    assert all(FLAC(path)["albumartist"] == [album.artist.name] for path in paths)


async def test_retag_catalog_album_rejects_duplicate_destination_mappings(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library_root = tmp_path / "library"
    album, paths, tracks = await _seed_imported_album(db_session, library_root)
    tracks[1].import_plans[0].destination_path = str(paths[0])
    await db_session.flush()

    with pytest.raises(ImportExecutionError, match="duplicate destination mapping"):
        await retag_catalog_album(db_session, album.id, library_root=library_root)


class _AddsAudioDuringPreparation(MutagenTagWriter):
    def __init__(self, folder: Path) -> None:
        self.folder = folder
        self.calls = 0

    def write_and_verify(self, path: Path, tags: dict[str, str], **kwargs) -> bool:
        result = super().write_and_verify(path, tags, **kwargs)
        self.calls += 1
        if self.calls == 2:
            (self.folder / "18 - Added.flac").write_bytes(_minimal_flac_bytes())
        return result


async def test_retag_catalog_album_rechecks_folder_membership_before_commit(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library_root = tmp_path / "library"
    album, paths, _tracks = await _seed_imported_album(db_session, library_root)
    original_bytes = [path.read_bytes() for path in paths]

    with pytest.raises(ImportExecutionError, match="album folder changed before retag commit"):
        await retag_catalog_album(
            db_session,
            album.id,
            library_root=library_root,
            tag_writer=_AddsAudioDuringPreparation(paths[0].parent),
        )

    assert [path.read_bytes() for path in paths] == original_bytes


async def test_retag_catalog_album_fsyncs_tagged_temps_before_replacement(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    album, _paths, _tracks = await _seed_imported_album(db_session, library_root)
    regular_file_fsyncs = 0
    real_fsync = os.fsync

    def tracking_fsync(fd: int) -> None:
        nonlocal regular_file_fsyncs
        if stat.S_ISREG(os.fstat(fd).st_mode):
            regular_file_fsyncs += 1
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", tracking_fsync)
    await retag_catalog_album(db_session, album.id, library_root=library_root)

    # One fsync after the copy and another after Mutagen writes, per file.
    assert regular_file_fsyncs >= 4


async def test_retag_catalog_album_runs_file_work_off_the_event_loop(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    album, _paths, _tracks = await _seed_imported_album(db_session, library_root)
    event_loop_thread = threading.get_ident()
    worker_threads: list[int] = []
    real_retag = library_import_module._retag_catalog_album_files

    def tracking_retag(*args, **kwargs):
        worker_threads.append(threading.get_ident())
        return real_retag(*args, **kwargs)

    monkeypatch.setattr(library_import_module, "_retag_catalog_album_files", tracking_retag)
    await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert worker_threads
    assert worker_threads[0] != event_loop_thread


async def test_retag_catalog_album_restores_all_files_when_replacement_fails(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    album, paths, _tracks = await _seed_imported_album(db_session, library_root)
    original_bytes = [path.read_bytes() for path in paths]
    real_replace = PinnedDestination.replace
    replacement_calls = 0

    def fail_second_replacement(
        self: PinnedDestination, source_name: str, destination_name: str
    ) -> None:
        nonlocal replacement_calls
        if source_name.endswith(self.destination.suffix) and destination_name == self.name:
            replacement_calls += 1
            if replacement_calls == 2:
                raise OSError("injected replacement failure")
        real_replace(self, source_name, destination_name)

    monkeypatch.setattr(PinnedDestination, "replace", fail_second_replacement)
    with pytest.raises(ImportExecutionError, match="injected replacement failure"):
        await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert [path.read_bytes() for path in paths] == original_bytes
    assert not list(paths[0].parent.glob(".*.retag-*"))


async def test_retag_catalog_album_close_failure_does_not_remove_committed_files(
    db_session: AsyncSession, tmp_path: Path, monkeypatch
) -> None:
    library_root = tmp_path / "library"
    album, paths, _tracks = await _seed_imported_album(db_session, library_root)
    real_close = PinnedDestination.close
    raised = False

    def close_then_fail_once(self: PinnedDestination) -> None:
        nonlocal raised
        real_close(self)
        if not raised:
            raised = True
            raise OSError("injected close failure")

    monkeypatch.setattr(PinnedDestination, "close", close_then_fail_once)
    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 2
    assert all(path.is_file() for path in paths)
    assert all(FLAC(path)["albumartist"] == [album.artist.name] for path in paths)


async def test_retag_compilation_projects_track_and_album_credits_without_relocating_folder(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library_root = tmp_path / "library"
    album, paths, tracks = await _seed_imported_album(db_session, library_root)
    album.release_type = "compilation"
    album.is_compilation = True
    album.album_artist_name = "Various Artists"
    performers = ["Olivia Rodrigo", "Rachel Zegler"]
    for catalog_track, track, performer, path in zip(
        album.tracks, tracks, performers, paths, strict=True
    ):
        catalog_track.artist_name = performer
        track.artist = album.artist.name
        track.album_artist = album.artist.name
        tagged = FLAC(path)
        tagged["replaygain_track_gain"] = "-7.0 dB"
        tagged.save()
    first = FLAC(paths[0])
    picture = Picture()
    picture.type = 3
    picture.mime = "image/jpeg"
    picture.data = b"existing-cover"
    first.add_picture(picture)
    first.save()
    original_folder = paths[0].parent

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 2
    assert result.files_renamed == 0
    assert result.folder == original_folder
    assert all(path.parent == original_folder and path.exists() for path in paths)
    for path, performer in zip(paths, performers, strict=True):
        repaired = FLAC(path)
        assert repaired["artist"] == [performer]
        assert repaired["album_artist"] == ["Various Artists"]
        assert repaired["albumartist"] == ["Various Artists"]
        assert repaired["replaygain_track_gain"] == ["-7.0 dB"]
    assert FLAC(paths[0]).pictures[0].data == b"existing-cover"


async def test_retag_compilation_missing_child_artist_uses_owner_in_legacy_folder(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    library_root = tmp_path / "library"
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
        disc=1, position=1, title="The Hanging Tree", artist_name=None
    )
    album.tracks.append(catalog_track)
    db_session.add(owner)
    await db_session.flush()
    folder = library_root / "Various Artists" / "Soundtrack (2023)"
    folder.mkdir(parents=True)
    path = folder / "01 - The Hanging Tree.flac"
    path.write_bytes(_minimal_flac_bytes())

    result = await retag_catalog_album(db_session, album.id, library_root=library_root)

    assert result.files_retagged == 1
    assert result.files_renamed == 0
    assert path.exists()
    assert FLAC(path)["artist"] == [owner.name]
    assert FLAC(path)["albumartist"] == ["Various Artists"]


@pytest.mark.parametrize(
    ("owner_name", "album_artist_name", "title", "year"),
    [
        ("../escape", "Various Artists", "Soundtrack", "2023"),
        ("Olivia Rodrigo", "/tmp/absolute", "Soundtrack", "2023"),
        ("Olivia Rodrigo", "Various Artists", "../escape", "2023"),
        ("Olivia Rodrigo", "Various Artists", "Soundtrack", "../escape"),
    ],
)
def test_legacy_discovery_rejects_unsafe_provider_folder_segments(
    tmp_path: Path,
    owner_name: str,
    album_artist_name: str,
    title: str,
    year: str,
) -> None:
    album = CatalogAlbum(
        artist=CatalogArtist(name=owner_name),
        title=title,
        year=year,
        release_type="compilation",
        is_compilation=True,
        album_artist_name=album_artist_name,
    )

    with pytest.raises(ImportExecutionError, match="unsafe legacy album folder segment"):
        _discover_legacy_album_files(album, tmp_path / "library")


def test_legacy_discovery_rejects_symlinked_artist_parent_outside_library(
    tmp_path: Path,
) -> None:
    library_root = tmp_path / "library"
    library_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (library_root / "Olivia Rodrigo").symlink_to(outside, target_is_directory=True)
    album = CatalogAlbum(
        artist=CatalogArtist(name="Olivia Rodrigo"),
        title="Soundtrack",
        year="2023",
        release_type="compilation",
        is_compilation=True,
        album_artist_name="Various Artists",
    )
    outside_folder = outside / "Soundtrack (2023)"
    outside_folder.mkdir()
    (outside_folder / "01 - Song.flac").write_bytes(_minimal_flac_bytes())

    with pytest.raises(ImportExecutionError, match="symlinked path"):
        _discover_legacy_album_files(album, library_root)
