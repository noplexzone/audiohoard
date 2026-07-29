from __future__ import annotations

import asyncio
import re

from pytest_httpx import HTTPXMock

from app.metadata import deezer as deezer_module
from app.metadata.deezer import DeezerClient
from app.metadata.itunes import ITunesClient, _parse_album_hit
from app.metadata.musicbrainz import MusicBrainzClient

UA = "test-app/0.3.0 (test@example.com)"


async def test_musicbrainz_catalog_artist_discography_album(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=re.compile(r"https://musicbrainz[.]org/ws/2/artist[?].*"),
        json={
            "artists": [
                {"id": "artist-mbid", "name": "Björk", "disambiguation": "Icelandic musician"}
            ]
        },
    )
    httpx_mock.add_response(
        url="https://musicbrainz.org/ws/2/artist/artist-mbid?fmt=json",
        json={"id": "artist-mbid", "name": "Björk", "disambiguation": "Icelandic musician"},
    )
    httpx_mock.add_response(
        url=re.compile(r"https://musicbrainz[.]org/ws/2/release-group[?].*"),
        json={
            "release-groups": [
                {
                    "id": "rg-1",
                    "title": "Homogenic",
                    "primary-type": "Album",
                    "first-release-date": "1997-09-22",
                },
                {
                    "id": "rg-dup",
                    "title": "Single",
                    "primary-type": "Single",
                    "first-release-date": "1997",
                },
            ]
        },
    )
    httpx_mock.add_response(
        url="https://coverartarchive.org/release-group/rg-1/front-250", status_code=404
    )
    httpx_mock.add_response(
        url="https://musicbrainz.org/ws/2/release-group/rg-1?inc=releases&fmt=json",
        json={
            "id": "rg-1",
            "title": "Homogenic",
            "primary-type": "Album",
            "first-release-date": "1997-09-22",
            "releases": [{"id": "rel-1"}],
        },
    )
    httpx_mock.add_response(
        url="https://musicbrainz.org/ws/2/release/rel-1?inc=recordings&fmt=json",
        json={
            "id": "rel-1",
            "media": [
                {
                    "position": 1,
                    "tracks": [
                        {
                            "position": 1,
                            "title": "Jóga",
                            "length": 301000,
                            "recording": {"id": "rec-1"},
                        }
                    ],
                }
            ],
        },
    )

    client = MusicBrainzClient(UA)
    artists = await client.search_artists("Björk && bad/query")
    assert artists[0].provider_id == "artist-mbid"
    detail = await client.get_artist("artist-mbid")
    assert detail.name == "Björk"
    albums = await client.get_discography("artist-mbid")
    assert {(a.title, a.release_kind) for a in albums} == {
        ("Homogenic", "album"),
        ("Single", "single"),
    }
    album = await client.get_album("rg-1")
    assert album.tracks[0].title == "Jóga"
    assert album.tracks[0].recording_mbid == "rec-1"
    assert album.artwork_url is None


async def test_deezer_catalog_provider(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=re.compile(r"https://api[.]deezer[.]com/search/artist.*"),
        json={"data": [{"id": 1, "name": "Daft Punk", "picture_medium": "artist.jpg"}]},
    )
    httpx_mock.add_response(
        url="https://api.deezer.com/artist/1",
        json={"id": 1, "name": "Daft Punk", "picture_medium": "artist.jpg"},
    )
    httpx_mock.add_response(
        url="https://api.deezer.com/artist/1/albums?limit=100",
        json={
            "data": [
                {
                    "id": 10,
                    "title": "Discovery",
                    "release_date": "2001-03-12",
                    "record_type": "album",
                    "cover_medium": "cover.jpg",
                    "nb_tracks": 14,
                    "explicit_lyrics": True,
                    "explicit_content_lyrics": 1,
                    "upc": "123456789",
                }
            ]
        },
    )
    httpx_mock.add_response(
        url="https://api.deezer.com/album/10",
        json={
            "id": 10,
            "title": "Discovery",
            "release_date": "2001-03-12",
            "record_type": "album",
            "cover_medium": "cover.jpg",
            "artist": {"id": 1, "name": "Daft Punk"},
            "tracks": {
                "data": [
                    {
                        "id": 100,
                        "title": "One More Time",
                        "duration": 320,
                        "disk_number": 1,
                        "track_position": 1,
                        "explicit_lyrics": True,
                        "explicit_content_lyrics": 1,
                    }
                ]
            },
        },
    )
    httpx_mock.add_response(
        url="https://api.deezer.com/album/10/tracks?limit=100",
        json={
            "data": [
                {
                    "id": 100,
                    "title": "One More Time",
                    "duration": 320,
                    "disk_number": 1,
                    "track_position": 1,
                    "explicit_lyrics": True,
                    "explicit_content_lyrics": 1,
                }
            ],
            "total": 1,
        },
    )
    client = DeezerClient()
    assert (await client.search_artists("Daft Punk"))[0].artwork_url == "artist.jpg"
    assert (await client.get_artist("1")).name == "Daft Punk"
    discography_album = (await client.get_discography("1"))[0]
    assert discography_album.track_count == 14
    assert discography_album.content_rating == "explicit"
    assert discography_album.upc == "123456789"
    album_detail = await client.get_album("10")
    assert album_detail.tracks[0].title == "One More Time"
    assert album_detail.tracks[0].content_rating == "explicit"


async def test_deezer_discography_backfills_counts_missing_from_artist_albums(
    httpx_mock: HTTPXMock,
) -> None:
    """Deezer's real artist-albums payload omits ``nb_tracks``."""
    httpx_mock.add_response(
        url="https://api.deezer.com/artist/1/albums?limit=100",
        json={
            "data": [
                {
                    "id": 10,
                    "title": "Discovery",
                    "record_type": "album",
                    "tracklist": "https://api.deezer.com/album/10/tracks",
                },
                {
                    "id": 11,
                    "title": "One More Time",
                    "record_type": "single",
                    "tracklist": "https://api.deezer.com/album/11/tracks",
                },
            ]
        },
    )
    httpx_mock.add_response(
        url="https://api.deezer.com/album/10/tracks?limit=1",
        json={"data": [{"id": 100}], "total": 14},
    )
    httpx_mock.add_response(
        url="https://api.deezer.com/album/11/tracks?limit=1",
        json={"data": [{"id": 101}], "total": 1},
    )

    albums = await DeezerClient().get_discography("1")

    assert {album.title: album.track_count for album in albums} == {
        "Discovery": 14,
        "One More Time": 1,
    }


async def test_deezer_count_backfill_has_an_overall_latency_budget(
    httpx_mock: HTTPXMock, monkeypatch
) -> None:
    httpx_mock.add_response(
        url="https://api.deezer.com/artist/1/albums?limit=100",
        json={"data": [{"id": 10, "title": "Slow Album", "record_type": "album"}]},
    )
    monkeypatch.setattr(deezer_module, "_DISCOGRAPHY_COUNT_BUDGET_SECONDS", 0.01)

    async def never_returns(self, http, album_id):
        await asyncio.Event().wait()

    monkeypatch.setattr(DeezerClient, "_get_album_track_count", never_returns)

    albums = await asyncio.wait_for(DeezerClient().get_discography("1"), timeout=0.25)

    assert albums[0].track_count is None


async def test_deezer_count_backfill_does_not_retry_rate_limits(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        url="https://api.deezer.com/artist/1/albums?limit=100",
        json={"data": [{"id": 10, "title": "Limited Album", "record_type": "album"}]},
    )
    httpx_mock.add_response(
        url="https://api.deezer.com/album/10/tracks?limit=1",
        status_code=429,
        headers={"Retry-After": "60"},
    )

    albums = await DeezerClient().get_discography("1")

    count_requests = [
        request for request in httpx_mock.get_requests() if request.url.path == "/album/10/tracks"
    ]
    assert albums[0].track_count is None
    assert len(count_requests) == 1


async def test_itunes_catalog_provider(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=re.compile(r"https://itunes[.]apple[.]com/search.*"),
        json={"results": [{"artistId": 1, "artistName": "Nirvana", "primaryGenreName": "Rock"}]},
    )
    httpx_mock.add_response(
        url=re.compile(r"https://itunes[.]apple[.]com/lookup[?]id=1&entity=album.*"),
        json={
            "results": [
                {"wrapperType": "artist", "artistId": 1, "artistName": "Nirvana"},
                {
                    "wrapperType": "collection",
                    "collectionId": 2,
                    "collectionName": "Nevermind",
                    "releaseDate": "1991-09-24T00:00:00Z",
                    "collectionType": "Album",
                    "artworkUrl100": "100x100bb.jpg",
                    "trackCount": 12,
                    "collectionExplicitness": "cleaned",
                    "contentAdvisoryRating": "Clean",
                },
            ]
        },
    )
    httpx_mock.add_response(
        url=re.compile(r"https://itunes[.]apple[.]com/lookup[?]id=2&entity=song.*"),
        json={
            "results": [
                {
                    "wrapperType": "collection",
                    "collectionId": 2,
                    "collectionName": "Nevermind",
                    "artistId": 1,
                    "artistName": "Nirvana",
                    "releaseDate": "1991-09-24T00:00:00Z",
                    "artworkUrl100": "100x100bb.jpg",
                },
                {
                    "wrapperType": "track",
                    "trackId": 3,
                    "trackName": "Smells Like Teen Spirit",
                    "trackNumber": 1,
                    "discNumber": 1,
                    "trackTimeMillis": 301000,
                    "trackExplicitness": "explicit",
                },
            ]
        },
    )
    client = ITunesClient()
    assert (await client.search_artists("Nirvana"))[0].provider_id == "1"
    discography_album = (await client.get_discography("1"))[0]
    assert discography_album.year == "1991"
    assert discography_album.content_rating == "clean"
    album_detail = await client.get_album("2")
    assert album_detail.tracks[0].duration_sec == 301
    assert album_detail.tracks[0].content_rating == "explicit"


async def test_musicbrainz_discography_does_not_probe_cover_art_per_album(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        url=re.compile(r"https://musicbrainz[.]org/ws/2/release-group[?].*"),
        json={
            "release-groups": [
                {
                    "id": "rg-no-probe",
                    "title": "No Probe",
                    "primary-type": "Album",
                    "first-release-date": "2026",
                }
            ]
        },
    )

    albums = await MusicBrainzClient(UA).get_discography("artist-mbid")

    assert albums[0].artwork_url == (
        "https://coverartarchive.org/release-group/rg-no-probe/front-250"
    )
    assert all(
        "coverartarchive.org" not in str(request.url) for request in httpx_mock.get_requests()
    )


def test_itunes_album_collection_titles_classify_single_and_ep() -> None:
    single = _parse_album_hit(
        {"collectionId": 1, "collectionName": "A New Song - Single", "collectionType": "Album"},
        artist_id="10",
    )
    ep = _parse_album_hit(
        {"collectionId": 2, "collectionName": "A Short Record – EP", "collectionType": "Album"},
        artist_id="10",
    )
    album = _parse_album_hit(
        {"collectionId": 3, "collectionName": "A Full Record", "collectionType": "Album"},
        artist_id="10",
    )

    assert single.release_type == "Single"
    assert ep.release_type == "EP"
    assert album.release_type == "Album"
