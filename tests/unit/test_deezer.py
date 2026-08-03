from __future__ import annotations

import re

import httpx
import pytest
from pytest_httpx import HTTPXMock

from app.metadata.deezer import DeezerClient, _parse_artist

_TRACK_DATA = {
    "id": 12345,
    "title": "Get Lucky",
    "title_short": "Get Lucky",
    "duration": 248,
    "bpm": 116.0,
    "gain": -12.5,
    "preview": "https://cdns-preview-d.dzcdn.net/stream/fake.mp3",
    "explicit_lyrics": False,
    "explicit_content_lyrics": 3,
    "rank": 900000,
    "artist": {"name": "Daft Punk"},
    "album": {"id": 67890, "title": "Random Access Memories"},
}


def test_parse_artist_prefers_picture_big() -> None:
    artist = _parse_artist(
        {
            "id": 1,
            "name": "Artist",
            "picture": "https://example.test/picture.jpg",
            "picture_medium": "https://example.test/picture-medium.jpg",
            "picture_big": "https://example.test/picture-big.jpg",
            "picture_xl": "https://example.test/picture-xl.jpg",
        }
    )

    assert artist.artwork_url == "https://example.test/picture-big.jpg"


def test_parse_artist_exposes_disambiguation_evidence_and_hides_placeholder_art() -> None:
    artist = _parse_artist(
        {
            "id": 10002824,
            "name": "Playboi Carti",
            "link": "https://www.deezer.com/artist/10002824",
            "picture_big": "https://cdn-images.dzcdn.net/images/artist/d41d8cd98f00b204e9800998ecf8427e/500x500.jpg",
            "nb_album": 14,
            "nb_fan": 825810,
        }
    )

    assert artist.artwork_url is None
    assert artist.external_url == "https://www.deezer.com/artist/10002824"
    assert artist.album_count == 14
    assert artist.fan_count == 825810


class TestDeezerSearch:
    async def test_discography_uses_only_paginated_artist_album_summaries(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="https://api.deezer.com/artist/7/albums?limit=100",
            json={
                "data": [
                    {
                        "id": 42,
                        "title": "Summary Album",
                        "release_date": "2026-01-02",
                        "record_type": "album",
                        "nb_tracks": 11,
                        "cover_big": "https://images.test/42.jpg",
                    }
                ],
                "next": "https://api.deezer.com/artist/7/albums?index=100&limit=100",
            },
        )
        httpx_mock.add_response(
            url="https://api.deezer.com/artist/7/albums?index=100&limit=100",
            json={"data": [{"id": 43, "title": "Deferred Detail"}]},
        )

        albums = await DeezerClient().get_discography("7")

        assert [album.provider_id for album in albums] == ["42", "43"]
        assert albums[0].track_count == 11
        assert albums[0].artwork_url == "https://images.test/42.jpg"
        assert albums[1].track_count is None
        assert albums[1].upc is None
        assert albums[1].content_rating == "unknown"
        assert all("/album/" not in str(request.url) for request in httpx_mock.get_requests())

    async def test_get_artist_rejects_http_200_error_envelope_for_stale_id(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="https://api.deezer.com/artist/10002824",
            json={
                "error": {
                    "type": "DataException",
                    "message": "no data",
                    "code": 800,
                }
            },
        )

        with pytest.raises(ValueError, match="valid matching artist identity"):
            await DeezerClient().get_artist("10002824")

    async def test_artist_search_adds_top_track_evidence(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url="https://api.deezer.com/search/artist?q=playboi+carti&limit=10",
            json={
                "data": [
                    {
                        "id": 10002824,
                        "name": "Playboi Carti",
                        "link": "https://www.deezer.com/artist/10002824",
                        "nb_album": 14,
                        "nb_fan": 825810,
                    }
                ],
                "total": 1,
            },
        )
        httpx_mock.add_response(
            url="https://api.deezer.com/artist/10002824/top?limit=5",
            json={"data": [{"title_short": "MUSIC"}, {"title": "Magnolia"}]},
        )

        results = await DeezerClient().search_artists("playboi carti")

        assert results[0].provider_id == "10002824"
        assert results[0].fan_count == 825810
        assert results[0].top_tracks == ("MUSIC", "Magnolia")

    async def test_search_returns_tracks(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url=re.compile(r"https://api[.]deezer[.]com/search.*"),
            json={"data": [_TRACK_DATA], "total": 1},
        )
        client = DeezerClient()
        results = await client.search_track("Get Lucky", "Daft Punk")
        assert len(results) == 1
        t = results[0]
        assert t.deezer_id == "12345"
        assert t.title == "Get Lucky"
        assert t.artist == "Daft Punk"
        assert t.bpm == 116.0
        assert t.gain == -12.5
        assert t.duration_sec == 248
        assert t.album_id == "67890"
        assert t.content_rating == "clean"

    async def test_search_empty_results(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url=re.compile(r"https://api[.]deezer[.]com/search.*"),
            json={"data": [], "total": 0},
        )
        client = DeezerClient()
        results = await client.search_track("Nonexistent Track", "Nobody")
        assert results == []

    async def test_get_track_success(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url="https://api.deezer.com/track/12345",
            json=_TRACK_DATA,
        )
        client = DeezerClient()
        track = await client.get_track("12345")
        assert track is not None
        assert track.deezer_id == "12345"
        assert track.preview_url is not None

    async def test_get_track_not_found(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url="https://api.deezer.com/track/99999",
            status_code=404,
            json={"error": {"type": "DataException", "message": "no data"}},
        )
        client = DeezerClient()
        track = await client.get_track("99999")
        assert track is None

    async def test_get_album_uses_tracklist_endpoint_for_real_positions(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="https://api.deezer.com/album/42",
            json={
                "id": 42,
                "title": "Album (Bonus Track Version)",
                "nb_tracks": 2,
                "artist": {"id": 7, "name": "Artist"},
                "tracks": {
                    "data": [
                        {"id": 101, "title": "First", "duration": 180},
                        {"id": 102, "title": "Second", "duration": 181},
                    ]
                },
            },
        )
        httpx_mock.add_response(
            url="https://api.deezer.com/album/42/tracks?limit=100",
            json={
                "data": [
                    {
                        "id": 101,
                        "title": "First",
                        "duration": 180,
                        "track_position": 1,
                        "disk_number": 1,
                    },
                    {
                        "id": 102,
                        "title": "Second",
                        "duration": 181,
                        "track_position": 2,
                        "disk_number": 1,
                    },
                ],
                "total": 2,
            },
        )

        album = await DeezerClient().get_album("42")

        assert [track.position for track in album.tracks] == [1, 2]

    async def test_get_album_rejects_embedded_tracks_without_authoritative_positions(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="https://api.deezer.com/album/43",
            json={
                "id": 43,
                "title": "Album",
                "nb_tracks": 2,
                "artist": {"id": 7, "name": "Artist"},
                "tracks": {
                    "data": [
                        {"id": 101, "title": "First", "duration": 180},
                        {"id": 102, "title": "Second", "duration": 181},
                    ]
                },
            },
        )
        for _ in range(3):
            httpx_mock.add_response(
                url="https://api.deezer.com/album/43/tracks?limit=100",
                status_code=503,
            )

        with pytest.raises(httpx.HTTPStatusError):
            await DeezerClient().get_album("43")

    async def test_get_album_follows_authoritative_tracklist_pagination(
        self, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            url="https://api.deezer.com/album/44",
            json={
                "id": 44,
                "title": "Long Album",
                "nb_tracks": 2,
                "artist": {"id": 7, "name": "Artist"},
            },
        )
        httpx_mock.add_response(
            url="https://api.deezer.com/album/44/tracks?limit=100",
            json={
                "data": [
                    {
                        "id": 101,
                        "title": "First",
                        "duration": 180,
                        "track_position": 1,
                        "disk_number": 1,
                    }
                ],
                "total": 2,
                "next": "https://api.deezer.com/album/44/tracks?limit=100&index=1",
            },
        )
        httpx_mock.add_response(
            url="https://api.deezer.com/album/44/tracks?limit=100&index=1",
            json={
                "data": [
                    {
                        "id": 102,
                        "title": "Second",
                        "duration": 181,
                        "track_position": 2,
                        "disk_number": 1,
                    }
                ],
                "total": 2,
            },
        )

        album = await DeezerClient().get_album("44")

        assert [track.position for track in album.tracks] == [1, 2]
        assert album.track_count == 2
