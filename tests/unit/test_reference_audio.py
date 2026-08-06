import json
from types import SimpleNamespace

from app.metadata.base import AlbumDetail, AlbumTrack
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.services.catalog_metadata import _store_track_previews
from app.services.reference_audio import (
    resolve_exact_deezer_catalog_reference,
    resolve_reference_audio,
)


class _UnexpectedProvider:
    async def get_track(self, *args, **kwargs):
        raise AssertionError("live provider lookup was not expected")

    async def search_track(self, *args, **kwargs):
        raise AssertionError("live provider lookup was not expected")


def test_catalog_hydration_persists_provider_preview_by_track_identity() -> None:
    album = CatalogAlbum(title="Album")
    provider_track = AlbumTrack(
        provider_track_id="42",
        position=3,
        disc=1,
        title="Track",
        preview_url="https://cdn.example/preview.mp3",
    )

    _store_track_previews(album, "deezer", [provider_track])

    assert json.loads(album.provenance_json or "{}") == {
        "track_previews": {
            "deezer": {"1:3": "https://cdn.example/preview.mp3"},
        }
    }


async def test_resolver_reuses_persisted_catalog_track_preview() -> None:
    album = CatalogAlbum(
        title="Album",
        provenance_json=json.dumps(
            {
                "track_previews": {
                    "deezer": {"1:3": "https://cdn.example/preview.mp3"},
                }
            }
        ),
    )
    catalog_track = CatalogAlbumTrack(position=3, disc=1, title="Track", album=album)
    track = SimpleNamespace(title="Track", deezer_id=None)
    provider = _UnexpectedProvider()

    reference = await resolve_reference_audio(
        track,
        catalog_track,
        artist_name="Artist",
        settings=SimpleNamespace(deezer_api_url="https://api.deezer.com"),
        deezer_client=provider,
        itunes_client=provider,
    )

    assert reference == {
        "url": "https://cdn.example/preview.mp3",
        "source": "deezer",
    }


class _CountingProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def get_track(self, *args, **kwargs):
        self.calls += 1
        return None

    async def search_track(self, *args, **kwargs):
        self.calls += 1
        return []


class _FreshDeezerProvider:
    def __init__(self) -> None:
        self.track_ids: list[str] = []

    async def get_track(self, track_id: str):
        self.track_ids.append(track_id)
        return SimpleNamespace(preview_url="https://cdn.example/fresh-preview.mp3")

    async def search_track(self, *args, **kwargs):
        raise AssertionError("exact Deezer track lookup should be used")


async def test_resolver_refreshes_expired_stored_deezer_preview() -> None:
    album = CatalogAlbum(
        title="Album",
        provenance_json=json.dumps(
            {
                "track_previews": {
                    "deezer": {
                        "1:3": "https://cdnt-preview.dzcdn.net/preview.mp3?hdnea=exp=1~acl=/*"
                    },
                    "itunes": {"1:3": "https://cdn.example/fallback-preview.m4a"},
                }
            }
        ),
    )
    catalog_track = CatalogAlbumTrack(position=3, disc=1, title="Track", album=album)
    track = SimpleNamespace(title="Track", deezer_id="42")
    provider = _FreshDeezerProvider()

    reference = await resolve_reference_audio(
        track,
        catalog_track,
        artist_name="Artist",
        settings=SimpleNamespace(deezer_api_url="https://api.deezer.com"),
        deezer_client=provider,
        itunes_client=_UnexpectedProvider(),
    )

    assert reference == {
        "url": "https://cdn.example/fresh-preview.mp3",
        "source": "deezer",
    }
    assert provider.track_ids == ["42"]


class _FuzzyDeezerProvider:
    async def get_track(self, *args, **kwargs):
        raise AssertionError("exact Deezer track lookup was not expected")

    async def search_track(self, *args, **kwargs):
        return [SimpleNamespace(preview_url="https://cdn.example/fuzzy-preview.mp3")]


async def test_expired_deezer_without_exact_id_preserves_stored_itunes_fallback() -> None:
    album = CatalogAlbum(
        title="Album",
        provenance_json=json.dumps(
            {
                "track_previews": {
                    "deezer": {
                        "1:3": "https://cdnt-preview.dzcdn.net/preview.mp3?hdnea=exp=1~acl=/*"
                    },
                    "itunes": {"1:3": "https://cdn.example/fallback-preview.m4a"},
                }
            }
        ),
    )
    catalog_track = CatalogAlbumTrack(position=3, disc=1, title="Track", album=album)
    track = SimpleNamespace(title="Track", deezer_id=None)

    reference = await resolve_reference_audio(
        track,
        catalog_track,
        artist_name="Artist",
        settings=SimpleNamespace(deezer_api_url="https://api.deezer.com"),
        deezer_client=_FuzzyDeezerProvider(),
        itunes_client=_UnexpectedProvider(),
    )

    assert reference == {
        "url": "https://cdn.example/fallback-preview.m4a",
        "source": "itunes",
    }


async def test_resolver_prefers_stored_itunes_preview_over_live_lookup() -> None:
    album = CatalogAlbum(
        title="Album",
        provenance_json=json.dumps(
            {
                "track_previews": {
                    "itunes": {"1:3": "https://cdn.example/preview.m4a"},
                }
            }
        ),
    )
    catalog_track = CatalogAlbumTrack(position=3, disc=1, title="Track", album=album)
    track = SimpleNamespace(title="Track", deezer_id=None)
    provider = _CountingProvider()

    reference = await resolve_reference_audio(
        track,
        catalog_track,
        artist_name="Artist",
        settings=SimpleNamespace(deezer_api_url="https://api.deezer.com"),
        deezer_client=provider,
        itunes_client=provider,
    )

    assert reference == {
        "url": "https://cdn.example/preview.m4a",
        "source": "itunes",
    }
    assert provider.calls == 0


class _ExactAlbumProvider:
    def __init__(self) -> None:
        self.album_ids: list[str] = []

    async def get_album(self, album_id: str) -> AlbumDetail:
        self.album_ids.append(album_id)
        return AlbumDetail(
            provider="deezer",
            provider_id=album_id,
            deezer_id=album_id,
            title="Album",
            artist_name="Artist",
            tracks=[
                AlbumTrack(
                    provider_track_id="101",
                    position=1,
                    disc=1,
                    title="Song",
                    artist_name="Artist",
                    duration_sec=181,
                    preview_url="https://cdnt-preview.dzcdn.net/preview.mp3?hdnea=signed",
                ),
                AlbumTrack(
                    provider_track_id="202",
                    position=1,
                    disc=2,
                    title="Song (Live)",
                    artist_name="Artist",
                    duration_sec=220,
                    preview_url="https://cdnt-preview.dzcdn.net/live.mp3?hdnea=signed",
                ),
            ],
        )


async def test_exact_catalog_deezer_resolver_uses_album_disc_and_position() -> None:
    album = CatalogAlbum(title="Album", deezer_id="55")
    catalog_track = CatalogAlbumTrack(position=1, disc=2, title="Song (Live)", album=album)
    provider = _ExactAlbumProvider()

    reference = await resolve_exact_deezer_catalog_reference(
        catalog_track,
        settings=SimpleNamespace(deezer_api_url="https://api.deezer.com"),
        deezer_client=provider,
    )

    assert reference is not None
    assert reference.provider_track_id == "202"
    assert reference.title == "Song (Live)"
    assert reference.duration_sec == 220
    assert reference.album_title == "Album"
    assert reference.artist_name == "Artist"
    assert reference.track_artist_name == "Artist"
    assert reference.preview_url.startswith("https://cdnt-preview.dzcdn.net/live.mp3")
    assert provider.album_ids == ["55"]


async def test_exact_catalog_deezer_resolver_never_uses_fuzzy_track_deezer_id() -> None:
    album = CatalogAlbum(title="Album", deezer_id=None)
    catalog_track = CatalogAlbumTrack(position=1, disc=1, title="Song", album=album)
    provider = _UnexpectedProvider()

    reference = await resolve_exact_deezer_catalog_reference(
        catalog_track,
        settings=SimpleNamespace(deezer_api_url="https://api.deezer.com"),
        deezer_client=provider,
    )

    assert reference is None


async def test_exact_catalog_deezer_resolver_rejects_duplicate_position() -> None:
    album = CatalogAlbum(title="Album", deezer_id="55")
    catalog_track = CatalogAlbumTrack(position=1, disc=1, title="Song", album=album)
    provider = _ExactAlbumProvider()
    original_get_album = provider.get_album

    async def duplicate_album(album_id: str) -> AlbumDetail:
        detail = await original_get_album(album_id)
        detail.tracks.append(detail.tracks[0])
        return detail

    provider.get_album = duplicate_album  # type: ignore[method-assign]

    assert (
        await resolve_exact_deezer_catalog_reference(
            catalog_track,
            settings=SimpleNamespace(deezer_api_url="https://api.deezer.com"),
            deezer_client=provider,
        )
        is None
    )
