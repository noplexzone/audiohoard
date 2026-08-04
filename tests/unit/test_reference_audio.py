import json
from types import SimpleNamespace

from app.metadata.base import AlbumTrack
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.services.catalog_metadata import _store_track_previews
from app.services.reference_audio import resolve_reference_audio


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
