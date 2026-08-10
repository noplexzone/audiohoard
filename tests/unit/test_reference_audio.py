from __future__ import annotations

import json
from types import SimpleNamespace

from app.metadata.base import AlbumDetail, AlbumTrack
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
from app.services.catalog_metadata import _store_track_previews
from app.services.reference_audio import (
    ReferenceAudio,
    resolve_exact_deezer_catalog_reference,
    resolve_exact_deezer_track_reference,
    resolve_reference_audio,
)

SETTINGS = SimpleNamespace(deezer_api_url="https://api.deezer.com")


class _UnexpectedProvider:
    async def get_track(self, *args, **kwargs):
        raise AssertionError("exact track lookup was not expected")

    async def get_album(self, *args, **kwargs):
        raise AssertionError("exact album lookup was not expected")

    async def search_track(self, *args, **kwargs):
        raise AssertionError("fuzzy search must not be used")


class _TrackProvider(_UnexpectedProvider):
    def __init__(self, returned_id: str = "42") -> None:
        self.returned_id = returned_id
        self.track_ids: list[str] = []

    async def get_track(self, track_id: str):
        self.track_ids.append(track_id)
        return SimpleNamespace(
            deezer_id=self.returned_id,
            preview_url="https://cdn.example/fresh.mp3",
        )


class _ExactAlbumProvider(_UnexpectedProvider):
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


def _catalog(
    *,
    album_id: str | None = None,
    provenance: dict[str, object] | None = None,
    disc: int = 1,
    position: int = 3,
) -> CatalogAlbumTrack:
    album = CatalogAlbum(
        title="Album",
        deezer_id=album_id,
        provenance_json=json.dumps(provenance) if provenance else None,
    )
    return CatalogAlbumTrack(album=album, title="Track", disc=disc, position=position)


async def _resolve(track, catalog_track, provider):
    return await resolve_reference_audio(
        track,
        catalog_track,
        artist_name="Artist",
        settings=SETTINGS,
        deezer_client=provider,
    )


def test_catalog_hydration_persists_exact_deezer_cache_provenance() -> None:
    album = CatalogAlbum(title="Album", deezer_id="55")
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
            "deezer": {
                "1:3": {
                    "url": "https://cdn.example/preview.mp3",
                    "provider_track_id": "42",
                    "provider_album_id": "55",
                    "match_method": "exact_album_position",
                    "disc": 1,
                    "position": 3,
                }
            }
        }
    }


def test_catalog_hydration_does_not_cache_ambiguous_deezer_position() -> None:
    album = CatalogAlbum(title="Album", deezer_id="55")
    tracks = [
        AlbumTrack(
            provider_track_id=provider_id,
            position=3,
            disc=1,
            title="Track",
            preview_url=f"https://cdn.example/{provider_id}.mp3",
        )
        for provider_id in ("42", "99")
    ]

    _store_track_previews(album, "deezer", tracks)

    assert json.loads(album.provenance_json or "{}") == {}


async def test_resolver_returns_reference_audio_for_exact_cached_deezer_preview() -> None:
    item = _catalog(
        album_id="55",
        provenance={
            "track_previews": {
                "deezer": {
                    "1:3": {
                        "url": "https://cdn.example/cached.mp3",
                        "provider_track_id": "42",
                        "provider_album_id": "55",
                        "match_method": "exact_album_position",
                        "disc": 1,
                        "position": 3,
                    }
                }
            }
        },
    )

    reference = await _resolve(
        SimpleNamespace(title="Track", deezer_id=None), item, _UnexpectedProvider()
    )

    assert reference == ReferenceAudio(
        url="https://cdn.example/cached.mp3",
        provider="deezer",
        provider_track_id="42",
        match_method="exact_album_position",
        cached=True,
        expires_at=None,
    )


async def test_legacy_string_deezer_cache_is_ignored() -> None:
    legacy = _catalog(
        provenance={"track_previews": {"deezer": {"1:3": "https://cdn.example/legacy.mp3"}}}
    )

    assert (
        await _resolve(
            SimpleNamespace(deezer_id=None, title="Track"), legacy, _UnexpectedProvider()
        )
        is None
    )


async def test_stored_itunes_preview_is_ignored() -> None:
    item = _catalog(
        provenance={"track_previews": {"itunes": {"1:3": "https://cdn.example/preview.m4a"}}}
    )

    assert (
        await _resolve(SimpleNamespace(deezer_id=None, title="Track"), item, _UnexpectedProvider())
        is None
    )


async def test_fuzzy_deezer_title_search_is_ignored() -> None:
    assert (
        await _resolve(
            SimpleNamespace(deezer_id=None, title="Would fuzzy match"),
            _catalog(),
            _UnexpectedProvider(),
        )
        is None
    )


async def test_fuzzy_enriched_track_deezer_id_is_not_verification_evidence() -> None:
    assert (
        await _resolve(
            SimpleNamespace(deezer_id="42", title="Fuzzy enrichment"),
            _catalog(),
            _UnexpectedProvider(),
        )
        is None
    )


async def test_expired_exact_reference_is_refreshed_by_track_identity() -> None:
    item = _catalog(
        provenance={
            "track_previews": {
                "deezer": {
                    "1:3": {
                        "url": "https://cdn.example/old.mp3?hdnea=exp=1~acl=/*",
                        "provider_track_id": "42",
                        "match_method": "exact_track_id",
                    }
                }
            }
        }
    )
    provider = _TrackProvider()

    reference = await _resolve(SimpleNamespace(deezer_id=None, title="Track"), item, provider)

    assert reference == ReferenceAudio(
        url="https://cdn.example/fresh.mp3",
        provider="deezer",
        provider_track_id="42",
        match_method="exact_track_id",
        cached=False,
        expires_at=None,
    )
    assert provider.track_ids == ["42"]


async def test_exact_deezer_track_id_resolution_requires_matching_identity() -> None:
    matching = _TrackProvider()
    result = await resolve_exact_deezer_track_reference(
        "42", settings=SETTINGS, deezer_client=matching
    )
    assert result == ReferenceAudio(
        url="https://cdn.example/fresh.mp3",
        provider="deezer",
        provider_track_id="42",
        match_method="exact_track_id",
        cached=False,
        expires_at=None,
    )
    assert matching.track_ids == ["42"]

    assert (
        await resolve_exact_deezer_track_reference(
            "42", settings=SETTINGS, deezer_client=_TrackProvider("99")
        )
        is None
    )


async def test_resolver_uses_exact_deezer_album_disc_and_position() -> None:
    provider = _ExactAlbumProvider()

    result = await _resolve(
        SimpleNamespace(deezer_id=None, title="Song (Live)"),
        _catalog(album_id="55", disc=2, position=1),
        provider,
    )

    assert result == ReferenceAudio(
        url="https://cdnt-preview.dzcdn.net/live.mp3?hdnea=signed",
        provider="deezer",
        provider_track_id="202",
        match_method="exact_album_position",
        cached=False,
        expires_at=None,
    )
    assert provider.album_ids == ["55"]


async def test_exact_catalog_deezer_resolver_uses_album_disc_and_position() -> None:
    album = CatalogAlbum(title="Album", deezer_id="55")
    catalog_track = CatalogAlbumTrack(position=1, disc=2, title="Song (Live)", album=album)
    provider = _ExactAlbumProvider()

    reference = await resolve_exact_deezer_catalog_reference(
        catalog_track,
        settings=SETTINGS,
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

    reference = await resolve_exact_deezer_catalog_reference(
        catalog_track,
        settings=SETTINGS,
        deezer_client=_UnexpectedProvider(),
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
            settings=SETTINGS,
            deezer_client=provider,
        )
        is None
    )
