from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack

if TYPE_CHECKING:
    from app.models.track import Track


@dataclass(frozen=True)
class CatalogArtistCredits:
    track_artist: str
    album_artist: str


def _clean_credit(value: str | None) -> str:
    return value.strip() if value and value.strip() else ""


def is_compilation_album(album: CatalogAlbum) -> bool:
    release_type = (album.release_type or "").strip().casefold()
    return bool(album.is_compilation or release_type in {"compile", "compilation"})


def project_catalog_artist_credits(
    album: CatalogAlbum,
    catalog_track: CatalogAlbumTrack | None = None,
    track: Track | None = None,
) -> CatalogArtistCredits:
    """Project catalog credits without flattening compilation performers to their owner."""
    owner = _clean_credit(album.artist.name if album.artist is not None else None)
    album_artist = _clean_credit(album.album_artist_name) or owner

    # Catalog providers often expose featured performers on ordinary artist albums.
    # Preserve the established owner credit there; per-track credits are authoritative
    # for compilations, where flattening them loses the actual performer identity.
    track_artist = owner
    if is_compilation_album(album):
        track_artist = (
            _clean_credit(catalog_track.artist_name if catalog_track is not None else None)
            or _clean_credit(track.artist if track is not None else None)
            or owner
        )

    return CatalogArtistCredits(track_artist=track_artist, album_artist=album_artist)


def catalog_track_artist_name(album: CatalogAlbum, track: CatalogAlbumTrack | None) -> str:
    return project_catalog_artist_credits(album, track).track_artist


def catalog_album_artist_name(album: CatalogAlbum) -> str:
    return project_catalog_artist_credits(album).album_artist
