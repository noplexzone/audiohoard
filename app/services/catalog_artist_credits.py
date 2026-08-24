from __future__ import annotations

from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack


def is_compilation_album(album: CatalogAlbum) -> bool:
    release_type = (album.release_type or "").strip().casefold()
    return bool(album.is_compilation or release_type in {"compile", "compilation"})


def catalog_track_artist_name(album: CatalogAlbum, track: CatalogAlbumTrack | None) -> str:
    if (
        is_compilation_album(album)
        and track is not None
        and track.artist_name
        and track.artist_name.strip()
    ):
        return track.artist_name.strip()
    if album.artist is not None and album.artist.name:
        return album.artist.name
    return ""


def catalog_album_artist_name(album: CatalogAlbum) -> str:
    if is_compilation_album(album):
        if album.album_artist_name and album.album_artist_name.strip():
            return album.album_artist_name.strip()
        return "Various Artists"
    if album.artist is not None and album.artist.name:
        return album.artist.name
    return ""
