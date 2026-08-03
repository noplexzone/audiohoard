from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Protocol, TypeVar, runtime_checkable

from app.sources.base import CapabilityState


@dataclass(frozen=True)
class ArtistHit:
    provider: str
    provider_id: str
    name: str
    mbid: str | None = None
    deezer_id: str | None = None
    itunes_id: str | None = None
    disambiguation: str | None = None
    artwork_url: str | None = None
    sort_name: str | None = None
    external_url: str | None = None
    album_count: int | None = None
    fan_count: int | None = None
    top_tracks: tuple[str, ...] = field(default_factory=tuple)
    type: str | None = None


@dataclass(frozen=True)
class ArtistDetail(ArtistHit):
    country: str | None = None


@dataclass(frozen=True)
class AlbumHit:
    provider: str
    provider_id: str
    title: str
    artist_name: str | None = None
    artist_provider_id: str | None = None
    year: str | None = None
    release_type: str | None = None
    release_kind: str = "unknown"
    release_type_raw: str | None = None
    mbid: str | None = None
    deezer_id: str | None = None
    itunes_id: str | None = None
    artwork_url: str | None = None
    track_count: int | None = None
    content_rating: str = "unknown"
    upc: str | None = None


@dataclass(frozen=True)
class AlbumTrack:
    position: int
    title: str
    disc: int = 1
    duration_sec: int | None = None
    recording_mbid: str | None = None
    provider_track_id: str | None = None
    content_rating: str = "unknown"


@dataclass(frozen=True)
class AlbumDetail(AlbumHit):
    tracks: list[AlbumTrack] = field(default_factory=list)


@runtime_checkable
class MetadataProvider(Protocol):
    name: str

    async def search_artists(self, query: str) -> list[ArtistHit]: ...
    async def get_artist(self, id: str) -> ArtistDetail: ...
    async def get_discography(self, id: str) -> list[AlbumHit]: ...
    async def get_album(self, id: str) -> AlbumDetail: ...
    async def health(self) -> CapabilityState: ...


T = TypeVar("T")


class TTLCache:
    def __init__(self) -> None:
        self._values: dict[str, tuple[float, object]] = {}

    def get(self, key: str) -> object | None:
        item = self._values.get(key)
        if item is None:
            return None
        expires, value = item
        if expires < time.monotonic():
            self._values.pop(key, None)
            return None
        return value

    def set(self, key: str, value: object, ttl_seconds: int) -> None:
        self._values[key] = (time.monotonic() + ttl_seconds, value)
