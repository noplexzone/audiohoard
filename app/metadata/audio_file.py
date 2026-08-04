from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from mutagen import File as MutagenFile

from app.services.library_import import MutagenTagWriter


@dataclass(frozen=True)
class AudioFileMetadata:
    title: str | None
    artist: str | None
    album_artist: str | None
    album: str | None
    year: str | None
    genre: str | None
    disc: int | None
    disc_total: int | None
    track: int | None
    track_total: int | None
    recording_mbid: str | None
    album_mbid: str | None
    release_group_mbid: str | None
    duration_sec: int | None
    file_format: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _number(value: str | None) -> int | None:
    if not value:
        return None
    try:
        number = int(value.split("/", 1)[0].strip())
    except ValueError:
        return None
    return number if number > 0 else None


def _total(value: str | None, explicit: str | None) -> int | None:
    if explicit:
        return _number(explicit)
    if value and "/" in value:
        return _number(value.split("/", 1)[1])
    return None


def read_audio_file_metadata(path: Path, *, suffix_hint: str | None = None) -> AudioFileMetadata:
    tags = MutagenTagWriter().read_tags(path, suffix_hint=suffix_hint)
    try:
        audio = MutagenFile(path)
    except Exception:
        audio = None
    length = getattr(getattr(audio, "info", None), "length", None)
    duration = round(float(length)) if length is not None and float(length) > 0 else None
    date = tags.get("releasedate") or tags.get("release_date") or tags.get("date")
    return AudioFileMetadata(
        title=tags.get("title"),
        artist=tags.get("artist"),
        album_artist=tags.get("album_artist"),
        album=tags.get("album"),
        year=date[:4] if date and len(date) >= 4 else None,
        genre=tags.get("genre"),
        disc=_number(tags.get("discnumber")),
        disc_total=_total(tags.get("discnumber"), tags.get("disctotal") or tags.get("totaldiscs")),
        track=_number(tags.get("tracknumber")),
        track_total=_total(
            tags.get("tracknumber"), tags.get("tracktotal") or tags.get("totaltracks")
        ),
        recording_mbid=tags.get("musicbrainz_trackid"),
        album_mbid=tags.get("musicbrainz_albumid"),
        release_group_mbid=tags.get("musicbrainz_releasegroupid"),
        duration_sec=duration,
        file_format=(suffix_hint or path.suffix).casefold().lstrip("."),
    )
