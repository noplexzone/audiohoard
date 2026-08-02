from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath

from app.media_formats import IMPORTABLE_AUDIO_EXTENSIONS
from app.metadata.filename_parse import normalize_for_catalog_match

_AUDIO_EXTS: frozenset[str] = IMPORTABLE_AUDIO_EXTENSIONS
_DISC_FOLDER_RE = re.compile(r"^(?:cd|disc)[ _.-]?(\d{1,2})$", re.IGNORECASE)


def _normalize_slash(path: str) -> str:
    return path.replace("\\", "/")


def _parent_dir(filename: str) -> str:
    """Return the normalized parent directory string for a slskd filename."""
    norm = _normalize_slash(filename)
    parent = str(PurePosixPath(norm).parent)
    return parent if parent != "." else ""


def _album_parent_and_disc(filename: str) -> tuple[str, int | None]:
    """Return grouping parent and disc number implied by a CD/Disc subfolder."""
    norm = _normalize_slash(filename)
    parent = PurePosixPath(norm).parent
    if parent == PurePosixPath("."):
        return "", None
    disc_match = _DISC_FOLDER_RE.match(parent.name)
    if disc_match is None:
        return str(parent), None
    album_parent = str(parent.parent)
    return ("" if album_parent == "." else album_parent), int(disc_match.group(1))


def _audio_ext(filename: str) -> str:
    norm = _normalize_slash(filename)
    suffix = PurePosixPath(norm).suffix.lstrip(".").casefold()
    return suffix if suffix in _AUDIO_EXTS else ""


@dataclass
class SlskdFile:
    filename: str
    size_bytes: int | None
    bit_rate: int | None
    sample_rate: int | None
    duration_sec: int | None = None
    disc: int | None = None


@dataclass
class AlbumFolder:
    username: str
    parent_dir: str
    audio_format: str
    files: list[SlskdFile] = field(default_factory=list)
    score: float = 0.0


def slskd_file_duration_seconds(file_data: dict[str, object]) -> int | None:
    for key in ("duration", "durationSeconds", "length", "lengthSeconds", "seconds"):
        raw = file_data.get(key)
        if isinstance(raw, (int, float)) and raw > 0:
            return int(raw)
    for key in ("durationMs", "lengthMs"):
        raw = file_data.get(key)
        if isinstance(raw, (int, float)) and raw > 0:
            return int(raw / 1000)
    return None


def group_slskd_files_into_folders(
    responses: list[dict[str, object]],
) -> list[AlbumFolder]:
    """Group individual slskd search response entries into per-folder, per-format buckets.

    Only audio files are included; artwork, logs, cue sheets etc. are ignored.
    Each bucket is uniquely identified by (username, parent_directory, audio_extension).
    """
    folders: dict[tuple[str, str, str], AlbumFolder] = {}
    for response in responses:
        username = str(response.get("username", ""))
        files = response.get("files")
        if not isinstance(files, list):
            continue
        for f in files:
            if not isinstance(f, dict):
                continue
            filename = str(f.get("filename", ""))
            ext = _audio_ext(filename)
            if not ext:
                continue
            parent, disc = _album_parent_and_disc(filename)
            key = (username, parent, ext)
            if key not in folders:
                folders[key] = AlbumFolder(username=username, parent_dir=parent, audio_format=ext)
            folders[key].files.append(
                SlskdFile(
                    filename=filename,
                    size_bytes=f.get("size") if isinstance(f.get("size"), int) else None,
                    bit_rate=f.get("bitRate") if isinstance(f.get("bitRate"), int) else None,
                    sample_rate=f.get("sampleRate")
                    if isinstance(f.get("sampleRate"), int)
                    else None,
                    duration_sec=slskd_file_duration_seconds(f),
                    disc=disc,
                )
            )
    for folder in folders.values():
        folder.files.sort(key=lambda item: (item.disc or 1, item.filename))
    return list(folders.values())


_FORMAT_RANK: dict[str, int] = {
    "flac": 0,
    "alac": 1,
    "wav": 2,
    "aiff": 3,
    "aif": 3,
    "m4a": 4,
    "mp4": 4,
    "ogg": 5,
    "oga": 5,
    "opus": 5,
    "mp3": 6,
    "wma": 7,
    "aac": 8,
}
_LOSSLESS_EXTS: frozenset[str] = frozenset({"flac", "alac", "wav", "aiff", "aif"})


def _format_family(audio_format: str) -> str:
    value = audio_format.casefold()
    return "m4a/aac" if value in {"m4a", "aac"} else value


def score_album_folder(
    folder: AlbumFolder,
    *,
    catalog_track_count: int | None,
    catalog_artist: str | None,
    catalog_album: str | None,
    format_preference: list[str],
    min_mp3_bitrate: int,
) -> float:
    """Score a candidate album folder.

    Higher is better.  Returns a float suitable for sorting descending.
    """
    score = 0.0
    file_count = len(folder.files)
    if file_count == 0:
        return -9999.0

    # --- release identity: folder path contains artist and album tokens ---
    folder_norm = normalize_for_catalog_match(folder.parent_dir)
    if catalog_artist and normalize_for_catalog_match(catalog_artist) in folder_norm:
        score += 15.0
    if catalog_album and normalize_for_catalog_match(catalog_album) in folder_norm:
        score += 15.0

    # --- completeness: how close to expected track count ---
    if catalog_track_count and catalog_track_count > 0:
        ratio = file_count / catalog_track_count
        if ratio == 1.0:
            score += 25.0
        elif ratio >= 0.8:
            score += 15.0
        elif ratio >= 0.5:
            score += 5.0
        else:
            score -= 10.0
        if file_count > catalog_track_count:
            score -= 5.0 * (file_count - catalog_track_count)

    # --- format preference ---
    fmt = folder.audio_format
    if format_preference:
        try:
            pref_index = [_format_family(p) for p in format_preference].index(_format_family(fmt))
            score += max(0.0, 20.0 - pref_index * 10.0)
        except ValueError:
            score -= 5.0
    else:
        score += max(0.0, 10.0 - _FORMAT_RANK.get(fmt, 9) * 1.5)

    # --- bitrate quality (MP3 only) ---
    if fmt == "mp3":
        bitrates = [f.bit_rate for f in folder.files if f.bit_rate]
        if bitrates:
            avg_bitrate = sum(bitrates) / len(bitrates)
            if avg_bitrate >= min_mp3_bitrate:
                score += 8.0
            else:
                score -= 8.0
        else:
            score -= 3.0  # unknown bitrate, mild penalty

    # --- lossless bonus ---
    if fmt in _LOSSLESS_EXTS:
        score += 5.0

    # --- sample depth bonus for FLAC / lossless ---
    if fmt == "flac":
        sample_rates = [f.sample_rate for f in folder.files if f.sample_rate]
        if sample_rates:
            avg_sr = sum(sample_rates) / len(sample_rates)
            if avg_sr >= 88200:
                score += 3.0

    return score


def select_best_folder(
    folders: list[AlbumFolder],
    *,
    catalog_track_count: int | None,
    catalog_artist: str | None,
    catalog_album: str | None,
    format_preference: list[str],
    min_mp3_bitrate: int,
    allow_lower_quality_fallback: bool,
) -> AlbumFolder | None:
    """Return the best-scoring folder, respecting quality profile.

    Every listed format is acceptable in preference order. With fallback disabled,
    candidates outside the list (or MP3s below the threshold) are rejected. With
    fallback enabled they are used only when no profile-compliant candidate exists.
    """
    if not folders:
        return None

    for folder in folders:
        folder.score = score_album_folder(
            folder,
            catalog_track_count=catalog_track_count,
            catalog_artist=catalog_artist,
            catalog_album=catalog_album,
            format_preference=format_preference,
            min_mp3_bitrate=min_mp3_bitrate,
        )

    sorted_folders = sorted(folders, key=lambda f: f.score, reverse=True)

    preferred_families = {_format_family(value) for value in format_preference}

    def meets_profile(folder: AlbumFolder) -> bool:
        if preferred_families and _format_family(folder.audio_format) not in preferred_families:
            return False
        if folder.audio_format != "mp3":
            return True
        return bool(folder.files) and all(
            file.bit_rate is not None and file.bit_rate >= min_mp3_bitrate for file in folder.files
        )

    compliant = [folder for folder in sorted_folders if meets_profile(folder)]
    if compliant:
        return compliant[0]
    return sorted_folders[0] if allow_lower_quality_fallback else None
