from __future__ import annotations

from pathlib import Path

# Formats for which Audiohoard can write the required metadata and verify it
# through Mutagen before committing an import.
IMPORTABLE_AUDIO_EXTENSIONS: frozenset[str] = frozenset(
    {"flac", "mp3", "m4a", "mp4", "ogg", "oga", "opus"}
)
IMPORTABLE_AUDIO_SUFFIXES: frozenset[str] = frozenset(
    f".{extension}" for extension in IMPORTABLE_AUDIO_EXTENSIONS
)


def normalized_audio_extension(path: Path | str) -> str:
    return Path(path).suffix.casefold().lstrip(".")


def is_importable_audio(path: Path | str) -> bool:
    return normalized_audio_extension(path) in IMPORTABLE_AUDIO_EXTENSIONS


def supported_audio_formats_display() -> str:
    return ", ".join(sorted(IMPORTABLE_AUDIO_EXTENSIONS))
