from __future__ import annotations

import re
import unicodedata
from pathlib import Path, PurePosixPath

_UNSAFE_CHARS: re.Pattern[str] = re.compile(r'[/\\:*?"<>|\x00-\x1f\x7f]')
_DOT_SEQUENCE: re.Pattern[str] = re.compile(r"\.{2,}")
_REPEATED_SPACE: re.Pattern[str] = re.compile(r" {2,}")
_REPEATED_UNDERSCORE: re.Pattern[str] = re.compile(r"_{2,}")
_TRAILING_DOTS_SPACES: re.Pattern[str] = re.compile(r"[. ]+$")
_LEADING_DOTS_SPACES: re.Pattern[str] = re.compile(r"^[. ]+")

_WINDOWS_RESERVED: frozenset[str] = frozenset(
    [
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    ]
)

_SEGMENT_MAX = 200
_EXTENSION_MAX = 32
_DEFAULT_TEMPLATE = "{album_artist}/{album} ({year})/{disc_track} - {title}.{ext}"

REQUIRED_TOKENS: frozenset[str] = frozenset(["title", "ext"])


class NamingError(ValueError):
    pass


def _sanitize_segment(value: str, *, max_length: int = _SEGMENT_MAX) -> str:
    """Sanitize a single path segment (no slashes allowed)."""
    value = unicodedata.normalize("NFC", value)
    value = _UNSAFE_CHARS.sub("_", value)
    value = _DOT_SEQUENCE.sub("_", value)
    value = _REPEATED_UNDERSCORE.sub("_", value)
    value = _REPEATED_SPACE.sub(" ", value)
    value = _LEADING_DOTS_SPACES.sub("", value)
    value = _TRAILING_DOTS_SPACES.sub("", value)
    value = value.strip()

    if value.upper() in _WINDOWS_RESERVED:
        value = f"_{value}"

    if not value:
        value = "_"

    return value[:max_length]


def _sanitize_extension(value: str) -> str:
    """Sanitize an extension token and cap it to fit filename preservation."""
    return _sanitize_segment(value.lstrip("."), max_length=_EXTENSION_MAX)


def _sanitize_filename_segment(value: str, ext: str) -> str:
    sanitized = _sanitize_segment(value, max_length=max(len(value), _SEGMENT_MAX))
    suffix = f".{ext}"
    if sanitized.endswith(suffix) and len(suffix) < _SEGMENT_MAX:
        stem = sanitized[: -len(suffix)]
        return stem[: _SEGMENT_MAX - len(suffix)] + suffix
    return _sanitize_segment(value)


def _render_disc_track(disc: int | None, disc_total: int | None, track_no: int | None) -> str:
    """Render the {disc_track} token.

    Single-disc or unspecified disc: TT (zero-padded 2-digit track number).
    Multi-disc: D-TT (disc number, dash, zero-padded track number).
    """
    track_str = f"{track_no:02d}" if track_no is not None else "00"
    if disc is not None and disc_total is not None and disc_total > 1:
        return f"{disc}-{track_str}"
    return track_str


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _value_from_source(source: object, name: str) -> object | None:
    from collections.abc import Mapping

    if source is None:
        return None
    if isinstance(source, Mapping):
        return source.get(name)
    return getattr(source, name, None)


def _string_value(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _ext_from_source(source: object | None) -> str | None:
    explicit = _value_from_source(source, "ext")
    if explicit is not None:
        return str(explicit)
    source_path = _value_from_source(source, "source_path")
    if source_path and "." in str(source_path).rsplit("/", 1)[-1]:
        return str(source_path).rsplit(".", 1)[-1]
    return None


def render_path(
    track_or_tokens: object | None = None,
    *,
    title: str | None = None,
    artist: str | None = None,
    album_artist: str | None = None,
    album: str | None = None,
    year: str | None = None,
    disc: int | None = None,
    disc_total: int | None = None,
    track_no: int | None = None,
    ext: str | None = None,
    template: str = _DEFAULT_TEMPLATE,
    library_root: Path | None = None,
) -> str:
    """Render a naming template to a relative path string.

    Accepts either a Track-like object, a dict of token values, explicit keyword
    tokens, or a combination where explicit keyword values take precedence.
    Raises NamingError if required tokens are absent or the resolved path would
    escape library_root (when library_root is provided).
    """
    if title is None:
        title = _string_value(_value_from_source(track_or_tokens, "title"))
    if artist is None:
        artist = _string_value(_value_from_source(track_or_tokens, "artist"))
    if album_artist is None:
        album_artist = _string_value(_value_from_source(track_or_tokens, "album_artist"))
    if album is None:
        album = _string_value(_value_from_source(track_or_tokens, "album"))
    if year is None:
        year = _string_value(_value_from_source(track_or_tokens, "year"))
    if disc is None:
        disc = _coerce_int(_value_from_source(track_or_tokens, "disc"))
    if disc_total is None:
        disc_total = _coerce_int(_value_from_source(track_or_tokens, "disc_total"))
    if track_no is None:
        track_no = _coerce_int(_value_from_source(track_or_tokens, "track_no"))
    ext = ext if ext is not None else _ext_from_source(track_or_tokens) or "flac"

    if not title:
        raise NamingError("title is a required token and must not be empty")
    if not ext:
        raise NamingError("ext is a required token and must not be empty")

    ext_clean = _sanitize_extension(ext)

    disc_track = _render_disc_track(disc, disc_total, track_no)

    raw_tokens: dict[str, str] = {
        "title": title,
        "artist": artist or "Unknown Artist",
        "album_artist": album_artist or artist or "Unknown Artist",
        "album": album or "Unknown Album",
        "year": year or "0000",
        "disc": str(disc) if disc is not None else "1",
        "track": f"{track_no:02d}" if track_no is not None else "00",
        "disc_track": disc_track,
        "ext": ext_clean,
    }
    sanitized_tokens = {
        key: (value if key == "ext" else _sanitize_segment(value))
        for key, value in raw_tokens.items()
    }

    try:
        raw_path = template.format(**sanitized_tokens)
    except KeyError as exc:
        raise NamingError(f"Unknown template token: {exc}") from exc

    if library_root is not None:
        root_resolved = library_root.resolve()
        candidate = (library_root / raw_path).resolve()
        if not str(candidate).startswith(str(root_resolved) + "/") and candidate != root_resolved:
            raise NamingError(
                f"Rendered path escapes library root: {candidate} vs {root_resolved}"
            )

    parts = PurePosixPath(raw_path).parts
    sanitized_parts = [
        _sanitize_filename_segment(p, sanitized_tokens["ext"])
        if index == len(parts) - 1
        else _sanitize_segment(p)
        for index, p in enumerate(parts)
    ]
    rendered = str(Path(*sanitized_parts)) if sanitized_parts else "_"

    return rendered
