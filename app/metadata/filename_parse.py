from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath

_EXT_RE = re.compile(r"\.(?:flac|mp3|m4a|aac|ogg|opus|wav|alac|aiff?|wma)$", re.I)
_TRACK_PREFIX_RE = re.compile(r"^(?:cd\s*)?\d{1,2}(?:[-_. ]*\d{1,2})?\s*(?:[-_.:]|\s-\s)\s*", re.I)
_JUNK_TERMS = {
    "flac",
    "mp3",
    "320",
    "320kbps",
    "v0",
    "v2",
    "web",
    "webrip",
    "cd",
    "cdrip",
    "lossless",
    "24bit",
    "16bit",
    "44.1khz",
    "48khz",
    "96khz",
    "hi-res",
    "hifi",
}
_HINT_RE = re.compile(r"^(?:19|20)\d{2}$|remaster(?:ed)?|deluxe|anniversary|mono|stereo", re.I)
_BRACKET_RE = re.compile(r"\s*(\[[^\]]+\]|\([^)]*\)|\{[^}]+\})")
_SEP_RE = re.compile(r"\s*(?: - | – | — |\s+by\s+)\s*", re.I)
_WS_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class FilenameGuess:
    artist: str | None
    album: str | None
    title: str
    confidence: float
    hints: tuple[str, ...] = ()


def _tail(filename: str) -> str:
    normalized = filename.replace("\\", "/")
    return PurePosixPath(normalized).name or PureWindowsPath(filename).name


def _clean_token(value: str) -> str:
    value = value.replace("_", " ").strip(" .-_\t")
    return _WS_RE.sub(" ", value).strip()


def _strip_brackets(value: str) -> tuple[str, list[str]]:
    hints: list[str] = []

    def repl(match: re.Match[str]) -> str:
        raw = match.group(1)[1:-1].strip()
        normalized = raw.casefold().replace(" ", "")
        if normalized in _JUNK_TERMS or raw.casefold() in _JUNK_TERMS:
            return " "
        if _HINT_RE.search(raw):
            hints.append(raw)
        return " "

    return _BRACKET_RE.sub(repl, value), hints


def parse_filename(filename: str) -> FilenameGuess:
    name = _tail(filename)
    name = _EXT_RE.sub("", name)
    name, hints = _strip_brackets(name)
    name = _TRACK_PREFIX_RE.sub("", name)
    name = _clean_token(name)
    if not name:
        return FilenameGuess(None, None, _tail(filename), 0.0, tuple(hints))
    parts = [_clean_token(p) for p in _SEP_RE.split(name) if _clean_token(p)]
    artist: str | None = None
    album: str | None = None
    title = name
    confidence = 0.35
    if len(parts) >= 3:
        artist, album = parts[0], parts[1]
        title = " - ".join(parts[2:])
        confidence = 0.9
    elif len(parts) == 2:
        artist, title = parts
        confidence = 0.78
    else:
        title = parts[0]
    title = _TRACK_PREFIX_RE.sub("", title)
    title = _clean_token(title)
    if hints and confidence < 0.95:
        confidence += 0.03
    return FilenameGuess(
        artist or None, album or None, title or name, min(confidence, 0.98), tuple(hints)
    )


def compose_search_query(
    query: str = "", artist: str | None = None, album: str | None = None, track: str | None = None
) -> str:
    parts = [p.strip() for p in (artist, album, track, query) if p and p.strip()]
    return " ".join(dict.fromkeys(parts))


# Parenthetical/bracketed suffixes that are pure non-identity descriptors — they
# do NOT change which recording a title refers to and may safely be stripped when
# comparing a provider filename against a catalog title.
# Space-separated track prefix: "01 Intro" (bare space, no dash/dot separator).
# Applied as a secondary pass in normalize_for_catalog_match after _TRACK_PREFIX_RE.
_SPACE_TRACK_PREFIX_RE = re.compile(r"^\d{1,2}\s+")

_DESCRIPTOR_RE = re.compile(
    r"\s*[\(\[]\s*(?:skit|interlude|intro|outro|transition|segue|reprise)"
    r"(?:\s+[^\)\]]{0,40})?\s*[\)\]]",
    re.I,
)
# Featured-artist annotations change recording identity for singles/remixes;
# keep them by default so plain and featured versions do not collapse.
_FEAT_RE = re.compile(r"\s*[\(\[]\s*(?:feat\.?|ft\.?|featuring)[^\)\]]*[\)\]]", re.I)
# Producer tag e.g. "(prod. by Metro Boomin)"
_PROD_RE = re.compile(r"\s*[\(\[]\s*prod\.?[^\)\]]*[\)\]]", re.I)
# Fancy apostrophes / quotation marks
_APOSTROPHE_RE = re.compile(r"[‘’ʼ`´]")
_QUOTE_RE = re.compile(r"[“”«»‹›]")


def normalize_for_catalog_match(title: str) -> str:
    """Return a normalized form of *title* suitable for catalog matching.

    Applies: strip leading track-number prefix, Unicode NFKD normalization,
    apostrophe/punctuation normalization, casefold, and whitespace collapse.
    Does NOT strip identity-changing descriptors like (live) or (acoustic).
    """
    t = _TRACK_PREFIX_RE.sub("", title)
    # Strip bare-space-separated prefix that _TRACK_PREFIX_RE leaves ("01 Intro" → "Intro")
    t = _SPACE_TRACK_PREFIX_RE.sub("", t)
    # Fancy punctuation → ASCII equivalents
    t = _APOSTROPHE_RE.sub("'", t)
    t = _QUOTE_RE.sub('"', t)
    # Unicode decomposition followed by ASCII-ification of combining chars
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.casefold()
    # Collapse whitespace / leading-trailing noise
    t = re.sub(r"\s+", " ", t).strip(" .-_\t")
    return t


def strip_non_identity_descriptors(title: str, *, preserve_featured_artists: bool = True) -> str:
    """Remove parenthetical descriptors that do not change recording identity.

    Safe to use only for *fuzzy* catalog matching — do not use for display.
    Does NOT remove (live), (acoustic), (demo), (instrumental) etc. which DO
    indicate a different recording.  Featured artists are preserved by default
    because they distinguish singles/remixes from plain album tracks; callers
    that compare external fingerprint titles may opt into stripping them.
    """
    t = _DESCRIPTOR_RE.sub("", title)
    if not preserve_featured_artists:
        t = _FEAT_RE.sub("", t)
    t = _PROD_RE.sub("", t)
    return t.strip()


def parsed_position_evidence(filename: str) -> dict[str, int]:
    """Preserve disc/track prefixes before display-title parsing removes them."""
    stem = _EXT_RE.sub("", _tail(filename))
    compound = re.match(
        r"^(?:cd\s*)?(\d{1,2})\s*[-_.]\s*(\d{1,2})(?:\s*[-_.:]|\s+-\s|\s+)", stem, re.I
    )
    if compound:
        return {"disc": int(compound.group(1)), "track_no": int(compound.group(2))}
    packed = re.match(r"^([1-9])(\d{2})(?:[-_.:]|\s+-\s|\s+)", stem, re.I)
    if packed:
        return {"disc": int(packed.group(1)), "track_no": int(packed.group(2))}
    simple = re.match(
        r"^(?:cd\s*(\d{1,2})\s*[-_.]?\s*)?(\d{1,2})(?:\s*[-_.:]|\s+-\s|\s+)", stem, re.I
    )
    if simple:
        return {"disc": int(simple.group(1) or 1), "track_no": int(simple.group(2))}
    return {}
