from __future__ import annotations

import re

SourceCandidateIdentity = tuple[str, str, str]
_DRIVE_PREFIX = re.compile(r"^([A-Za-z]:)(.*)$")


def _collapse_path_parts(parts: list[str], *, anchored: bool) -> list[str]:
    normalized: list[str] = []
    for part in parts:
        if not part or part == ".":
            continue
        if part == "..":
            if normalized and normalized[-1] != "..":
                normalized.pop()
            elif not anchored:
                normalized.append(part)
            continue
        normalized.append(part)
    return normalized


def _normalize_slskd_remote_path(value: str) -> str:
    """Canonicalize separators/dots while preserving the path namespace and case."""
    path = value.strip().replace("\\", "/")
    if not path:
        return ""

    if path.startswith("//"):
        components = [part for part in path[2:].split("/") if part]
        if len(components) < 3 or any(anchor in {"", ".", ".."} for anchor in components[:2]):
            return ""
        server, share, *tail = components
        normalized_tail = _collapse_path_parts(tail, anchored=True)
        if not normalized_tail:
            return ""
        return "//" + "/".join([server, share, *normalized_tail])

    drive_match = _DRIVE_PREFIX.match(path)
    if drive_match is not None:
        drive, remainder = drive_match.groups()
        absolute = remainder.startswith("/")
        normalized = _collapse_path_parts(remainder.split("/"), anchored=absolute)
        if not normalized or all(part == ".." for part in normalized):
            return ""
        separator = "/" if absolute else ""
        return drive + separator + "/".join(normalized)

    if path.startswith("/"):
        normalized = _collapse_path_parts(path.split("/"), anchored=True)
        if not normalized:
            return ""
        return "/" + "/".join(normalized)

    normalized = _collapse_path_parts(path.split("/"), anchored=False)
    if not normalized or all(part == ".." for part in normalized):
        return ""
    return "/".join(normalized)


def normalize_source_candidate_identity(
    provider: object, peer: object, remote_path: object
) -> SourceCandidateIdentity | None:
    """Return the exact canonical provider artifact identity when it is complete."""
    normalized_provider = str(provider or "").strip().casefold()
    if normalized_provider != "slskd":
        return None
    normalized_peer = str(peer or "").strip()
    normalized_path = _normalize_slskd_remote_path(str(remote_path or ""))
    if not normalized_peer or not normalized_path:
        return None
    return normalized_provider, normalized_peer, normalized_path
