from __future__ import annotations

SourceCandidateIdentity = tuple[str, str, str]


def _normalize_slskd_remote_path(value: str) -> str:
    """Canonicalize a Soulseek virtual path without changing case or basename scope."""
    parts: list[str] = []
    for part in value.strip().replace("\\", "/").split("/"):
        if not part or part == ".":
            continue
        if part == "..":
            if parts and parts[-1] != "..":
                parts.pop()
            else:
                parts.append(part)
            continue
        parts.append(part)
    return "/".join(parts)


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
