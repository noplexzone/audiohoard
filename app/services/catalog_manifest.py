from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol


class CatalogManifestTrack(Protocol):
    disc: int
    position: int


def catalog_manifest_issue(
    tracks: Sequence[CatalogManifestTrack], expected_count: int | None
) -> str | None:
    """Validate catalog manifest cardinality and disc-position identity.

    Error codes intentionally retain the runner's established external contract.
    """
    if not tracks:
        return "catalog_tracks_empty"
    identities: set[tuple[int, int]] = set()
    for track in tracks:
        identity = (track.disc, track.position)
        if track.disc < 1 or track.position < 1 or identity in identities:
            return "catalog_tracks_invalid_positions"
        identities.add(identity)
    if expected_count and len(tracks) < expected_count:
        return "catalog_tracks_incomplete"
    if expected_count and len(tracks) > expected_count:
        return "catalog_tracks_overfull"
    return None
