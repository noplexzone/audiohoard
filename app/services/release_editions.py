from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from typing import NamedTuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.metadata.content_rating import normalize_content_rating
from app.models.catalog_entities import (
    CatalogAlbumProvider,
    CatalogArtist,
    CatalogArtistIdentity,
)

_RATING_LABEL = re.compile(
    r"(?:\s*[\[(]\s*(?:clean|explicit|not[\s_-]*explicit)\s*[\])]"
    r"|\s*(?:-|–|—|:|\|)\s*(?:clean|explicit|not[\s_-]*explicit))\s*$",
    re.IGNORECASE,
)
_EDITION_WORDS = (
    "anniversary",
    "bonus",
    "deluxe",
    "demo",
    "expanded",
    "live",
    "remaster",
    "remastered",
    "remix",
    "special edition",
    "super deluxe",
)


class ReleaseFamilyKey(NamedTuple):
    artist_identity_id: int
    provider: str
    normalized_title: str
    year: str | None
    release_kind: str
    edition_descriptor: str


@dataclass(frozen=True)
class ReleaseFamily:
    key: ReleaseFamilyKey
    releases: tuple[CatalogAlbumProvider, ...]
    representatives: dict[str, CatalogAlbumProvider]
    preferred: CatalogAlbumProvider


def _normalize_text(value: object) -> str:
    folded = unicodedata.normalize("NFKD", str(value or ""))
    ascii_text = folded.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", ascii_text.casefold())).strip()


def _normalized_title(value: object) -> str:
    title = str(value or "").strip()
    previous = None
    while title != previous:
        previous = title
        title = _RATING_LABEL.sub("", title).strip(" -_()[]")
    return _normalize_text(title)


def _edition_descriptor(value: object) -> str:
    normalized = _normalized_title(value)
    return ":".join(word for word in _EDITION_WORDS if word in normalized)


def _provider_name(release: CatalogAlbumProvider) -> str:
    identity = getattr(release, "artist_identity", None)
    return str(getattr(identity, "provider", "") or "")


def _normalized_kind(release: CatalogAlbumProvider) -> str:
    kind = _normalize_text(release.release_kind)
    if kind in {"album", "single", "ep", "compilation", "other"}:
        return kind
    raw = _normalize_text(release.release_type_raw)
    if "compilation" in raw:
        return "compilation"
    if raw in {"ep", "e p"} or "extended play" in raw:
        return "ep"
    if "single" in raw:
        return "single"
    if "album" in raw:
        return "album"
    return "other"


def release_family_key(release: CatalogAlbumProvider) -> ReleaseFamilyKey:
    """Return the provider-scoped identity shared by compatible rating editions."""
    return ReleaseFamilyKey(
        artist_identity_id=release.artist_identity_id,
        provider=_provider_name(release),
        normalized_title=_normalized_title(release.title),
        year=str(release.year).strip() if release.year else None,
        release_kind=_normalized_kind(release),
        edition_descriptor=_edition_descriptor(release.title),
    )


def _rank(release: CatalogAlbumProvider) -> tuple[int, int, int, int, str]:
    try:
        metadata = json.loads(release.metadata_json or "{}")
    except (json.JSONDecodeError, TypeError):
        metadata = {}
    metadata_score = len(metadata) if isinstance(metadata, dict) else 0
    stable_id = release.id if release.id is not None else 2**31
    return (
        int(release.track_count is not None),
        metadata_score,
        int(bool(release.artwork_url)),
        -stable_id,
        release.provider_album_id,
    )


def _representatives(
    releases: list[CatalogAlbumProvider],
) -> dict[str, CatalogAlbumProvider]:
    by_rating: dict[str, list[CatalogAlbumProvider]] = {}
    for release in releases:
        rating = normalize_content_rating(release.content_rating)
        by_rating.setdefault(rating, []).append(release)
    return {
        rating: max(rows, key=lambda row: (row.monitor_override is True, _rank(row)))
        for rating, rows in by_rating.items()
    }


def project_release_families(
    releases: list[CatalogAlbumProvider],
) -> list[ReleaseFamily]:
    """Project provider rows into compatible families without mutating persistence."""
    base_groups: dict[ReleaseFamilyKey, list[CatalogAlbumProvider]] = {}
    order: list[ReleaseFamilyKey] = []
    for release in releases:
        key = release_family_key(release)
        if key not in base_groups:
            order.append(key)
        base_groups.setdefault(key, []).append(release)

    families: list[ReleaseFamily] = []
    for key in order:
        group = base_groups[key]
        concrete_counts = sorted(
            {release.track_count for release in group if release.track_count is not None}
        )
        buckets: list[list[CatalogAlbumProvider]]
        if len(concrete_counts) <= 1:
            buckets = [group]
        else:
            buckets = [
                [release for release in group if release.track_count == count]
                for count in concrete_counts
            ]
            unknown_counts = [release for release in group if release.track_count is None]
            if unknown_counts:
                # A null count cannot safely bridge incompatible concrete manifests.
                buckets.append(unknown_counts)
        for bucket in buckets:
            representatives = _representatives(bucket)
            preferred = next(
                (
                    representatives[rating]
                    for rating in ("explicit", "unknown", "clean", "not_explicit")
                    if rating in representatives
                ),
                max(bucket, key=_rank),
            )
            families.append(
                ReleaseFamily(
                    key=key,
                    releases=tuple(bucket),
                    representatives=representatives,
                    preferred=preferred,
                )
            )
    return families


def _outer_gate(artist: CatalogArtist, family: ReleaseFamily) -> bool:
    if not artist.monitored or artist.monitor_policy == "none_new":
        return False
    provider = family.key.provider
    if provider and provider != (artist.watchlist_provider or ""):
        return False
    kind = family.key.release_kind
    if artist.monitor_policy == "albums_only":
        return kind == "album"
    enabled = {
        "album": True
        if artist.watchlist_release_albums is None
        else artist.watchlist_release_albums,
        "single": False
        if artist.watchlist_release_singles is None
        else artist.watchlist_release_singles,
        "ep": False if artist.watchlist_release_eps is None else artist.watchlist_release_eps,
    }
    return bool(enabled.get(kind, False))


def apply_release_monitoring_policy(
    artist: CatalogArtist, releases: list[CatalogAlbumProvider]
) -> int:
    """Reconcile complete provider families and return the number of changed rows."""
    changed = 0
    for family in project_release_families(releases):
        gate = _outer_gate(artist, family)
        has_overrides = any(release.monitor_override is not None for release in family.releases)
        selected: CatalogAlbumProvider | None = None
        manual_selected: set[int] = set()
        if gate and not has_overrides:
            selected = family.representatives.get("explicit") or family.representatives.get(
                "unknown"
            )
        elif gate:
            for rating, representative in family.representatives.items():
                rating_rows = [
                    release
                    for release in family.releases
                    if normalize_content_rating(release.content_rating) == rating
                ]
                if any(release.monitor_override is True for release in rating_rows):
                    manual_selected.add(id(representative))
        for release in family.releases:
            desired = (
                gate and id(release) in manual_selected
                if has_overrides
                else gate and release is selected
            )
            current = bool(release.monitored)
            if current != desired:
                release.monitored = desired
                changed += 1
            elif release.monitored is not desired:
                release.monitored = desired
    return changed


def set_family_monitor_overrides(
    releases: list[CatalogAlbumProvider], selected_ids: set[int]
) -> None:
    """Persist exact manual choices for every sibling in the submitted provider."""
    for release in releases:
        release.monitor_override = release.id in selected_ids


def sync_canonical_monitoring(artist: CatalogArtist, releases: list[CatalogAlbumProvider]) -> None:
    """Project concrete provider monitoring onto canonical albums."""
    for album in artist.albums:
        album.monitored = False
    if not artist.monitored:
        return
    for release in releases:
        if release.monitored and release.catalog_album is not None:
            release.catalog_album.monitored = True


async def reconcile_release_monitoring(db: AsyncSession, artist_id: int | None = None) -> int:
    """Idempotently reconcile persisted rows using database state only."""
    stmt = select(CatalogArtist).options(
        selectinload(CatalogArtist.albums),
        selectinload(CatalogArtist.identities)
        .selectinload(CatalogArtistIdentity.releases)
        .selectinload(CatalogAlbumProvider.catalog_album),
    )
    if artist_id is not None:
        stmt = stmt.where(CatalogArtist.id == artist_id)
    artists = list((await db.scalars(stmt)).unique().all())
    changed = 0
    for artist in artists:
        releases = [release for identity in artist.identities for release in identity.releases]
        changed += apply_release_monitoring_policy(artist, releases)
        sync_canonical_monitoring(artist, releases)
    await db.flush()
    return changed
