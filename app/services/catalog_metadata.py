from __future__ import annotations

import asyncio
import json
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.metadata.base import (
    AlbumDetail,
    AlbumHit,
    AlbumTrack,
    ArtistDetail,
    ArtistHit,
    MetadataProvider,
)
from app.metadata.content_rating import (
    CONTENT_RATING_UNKNOWN,
    content_ratings_compatible,
    normalize_content_rating,
)
from app.metadata.deezer import DeezerClient
from app.metadata.itunes import ITunesClient
from app.metadata.musicbrainz import MusicBrainzClient
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.job import Job
from app.models.track import IdentityResolutionState, Track
from app.sources.base import CapabilityState

VALID_METADATA_PROVIDERS = {"musicbrainz", "deezer", "itunes"}
_ARTIST_VALIDATION_TIMEOUT_SECONDS = 2.0
_VALID_ARTIST_TYPES = {"person", "group", "orchestra", "choir", "character", "other"}


@dataclass(frozen=True)
class ProviderOutcome:
    provider: str
    artists: list[ArtistHit]
    state: CapabilityState


def build_metadata_provider(name: str, settings: Settings) -> MetadataProvider | None:
    if name == "musicbrainz":
        return MusicBrainzClient(settings.musicbrainz_user_agent)
    if name == "deezer":
        return DeezerClient(settings.deezer_api_url)
    if name == "itunes":
        return ITunesClient()
    return None


def provider_ids_for_hit(
    hit: ArtistHit | ArtistDetail | AlbumHit | AlbumDetail,
) -> dict[str, str | None]:
    return {"mbid": hit.mbid, "deezer_id": hit.deezer_id, "itunes_id": hit.itunes_id}


def validate_artist_detail(
    detail: ArtistDetail, provider_name: str, provider_id: str
) -> ArtistDetail:
    native_ids = provider_ids_for_hit(detail)
    expected_field = {
        "musicbrainz": "mbid",
        "deezer": "deezer_id",
        "itunes": "itunes_id",
    }.get(provider_name)
    if (
        provider_name not in VALID_METADATA_PROVIDERS
        or not provider_id
        or detail.provider != provider_name
        or detail.provider_id != provider_id
        or expected_field is None
        or native_ids[expected_field] != provider_id
        or not detail.name.strip()
        or (detail.type is not None and detail.type.casefold() not in _VALID_ARTIST_TYPES)
    ):
        raise ValueError("Provider returned an invalid artist identity")
    return detail


async def validated_artist_hits(
    provider: MetadataProvider, name: str, artists: list[ArtistHit]
) -> list[ArtistHit]:
    expected_field = {
        "musicbrainz": "mbid",
        "deezer": "deezer_id",
        "itunes": "itunes_id",
    }.get(name)
    structurally_valid = [
        hit
        for hit in artists
        if expected_field is not None
        and hit.provider == name
        and bool(hit.provider_id)
        and bool(hit.name.strip())
        and getattr(hit, expected_field) == hit.provider_id
        and (hit.type is None or hit.type.casefold() in _VALID_ARTIST_TYPES)
    ]
    # Deezer can return stale search rows whose detail endpoint is an HTTP-200
    # error envelope. MusicBrainz and iTunes search rows already carry their
    # provider-native identity, so serial detail requests add latency but no
    # stronger identity evidence.
    if name != "deezer":
        return structurally_valid

    semaphore = asyncio.Semaphore(5)

    async def validate(hit: ArtistHit) -> ArtistHit | None:
        try:
            async with semaphore:
                detail = await asyncio.wait_for(
                    provider.get_artist(hit.provider_id),
                    timeout=_ARTIST_VALIDATION_TIMEOUT_SECONDS,
                )
            validate_artist_detail(detail, name, hit.provider_id)
        except ValueError:
            # Error envelopes and mismatched identities are definitive.
            return None
        except Exception:
            # Timeouts and provider transport failures are transient. Keep the
            # usable search identity rather than silently truncating the row set.
            return hit
        return hit

    checked = await asyncio.gather(*(validate(hit) for hit in structurally_valid))
    filtered = [hit for hit in checked if hit is not None]
    filtered.sort(key=lambda hit: (hit.fan_count is None, -(hit.fan_count or 0)))
    return filtered


async def search_catalog_artists(
    settings: Settings, query: str, providers: list[str]
) -> list[ProviderOutcome]:
    async def _one(name: str) -> ProviderOutcome:
        started = time.perf_counter()
        provider = build_metadata_provider(name, settings)
        if provider is None:
            return ProviderOutcome(name, [], CapabilityState(False, "Unknown metadata provider"))
        state = await provider.health()
        if not state.available:
            return ProviderOutcome(name, [], state)
        try:
            artists = await provider.search_artists(query)
            artists = await validated_artist_hits(provider, name, artists)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return ProviderOutcome(
                name, artists, CapabilityState(True, extra={"elapsed_ms": elapsed_ms})
            )
        except Exception as exc:
            return ProviderOutcome(
                name,
                [],
                CapabilityState(
                    False, "Metadata provider search failed", {"error": exc.__class__.__name__}
                ),
            )

    outcomes = list(
        await asyncio.gather(*[_one(p) for p in providers if p in VALID_METADATA_PROVIDERS])
    )
    return sorted(outcomes, key=lambda outcome: 0 if outcome.provider == "deezer" else 1)


async def upsert_catalog_artist(db: AsyncSession, hit: ArtistHit | ArtistDetail) -> CatalogArtist:
    try:
        return await _upsert_catalog_artist(db, hit)
    except IntegrityError:
        await db.rollback()
        return await _upsert_catalog_artist(db, hit)


async def _upsert_catalog_artist(db: AsyncSession, hit: ArtistHit | ArtistDetail) -> CatalogArtist:
    ids = provider_ids_for_hit(hit)
    filters = []
    if ids["mbid"]:
        filters.append(CatalogArtist.mbid == ids["mbid"])
    if ids["deezer_id"]:
        filters.append(CatalogArtist.deezer_id == ids["deezer_id"])
    if ids["itunes_id"]:
        filters.append(CatalogArtist.itunes_id == ids["itunes_id"])
    candidates: list[CatalogArtist] = []
    with db.no_autoflush:
        if filters:
            candidates.extend(
                list(
                    (
                        await db.scalars(
                            select(CatalogArtist)
                            .where(or_(*filters))
                            .options(selectinload(CatalogArtist.albums))
                        )
                    ).all()
                )
            )
        identity_artist = (
            await db.scalars(
                select(CatalogArtist)
                .join(CatalogArtistIdentity)
                .where(
                    CatalogArtistIdentity.provider == hit.provider,
                    CatalogArtistIdentity.provider_artist_id == hit.provider_id,
                )
                .options(selectinload(CatalogArtist.albums))
                .limit(1)
            )
        ).first()
        if identity_artist is not None:
            candidates.append(identity_artist)
    candidates = list({candidate.id: candidate for candidate in candidates}.values())
    artist: CatalogArtist | None = None
    if candidates:
        artist = candidates[0]
        for duplicate in candidates[1:]:
            artist = await merge_catalog_artists(db, artist, duplicate)
    if artist is None:
        artist = CatalogArtist(name=hit.name)
        db.add(artist)
        await db.flush()
    artist = await _merge_artist_id_collisions(db, artist, ids)
    artist.name = hit.name or artist.name
    artist.artwork_url = hit.artwork_url or artist.artwork_url
    artist.mbid = artist.mbid or ids["mbid"]
    artist.deezer_id = artist.deezer_id or ids["deezer_id"]
    artist.itunes_id = artist.itunes_id or ids["itunes_id"]
    await db.flush()
    await upsert_artist_identity(db, artist, hit)
    return artist


async def upsert_artist_identity(
    db: AsyncSession, artist: CatalogArtist, hit: ArtistHit | ArtistDetail
) -> CatalogArtistIdentity:
    if hit.provider not in VALID_METADATA_PROVIDERS or not hit.provider_id:
        raise ValueError("Invalid provider identity")
    identity = (
        await db.scalars(
            select(CatalogArtistIdentity).where(
                CatalogArtistIdentity.provider == hit.provider,
                CatalogArtistIdentity.provider_artist_id == hit.provider_id,
            )
        )
    ).first()
    if identity is None:
        conflicting_identity = (
            await db.scalars(
                select(CatalogArtistIdentity).where(
                    CatalogArtistIdentity.artist_id == artist.id,
                    CatalogArtistIdentity.provider == hit.provider,
                )
            )
        ).first()
        if conflicting_identity is not None:
            raise ValueError("Artist already has a different provider identity")
        identity = CatalogArtistIdentity(
            artist_id=artist.id,
            provider=hit.provider,
            provider_artist_id=hit.provider_id,
            name=hit.name or artist.name,
        )
        db.add(identity)
    else:
        identity.artist_id = artist.id
    identity.name = hit.name or identity.name
    identity.artwork_url = hit.artwork_url or identity.artwork_url
    identity.metadata_json = json.dumps({"source": "provider", "complete": True}, sort_keys=True)
    identity.last_enriched_at = datetime.now(tz=UTC)
    await db.flush()
    return identity


async def fetch_catalog_artist_detail(
    settings: Settings, provider_name: str, provider_id: str
) -> ArtistDetail:
    provider = build_metadata_provider(provider_name, settings)
    if provider is None:
        raise ValueError("Unknown metadata provider")
    detail = await provider.get_artist(provider_id)
    return validate_artist_detail(detail, provider_name, provider_id)


async def open_catalog_artist(
    db: AsyncSession, settings: Settings, provider_name: str, provider_id: str
) -> CatalogArtist:
    detail = await fetch_catalog_artist_detail(settings, provider_name, provider_id)
    return await upsert_catalog_artist(db, detail)


def artist_provider_id(artist: CatalogArtist, provider_name: str) -> str | None:
    if provider_name == "musicbrainz":
        return artist.mbid
    if provider_name == "deezer":
        return artist.deezer_id
    if provider_name == "itunes":
        return artist.itunes_id
    return None


def _artist_provider_ref(artist: CatalogArtist) -> tuple[str, str] | None:
    for provider_name in ("musicbrainz", "deezer", "itunes"):
        provider_id = artist_provider_id(artist, provider_name)
        if provider_id:
            return provider_name, provider_id
    return None


def _album_provider_ref(album: CatalogAlbum) -> tuple[str, str] | None:
    # Audiohoard treats Deezer as the catalog authority for monitored artists.
    # A hybrid row may also carry MusicBrainz IDs for track verification, but
    # provider hydration must not switch a Deezer-backed release to a different
    # MusicBrainz edition with a conflicting manifest.
    if album.deezer_id:
        return "deezer", album.deezer_id
    if album.mbid:
        return "musicbrainz", album.mbid
    if album.itunes_id:
        return "itunes", album.itunes_id
    return None


async def fetch_and_store_discography(
    db: AsyncSession,
    settings: Settings,
    artist: CatalogArtist,
    provider_name: str,
) -> list[CatalogAlbumProvider]:
    if provider_name not in VALID_METADATA_PROVIDERS:
        return []
    identity = (
        await db.scalars(
            select(CatalogArtistIdentity).where(
                CatalogArtistIdentity.artist_id == artist.id,
                CatalogArtistIdentity.provider == provider_name,
            )
        )
    ).first()
    if identity is None:
        provider_id = artist_provider_id(artist, provider_name)
        if not provider_id:
            return []
        identity = CatalogArtistIdentity(
            artist_id=artist.id,
            provider=provider_name,
            provider_artist_id=provider_id,
            name=artist.name,
            artwork_url=artist.artwork_url,
            provenance_json=json.dumps({"source": "legacy_fixed_id"}),
        )
        db.add(identity)
        await db.flush()
    provider = build_metadata_provider(provider_name, settings)
    if provider is None:
        return []
    albums = await provider.get_discography(identity.provider_artist_id)
    albums = _compact_provider_discography(provider_name, albums)
    releases = [await upsert_provider_release(db, artist, identity, hit) for hit in albums]
    if provider_name == "deezer":
        await reconcile_deezer_release_snapshots(db, artist.id)
    await db.flush()
    if artist.watchlist_provider == provider_name:
        from app.services.release_editions import apply_release_monitoring_policy

        complete = list(
            (
                await db.scalars(
                    select(CatalogAlbumProvider)
                    .where(CatalogAlbumProvider.artist_identity_id == identity.id)
                    .options(selectinload(CatalogAlbumProvider.artist_identity))
                )
            ).all()
        )
        apply_release_monitoring_policy(artist, complete)
    identity.last_discography_at = datetime.now(tz=UTC)
    await db.flush()
    return releases


def normalize_release_kind(hit: AlbumHit | AlbumDetail) -> str:
    if hit.release_kind in {"album", "single", "ep", "compilation", "other"}:
        return hit.release_kind
    raw = hit.release_type_raw or hit.release_type
    bucket = release_bucket(raw)
    return {"album": "album", "single_ep": "single", "compilation": "compilation"}[bucket]


def _provider_release_family_key(hit: Any) -> tuple[str, str | None, str, str, str]:
    """Identify provider snapshots of one artist-intended release.

    The title remains part of the key, so named Deluxe/Remaster/etc. editions stay
    distinct. Ratings are exact rather than merely compatible so clean, explicit,
    and unknown provider rows cannot absorb one another.
    """
    release_kind = getattr(hit, "release_kind", None)
    if release_kind not in {"album", "single", "ep", "compilation", "other"}:
        raw_kind = getattr(hit, "release_type_raw", None) or getattr(hit, "release_type", None)
        if raw_kind:
            release_kind = {
                "album": "album",
                "single_ep": "single",
                "compilation": "compilation",
            }.get(release_bucket(raw_kind), "other")
        else:
            release_kind = "unknown"
    title = str(getattr(hit, "title", ""))
    return (
        _norm_title(title),
        getattr(hit, "year", None),
        _edition_marker(title),
        release_kind,
        normalize_content_rating(getattr(hit, "content_rating", None)),
    )


def _compact_provider_discography(provider_name: str, hits: list[AlbumHit]) -> list[AlbumHit]:
    """Keep the richest Deezer snapshot for each exact release family."""
    if provider_name != "deezer":
        return hits
    grouped: dict[tuple[str, str | None, str, str, str], list[AlbumHit]] = {}
    for hit in hits:
        key = _provider_release_family_key(hit)
        grouped.setdefault(key, []).append(hit)
    compacted: list[AlbumHit] = []
    for group in grouped.values():
        known_counts = {hit.track_count for hit in group if hit.track_count is not None}
        if len(known_counts) < 2:
            compacted.extend(group)
            continue
        richest_count = max(known_counts)
        richest = [hit for hit in group if hit.track_count == richest_count]
        unknown = [hit for hit in group if hit.track_count is None]
        compacted.extend(richest)
        compacted.extend(unknown)
    return compacted


async def upsert_provider_release(
    db: AsyncSession,
    artist: CatalogArtist,
    identity: CatalogArtistIdentity,
    hit: AlbumHit | AlbumDetail,
) -> CatalogAlbumProvider:
    release = (
        await db.scalars(
            select(CatalogAlbumProvider).where(
                CatalogAlbumProvider.artist_identity_id == identity.id,
                CatalogAlbumProvider.provider_album_id == hit.provider_id,
            )
        )
    ).first()
    canonical = await upsert_catalog_album(db, artist, hit, match_release_type=False)
    if release is None:
        release = CatalogAlbumProvider(
            artist_identity_id=identity.id,
            provider_album_id=hit.provider_id,
            title=hit.title,
        )
        db.add(release)
    release.catalog_album_id = canonical.id
    release.title = hit.title
    release.year = hit.year
    release.artwork_url = hit.artwork_url
    if hit.track_count is not None:
        release.track_count = hit.track_count
    release.release_kind = normalize_release_kind(hit)
    release.release_type_raw = hit.release_type_raw or hit.release_type
    release.content_rating = normalize_content_rating(hit.content_rating)
    release.upc = hit.upc or release.upc
    release.metadata_json = json.dumps(
        {
            "source": "provider",
            "complete": True,
            "track_count_checked": release.track_count is not None,
        },
        sort_keys=True,
    )
    await db.flush()
    return release


async def upsert_catalog_album(
    db: AsyncSession,
    artist: CatalogArtist,
    hit: AlbumHit | AlbumDetail,
    *,
    match_release_type: bool = True,
) -> CatalogAlbum:
    ids = provider_ids_for_hit(hit)
    filters = []
    if ids["mbid"]:
        filters.append(CatalogAlbum.mbid == ids["mbid"])
    if ids["deezer_id"]:
        filters.append(CatalogAlbum.deezer_id == ids["deezer_id"])
    if ids["itunes_id"]:
        filters.append(CatalogAlbum.itunes_id == ids["itunes_id"])
    album = None
    if filters:
        album = (await db.scalars(select(CatalogAlbum).where(or_(*filters)).limit(1))).first()
    if album is None:
        candidates = list(
            (
                await db.scalars(select(CatalogAlbum).where(CatalogAlbum.artist_id == artist.id))
            ).all()
        )
        matcher = _album_keys_match if match_release_type else _canonical_album_keys_match
        album = next((candidate for candidate in candidates if matcher(candidate, hit)), None)
    if album is None:
        album = CatalogAlbum(artist_id=artist.id, title=hit.title)
        db.add(album)
    _apply_album_hit(album, artist, hit, ids)
    await db.flush()
    return album


def _apply_album_hit(
    album: CatalogAlbum,
    artist: CatalogArtist,
    hit: AlbumHit | AlbumDetail,
    ids: dict[str, str | None] | None = None,
) -> None:
    ids = ids or provider_ids_for_hit(hit)
    album.artist_id = artist.id
    album.title = album.title or hit.title
    album.year = hit.year or album.year
    album.release_type = album.release_type or hit.release_type
    album.artwork_url = hit.artwork_url or album.artwork_url
    incoming_rating = normalize_content_rating(hit.content_rating)
    if (
        normalize_content_rating(album.content_rating) == CONTENT_RATING_UNKNOWN
        and incoming_rating != CONTENT_RATING_UNKNOWN
    ):
        album.content_rating = incoming_rating
    album.upc = album.upc or hit.upc
    if hit.track_count and album.track_count is None:
        album.track_count = hit.track_count
    album.itunes_id = album.itunes_id or ids["itunes_id"]
    album.mbid = album.mbid or ids["mbid"]
    album.deezer_id = album.deezer_id or ids["deezer_id"]
    if hit.provider in VALID_METADATA_PROVIDERS:
        album.providers_json = _merge_provider_json(
            album.providers_json, json.dumps([hit.provider])
        )


def _store_track_previews(album: CatalogAlbum, provider: str, tracks: list[AlbumTrack]) -> None:
    """Persist provider previews in existing catalog metadata, keyed to track identity."""
    provider = provider.casefold()
    if provider not in {"deezer", "itunes"}:
        return
    previews: dict[str, str | dict[str, object]]
    if provider == "deezer":
        album_id = str(album.deezer_id or "").strip()
        position_counts: dict[tuple[int, int], int] = {}
        for track in tracks:
            key = (track.disc, track.position)
            position_counts[key] = position_counts.get(key, 0) + 1
        previews = {
            f"{track.disc}:{track.position}": {
                "url": track.preview_url.strip(),
                "provider_track_id": str(track.provider_track_id).strip(),
                "provider_album_id": album_id,
                "match_method": "exact_album_position",
                "disc": track.disc,
                "position": track.position,
            }
            for track in tracks
            if album_id
            and position_counts[(track.disc, track.position)] == 1
            and track.provider_track_id
            and isinstance(track.preview_url, str)
            and track.preview_url.strip()
        }
    else:
        previews = {
            f"{track.disc}:{track.position}": track.preview_url.strip()
            for track in tracks
            if isinstance(track.preview_url, str) and track.preview_url.strip()
        }
    if not previews:
        return
    try:
        provenance = json.loads(album.provenance_json or "{}")
    except (json.JSONDecodeError, TypeError):
        provenance = {}
    if not isinstance(provenance, dict):
        provenance = {}
    stored = provenance.get("track_previews")
    if not isinstance(stored, dict):
        stored = {}
        provenance["track_previews"] = stored
    provider_previews = stored.get(provider)
    if not isinstance(provider_previews, dict):
        provider_previews = {}
        stored[provider] = provider_previews
    provider_previews.update(previews)
    album.provenance_json = json.dumps(provenance, sort_keys=True)


async def fetch_and_store_album(
    db: AsyncSession, settings: Settings, album: CatalogAlbum
) -> CatalogAlbum:
    ref = _album_provider_ref(album)
    if ref is None:
        return album
    provider_name, provider_id = ref
    provider = build_metadata_provider(provider_name, settings)
    if provider is None:
        return album
    known_track_count = album.track_count or 0
    artist = await db.get(CatalogArtist, album.artist_id)
    if artist is None:
        raise RuntimeError(f"Catalog artist {album.artist_id} not found for album {album.id}")
    detail = await provider.get_album(provider_id)
    album = await upsert_catalog_album(db, artist, detail)
    _store_track_previews(album, provider_name, detail.tracks)
    existing_tracks = list(
        (
            await db.scalars(
                select(CatalogAlbumTrack).where(CatalogAlbumTrack.album_id == album.id)
            )
        ).all()
    )
    expected_manifest_count = max(known_track_count, detail.track_count or 0)
    if (
        existing_tracks
        and expected_manifest_count
        and len(detail.tracks) < expected_manifest_count
    ):
        raise RuntimeError(
            "metadata provider returned an incomplete album manifest "
            f"({len(detail.tracks)}/{expected_manifest_count} tracks)"
        )
    provider_identities: set[tuple[int, int]] = set()
    for provider_track in detail.tracks:
        identity = (provider_track.disc, provider_track.position)
        if (
            provider_track.disc < 1
            or provider_track.position < 1
            or identity in provider_identities
        ):
            raise RuntimeError("metadata provider returned invalid album track positions")
        provider_identities.add(identity)

    unmatched = list(existing_tracks)
    reconciliation: list[tuple[AlbumTrack, CatalogAlbumTrack | None]] = []
    for provider_track in detail.tracks:
        existing = _match_existing_catalog_track(unmatched, provider_track)
        if existing is not None:
            unmatched.remove(existing)
        reconciliation.append((provider_track, existing))

    if unmatched:
        stale_ids = [track.id for track in unmatched]
        linked_track_id = await db.scalar(
            select(Track.id).where(Track.catalog_track_id.in_(stale_ids)).limit(1)
        )
        linked_job_id = await db.scalar(
            select(Job.id).where(Job.catalog_track_id.in_(stale_ids)).limit(1)
        )
        if linked_track_id is not None or linked_job_id is not None:
            raise RuntimeError("metadata reconciliation left referenced catalog tracks unmatched")

    for provider_track, existing in reconciliation:
        if existing is None:
            db.add(
                CatalogAlbumTrack(
                    album_id=album.id,
                    position=provider_track.position,
                    disc=provider_track.disc,
                    title=provider_track.title,
                    duration_sec=provider_track.duration_sec,
                    recording_mbid=provider_track.recording_mbid,
                    content_rating=normalize_content_rating(provider_track.content_rating),
                )
            )
            continue
        existing.position = provider_track.position
        existing.disc = provider_track.disc
        existing.title = provider_track.title
        existing.duration_sec = provider_track.duration_sec
        existing.recording_mbid = provider_track.recording_mbid or existing.recording_mbid
        incoming_rating = normalize_content_rating(provider_track.content_rating)
        if (
            normalize_content_rating(existing.content_rating) == CONTENT_RATING_UNKNOWN
            and incoming_rating != CONTENT_RATING_UNKNOWN
        ):
            existing.content_rating = incoming_rating
        linked_values: dict[str, object] = {
            "title": provider_track.title,
            "track_no": provider_track.position,
            "disc": provider_track.disc,
        }
        if provider_track.duration_sec is not None:
            linked_values["duration_sec"] = provider_track.duration_sec
        if provider_track.recording_mbid:
            linked_values["mbid"] = provider_track.recording_mbid
            linked_values["identity_state"] = IdentityResolutionState.resolved
        await db.execute(
            update(Track).where(Track.catalog_track_id == existing.id).values(**linked_values)
        )
    for stale in unmatched:
        await db.delete(stale)
    hydrated_track_count = max(detail.track_count or 0, len(detail.tracks))
    album.track_count = max(known_track_count, hydrated_track_count) or None
    await db.flush()
    await db.refresh(album, ["tracks"])
    return album


def _match_existing_catalog_track(
    existing_tracks: list[CatalogAlbumTrack], provider_track: AlbumTrack
) -> CatalogAlbumTrack | None:
    if provider_track.recording_mbid:
        mbid_matches = [
            existing
            for existing in existing_tracks
            if existing.recording_mbid == provider_track.recording_mbid
        ]
        if len(mbid_matches) > 1:
            raise RuntimeError(
                f"ambiguous recording identity for catalog track {provider_track.title!r}"
            )
        if mbid_matches:
            return mbid_matches[0]
    title_matches = [
        existing
        for existing in existing_tracks
        if _norm_title(existing.title) == _norm_title(provider_track.title)
    ]
    if provider_track.recording_mbid:
        conflicting = [
            existing
            for existing in title_matches
            if existing.recording_mbid and existing.recording_mbid != provider_track.recording_mbid
        ]
        title_matches = [existing for existing in title_matches if not existing.recording_mbid]
        if conflicting and not title_matches:
            raise RuntimeError(
                f"conflicting recording identity for catalog track {provider_track.title!r}"
            )
    if not title_matches:
        return None
    if len(title_matches) == 1:
        return title_matches[0]
    if provider_track.duration_sec is None or any(
        existing.duration_sec is None for existing in title_matches
    ):
        raise RuntimeError(f"ambiguous catalog track title {provider_track.title!r}")
    provider_duration = provider_track.duration_sec
    assert provider_duration is not None
    ranked = sorted(
        title_matches,
        key=lambda existing: (
            abs((existing.duration_sec or 0) - provider_duration),
            existing.id,
        ),
    )
    first_distance = abs((ranked[0].duration_sec or 0) - provider_duration)
    second_distance = abs((ranked[1].duration_sec or 0) - provider_duration)
    if first_distance == second_distance:
        raise RuntimeError(f"ambiguous catalog track title {provider_track.title!r}")
    return ranked[0]


_PUNCT_TRANSLATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201a": "'",
        "\u201b": "'",
        "\u2032": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u201e": '"',
        "\u201f": '"',
        "\u2033": '"',
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2015": "-",
        "\u2212": "-",
    }
)


def _norm_title(value: str) -> str:
    folded = unicodedata.normalize("NFKD", value.translate(_PUNCT_TRANSLATION))
    ascii_text = folded.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", ascii_text.casefold())).strip()


def _artist_data_score(artist: CatalogArtist) -> tuple[int, int, int, int]:
    """Rank identity richness first, then preserve the oldest row as the tie-breaker."""
    normalized_name = _norm_title(artist.name)
    return (
        1 if artist.mbid else 0,
        1 if artist.artwork_url else 0,
        len(normalized_name.replace(" ", "")),
        -(artist.id or 0),
    )


def _load_provenance(raw: str | None) -> dict[str, object]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


async def merge_catalog_artists(
    db: AsyncSession, left: CatalogArtist, right: CatalogArtist
) -> CatalogArtist:
    """Merge two artist identities and return the richer/older surviving row."""
    if left.id == right.id:
        return left
    survivor, duplicate = (
        max((left, right), key=_artist_data_score),
        min((left, right), key=_artist_data_score),
    )
    with db.no_autoflush:
        await db.refresh(survivor, ["albums"])
        await db.refresh(duplicate, ["albums"])
        identities = list(
            (
                await db.scalars(
                    select(CatalogArtistIdentity)
                    .where(CatalogArtistIdentity.artist_id.in_((survivor.id, duplicate.id)))
                    .options(selectinload(CatalogArtistIdentity.releases))
                )
            ).all()
        )
        survivor_identities = {
            identity.provider: identity
            for identity in identities
            if identity.artist_id == survivor.id
        }
        duplicate_identities = [
            identity for identity in identities if identity.artist_id == duplicate.id
        ]
        duplicate_ids = {
            "mbid": duplicate.mbid,
            "deezer_id": duplicate.deezer_id,
            "itunes_id": duplicate.itunes_id,
        }
        duplicate.mbid = None
        duplicate.deezer_id = None
        duplicate.itunes_id = None
        await db.flush()

        survivor.monitored = bool(survivor.monitored or duplicate.monitored)
        survivor.watchlist_provider = survivor.watchlist_provider or duplicate.watchlist_provider
        if not survivor.monitor_policy:
            survivor.monitor_policy = duplicate.monitor_policy
        survivor.name = survivor.name or duplicate.name
        survivor.artwork_url = survivor.artwork_url or duplicate.artwork_url
        survivor.mbid = survivor.mbid or duplicate_ids["mbid"]
        survivor.deezer_id = survivor.deezer_id or duplicate_ids["deezer_id"]
        survivor.itunes_id = survivor.itunes_id or duplicate_ids["itunes_id"]
        survivor.last_enriched_at = survivor.last_enriched_at or duplicate.last_enriched_at
        survivor.last_refreshed_at = survivor.last_refreshed_at or duplicate.last_refreshed_at
        provenance = _load_provenance(duplicate.provenance_json)
        provenance.update(_load_provenance(survivor.provenance_json))
        survivor.provenance_json = json.dumps(provenance, sort_keys=True) if provenance else None

        for album in list(duplicate.albums):
            album.artist = survivor
        for duplicate_identity in duplicate_identities:
            survivor_identity = survivor_identities.get(duplicate_identity.provider)
            if survivor_identity is None:
                duplicate_identity.artist = survivor
                survivor_identities[duplicate_identity.provider] = duplicate_identity
                continue
            existing_releases = {
                release.provider_album_id: release for release in survivor_identity.releases
            }
            for release in list(duplicate_identity.releases):
                existing_release = existing_releases.get(release.provider_album_id)
                if existing_release is None:
                    release.artist_identity = survivor_identity
                    existing_releases[release.provider_album_id] = release
                    continue
                existing_release.catalog_album_id = (
                    existing_release.catalog_album_id or release.catalog_album_id
                )
                existing_release.monitored = bool(existing_release.monitored or release.monitored)
                existing_release.title = existing_release.title or release.title
                existing_release.year = existing_release.year or release.year
                existing_release.artwork_url = existing_release.artwork_url or release.artwork_url
                existing_release.track_count = existing_release.track_count or release.track_count
                if existing_release.content_rating == CONTENT_RATING_UNKNOWN:
                    existing_release.content_rating = release.content_rating
                existing_release.upc = existing_release.upc or release.upc
                await db.delete(release)
            survivor_identity.name = survivor_identity.name or duplicate_identity.name
            survivor_identity.artwork_url = (
                survivor_identity.artwork_url or duplicate_identity.artwork_url
            )
            survivor_identity.last_enriched_at = (
                survivor_identity.last_enriched_at or duplicate_identity.last_enriched_at
            )
            survivor_identity.last_discography_at = (
                survivor_identity.last_discography_at or duplicate_identity.last_discography_at
            )
        await db.flush()
        await db.delete(duplicate)
        await db.flush()
        await reconcile_duplicate_catalog_albums(db, survivor.id)
        await db.refresh(survivor, ["albums"])
    return survivor


async def _merge_artist_id_collisions(
    db: AsyncSession, artist: CatalogArtist, ids: dict[str, str | None]
) -> CatalogArtist:
    """Merge every row that owns an incoming provider id before assigning it."""
    for field in ("mbid", "deezer_id", "itunes_id"):
        value = ids[field]
        if not value:
            continue
        with db.no_autoflush:
            existing = (
                await db.scalars(
                    select(CatalogArtist)
                    .where(getattr(CatalogArtist, field) == value, CatalogArtist.id != artist.id)
                    .options(selectinload(CatalogArtist.albums))
                    .limit(1)
                )
            ).first()
        if existing is not None:
            artist = await merge_catalog_artists(db, artist, existing)
    return artist


def _artist_identity_evidence(artist: CatalogArtist) -> dict[str, set[str]]:
    evidence: dict[str, set[str]] = {
        "musicbrainz": set(),
        "deezer": set(),
        "itunes": set(),
    }
    canonical = {
        "musicbrainz": artist.mbid,
        "deezer": artist.deezer_id,
        "itunes": artist.itunes_id,
    }
    for provider, provider_id in canonical.items():
        if provider_id:
            evidence[provider].add(provider_id)
    for identity in artist.identities:
        if identity.provider in evidence and identity.provider_artist_id:
            evidence[identity.provider].add(identity.provider_artist_id)
    return evidence


def _artists_should_merge(left: CatalogArtist, right: CatalogArtist) -> bool:
    left_evidence = _artist_identity_evidence(left)
    right_evidence = _artist_identity_evidence(right)
    if any(
        left_evidence[provider]
        and right_evidence[provider]
        and left_evidence[provider].isdisjoint(right_evidence[provider])
        for provider in VALID_METADATA_PROVIDERS
    ):
        return False
    return any(
        left_evidence[provider] & right_evidence[provider] for provider in VALID_METADATA_PROVIDERS
    )


async def reconcile_duplicate_catalog_artists(db: AsyncSession) -> int:
    """Repair legacy duplicate artist identities; safe to run repeatedly at startup."""
    merged = 0
    while True:
        with db.no_autoflush:
            artists = list(
                (
                    await db.scalars(
                        select(CatalogArtist)
                        .options(
                            selectinload(CatalogArtist.albums),
                            selectinload(CatalogArtist.identities),
                        )
                        .order_by(CatalogArtist.id)
                    )
                ).all()
            )
        pair = next(
            (
                (left, right)
                for index, left in enumerate(artists)
                for right in artists[index + 1 :]
                if _artists_should_merge(left, right)
            ),
            None,
        )
        if pair is None:
            break
        await merge_catalog_artists(db, *pair)
        merged += 1
    await db.flush()
    return merged


def release_bucket(value: str | None) -> str:
    """Return the stable UI/policy bucket for provider-specific release type variants."""
    normalized = _norm_title(value or "album")
    tokens = set(normalized.split())
    if "single" in tokens or "ep" in tokens or ({"e", "p"} <= tokens):
        return "single_ep"
    if "compilation" in tokens or "compilations" in tokens:
        return "compilation"
    return "album"


def _norm_release_type(value: str | None) -> str:
    return release_bucket(value)


def album_providers(album: CatalogAlbum) -> set[str]:
    providers: set[str] = set()
    if album.providers_json:
        try:
            parsed = json.loads(album.providers_json)
        except (json.JSONDecodeError, TypeError):
            parsed = []
        if isinstance(parsed, list):
            providers.update(str(item) for item in parsed if item in VALID_METADATA_PROVIDERS)
    if album.mbid:
        providers.add("musicbrainz")
    if album.deezer_id:
        providers.add("deezer")
    if album.itunes_id:
        providers.add("itunes")
    return providers


def album_has_provider(album: CatalogAlbum, provider_name: str) -> bool:
    return provider_name in VALID_METADATA_PROVIDERS and provider_name in album_providers(album)


def available_artist_providers(artist: CatalogArtist) -> list[str]:
    available = {identity.provider for identity in artist.identities}
    available.update(
        provider for provider in VALID_METADATA_PROVIDERS if artist_provider_id(artist, provider)
    )
    return [name for name in ("musicbrainz", "deezer", "itunes") if name in available]


async def ensure_legacy_provider_snapshots(db: AsyncSession, artist: CatalogArtist) -> None:
    """Repair canonical-only rows left by old databases or direct test fixtures."""
    identities = {
        identity.provider: identity
        for identity in (
            await db.scalars(
                select(CatalogArtistIdentity)
                .where(CatalogArtistIdentity.artist_id == artist.id)
                .options(selectinload(CatalogArtistIdentity.releases))
            )
        ).all()
    }
    for provider in ("musicbrainz", "deezer", "itunes"):
        provider_id = artist_provider_id(artist, provider)
        if not provider_id or provider in identities:
            continue
        identity = CatalogArtistIdentity(
            artist_id=artist.id,
            provider=provider,
            provider_artist_id=provider_id,
            name=artist.name,
            artwork_url=artist.artwork_url,
            provenance_json=json.dumps({"source": "legacy_runtime_repair"}),
        )
        db.add(identity)
        await db.flush()
        identities[provider] = identity

    for album in artist.albums:
        for provider in album_providers(album):
            legacy_identity = identities.get(provider)
            if legacy_identity is None:
                continue
            provider_album_id = {
                "musicbrainz": album.mbid,
                "deezer": album.deezer_id,
                "itunes": album.itunes_id,
            }[provider] or f"legacy:album:{album.id}:{provider}"
            existing = (
                await db.scalars(
                    select(CatalogAlbumProvider).where(
                        CatalogAlbumProvider.artist_identity_id == legacy_identity.id,
                        CatalogAlbumProvider.provider_album_id == provider_album_id,
                    )
                )
            ).first()
            if existing is not None:
                continue
            bucket = release_bucket(album.release_type)
            db.add(
                CatalogAlbumProvider(
                    artist_identity_id=legacy_identity.id,
                    catalog_album_id=album.id,
                    provider_album_id=provider_album_id,
                    title=album.title,
                    year=album.year,
                    artwork_url=album.artwork_url,
                    track_count=album.track_count,
                    release_kind={
                        "album": "album",
                        "single_ep": "single",
                        "compilation": "compilation",
                    }[bucket],
                    release_type_raw=album.release_type,
                    content_rating=album.content_rating,
                    upc=album.upc,
                    metadata_json=json.dumps(
                        {"legacy_runtime_repair": True, "lossy": True}, sort_keys=True
                    ),
                    monitored=bool(album.monitored and artist.watchlist_provider == provider),
                )
            )
    await db.flush()


def _edition_marker(value: str) -> str:
    lowered = _norm_title(value)
    markers = [m for m in ["deluxe", "remaster", "anniversary", "expanded"] if m in lowered]
    return ":".join(markers)


def _album_key(hit: Any) -> tuple[str, str | None, str, str, int | None, str]:
    return (
        _norm_title(str(hit.title)),
        getattr(hit, "year", None),
        _edition_marker(str(hit.title)),
        _norm_release_type(getattr(hit, "release_type", None)),
        getattr(hit, "track_count", None),
        normalize_content_rating(getattr(hit, "content_rating", None)),
    )


def _provider_ids_compatible(left: Any, right: Any) -> bool:
    for field in ("mbid", "deezer_id", "itunes_id"):
        left_value = getattr(left, field, None)
        right_value = getattr(right, field, None)
        if left_value and right_value and left_value != right_value:
            return False
    return True


def _album_keys_match(left: Any, right: Any) -> bool:
    lt, ly, le, lr, lc, lrating = _album_key(left)
    rt, ry, re_, rr, rc, rrating = _album_key(right)
    if not _provider_ids_compatible(left, right):
        return False
    if (lt, le, lr) != (rt, re_, rr):
        return False
    if ly != ry and ly is not None and ry is not None:
        return False
    if lc != rc and lc is not None and rc is not None:
        return False
    return content_ratings_compatible(lrating, rrating)


def _canonical_album_keys_match(left: Any, right: Any) -> bool:
    lt, ly, le, _, lc, lrating = _album_key(left)
    rt, ry, re_, _, rc, rrating = _album_key(right)
    if not _provider_ids_compatible(left, right):
        return False
    if (lt, le) != (rt, re_):
        return False
    if ly != ry and ly is not None and ry is not None:
        return False
    if lc != rc and lc is not None and rc is not None:
        return False
    return content_ratings_compatible(lrating, rrating)


def _merge_provider_json(*values: str | None) -> str | None:
    providers: set[str] = set()
    for raw in values:
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list):
            providers.update(str(v) for v in parsed if v)
    return json.dumps(sorted(providers)) if providers else None


def _album_data_score(album: CatalogAlbum) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        1 if album.content_rating != CONTENT_RATING_UNKNOWN else 0,
        1 if album.upc else 0,
        1 if album.mbid else 0,
        1 if album.track_count else 0,
        1 if album.artwork_url else 0,
        1 if album.deezer_id else 0,
        1 if album.itunes_id else 0,
        album.id or 0,
    )


def _name_similarity(a: str, b: str) -> float:
    na, nb = _norm_title(a), _norm_title(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    aw, bw = set(na.split()), set(nb.split())
    return len(aw & bw) / max(len(aw | bw), 1)


async def enrich_catalog_artist(
    db: AsyncSession,
    settings: Settings,
    artist: CatalogArtist,
    enabled_providers: list[str],
    *,
    choices: dict[str, str] | None = None,
) -> dict[str, object]:
    """Best-effort enrichment across every enabled provider without clobbering IDs."""
    choices = choices or {}
    provenance = _load_provenance(artist.provenance_json)
    failures: dict[str, dict[str, str]] = {}
    ambiguities: list[dict[str, object]] = []
    outcomes: dict[str, str] = {}

    for provider_name in dict.fromkeys(enabled_providers):
        if provider_name not in VALID_METADATA_PROVIDERS:
            continue
        provider = build_metadata_provider(provider_name, settings)
        if provider is None:
            failures[provider_name] = {"error": "UnavailableProvider"}
            outcomes[provider_name] = "failed"
            continue
        try:
            known_identity = (
                await db.scalars(
                    select(CatalogArtistIdentity).where(
                        CatalogArtistIdentity.artist_id == artist.id,
                        CatalogArtistIdentity.provider == provider_name,
                    )
                )
            ).first()
            provider_id = (
                known_identity.provider_artist_id
                if known_identity is not None
                else artist_provider_id(artist, provider_name)
            )
            if provider_id is None:
                hits = await provider.search_artists(artist.name)
                existing_albums = list(artist.albums)
                scored: list[tuple[float, ArtistHit]] = []
                for hit in hits[:5]:
                    score = _name_similarity(artist.name, hit.name)
                    try:
                        candidate_albums = await provider.get_discography(hit.provider_id)
                        overlap = sum(
                            1
                            for candidate in candidate_albums
                            if any(
                                _album_keys_match(candidate, existing)
                                for existing in existing_albums
                            )
                        )
                        score += min(overlap / max(len(existing_albums), 1), 1.0)
                    except Exception:
                        pass
                    scored.append((score, hit))
                scored.sort(key=lambda item: item[0], reverse=True)
                selected_choice = choices.get(provider_name)
                selected_hit = next(
                    (hit for _, hit in scored if hit.provider_id == selected_choice), None
                )
                if selected_hit is not None:
                    provider_id = selected_hit.provider_id
                elif not scored or scored[0][0] < 0.82:
                    ambiguities.append({"provider": provider_name, "reason": "no_confident_match"})
                    outcomes[provider_name] = "ambiguous"
                    continue
                elif len(scored) > 1 and scored[0][0] - scored[1][0] < 0.15:
                    ambiguities.append(
                        {
                            "provider": provider_name,
                            "reason": "multiple_matches",
                            "candidates": [
                                {"provider_id": hit.provider_id, "name": hit.name}
                                for _, hit in scored[:3]
                            ],
                        }
                    )
                    outcomes[provider_name] = "ambiguous"
                    continue
                else:
                    provider_id = scored[0][1].provider_id

            detail = await provider.get_artist(provider_id)
            discography = await provider.get_discography(provider_id)
            async with db.begin_nested():
                ids = provider_ids_for_hit(detail)
                artist = await _merge_artist_id_collisions(db, artist, ids)
                for field in ("mbid", "deezer_id", "itunes_id"):
                    if not getattr(artist, field) and ids[field]:
                        setattr(artist, field, ids[field])
                        provenance[field] = provider_name
                if detail.artwork_url and not artist.artwork_url:
                    artist.artwork_url = detail.artwork_url
                    provenance["artwork_url"] = provider_name
                identity = await upsert_artist_identity(db, artist, detail)
                for album_hit in _compact_provider_discography(provider_name, discography):
                    await upsert_provider_release(db, artist, identity, album_hit)
                if provider_name == "deezer":
                    await reconcile_deezer_release_snapshots(db, artist.id)
                identity.last_discography_at = datetime.now(tz=UTC)
            outcomes[provider_name] = "ok"
        except Exception as exc:
            failures[provider_name] = {"error": type(exc).__name__}
            outcomes[provider_name] = "failed"

    if failures:
        provenance["provider_failures"] = failures
    else:
        provenance.pop("provider_failures", None)
    if ambiguities:
        provenance["provider_ambiguities"] = ambiguities
    else:
        provenance.pop("provider_ambiguities", None)
    provenance.pop("last_enrichment_error", None)
    artist.provenance_json = json.dumps(provenance, sort_keys=True)
    artist.last_enriched_at = datetime.now(tz=UTC)
    if artist.monitored and artist.watchlist_provider:
        from app.services.artist_monitoring import apply_monitor_policy

        selected_identity = (
            await db.scalars(
                select(CatalogArtistIdentity)
                .where(
                    CatalogArtistIdentity.artist_id == artist.id,
                    CatalogArtistIdentity.provider == artist.watchlist_provider,
                )
                .options(selectinload(CatalogArtistIdentity.releases))
                .execution_options(populate_existing=True)
            )
        ).first()
        if selected_identity is not None:
            apply_monitor_policy(artist, selected_identity.releases)
    await db.flush()
    status = "partial" if failures else "ambiguous" if ambiguities else "ok"
    result: dict[str, object] = {
        "status": status,
        "artist_id": artist.id,
        "providers": outcomes,
    }
    if ambiguities:
        result["candidates"] = ambiguities
    return result


async def reconcile_deezer_release_snapshots(
    db: AsyncSession, artist_id: int | None = None
) -> int:
    """Hide superseded Deezer snapshots while preserving canonical catalog state.

    Only provider rows with different known track counts are condensed. Canonical
    albums, manifests, jobs, imported tracks, and non-Deezer provider ownership
    remain untouched; the richer Deezer row is the sole selected-provider card.
    """
    stmt = (
        select(CatalogAlbumProvider)
        .join(CatalogArtistIdentity)
        .where(CatalogArtistIdentity.provider == "deezer")
        .order_by(CatalogAlbumProvider.artist_identity_id, CatalogAlbumProvider.id)
    )
    if artist_id is not None:
        stmt = stmt.where(CatalogArtistIdentity.artist_id == artist_id)
    releases = list((await db.scalars(stmt)).unique().all())
    groups: dict[tuple[int, str, str | None, str, str, str], list[CatalogAlbumProvider]] = {}
    for release in releases:
        family = _provider_release_family_key(release)
        groups.setdefault((release.artist_identity_id, *family), []).append(release)

    condensed = 0
    for group in groups.values():
        known_counts = {
            release.track_count for release in group if release.track_count is not None
        }
        if len(known_counts) < 2:
            continue
        richest_count = max(known_counts)
        richest = [release for release in group if release.track_count == richest_count]
        if len(richest) != 1 or richest[0].catalog_album_id is None:
            continue
        winner = richest[0]
        for loser in group:
            if loser.id == winner.id or loser.track_count in {None, richest_count}:
                continue
            winner.monitored = bool(winner.monitored or loser.monitored)
            if winner.monitor_override is None or loser.monitor_override is True:
                winner.monitor_override = loser.monitor_override
            winner.artwork_url = winner.artwork_url or loser.artwork_url
            winner.upc = winner.upc or loser.upc
            await db.delete(loser)
            condensed += 1
    await db.flush()
    return condensed


async def reconcile_duplicate_catalog_albums(
    db: AsyncSession, artist_id: int | None = None
) -> int:
    """Merge legacy duplicate catalog albums produced by older title normalization.

    Idempotent: album title/type groups only merge when normalized titles and edition
    markers match, with a missing year matching the known year for that title/type.
    """
    stmt = select(CatalogAlbum)
    if artist_id is not None:
        stmt = stmt.where(CatalogAlbum.artist_id == artist_id)
    albums = list((await db.scalars(stmt.order_by(CatalogAlbum.artist_id, CatalogAlbum.id))).all())
    merged = 0
    consumed: set[int] = set()
    for album in albums:
        if album.id in consumed:
            continue
        group = [
            other
            for other in albums
            if other.id not in consumed and _album_keys_match(album, other)
        ]
        if len(group) < 2:
            continue
        winner = max(group, key=_album_data_score)
        for loser in group:
            if loser.id == winner.id:
                continue
            loser_mbid = loser.mbid
            loser_deezer_id = loser.deezer_id
            loser_itunes_id = loser.itunes_id
            loser.mbid = None
            loser.deezer_id = None
            loser.itunes_id = None
            await db.flush()
            winner.monitored = bool(winner.monitored or loser.monitored)
            winner.in_library = bool(winner.in_library or loser.in_library)
            winner.year = winner.year or loser.year
            winner.release_type = winner.release_type or loser.release_type
            winner.artwork_url = winner.artwork_url or loser.artwork_url
            winner.track_count = winner.track_count or loser.track_count
            winner.mbid = winner.mbid or loser_mbid
            winner.deezer_id = winner.deezer_id or loser_deezer_id
            winner.itunes_id = winner.itunes_id or loser_itunes_id
            winner.providers_json = _merge_provider_json(
                winner.providers_json, loser.providers_json
            )
            await db.execute(
                update(CatalogAlbumTrack)
                .where(CatalogAlbumTrack.album_id == loser.id)
                .values(album_id=winner.id)
            )
            await db.execute(
                update(Job)
                .where(Job.catalog_album_id == loser.id)
                .values(catalog_album_id=winner.id)
            )
            await db.execute(
                update(Track)
                .where(Track.catalog_album_id == loser.id)
                .values(catalog_album_id=winner.id)
            )
            await db.refresh(loser, ["provider_releases"])
            for provider_release in list(loser.provider_releases):
                provider_release.catalog_album = winner
            await db.delete(loser)
            consumed.add(loser.id)
            merged += 1
        consumed.add(winner.id)
    await db.flush()
    return merged
