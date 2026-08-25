from __future__ import annotations

import hashlib
import json

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.acquisition_claim import AcquisitionDispatchClaim
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.discography_batch import (
    DiscographyBatchItem,
    DiscographyBatchItemState,
    DiscographyScopeKind,
)
from app.models.job import Job, JobStatus
from app.services.discography_batches import (
    DiscographyScopeError,
    canonicalize_scope,
    create_discography_batch_preview,
)
from app.settings_service import QualityProfile

PROFILE = QualityProfile(
    format_preference=["flac", "mp3", "m4a/aac", "ogg", "opus"],
    min_mp3_bitrate=320,
    allow_lower_quality_fallback=True,
)


async def _artist(db: AsyncSession) -> tuple[CatalogArtist, CatalogArtistIdentity]:
    artist = CatalogArtist(name="Scope Artist", monitored=True)
    identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="artist-1", name=artist.name
    )
    artist.identities.append(identity)
    db.add(artist)
    await db.flush()
    return artist, identity


def _release(
    identity: CatalogArtistIdentity,
    provider_id: str,
    title: str,
    year: str,
    kind: str,
    monitored: bool,
    album: CatalogAlbum | None = None,
    expected: int | None = None,
) -> CatalogAlbumProvider:
    return CatalogAlbumProvider(
        artist_identity=identity,
        provider_album_id=provider_id,
        title=title,
        year=year,
        release_kind=kind,
        monitored=monitored,
        catalog_album=album,
        track_count=expected,
    )


def _scope(artist_id: int, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "artist_id": artist_id,
        "provider": "deezer",
        "release_type": "all",
        "year_from": None,
        "year_to": None,
        "monitoring_status": "all",
    }
    payload.update(overrides)
    return payload


def test_artist_scope_is_immutable_canonical_and_validates_year_order() -> None:
    scope, encoded = canonicalize_scope(
        DiscographyScopeKind.artist,
        {
            "provider": " deezer ",
            "artist_id": "7",
            "release_type": "single_ep",
            "year_from": "1999",
            "year_to": "2001",
            "monitoring_status": "monitored",
        },
    )
    assert scope.artist_id == 7
    assert encoded == (
        '{"artist_id":7,"monitoring_status":"monitored","provider":"deezer",'
        '"release_type":"single_ep","year_from":"1999","year_to":"2001"}'
    )
    with pytest.raises((AttributeError, TypeError)):
        scope.artist_id = 8  # type: ignore[misc]
    with pytest.raises(DiscographyScopeError, match="year_from"):
        canonicalize_scope(
            DiscographyScopeKind.artist, _scope(7, year_from="2002", year_to="2001")
        )
    with pytest.raises(DiscographyScopeError, match="4-digit"):
        canonicalize_scope(DiscographyScopeKind.artist, _scope(7, year_from="99"))


async def test_artist_filters_compose_provider_owned_unbound_and_dedup(
    db_session: AsyncSession,
) -> None:
    artist, identity = await _artist(db_session)
    album = CatalogAlbum(
        artist=artist, title="Chosen", year="2000", release_type="single", track_count=1
    )
    album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Track"))
    db_session.add_all(
        [
            _release(identity, "a", "Chosen", "2000", "single", True, album, 1),
            _release(identity, "b", "Chosen duplicate", "2000", "ep", True, album, 1),
            _release(identity, "unbound", "Unbound EP", "2001", "ep", True),
            _release(identity, "wrong-kind", "Album", "2000", "album", True),
            _release(identity, "wrong-monitor", "Single", "2000", "single", False),
        ]
    )
    other = CatalogArtistIdentity(
        artist=artist, provider="itunes", provider_artist_id="other", name=artist.name
    )
    db_session.add(other)
    db_session.add(_release(other, "other-provider", "Other", "2000", "single", True))
    await db_session.flush()
    preview = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.artist,
        _scope(
            artist.id,
            release_type="single_ep",
            year_from="2000",
            year_to="2001",
            monitoring_status="monitored",
        ),
        quality_profile=PROFILE,
    )
    assert preview.matching_count == 3
    assert preview.skipped_count == 1
    items = list(
        (
            await db_session.scalars(
                select(DiscographyBatchItem)
                .where(DiscographyBatchItem.batch_id == preview.id)
                .order_by(DiscographyBatchItem.release_title)
            )
        ).all()
    )
    assert [(item.release_title, item.state, item.reason_code) for item in items] == [
        ("Chosen", DiscographyBatchItemState.preview, None),
        ("Unbound EP", DiscographyBatchItemState.skipped, "catalog_release_unbound"),
    ]


async def test_duplicate_release_identity_affects_hash_but_not_actionable_count(
    db_session: AsyncSession,
) -> None:
    artist, identity = await _artist(db_session)
    album = CatalogAlbum(artist=artist, title="One", year="2020", track_count=1)
    album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Track"))
    db_session.add(_release(identity, "a", "One", "2020", "album", True, album, 1))
    await db_session.flush()
    first = await create_discography_batch_preview(
        db_session, DiscographyScopeKind.artist, _scope(artist.id), quality_profile=PROFILE
    )
    db_session.add(_release(identity, "b", "One deluxe", "2020", "album", True, album, 1))
    await db_session.flush()
    second = await create_discography_batch_preview(
        db_session, DiscographyScopeKind.artist, _scope(artist.id), quality_profile=PROFILE
    )
    assert second.matching_count == 2 and second.scope_hash != first.scope_hash
    assert (
        await db_session.scalar(
            select(func.count(DiscographyBatchItem.id)).where(
                DiscographyBatchItem.batch_id == second.id
            )
        )
        == 1
    )


async def test_wanted_scopes_intersect_server_missing_and_retain_semantics(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Wanted", monitored=True)
    albums: list[CatalogAlbum] = []
    for index in range(3):
        album = CatalogAlbum(
            artist=artist,
            title=f"Release {index}",
            year=f"202{index}",
            monitored=True,
            track_count=1,
        )
        album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Track"))
        albums.append(album)
    db_session.add(artist)
    await db_session.flush()
    selected = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_selected,
        {"album_ids": [albums[0].id, albums[0].id, 999999]},
        quality_profile=PROFILE,
    )
    page = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_page,
        {"album_ids": [albums[1].id]},
        quality_profile=PROFILE,
    )
    all_matching = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_all_matching,
        {"q": "Wanted", "sort": "title", "status": "all"},
        quality_profile=PROFILE,
    )
    assert (selected.scope_kind, selected.matching_count) == (
        DiscographyScopeKind.wanted_selected,
        1,
    )
    assert (page.scope_kind, page.matching_count) == (DiscographyScopeKind.wanted_page, 1)
    assert all_matching.matching_count == 3


@pytest.mark.parametrize(
    ("tracks", "expected", "reason"),
    [
        ([], 2, "catalog_manifest_missing"),
        ([(1, 1)], 2, "catalog_manifest_incomplete"),
        ([(1, 1), (1, 2)], 1, "catalog_manifest_overfull"),
        ([(1, 1), (1, 1)], 2, "catalog_manifest_invalid_positions"),
        ([(0, 1)], 1, "catalog_manifest_invalid_positions"),
    ],
)
async def test_preview_manifest_hydration_without_jobs(
    db_session: AsyncSession, tracks: list[tuple[int, int]], expected: int, reason: str
) -> None:
    artist = CatalogArtist(name="Manifest", monitored=True)
    album = CatalogAlbum(artist=artist, title="Manifest", monitored=True, track_count=expected)
    for disc, position in tracks:
        album.tracks.append(CatalogAlbumTrack(disc=disc, position=position, title="Track"))
    db_session.add(artist)
    await db_session.flush()
    preview = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_selected,
        {"album_ids": [album.id]},
        quality_profile=PROFILE,
    )
    item = await db_session.scalar(
        select(DiscographyBatchItem).where(DiscographyBatchItem.batch_id == preview.id)
    )
    assert item is not None and item.reason_code == reason
    assert preview.hydration_required_count == 1
    assert preview.missing_count == expected and preview.estimated_job_count == expected
    assert await db_session.scalar(select(func.count(Job.id))) == 0


async def test_preview_counts_active_missing_and_estimates(db_session: AsyncSession) -> None:
    artist = CatalogArtist(name="Counts", monitored=True)
    unknown = CatalogAlbum(artist=artist, title="Unknown", monitored=True, track_count=None)
    active = CatalogAlbum(artist=artist, title="Active", monitored=True, track_count=2)
    for index in range(1, 3):
        active.tracks.append(CatalogAlbumTrack(disc=1, position=index, title=f"T{index}"))
    db_session.add(artist)
    await db_session.flush()
    owner = Job(
        source="priority",
        query="active",
        status=JobStatus.running,
        catalog_album_id=active.id,
        catalog_track_id=active.tracks[0].id,
    )
    db_session.add(owner)
    await db_session.flush()
    db_session.add(
        AcquisitionDispatchClaim(
            catalog_album_id=active.id, catalog_track_id=active.tracks[0].id, job_id=owner.id
        )
    )
    await db_session.flush()
    preview = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_selected,
        {"album_ids": [unknown.id, active.id]},
        quality_profile=PROFILE,
    )
    assert (
        preview.matching_count,
        preview.hydration_required_count,
        preview.active_count,
        preview.missing_count,
        preview.estimated_job_count,
    ) == (2, 1, 1, 2, 1)


async def test_scope_hash_is_deterministic(db_session: AsyncSession) -> None:
    artist = CatalogArtist(name="Hash", monitored=True)
    album = CatalogAlbum(artist=artist, title="Hash", monitored=True, track_count=1)
    album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Track"))
    db_session.add(artist)
    await db_session.flush()
    first = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_selected,
        {"album_ids": [album.id]},
        quality_profile=PROFILE,
    )
    second = await create_discography_batch_preview(
        db_session,
        DiscographyScopeKind.wanted_selected,
        {"album_ids": [album.id]},
        quality_profile=PROFILE,
    )
    assert first.scope_hash == second.scope_hash
    identities = json.dumps([f"catalog_album:{album.id}"], separators=(",", ":"))
    assert (
        first.scope_hash
        == hashlib.sha256(f"{first.scope_json}\n{identities}".encode()).hexdigest()
    )
