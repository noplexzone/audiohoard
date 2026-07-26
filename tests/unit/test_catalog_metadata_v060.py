from __future__ import annotations

import asyncio
import json

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.config import Settings
from app.database import Base
from app.metadata.base import AlbumHit, ArtistDetail, ArtistHit
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job, JobStatus
from app.models.track import Track
from app.services import catalog_metadata
from app.services.catalog_metadata import (
    _album_keys_match,
    _norm_title,
    enrich_catalog_artist,
    reconcile_duplicate_catalog_albums,
    reconcile_duplicate_catalog_artists,
    upsert_catalog_artist,
)


def test_album_title_normalization_folds_typographic_punctuation() -> None:
    assert _norm_title("We Don’t Get Along") == _norm_title("We Don't Get Along")
    mb = AlbumHit(
        provider="musicbrainz",
        provider_id="mb",
        title="We Don’t Get Along",
        year=None,
        release_type="Single",
    )
    dz = AlbumHit(
        provider="deezer",
        provider_id="dz",
        title="We Don't Get Along",
        year="2024",
        release_type="single",
    )
    assert _album_keys_match(mb, dz)


async def test_reconcile_duplicate_catalog_albums_merges_legacy_curly_apostrophe_duplicate(
    db_session,
) -> None:
    artist = CatalogArtist(name="Example")
    db_session.add(artist)
    await db_session.flush()
    loser = CatalogAlbum(
        artist_id=artist.id,
        title="We Don't Get Along",
        year="2024",
        release_type="Single",
        deezer_id="dz1",
        monitored=True,
    )
    winner = CatalogAlbum(
        artist_id=artist.id,
        title="We Don’t Get Along",
        year=None,
        release_type="single",
        mbid="mb1",
        track_count=12,
        artwork_url="cover.jpg",
    )
    db_session.add_all([loser, winner])
    await db_session.flush()
    db_session.add(CatalogAlbumTrack(album_id=loser.id, position=1, disc=1, title="Track"))
    job = Job(
        source="priority",
        query="Example We Don't Get Along",
        status=JobStatus.pending,
        catalog_album_id=loser.id,
    )
    db_session.add(job)
    await db_session.flush()
    db_session.add(Track(job_id=job.id, source="test", catalog_album_id=loser.id, title="Track"))
    await db_session.flush()

    merged = await reconcile_duplicate_catalog_albums(db_session, artist.id)
    await db_session.commit()

    assert merged == 1
    albums = list((await db_session.scalars(select(CatalogAlbum))).all())
    assert len(albums) == 1
    kept = albums[0]
    assert kept.mbid == "mb1"
    assert kept.deezer_id == "dz1"
    assert kept.monitored is True
    assert (await db_session.scalars(select(CatalogAlbumTrack.album_id))).one() == kept.id
    assert (await db_session.scalars(select(Job.catalog_album_id))).one() == kept.id
    assert (await db_session.scalars(select(Track.catalog_album_id))).one() == kept.id
    assert await reconcile_duplicate_catalog_albums(db_session, artist.id) == 0


class FakeMusicBrainzProvider:
    async def search_artists(self, query: str) -> list[ArtistHit]:
        return [
            ArtistHit(
                provider="musicbrainz", provider_id="artist-mbid", name=query, mbid="artist-mbid"
            )
        ]

    async def get_artist(self, id: str) -> ArtistDetail:
        return ArtistDetail(provider="musicbrainz", provider_id=id, name="Known Artist", mbid=id)

    async def get_discography(self, id: str) -> list[AlbumHit]:
        return [
            AlbumHit(
                provider="musicbrainz",
                provider_id="mb-album",
                title="Known Album",
                year="2024",
                release_type="Album",
                mbid="mb-album",
            )
        ]


async def test_enrichment_resolves_mbid_from_conservative_discography_overlap(
    db_session, monkeypatch, test_settings: Settings
) -> None:
    artist = CatalogArtist(name="Known Artist", deezer_id="123")
    db_session.add(artist)
    await db_session.flush()
    db_session.add(
        CatalogAlbum(
            artist_id=artist.id,
            title="Known Album",
            year="2024",
            release_type="Album",
            deezer_id="dz-album",
        )
    )
    await db_session.flush()
    await db_session.refresh(artist, ["albums"])

    monkeypatch.setattr(
        catalog_metadata,
        "build_metadata_provider",
        lambda name, settings: FakeMusicBrainzProvider() if name == "musicbrainz" else None,
    )

    outcome = await enrich_catalog_artist(db_session, test_settings, artist, ["musicbrainz"])
    assert outcome["status"] == "ok"
    assert artist.mbid == "artist-mbid"
    assert json.loads(artist.provenance_json or "{}")["mbid"] == "musicbrainz"


async def test_upsert_reuses_normalized_unresolved_artist_for_resolved_mbid(db_session) -> None:
    existing = CatalogArtist(name="Juice  WRLD")
    db_session.add(existing)
    await db_session.flush()

    result = await upsert_catalog_artist(
        db_session,
        ArtistDetail(
            provider="musicbrainz",
            provider_id="juice-mbid",
            name="Juice WRLD",
            mbid="juice-mbid",
        ),
    )

    assert result.id == existing.id
    assert result.mbid == "juice-mbid"
    assert len(list((await db_session.scalars(select(CatalogArtist))).all())) == 1


async def test_enrichment_merges_artist_before_assigning_colliding_mbid(
    db_session, monkeypatch, test_settings: Settings
) -> None:
    owner = CatalogArtist(name="Juice WRLD", mbid="artist-mbid", artwork_url="owner.jpg")
    duplicate = CatalogArtist(name="Juice Wrld", deezer_id="123", monitored=True)
    db_session.add_all([owner, duplicate])
    await db_session.flush()
    db_session.add(
        CatalogAlbum(
            artist_id=duplicate.id,
            title="Known Album",
            year="2024",
            release_type="Album",
        )
    )
    await db_session.flush()
    await db_session.refresh(duplicate, ["albums"])

    monkeypatch.setattr(
        catalog_metadata,
        "build_metadata_provider",
        lambda name, settings: FakeMusicBrainzProvider() if name == "musicbrainz" else None,
    )
    outcome = await enrich_catalog_artist(db_session, test_settings, duplicate, ["musicbrainz"])
    await db_session.commit()

    artists = list((await db_session.scalars(select(CatalogArtist))).all())
    assert len(artists) == 1
    assert artists[0].id == owner.id
    assert artists[0].monitored is True
    assert outcome["artist_id"] == owner.id
    assert (await db_session.scalars(select(CatalogAlbum.artist_id))).one() == owner.id


async def test_repair_merges_exact_juice_wrld_fixture_and_success_clears_error(
    db_session, test_settings: Settings
) -> None:
    owner = CatalogArtist(name="Juice WRLD", mbid="juice-mbid", monitor_policy="albums_only")
    duplicate = CatalogArtist(
        name="Juice  WRLD",
        monitored=True,
        monitor_policy="none_new",
        provenance_json=json.dumps(
            {"last_enrichment_error": {"at": "2026-01-01", "message": "IntegrityError: old"}}
        ),
    )
    db_session.add_all([owner, duplicate])
    await db_session.flush()
    owner_album = CatalogAlbum(
        artist_id=owner.id,
        title="Death Race for Love",
        year="2019",
        release_type="Album",
        mbid="death-race",
    )
    duplicate_same_album = CatalogAlbum(
        artist_id=duplicate.id,
        title="Death Race for Love",
        year="2019",
        release_type="album",
        deezer_id="1234",
        monitored=True,
    )
    duplicate_unique_album = CatalogAlbum(
        artist_id=duplicate.id,
        title="Legends Never Die",
        year="2020",
        release_type="Album",
    )
    db_session.add_all([owner_album, duplicate_same_album, duplicate_unique_album])
    await db_session.flush()
    job = Job(
        source="priority",
        query="Juice WRLD Death Race for Love",
        status=JobStatus.pending,
        catalog_album_id=duplicate_same_album.id,
    )
    db_session.add(job)
    await db_session.flush()

    assert await reconcile_duplicate_catalog_artists(db_session) == 1
    await db_session.commit()

    artists = list((await db_session.scalars(select(CatalogArtist))).all())
    assert len(artists) == 1
    survivor = artists[0]
    assert survivor.id == owner.id
    assert survivor.monitored is True
    assert survivor.monitor_policy == "albums_only"
    assert "last_enrichment_error" in json.loads(survivor.provenance_json or "{}")
    albums = list((await db_session.scalars(select(CatalogAlbum))).all())
    assert {album.title for album in albums} == {"Death Race for Love", "Legends Never Die"}
    assert all(album.artist_id == survivor.id for album in albums)
    kept_duplicate = next(album for album in albums if album.title == "Death Race for Love")
    assert kept_duplicate.mbid == "death-race"
    assert kept_duplicate.deezer_id == "1234"
    assert (await db_session.scalars(select(Job.catalog_album_id))).one() == kept_duplicate.id
    assert await reconcile_duplicate_catalog_artists(db_session) == 0

    await db_session.refresh(survivor, ["albums"])
    outcome = await enrich_catalog_artist(db_session, test_settings, survivor, ["musicbrainz"])
    assert outcome["status"] == "ok"
    assert "last_enrichment_error" not in json.loads(survivor.provenance_json or "{}")


@pytest.mark.asyncio
async def test_concurrent_upsert_both_sessions_complete_one_artist_survives(
    tmp_path,
) -> None:
    """Two sessions racing to upsert the same mbid: both complete without exception and exactly
    one artist row survives with the provider ID intact."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'race.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    hit = ArtistHit(
        provider="musicbrainz",
        provider_id="concurrent-mbid",
        name="Concurrent Artist",
        mbid="concurrent-mbid",
    )

    async def run_upsert() -> CatalogArtist:
        async with AsyncSession(engine, expire_on_commit=False) as db:
            artist = await upsert_catalog_artist(db, hit)
            await db.commit()
            return artist

    results = await asyncio.gather(run_upsert(), run_upsert(), return_exceptions=True)
    assert not any(isinstance(r, Exception) for r in results), results

    async with AsyncSession(engine) as check:
        artists = list((await check.scalars(select(CatalogArtist))).all())
    assert len(artists) == 1
    assert artists[0].mbid == "concurrent-mbid"
    await engine.dispose()
