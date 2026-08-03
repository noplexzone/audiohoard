from __future__ import annotations

import asyncio
import json

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.config import Settings
from app.database import Base
from app.metadata.base import AlbumDetail, AlbumHit, AlbumTrack, ArtistDetail, ArtistHit
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.job import Job, JobStatus
from app.models.track import Track
from app.services import catalog_metadata
from app.services.catalog_metadata import (
    _album_keys_match,
    _norm_title,
    enrich_catalog_artist,
    fetch_and_store_discography,
    reconcile_duplicate_catalog_albums,
    reconcile_duplicate_catalog_artists,
    upsert_catalog_artist,
    upsert_provider_release,
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


def test_album_keys_keep_clean_and_explicit_singles_separate() -> None:
    clean = AlbumHit(
        provider="deezer",
        provider_id="927173351",
        deezer_id="927173351",
        title="We Don’t Get Along",
        year="2026",
        release_type="single",
        track_count=1,
        content_rating="clean",
    )
    explicit = AlbumHit(
        provider="deezer",
        provider_id="927037751",
        deezer_id="927037751",
        title="We Don’t Get Along",
        year="2026",
        release_type="single",
        track_count=1,
        content_rating="explicit",
    )

    assert not _album_keys_match(clean, explicit)


async def test_upsert_provider_release_splits_existing_clean_explicit_siblings(db_session) -> None:
    artist = CatalogArtist(name="Juice WRLD")
    identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="juice", name=artist.name
    )
    artist.identities.append(identity)
    merged = CatalogAlbum(
        artist=artist,
        title="AGATS2 (Insecure)",
        year="2024",
        release_type="single",
        deezer_id="670364511",
        track_count=1,
    )
    identity.releases.extend(
        [
            CatalogAlbumProvider(
                provider_album_id="670364511", title="AGATS2 (Insecure)", catalog_album=merged
            ),
            CatalogAlbumProvider(
                provider_album_id="670362621", title="AGATS2 (Insecure)", catalog_album=merged
            ),
        ]
    )
    db_session.add(artist)
    await db_session.flush()

    clean = await upsert_provider_release(
        db_session,
        artist,
        identity,
        AlbumHit(
            provider="deezer",
            provider_id="670364511",
            deezer_id="670364511",
            title="AGATS2 (Insecure)",
            year="2024",
            release_type="single",
            release_kind="single",
            track_count=1,
            content_rating="clean",
            upc="602475636335",
        ),
    )
    explicit = await upsert_provider_release(
        db_session,
        artist,
        identity,
        AlbumHit(
            provider="deezer",
            provider_id="670362621",
            deezer_id="670362621",
            title="AGATS2 (Insecure)",
            year="2024",
            release_type="single",
            release_kind="single",
            track_count=1,
            content_rating="explicit",
            upc="602475636328",
        ),
    )
    await db_session.flush()

    assert clean.catalog_album_id != explicit.catalog_album_id
    albums = list((await db_session.scalars(select(CatalogAlbum))).all())
    assert {(album.deezer_id, album.content_rating, album.upc) for album in albums} == {
        ("670364511", "clean", "602475636335"),
        ("670362621", "explicit", "602475636328"),
    }


async def test_mbid_upsert_does_not_absorb_distinct_deezer_same_name(db_session) -> None:
    deezer_artist = CatalogArtist(
        name="Playboi Carti",
        deezer_id="10002824",
        monitored=True,
        watchlist_provider="deezer",
    )
    db_session.add(deezer_artist)
    await db_session.flush()
    deezer_artist_id = deezer_artist.id

    mb_artist = await upsert_catalog_artist(
        db_session,
        ArtistDetail(
            provider="musicbrainz",
            provider_id="2baf3276-ed6a-4349-8d2e-f4601e7b2167",
            name="Playboi Carti",
            mbid="2baf3276-ed6a-4349-8d2e-f4601e7b2167",
        ),
    )
    await db_session.flush()

    assert mb_artist.id != deezer_artist_id
    rows = list((await db_session.scalars(select(CatalogArtist))).all())
    assert {(row.id, row.deezer_id, row.mbid) for row in rows} == {
        (deezer_artist_id, "10002824", None),
        (mb_artist.id, None, "2baf3276-ed6a-4349-8d2e-f4601e7b2167"),
    }


async def test_duplicate_reconciliation_keeps_distinct_provider_same_name_rows(db_session) -> None:
    resolved = CatalogArtist(
        name="Playboi Carti",
        mbid="2baf3276-ed6a-4349-8d2e-f4601e7b2167",
    )
    provider_native = CatalogArtist(name="Playboi Carti", deezer_id="10002824")
    placeholder = CatalogArtist(name="Playboi Carti")
    db_session.add_all([resolved, provider_native, placeholder])
    await db_session.flush()
    provider_native_id = provider_native.id

    merged = await reconcile_duplicate_catalog_artists(db_session)
    await db_session.flush()

    assert merged == 1
    remaining = list((await db_session.scalars(select(CatalogArtist))).all())
    assert len(remaining) == 2
    assert any(row.id == provider_native_id and row.deezer_id == "10002824" for row in remaining)
    assert any(row.mbid == "2baf3276-ed6a-4349-8d2e-f4601e7b2167" for row in remaining)


async def test_provider_refresh_does_not_erase_a_known_track_count(db_session) -> None:
    artist = CatalogArtist(name="Known Count Artist")
    identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="artist-1", name=artist.name
    )
    release = CatalogAlbumProvider(
        provider_album_id="album-1",
        title="Known Count Album",
        track_count=7,
        release_kind="album",
    )
    identity.releases.append(release)
    artist.identities.append(identity)
    db_session.add(artist)
    await db_session.flush()

    refreshed = await upsert_provider_release(
        db_session,
        artist,
        identity,
        AlbumHit(
            provider="deezer",
            provider_id="album-1",
            title="Known Count Album",
            release_kind="album",
            track_count=None,
        ),
    )

    assert refreshed.track_count == 7
    assert json.loads(refreshed.metadata_json or "{}")["track_count_checked"] is True


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


async def test_fetch_and_store_album_prefers_deezer_identity_for_hybrid_catalog_album(
    db_session: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    artist = CatalogArtist(name="Lil Uzi Vert")
    album = CatalogAlbum(
        artist=artist,
        title="RED & WHITE",
        year="2022",
        release_type="EP",
        mbid="musicbrainz-five-track",
        deezer_id="deezer-nine-track",
        track_count=9,
    )
    db_session.add(artist)
    await db_session.flush()
    requested: list[tuple[str, str]] = []

    class FakeProvider:
        def __init__(self, name: str) -> None:
            self.name = name

        async def get_album(self, id: str) -> AlbumDetail:
            requested.append((self.name, id))
            return AlbumDetail(
                provider=self.name,
                provider_id=id,
                deezer_id=id if self.name == "deezer" else None,
                mbid=id if self.name == "musicbrainz" else None,
                title="RED & WHITE",
                year="2022",
                release_type="ep",
                release_kind="ep",
                track_count=9,
                tracks=[
                    AlbumTrack(position=1, title="SPACE CADET"),
                    AlbumTrack(position=2, title="I KNOW"),
                    AlbumTrack(position=3, title="FLEX UP"),
                    AlbumTrack(position=4, title="HITTIN MY SHOULDER"),
                    AlbumTrack(position=5, title="FOR FUN"),
                    AlbumTrack(position=6, title="CIGARETTE"),
                    AlbumTrack(position=7, title="ISSA HIT"),
                    AlbumTrack(position=8, title="GLOCK IN MY PURSE"),
                    AlbumTrack(position=9, title="F.F."),
                ],
            )

    def fake_build_metadata_provider(name: str, settings: Settings) -> FakeProvider:
        return FakeProvider(name)

    monkeypatch.setattr(catalog_metadata, "build_metadata_provider", fake_build_metadata_provider)

    hydrated = await catalog_metadata.fetch_and_store_album(db_session, test_settings, album)

    assert requested == [("deezer", "deezer-nine-track")]
    assert hydrated.id == album.id
    assert hydrated.track_count == 9
    assert [track.title for track in hydrated.tracks] == [
        "SPACE CADET",
        "I KNOW",
        "FLEX UP",
        "HITTIN MY SHOULDER",
        "FOR FUN",
        "CIGARETTE",
        "ISSA HIT",
        "GLOCK IN MY PURSE",
        "F.F.",
    ]


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
    assert outcome["status"] == "ok", outcome
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
    db_session, monkeypatch, test_settings: Settings
) -> None:
    owner = CatalogArtist(name="Juice WRLD", mbid="juice-mbid", monitor_policy="albums_only")
    duplicate = CatalogArtist(
        name="Juice  WRLD",
        monitored=True,
        monitor_policy="none_new",
        watchlist_provider="deezer",
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
    duplicate_identity = CatalogArtistIdentity(
        artist_id=duplicate.id,
        provider="deezer",
        provider_artist_id="juice-deezer",
        name="Juice WRLD",
    )
    db_session.add(duplicate_identity)
    await db_session.flush()
    db_session.add(
        CatalogAlbumProvider(
            artist_identity_id=duplicate_identity.id,
            catalog_album_id=duplicate_same_album.id,
            provider_album_id="1234",
            title="Death Race for Love",
            release_kind="album",
            release_type_raw="album",
            monitored=True,
        )
    )
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
    assert survivor.watchlist_provider == "deezer"
    assert "last_enrichment_error" in json.loads(survivor.provenance_json or "{}")
    albums = list((await db_session.scalars(select(CatalogAlbum))).all())
    assert {album.title for album in albums} == {"Death Race for Love", "Legends Never Die"}
    assert all(album.artist_id == survivor.id for album in albums)
    kept_duplicate = next(album for album in albums if album.title == "Death Race for Love")
    assert kept_duplicate.mbid == "death-race"
    assert kept_duplicate.deezer_id == "1234"
    assert (await db_session.scalars(select(Job.catalog_album_id))).one() == kept_duplicate.id
    identity = (await db_session.scalars(select(CatalogArtistIdentity))).one()
    provider_release = (await db_session.scalars(select(CatalogAlbumProvider))).one()
    assert identity.artist_id == survivor.id
    assert provider_release.artist_identity_id == identity.id
    assert provider_release.catalog_album_id == kept_duplicate.id
    assert provider_release.monitored is True
    assert await reconcile_duplicate_catalog_artists(db_session) == 0

    await db_session.refresh(survivor, ["albums"])
    provider = DirectKnownProvider("musicbrainz")
    monkeypatch.setattr(
        catalog_metadata, "build_metadata_provider", lambda name, settings: provider
    )
    outcome = await enrich_catalog_artist(db_session, test_settings, survivor, ["musicbrainz"])
    assert outcome["status"] == "ok", outcome
    assert "last_enrichment_error" not in json.loads(survivor.provenance_json or "{}")


async def test_artist_merge_preserves_distinct_releases_from_same_provider(
    db_session,
) -> None:
    owner = CatalogArtist(name="Same Provider", mbid="owner-mbid")
    duplicate = CatalogArtist(
        name="Same  Provider",
        monitored=True,
        watchlist_provider="deezer",
    )
    db_session.add_all([owner, duplicate])
    await db_session.flush()
    owner_identity = CatalogArtistIdentity(
        artist_id=owner.id,
        provider="deezer",
        provider_artist_id="owner-deezer",
        name="Same Provider",
    )
    duplicate_identity = CatalogArtistIdentity(
        artist_id=duplicate.id,
        provider="deezer",
        provider_artist_id="duplicate-deezer",
        name="Same Provider",
    )
    db_session.add_all([owner_identity, duplicate_identity])
    await db_session.flush()
    db_session.add_all(
        [
            CatalogAlbumProvider(
                artist_identity_id=owner_identity.id,
                provider_album_id="owner-release",
                title="Owner Release",
                release_kind="album",
            ),
            CatalogAlbumProvider(
                artist_identity_id=duplicate_identity.id,
                provider_album_id="duplicate-release",
                title="Duplicate Release",
                release_kind="album",
                monitored=True,
            ),
        ]
    )
    await db_session.flush()

    assert await reconcile_duplicate_catalog_artists(db_session) == 1
    await db_session.commit()

    artists = list((await db_session.scalars(select(CatalogArtist))).all())
    identities = list((await db_session.scalars(select(CatalogArtistIdentity))).all())
    releases = list((await db_session.scalars(select(CatalogAlbumProvider))).all())
    assert len(artists) == 1
    assert artists[0].watchlist_provider == "deezer"
    assert len(identities) == 1
    assert identities[0].artist_id == artists[0].id
    assert {release.provider_album_id for release in releases} == {
        "owner-release",
        "duplicate-release",
    }
    assert all(release.artist_identity_id == identities[0].id for release in releases)


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


class DirectKnownProvider:
    def __init__(self, name: str, *, fail: bool = False) -> None:
        self.name = name
        self.fail = fail
        self.search_calls = 0
        self.discography_ids: list[str] = []

    async def search_artists(self, query: str) -> list[ArtistHit]:
        self.search_calls += 1
        return []

    async def get_artist(self, id: str) -> ArtistDetail:
        if self.fail:
            raise RuntimeError("secret provider detail")
        values = {"mbid": None, "deezer_id": None, "itunes_id": None}
        field = {"musicbrainz": "mbid", "deezer": "deezer_id", "itunes": "itunes_id"}[self.name]
        values[field] = id
        return ArtistDetail(provider=self.name, provider_id=id, name="Known Artist", **values)

    async def get_discography(self, id: str) -> list[AlbumHit]:
        self.discography_ids.append(id)
        if self.fail:
            raise RuntimeError("secret provider discography")
        values = {"mbid": None, "deezer_id": None, "itunes_id": None}
        field = {"musicbrainz": "mbid", "deezer": "deezer_id", "itunes": "itunes_id"}[self.name]
        values[field] = f"{self.name}-album"
        return [
            AlbumHit(
                provider=self.name,
                provider_id=f"{self.name}-album",
                title=f"{self.name.title()} Release",
                release_type="Album",
                **values,
            )
        ]


async def test_fetch_discography_uses_explicit_provider_and_tags_membership(
    db_session, monkeypatch, test_settings: Settings
) -> None:
    artist = CatalogArtist(name="Known Artist", mbid="mb-artist", deezer_id="dz-artist")
    db_session.add(artist)
    await db_session.flush()
    deezer = DirectKnownProvider("deezer")
    monkeypatch.setattr(catalog_metadata, "build_metadata_provider", lambda name, settings: deezer)

    albums = await fetch_and_store_discography(
        db_session, test_settings, artist, provider_name="deezer"
    )

    assert deezer.discography_ids == ["dz-artist"]
    identity = (
        await db_session.scalars(
            select(CatalogArtistIdentity).where(CatalogArtistIdentity.provider == "deezer")
        )
    ).one()
    assert albums[0].artist_identity_id == identity.id
    assert albums[0].release_kind == "album"


async def test_enrichment_attempts_known_provider_and_isolates_provider_failure(
    db_session, monkeypatch, test_settings: Settings
) -> None:
    artist = CatalogArtist(name="Known Artist", mbid="mb-artist", deezer_id="dz-artist")
    db_session.add(artist)
    await db_session.flush()
    await db_session.refresh(artist, ["albums"])
    musicbrainz = DirectKnownProvider("musicbrainz")
    deezer = DirectKnownProvider("deezer", fail=True)
    monkeypatch.setattr(
        catalog_metadata,
        "build_metadata_provider",
        lambda name, settings: {"musicbrainz": musicbrainz, "deezer": deezer}[name],
    )

    outcome = await enrich_catalog_artist(
        db_session, test_settings, artist, ["musicbrainz", "deezer"]
    )

    assert outcome["status"] == "partial"
    assert musicbrainz.search_calls == 0
    assert musicbrainz.discography_ids == ["mb-artist"]
    albums = list((await db_session.scalars(select(CatalogAlbum))).all())
    assert json.loads(albums[0].providers_json or "[]") == ["musicbrainz"]
    failures = json.loads(artist.provenance_json or "{}")["provider_failures"]
    assert failures["deezer"]["error"] == "RuntimeError"
    assert "secret" not in json.dumps(failures)
