from __future__ import annotations

import asyncio
import json

import httpx
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
    _compact_provider_discography,
    _norm_title,
    enrich_catalog_artist,
    fetch_and_store_discography,
    reconcile_deezer_release_snapshots,
    reconcile_duplicate_catalog_albums,
    reconcile_duplicate_catalog_artists,
    search_catalog_artists,
    upsert_catalog_artist,
    upsert_provider_release,
)


async def test_artist_search_filters_invalid_identity_and_ranks_deezer_fans(
    monkeypatch: pytest.MonkeyPatch, test_settings: Settings
) -> None:
    class FakeProvider:
        def __init__(self, name: str) -> None:
            self.name = name

        async def health(self):
            from app.sources.base import CapabilityState

            return CapabilityState(True)

        async def search_artists(self, query: str) -> list[ArtistHit]:
            del query
            if self.name == "deezer":
                return [
                    ArtistHit("deezer", "fanless", "Fanless", deezer_id="fanless"),
                    ArtistHit("deezer", "low", "Low", deezer_id="low", fan_count=5),
                    ArtistHit("deezer", "10002824", "Stale", deezer_id="10002824", fan_count=99),
                    ArtistHit("deezer", "high", "High", deezer_id="high", fan_count=50),
                ]
            native_field = {"musicbrainz": "mbid", "itunes": "itunes_id"}[self.name]
            return [
                ArtistHit(
                    self.name,
                    f"{self.name}-1",
                    "First",
                    **{native_field: f"{self.name}-1"},
                ),
                ArtistHit(
                    self.name,
                    f"{self.name}-2",
                    "Second",
                    **{native_field: f"{self.name}-2"},
                ),
            ]

        async def get_artist(self, id: str) -> ArtistDetail:
            if id == "10002824":
                return ArtistDetail("deezer", "", "", deezer_id=None)
            return ArtistDetail(
                self.name,
                id,
                id.title(),
                deezer_id=id if self.name == "deezer" else None,
                mbid=id if self.name == "musicbrainz" else None,
                itunes_id=id if self.name == "itunes" else None,
                type="Group" if self.name == "musicbrainz" else None,
            )

    monkeypatch.setattr(
        catalog_metadata,
        "build_metadata_provider",
        lambda name, settings: FakeProvider(name),
    )

    outcomes = await search_catalog_artists(
        test_settings, "artist", ["musicbrainz", "deezer", "itunes"]
    )

    assert [outcome.provider for outcome in outcomes] == ["deezer", "musicbrainz", "itunes"]
    assert [hit.provider_id for outcome in outcomes for hit in outcome.artists] == [
        "high",
        "low",
        "fanless",
        "musicbrainz-1",
        "musicbrainz-2",
        "itunes-1",
        "itunes-2",
    ]


async def test_musicbrainz_search_rows_with_native_ids_do_not_require_serial_detail_validation(
    monkeypatch: pytest.MonkeyPatch, test_settings: Settings
) -> None:
    class MusicBrainzSearchProvider:
        async def health(self):
            from app.sources.base import CapabilityState

            return CapabilityState(True)

        async def search_artists(self, query: str) -> list[ArtistHit]:
            del query
            return [
                *[
                    ArtistHit(
                        "musicbrainz",
                        f"mb-{index}",
                        f"Artist {index}",
                        mbid=f"mb-{index}",
                        type="Group",
                    )
                    for index in range(10)
                ],
                ArtistHit(
                    "musicbrainz",
                    "invalid-event",
                    "Not an artist",
                    mbid="invalid-event",
                    type="Event",
                ),
            ]

        async def get_artist(self, id: str) -> ArtistDetail:
            raise AssertionError(f"MusicBrainz search row {id} was unnecessarily revalidated")

    monkeypatch.setattr(
        catalog_metadata,
        "build_metadata_provider",
        lambda name, settings: MusicBrainzSearchProvider(),
    )

    outcomes = await search_catalog_artists(test_settings, "artist", ["musicbrainz"])

    assert [hit.provider_id for hit in outcomes[0].artists] == [
        f"mb-{index}" for index in range(10)
    ]


async def test_deezer_validation_fails_open_for_transport_errors_but_rejects_invalid_identity(
    monkeypatch: pytest.MonkeyPatch, test_settings: Settings
) -> None:
    class DeezerSearchProvider:
        async def health(self):
            from app.sources.base import CapabilityState

            return CapabilityState(True)

        async def search_artists(self, query: str) -> list[ArtistHit]:
            del query
            return [
                ArtistHit("deezer", "transient", "Transient", deezer_id="transient"),
                ArtistHit("deezer", "invalid", "Invalid", deezer_id="invalid"),
            ]

        async def get_artist(self, id: str) -> ArtistDetail:
            if id == "transient":
                raise httpx.ReadTimeout("temporary provider timeout")
            raise ValueError("definitive provider error envelope")

    monkeypatch.setattr(
        catalog_metadata,
        "build_metadata_provider",
        lambda name, settings: DeezerSearchProvider(),
    )

    outcomes = await search_catalog_artists(test_settings, "artist", ["deezer"])

    assert [hit.provider_id for hit in outcomes[0].artists] == ["transient"]


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

    assert merged == 0
    remaining = list((await db_session.scalars(select(CatalogArtist))).all())
    assert len(remaining) == 3
    assert any(row.id == provider_native_id and row.deezer_id == "10002824" for row in remaining)
    assert any(row.mbid == "2baf3276-ed6a-4349-8d2e-f4601e7b2167" for row in remaining)
    assert any(row.id == placeholder.id for row in remaining)


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


def test_compact_deezer_discography_keeps_richest_snapshot_and_real_editions() -> None:
    hits = [
        AlbumHit(
            provider="deezer",
            provider_id="338552127",
            deezer_id="338552127",
            title="RED & WHITE",
            year="2022",
            release_type="EP",
            release_kind="ep",
            track_count=5,
            content_rating="explicit",
        ),
        AlbumHit(
            provider="deezer",
            provider_id="339525867",
            deezer_id="339525867",
            title="RED & WHITE",
            year="2022",
            release_type="EP",
            release_kind="ep",
            track_count=9,
            content_rating="explicit",
        ),
        AlbumHit(
            provider="deezer",
            provider_id="deluxe",
            deezer_id="deluxe",
            title="RED & WHITE (Deluxe Edition)",
            year="2022",
            release_type="EP",
            release_kind="ep",
            track_count=12,
            content_rating="explicit",
        ),
        AlbumHit(
            provider="deezer",
            provider_id="clean",
            deezer_id="clean",
            title="RED & WHITE",
            year="2022",
            release_type="EP",
            release_kind="ep",
            track_count=9,
            content_rating="clean",
        ),
        AlbumHit(
            provider="deezer",
            provider_id="928878",
            deezer_id="928878",
            title="VICES & VIRTUES",
            year="2011",
            release_type="album",
            release_kind="album",
            track_count=10,
            content_rating="unknown",
        ),
        AlbumHit(
            provider="deezer",
            provider_id="809473311",
            deezer_id="809473311",
            title="Vices & Virtues",
            year="2011",
            release_type="album",
            release_kind="album",
            track_count=12,
            content_rating="unknown",
        ),
    ]

    compacted = _compact_provider_discography("deezer", hits)

    assert [hit.provider_id for hit in compacted] == [
        "339525867",
        "deluxe",
        "clean",
        "809473311",
    ]
    assert _compact_provider_discography("itunes", hits) == hits
    equal_snapshots = [
        AlbumHit(
            provider="deezer",
            provider_id="equal-a",
            title="Equal Single",
            year="2026",
            release_kind="single",
            track_count=1,
        ),
        AlbumHit(
            provider="deezer",
            provider_id="equal-b",
            title="Equal Single",
            year="2026",
            release_kind="single",
            track_count=1,
        ),
    ]
    assert _compact_provider_discography("deezer", equal_snapshots) == equal_snapshots


async def test_reconcile_deezer_snapshots_preserves_equal_count_releases(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Equal Count Artist")
    identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="equal", name=artist.name
    )
    artist.identities.append(identity)
    identity.releases.extend(
        [
            CatalogAlbumProvider(
                provider_album_id="equal-a",
                title="Equal Single",
                year="2026",
                track_count=1,
                release_kind="single",
                content_rating="unknown",
            ),
            CatalogAlbumProvider(
                provider_album_id="equal-b",
                title="Equal Single",
                year="2026",
                track_count=1,
                release_kind="single",
                content_rating="unknown",
            ),
        ]
    )
    db_session.add(artist)
    await db_session.flush()

    assert await reconcile_deezer_release_snapshots(db_session, artist.id) == 0
    assert len(list((await db_session.scalars(select(CatalogAlbumProvider))).all())) == 2


async def test_reconcile_deezer_snapshots_keeps_larger_provider_and_preserves_canonical_state(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Lil Uzi Vert")
    deezer_identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="13", name=artist.name
    )
    musicbrainz_identity = CatalogArtistIdentity(
        provider="musicbrainz", provider_artist_id="uzi-mbid", name=artist.name
    )
    artist.identities.extend([deezer_identity, musicbrainz_identity])
    smaller = CatalogAlbum(
        artist=artist,
        title="RED & WHITE",
        year="2022",
        release_type="EP",
        mbid="red-white-mbid",
        deezer_id="338552127",
        track_count=5,
        content_rating="explicit",
        monitored=True,
    )
    larger = CatalogAlbum(
        artist=artist,
        title="RED & WHITE",
        year="2022",
        release_type="EP",
        deezer_id="339525867",
        track_count=9,
        content_rating="explicit",
    )
    deluxe = CatalogAlbum(
        artist=artist,
        title="RED & WHITE (Deluxe Edition)",
        year="2022",
        release_type="EP",
        deezer_id="deluxe",
        track_count=12,
        content_rating="explicit",
    )
    for album, count in ((smaller, 5), (larger, 9), (deluxe, 12)):
        album.tracks.extend(
            CatalogAlbumTrack(position=position, disc=1, title=f"Song {position}")
            for position in range(1, count + 1)
        )
    deezer_identity.releases.extend(
        [
            CatalogAlbumProvider(
                catalog_album=smaller,
                provider_album_id="338552127",
                title=smaller.title,
                year="2022",
                track_count=5,
                release_kind="ep",
                content_rating="explicit",
                monitored=True,
                monitor_override=True,
            ),
            CatalogAlbumProvider(
                catalog_album=larger,
                provider_album_id="339525867",
                title=larger.title,
                year="2022",
                track_count=9,
                release_kind="ep",
                content_rating="explicit",
            ),
            CatalogAlbumProvider(
                catalog_album=deluxe,
                provider_album_id="deluxe",
                title=deluxe.title,
                year="2022",
                track_count=12,
                release_kind="ep",
                content_rating="explicit",
            ),
        ]
    )
    musicbrainz_identity.releases.append(
        CatalogAlbumProvider(
            catalog_album=smaller,
            provider_album_id="red-white-mbid",
            title=smaller.title,
            year="2022",
            track_count=5,
            release_kind="ep",
            content_rating="explicit",
        )
    )
    db_session.add(artist)
    await db_session.flush()
    job = Job(
        source="priority",
        query="Lil Uzi Vert RED & WHITE",
        status=JobStatus.pending,
        catalog_album_id=smaller.id,
        catalog_track_id=smaller.tracks[0].id,
    )
    db_session.add(job)
    await db_session.flush()
    db_session.add(
        Track(
            job_id=job.id,
            source="test",
            title="Song 1",
            catalog_album_id=smaller.id,
            catalog_track_id=smaller.tracks[0].id,
        )
    )
    await db_session.flush()

    assert await reconcile_deezer_release_snapshots(db_session, artist.id) == 1
    await db_session.flush()

    releases = list((await db_session.scalars(select(CatalogAlbumProvider))).all())
    assert {release.provider_album_id for release in releases} == {
        "339525867",
        "deluxe",
        "red-white-mbid",
    }
    winner = next(release for release in releases if release.provider_album_id == "339525867")
    assert winner.track_count == 9
    assert winner.monitored is True
    assert winner.monitor_override is True
    albums = list((await db_session.scalars(select(CatalogAlbum))).all())
    assert {album.id for album in albums} == {smaller.id, larger.id, deluxe.id}
    assert len(smaller.tracks) == 5
    assert len(larger.tracks) == 9
    assert (await db_session.scalars(select(Job.catalog_album_id))).one() == smaller.id
    assert (await db_session.scalars(select(Job.catalog_track_id))).one() == smaller.tracks[0].id
    assert (await db_session.scalars(select(Track.catalog_album_id))).one() == smaller.id
    assert (await db_session.scalars(select(Track.catalog_track_id))).one() == smaller.tracks[0].id
    assert await reconcile_deezer_release_snapshots(db_session, artist.id) == 0


async def test_reconcile_deezer_snapshots_handles_unknown_release_kind(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Legacy Deezer Artist")
    identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="legacy", name=artist.name
    )
    artist.identities.append(identity)
    smaller = CatalogAlbum(artist=artist, title="Legacy Release", track_count=5)
    larger = CatalogAlbum(artist=artist, title="Legacy Release", track_count=9)
    identity.releases.extend(
        [
            CatalogAlbumProvider(
                catalog_album=smaller,
                provider_album_id="legacy-small",
                title="Legacy Release",
                track_count=5,
                release_kind="unknown",
                release_type_raw=None,
            ),
            CatalogAlbumProvider(
                catalog_album=larger,
                provider_album_id="legacy-large",
                title="Legacy Release",
                track_count=9,
                release_kind="unknown",
                release_type_raw=None,
            ),
        ]
    )
    db_session.add(artist)
    await db_session.flush()

    assert await reconcile_deezer_release_snapshots(db_session, artist.id) == 1
    releases = list((await db_session.scalars(select(CatalogAlbumProvider))).all())
    assert [release.provider_album_id for release in releases] == ["legacy-large"]


async def test_reconcile_deezer_snapshots_skips_unlinked_richest_release(
    db_session: AsyncSession,
) -> None:
    artist = CatalogArtist(name="Unlinked Deezer Artist")
    identity = CatalogArtistIdentity(
        provider="deezer", provider_artist_id="unlinked", name=artist.name
    )
    artist.identities.append(identity)
    smaller = CatalogAlbum(artist=artist, title="Unlinked Release", track_count=5)
    identity.releases.extend(
        [
            CatalogAlbumProvider(
                catalog_album=smaller,
                provider_album_id="linked-small",
                title="Unlinked Release",
                track_count=5,
                release_kind="album",
            ),
            CatalogAlbumProvider(
                provider_album_id="unlinked-large",
                title="Unlinked Release",
                track_count=9,
                release_kind="album",
            ),
        ]
    )
    db_session.add(artist)
    await db_session.flush()

    assert await reconcile_deezer_release_snapshots(db_session, artist.id) == 0
    assert len(list((await db_session.scalars(select(CatalogAlbumProvider))).all())) == 2


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


async def test_upsert_does_not_reuse_name_only_artist_for_resolved_mbid(db_session) -> None:
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

    assert result.id != existing.id
    assert result.mbid == "juice-mbid"
    assert len(list((await db_session.scalars(select(CatalogArtist))).all())) == 2


async def test_upsert_keeps_same_name_provider_identities_distinct_when_canonical_ids_are_blank(
    db_session,
) -> None:
    first = CatalogArtist(name="Shared Stage Name")
    first.identities.append(
        CatalogArtistIdentity(
            provider="musicbrainz", provider_artist_id="first-mbid", name=first.name
        )
    )
    db_session.add(first)
    await db_session.flush()

    second = await upsert_catalog_artist(
        db_session,
        ArtistDetail(
            provider="musicbrainz",
            provider_id="second-mbid",
            name="Shared Stage Name",
            mbid="second-mbid",
        ),
    )
    await db_session.flush()

    assert second.id != first.id
    rows = list((await db_session.scalars(select(CatalogArtist))).all())
    assert len(rows) == 2
    identities = list((await db_session.scalars(select(CatalogArtistIdentity))).all())
    assert {(identity.artist_id, identity.provider_artist_id) for identity in identities} == {
        (first.id, "first-mbid"),
        (second.id, "second-mbid"),
    }


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
    owner = CatalogArtist(
        name="Juice WRLD",
        mbid="juice-mbid",
        deezer_id="juice-deezer",
        monitor_policy="albums_only",
    )
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


async def test_reconciliation_does_not_merge_distinct_identities_from_same_provider(
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

    assert await reconcile_duplicate_catalog_artists(db_session) == 0
    await db_session.commit()

    artists = list((await db_session.scalars(select(CatalogArtist))).all())
    identities = list((await db_session.scalars(select(CatalogArtistIdentity))).all())
    releases = list((await db_session.scalars(select(CatalogAlbumProvider))).all())
    assert len(artists) == 2
    assert len(identities) == 2
    assert {identity.provider_artist_id for identity in identities} == {
        "owner-deezer",
        "duplicate-deezer",
    }
    assert {release.provider_album_id for release in releases} == {
        "owner-release",
        "duplicate-release",
    }
    assert {release.artist_identity_id for release in releases} == {
        identity.id for identity in identities
    }


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


async def test_deezer_refresh_does_not_persist_smaller_same_release_snapshot(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    test_settings: Settings,
) -> None:
    artist = CatalogArtist(name="Panic! At the Disco", deezer_id="1")
    db_session.add(artist)
    await db_session.flush()

    class UpdatedReleaseProvider:
        async def get_discography(self, id: str) -> list[AlbumHit]:
            assert id == "1"
            return [
                AlbumHit(
                    provider="deezer",
                    provider_id="928878",
                    deezer_id="928878",
                    title="VICES & VIRTUES",
                    year="2011",
                    release_type="album",
                    release_kind="album",
                    track_count=10,
                    content_rating="unknown",
                ),
                AlbumHit(
                    provider="deezer",
                    provider_id="809473311",
                    deezer_id="809473311",
                    title="Vices & Virtues",
                    year="2011",
                    release_type="album",
                    release_kind="album",
                    track_count=12,
                    content_rating="unknown",
                ),
            ]

    monkeypatch.setattr(
        catalog_metadata,
        "build_metadata_provider",
        lambda name, settings: UpdatedReleaseProvider(),
    )

    releases = await fetch_and_store_discography(
        db_session, test_settings, artist, provider_name="deezer"
    )

    assert len(releases) == 1
    assert releases[0].provider_album_id == "809473311"
    assert releases[0].track_count == 12
    assert (await db_session.scalars(select(CatalogAlbumProvider))).one().provider_album_id == (
        "809473311"
    )


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
