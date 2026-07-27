from __future__ import annotations

from datetime import UTC, datetime

from httpx import AsyncClient
from sqlalchemy import select

from app.database import get_session_factory
from app.jobs.runner import _catalog_track_for_result
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.job import Job
from app.schemas.search import SearchResult


async def _seed_catalog() -> int:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(
            name="Daft Punk",
            mbid="artist-mbid",
            artwork_url="artist.jpg",
            last_enriched_at=datetime.now(tz=UTC),
        )
        db.add(artist)
        await db.flush()
        album = CatalogAlbum(
            artist_id=artist.id,
            title="Discovery",
            year="2001",
            release_type="Album",
            mbid="album-mbid",
            artwork_url="cover.jpg",
            track_count=2,
        )
        db.add(album)
        await db.flush()
        db.add_all(
            [
                CatalogAlbumTrack(
                    album_id=album.id,
                    position=1,
                    disc=1,
                    title="One More Time",
                    duration_sec=320,
                    recording_mbid="rec-1",
                ),
                CatalogAlbumTrack(album_id=album.id, position=2, disc=1, title="Aerodynamic"),
            ]
        )
        await db.commit()
        return artist.id


async def test_catalog_artist_album_pages_and_album_download_create_linked_job(
    client: AsyncClient,
) -> None:
    artist_id = await _seed_catalog()

    artist_page = await client.get(f"/artists/catalog/{artist_id}")
    assert artist_page.status_code == 200
    assert "Discovery" in artist_page.text, artist_page.text
    assert "Download album" in artist_page.text

    factory = get_session_factory()
    async with factory() as db:
        album = (
            await db.scalars(select(CatalogAlbum).where(CatalogAlbum.title == "Discovery"))
        ).one()
        album_id = album.id

    album_page = await client.get(f"/albums/{album_id}")
    assert album_page.status_code == 200
    assert "One More Time" in album_page.text
    assert "1-01" in album_page.text

    response = await client.post(f"/albums/{album_id}/download", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/downloads"

    async with factory() as db:
        job = (await db.scalars(select(Job).where(Job.catalog_album_id == album_id))).first()
        assert job is not None
        assert job.query == "Daft Punk Discovery"


def test_catalog_track_matching_requires_title_or_explicit_selection() -> None:
    tracks = [
        CatalogAlbumTrack(id=1, album_id=1, position=1, disc=1, title="One More Time"),
        CatalogAlbumTrack(id=2, album_id=1, position=2, disc=1, title="Aerodynamic"),
    ]
    result = SearchResult(source="slskd", title="Aerodynamic")
    assert _catalog_track_for_result(result, tracks, None).id == 2
    assert (
        _catalog_track_for_result(SearchResult(source="slskd", title="Unknown"), tracks, None)
        is None
    )
    assert _catalog_track_for_result(result, tracks, 1).id == 1


async def test_quick_monitor_toggles_catalog_artist_and_albums(client: AsyncClient) -> None:
    artist_id = await _seed_catalog()

    response = await client.post(
        f"/artists/catalog/{artist_id}/monitor",
        data={"quick": "1", "csrf_token": client.cookies.get("csrf", "")},
        follow_redirects=False,
    )
    assert response.status_code == 303

    factory = get_session_factory()
    async with factory() as db:
        artist = (
            await db.scalars(select(CatalogArtist).where(CatalogArtist.id == artist_id))
        ).one()
        album = (
            await db.scalars(select(CatalogAlbum).where(CatalogAlbum.artist_id == artist_id))
        ).one()
        assert artist.monitored is True
        assert artist.monitor_policy == "all"
        assert album.monitored is True

    response = await client.post(
        f"/artists/catalog/{artist_id}/monitor",
        data={"quick": "1", "csrf_token": client.cookies.get("csrf", "")},
        follow_redirects=False,
    )
    assert response.status_code == 303

    async with factory() as db:
        artist = (
            await db.scalars(select(CatalogArtist).where(CatalogArtist.id == artist_id))
        ).one()
        album = (
            await db.scalars(select(CatalogAlbum).where(CatalogAlbum.artist_id == artist_id))
        ).one()
        assert artist.monitored is False
        assert album.monitored is False


async def test_search_card_monitor_opens_artist_as_monitored(
    client: AsyncClient, monkeypatch
) -> None:
    async def fake_open(db, settings, provider_name: str, provider_id: str):
        artist = CatalogArtist(name="Search Artist", mbid=provider_id)
        db.add(artist)
        await db.flush()
        return artist

    import app.routers.catalog as catalog_router

    monkeypatch.setattr(catalog_router, "open_catalog_artist", fake_open)

    response = await client.post(
        "/artists/catalog/open",
        data={
            "provider": "musicbrainz",
            "provider_id": "search-artist-mbid",
            "monitor": "true",
            "csrf_token": client.cookies.get("csrf", ""),
        },
        follow_redirects=False,
    )
    assert response.status_code == 303

    factory = get_session_factory()
    async with factory() as db:
        artist = (
            await db.scalars(select(CatalogArtist).where(CatalogArtist.name == "Search Artist"))
        ).one()
        assert artist.monitored is True
        assert artist.monitor_policy == "all"


async def test_artists_is_single_watchlist_page(client: AsyncClient) -> None:
    artist_id = await _seed_catalog()
    await client.post(
        f"/artists/catalog/{artist_id}/monitor",
        data={"quick": "1", "csrf_token": client.cookies.get("csrf", "")},
    )

    artists = await client.get("/artists")
    monitored = await client.get("/artists/monitored", follow_redirects=False)
    wanted = await client.get("/wanted", follow_redirects=False)

    assert artists.status_code == 200
    assert "Daft Punk" in artists.text
    assert "Watchlisted" in artists.text
    assert "Monitored (" not in artists.text
    assert "Wanted" not in artists.text
    for legacy in (monitored, wanted):
        assert legacy.status_code == 303
        assert legacy.headers["location"] == "/artists"


async def test_artist_page_switches_provider_and_album_filter_excludes_singles(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(
            name="Provider Artist",
            mbid="provider-mb",
            deezer_id="provider-dz",
            watchlist_provider="deezer",
            last_enriched_at=datetime.now(tz=UTC),
        )
        artist.albums.extend(
            [
                CatalogAlbum(
                    title="MB Album", release_type="Album", providers_json='["musicbrainz"]'
                ),
                CatalogAlbum(title="DZ Album", release_type="album", providers_json='["deezer"]'),
                CatalogAlbum(
                    title="DZ Single", release_type="Single / EP", providers_json='["deezer"]'
                ),
            ]
        )
        db.add(artist)
        await db.commit()
        artist_id = artist.id

    deezer = await client.get(f"/artists/catalog/{artist_id}?provider=deezer")
    assert "DZ Album" in deezer.text
    assert "DZ Single" in deezer.text
    assert "MB Album" not in deezer.text
    assert 'name="watchlist_provider"' in deezer.text
    assert "MusicBrainz discography" in deezer.text

    albums = await client.get(
        f"/artists/catalog/{artist_id}?provider=deezer&release_type=Album&sort=asc"
    )
    assert "DZ Album" in albums.text
    assert "DZ Single" not in albums.text
    assert "provider=deezer" in albums.text

    invalid = await client.get(f"/artists/catalog/{artist_id}?provider=javascript:bad")
    assert "MB Album" in invalid.text
    assert "DZ Album" not in invalid.text


async def test_watchlist_bulk_only_changes_selected_provider_discography(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(
            name="Bulk Artist",
            monitored=True,
            mbid="bulk-mb",
            deezer_id="bulk-dz",
            watchlist_provider="musicbrainz",
            last_enriched_at=datetime.now(tz=UTC),
        )
        selected = CatalogAlbum(
            title="Selected Album", release_type="Album", providers_json='["deezer"]'
        )
        shared = CatalogAlbum(
            title="Shared Album",
            release_type="Album",
            providers_json='["deezer", "musicbrainz"]',
        )
        other = CatalogAlbum(
            title="Other Album",
            release_type="Album",
            providers_json='["musicbrainz"]',
            monitored=True,
        )
        artist.albums.extend([selected, shared, other])
        db.add(artist)
        await db.commit()
        artist_id = artist.id
        ids = selected.id, shared.id, other.id

    response = await client.post(
        f"/artists/catalog/{artist_id}/monitor",
        data={
            "csrf_token": client.cookies.get("csrf", ""),
            "monitored": "true",
            "watchlist_provider": "deezer",
            "bulk": "all",
            "provider": "deezer",
            "release_type": "Album",
            "sort": "asc",
        },
        follow_redirects=False,
    )

    assert response.headers["location"] == (
        f"/artists/catalog/{artist_id}?provider=deezer&release_type=Album&sort=asc"
    )
    async with factory() as db:
        refreshed = [await db.get(CatalogAlbum, album_id) for album_id in ids]
        artist = await db.get(CatalogArtist, artist_id)
    assert artist is not None and artist.watchlist_provider == "deezer"
    assert [album.monitored for album in refreshed if album is not None] == [True, True, False]


async def test_switching_watchlist_provider_preserves_target_provider_choices(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(
            name="Switch Artist",
            monitored=True,
            watchlist_provider="musicbrainz",
            last_enriched_at=datetime.now(tz=UTC),
        )
        db.add(artist)
        await db.flush()
        musicbrainz = CatalogArtistIdentity(
            artist_id=artist.id,
            provider="musicbrainz",
            provider_artist_id="switch-mb",
            name="Switch Artist",
        )
        deezer = CatalogArtistIdentity(
            artist_id=artist.id,
            provider="deezer",
            provider_artist_id="switch-dz",
            name="Switch Artist",
        )
        db.add_all([musicbrainz, deezer])
        await db.flush()
        mb_release = CatalogAlbumProvider(
            artist_identity_id=musicbrainz.id,
            provider_album_id="mb-release",
            title="MB Release",
            release_kind="album",
            monitored=True,
        )
        dz_release = CatalogAlbumProvider(
            artist_identity_id=deezer.id,
            provider_album_id="dz-release",
            title="DZ Release",
            release_kind="album",
            monitored=True,
        )
        db.add_all([mb_release, dz_release])
        await db.commit()
        artist_id = artist.id
        mb_release_id = mb_release.id
        dz_release_id = dz_release.id

    response = await client.post(
        f"/artists/catalog/{artist_id}/monitor",
        data={
            "csrf_token": client.cookies.get("csrf", ""),
            "monitored": "true",
            "watchlist_provider": "deezer",
            "provider": "musicbrainz",
            "album_monitored": str(mb_release_id),
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    async with factory() as db:
        refreshed_artist = await db.get(CatalogArtist, artist_id)
        refreshed_mb = await db.get(CatalogAlbumProvider, mb_release_id)
        refreshed_dz = await db.get(CatalogAlbumProvider, dz_release_id)
    assert refreshed_artist is not None
    assert refreshed_artist.watchlist_provider == "deezer"
    assert refreshed_mb is not None and refreshed_mb.monitored is True
    assert refreshed_dz is not None and refreshed_dz.monitored is True
