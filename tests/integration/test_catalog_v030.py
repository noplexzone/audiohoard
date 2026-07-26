from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy import select

from app.database import get_session_factory
from app.jobs.runner import _catalog_track_for_result
from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack, CatalogArtist
from app.models.job import Job
from app.schemas.search import SearchResult


async def _seed_catalog() -> int:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(name="Daft Punk", mbid="artist-mbid", artwork_url="artist.jpg")
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
    assert "Discovery" in artist_page.text
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
