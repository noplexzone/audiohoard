from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
from httpx import AsyncClient
from pytest_httpx import HTTPXMock
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_session_factory
from app.jobs.runner import _catalog_track_for_result
from app.metadata.base import ArtistDetail, ArtistHit
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.job import Job
from app.models.monitoring import MonitoringRecord, MonitoringStatus
from app.schemas.search import SearchResult
from app.services.catalog_metadata import ProviderOutcome
from app.sources.base import CapabilityState


@pytest.fixture(autouse=True)
def _disable_catalog_enrichment_background_tasks(monkeypatch):
    """Keep catalog integration tests deterministic and off the live provider network."""
    from app.routers import catalog as catalog_router

    async def do_not_queue(*args, **kwargs):
        del args, kwargs
        return False

    monkeypatch.setattr(catalog_router, "_queue_artist_enrichment", do_not_queue)


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
        jobs = list((await db.scalars(select(Job).where(Job.catalog_album_id == album_id))).all())
        assert [job.query for job in jobs] == ["Daft Punk One More Time", "Daft Punk Aerodynamic"]
        assert all(job.catalog_track_id is not None for job in jobs)


async def test_catalog_artist_page_does_not_queue_refresh_on_get(
    client: AsyncClient, monkeypatch
) -> None:
    from app.routers import catalog as catalog_router

    artist_id = await _seed_catalog()

    async def unexpected_queue(*args, **kwargs):
        del args, kwargs
        raise AssertionError("catalog artist GET must not write or queue refresh work")

    monkeypatch.setattr(catalog_router, "_queue_artist_enrichment", unexpected_queue)

    response = await client.get(f"/artists/catalog/{artist_id}")

    assert response.status_code == 200
    assert "Daft Punk" in response.text
    assert "OperationalError" not in response.text


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


async def test_quick_monitor_uses_artist_primary_source_for_watchlist(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(
            name="Primary Source Artist",
            mbid="primary-mb",
            deezer_id="primary-dz",
            primary_metadata_provider="deezer",
            last_enriched_at=datetime.now(tz=UTC),
        )
        db.add(artist)
        await db.commit()
        artist_id = artist.id

    response = await client.post(
        f"/artists/catalog/{artist_id}/monitor",
        data={"quick": "1", "csrf_token": client.cookies.get("csrf", "")},
        follow_redirects=False,
    )

    assert response.status_code == 303
    async with factory() as db:
        refreshed = await db.get(CatalogArtist, artist_id)
    assert refreshed is not None
    assert refreshed.monitored is True
    assert refreshed.watchlist_provider == "deezer"


async def test_search_card_monitor_opens_artist_as_monitored(
    client: AsyncClient, monkeypatch
) -> None:
    async def fake_fetch(settings, provider_name: str, provider_id: str):
        del settings
        return ArtistDetail(
            provider=provider_name,
            provider_id=provider_id,
            name="Search Artist",
            mbid=provider_id,
        )

    import app.routers.catalog as catalog_router

    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", fake_fetch)

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


async def test_fetch_watchlisting_is_idempotent_and_returns_saved_defaults(
    client: AsyncClient, monkeypatch
) -> None:
    async def fake_fetch(settings, provider_name: str, provider_id: str):
        del settings
        return ArtistDetail(
            provider=provider_name,
            provider_id=provider_id,
            name="Fetch Artist",
            deezer_id=provider_id,
        )

    import app.routers.catalog as catalog_router

    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", fake_fetch)
    payload = {
        "provider": "deezer",
        "provider_id": "fetch-dz",
        "monitor": "true",
        "csrf_token": client.cookies.get("csrf", ""),
    }
    headers = {"Accept": "application/json", "X-Requested-With": "fetch"}

    first = await client.post("/artists/catalog/open", data=payload, headers=headers)
    second = await client.post("/artists/catalog/open", data=payload, headers=headers)

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert first.json() == {
        "artist_id": first.json()["artist_id"],
        "watched": True,
        "watchlist_release_albums": True,
        "watchlist_release_singles": False,
        "watchlist_release_eps": False,
        "watchlist_monitor_upgrades": False,
        "configure_url": f"/artists/catalog/{first.json()['artist_id']}/monitor",
        "discography_url": f"/artists/catalog/{first.json()['artist_id']}",
    }
    factory = get_session_factory()
    async with factory() as db:
        assert await db.scalar(select(func.count(CatalogArtist.id))) == 1
        assert await db.scalar(select(func.count(CatalogArtistIdentity.id))) == 1


async def test_duplicate_fetch_watchlisting_preserves_customized_policy(
    client: AsyncClient, monkeypatch
) -> None:
    async def fake_fetch(settings, provider_name: str, provider_id: str):
        del settings
        return ArtistDetail(
            provider=provider_name,
            provider_id=provider_id,
            name="Customized Artist",
            deezer_id=provider_id,
        )

    async def no_queue(db, artist_id: int) -> bool:
        del db, artist_id
        return False

    import app.routers.catalog as catalog_router

    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", fake_fetch)
    monkeypatch.setattr(catalog_router, "_queue_artist_enrichment", no_queue)
    headers = {"Accept": "application/json", "X-Requested-With": "fetch"}
    form = {
        "provider": "deezer",
        "provider_id": "custom-dz",
        "monitor": "true",
        "csrf_token": client.cookies.get("csrf", ""),
    }
    opened = await client.post("/artists/catalog/open", data=form, headers=headers)
    artist_id = opened.json()["artist_id"]
    configured = await client.post(
        f"/artists/catalog/{artist_id}/monitor",
        data={
            "monitored": "true",
            "provider": "deezer",
            "watchlist_release_singles": "true",
            "watchlist_release_eps": "true",
            "watchlist_monitor_upgrades": "true",
            "monitor_policy": "none_new",
            "csrf_token": client.cookies.get("csrf", ""),
        },
        headers=headers,
    )
    repeated = await client.post("/artists/catalog/open", data=form, headers=headers)

    assert configured.status_code == repeated.status_code == 200
    assert repeated.json()["watchlist_release_albums"] is False
    assert repeated.json()["watchlist_release_singles"] is True
    assert repeated.json()["watchlist_release_eps"] is True
    assert repeated.json()["watchlist_monitor_upgrades"] is True
    factory = get_session_factory()
    async with factory() as db:
        artist = await db.get(CatalogArtist, artist_id)
    assert artist is not None and artist.monitor_policy == "none_new"


async def test_direct_open_closes_database_transaction_before_provider_http(
    client: AsyncClient, monkeypatch
) -> None:
    import app.routers.catalog as catalog_router

    rolled_back_sessions: list[AsyncSession] = []
    original_rollback = AsyncSession.rollback

    async def tracking_rollback(session: AsyncSession) -> None:
        await original_rollback(session)
        rolled_back_sessions.append(session)

    async def asserting_fetch(settings, provider_name: str, provider_id: str):
        del settings
        assert rolled_back_sessions
        assert not rolled_back_sessions[-1].in_transaction()
        return ArtistDetail(
            provider=provider_name,
            provider_id=provider_id,
            name="Transaction Artist",
            deezer_id=provider_id,
        )

    async def no_queue(db, artist_id: int) -> bool:
        del db, artist_id
        return False

    monkeypatch.setattr(AsyncSession, "rollback", tracking_rollback)
    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", asserting_fetch)
    monkeypatch.setattr(catalog_router, "_queue_artist_enrichment", no_queue)

    response = await client.post(
        "/artists/catalog/open",
        data={
            "provider": "deezer",
            "provider_id": "transaction-dz",
            "monitor": "true",
            "csrf_token": client.cookies.get("csrf", ""),
        },
        headers={"Accept": "application/json", "X-Requested-With": "fetch"},
    )

    assert response.status_code == 200


async def test_search_cards_mark_only_matching_provider_identity_watched(
    client: AsyncClient, monkeypatch
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(name="Same Name", monitored=True, deezer_id="watched-dz")
        artist.identities.append(
            CatalogArtistIdentity(
                provider="deezer", provider_artist_id="watched-dz", name="Same Name"
            )
        )
        db.add(artist)
        await db.commit()

    async def fake_search(settings, query: str, providers: list[str]):
        del settings, query, providers
        return [
            ProviderOutcome(
                "deezer",
                [ArtistHit("deezer", "watched-dz", "Same Name", deezer_id="watched-dz")],
                CapabilityState(True),
            ),
            ProviderOutcome(
                "musicbrainz",
                [ArtistHit("musicbrainz", "other-mb", "Same Name", mbid="other-mb")],
                CapabilityState(True),
            ),
        ]

    import app.routers.search as search_router

    monkeypatch.setattr(search_router, "search_catalog_artists", fake_search)
    response = await client.get("/search?q=Same+Name&provider=all")

    assert response.status_code == 200
    assert (
        'data-provider="deezer" data-provider-id="watched-dz" data-watched="true"' in response.text
    )
    assert (
        'data-provider="musicbrainz" data-provider-id="other-mb" data-watched="false"'
        in response.text
    )


async def test_fetch_dialog_updates_only_selected_artist(client: AsyncClient) -> None:
    factory = get_session_factory()
    async with factory() as db:
        selected = CatalogArtist(name="Selected", monitored=True, deezer_id="selected-dz")
        selected.identities.append(
            CatalogArtistIdentity(
                provider="deezer", provider_artist_id="selected-dz", name="Selected"
            )
        )
        other = CatalogArtist(
            name="Other", monitored=True, deezer_id="other-dz", watchlist_release_albums=True
        )
        other.identities.append(
            CatalogArtistIdentity(provider="deezer", provider_artist_id="other-dz", name="Other")
        )
        db.add_all([selected, other])
        await db.commit()
        selected_id, other_id = selected.id, other.id

    response = await client.post(
        f"/artists/catalog/{selected_id}/monitor",
        data={
            "monitored": "true",
            "provider": "deezer",
            "watchlist_release_singles": "true",
            "watchlist_release_eps": "true",
            "watchlist_monitor_upgrades": "true",
            "csrf_token": client.cookies.get("csrf", ""),
        },
        headers={"Accept": "application/json", "X-Requested-With": "fetch"},
    )

    assert response.status_code == 200
    assert response.json()["artist_id"] == selected_id
    assert response.json()["watched"] is True
    async with factory() as db:
        refreshed_selected = await db.get(CatalogArtist, selected_id)
        refreshed_other = await db.get(CatalogArtist, other_id)
    assert refreshed_selected is not None
    assert refreshed_selected.watchlist_release_albums is False
    assert refreshed_selected.watchlist_release_singles is True
    assert refreshed_selected.watchlist_release_eps is True
    assert refreshed_selected.watchlist_monitor_upgrades is True
    assert refreshed_other is not None and refreshed_other.watchlist_release_albums is True


async def test_invalid_direct_artist_open_returns_safe_errors_without_persistence(
    client: AsyncClient, monkeypatch
) -> None:
    async def invalid_fetch(settings, provider_name: str, provider_id: str):
        del settings, provider_name, provider_id
        raise ValueError("Provider returned an invalid artist identity")

    import app.routers.catalog as catalog_router

    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", invalid_fetch)

    html = await client.get(
        "/artists/catalog/open?provider=deezer&provider_id=10002824",
        follow_redirects=False,
    )
    json_response = await client.post(
        "/artists/catalog/open",
        data={
            "provider": "deezer",
            "provider_id": "10002824",
            "monitor": "true",
            "csrf_token": client.cookies.get("csrf", ""),
        },
        headers={"Accept": "application/json", "X-Requested-With": "fetch"},
        follow_redirects=False,
    )

    assert html.status_code == 422
    assert "invalid artist identity" in html.text.casefold()
    assert json_response.status_code == 422
    assert json_response.json() == {
        "error": "invalid_artist_identity",
        "message": "The selected artist is no longer available from this provider.",
    }
    factory = get_session_factory()
    async with factory() as db:
        assert await db.scalar(select(func.count(CatalogArtist.id))) == 0


async def test_direct_artist_open_maps_provider_transport_failure_to_safe_502(
    client: AsyncClient, monkeypatch
) -> None:
    async def failed_fetch(settings, provider_name: str, provider_id: str):
        del settings, provider_name, provider_id
        raise httpx.ReadTimeout("private upstream timeout detail")

    import app.routers.catalog as catalog_router

    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", failed_fetch)

    html = await client.get("/artists/catalog/open?provider=deezer&provider_id=stale")
    json_response = await client.post(
        "/artists/catalog/open",
        data={
            "provider": "deezer",
            "provider_id": "stale",
            "csrf_token": client.cookies.get("csrf", ""),
        },
        headers={"Accept": "application/json", "X-Requested-With": "fetch"},
    )

    assert html.status_code == 502
    assert "private upstream timeout detail" not in html.text
    assert json_response.status_code == 502
    assert json_response.json() == {
        "error": "metadata_provider_unavailable",
        "message": "The metadata provider could not be reached. Please try again.",
    }


async def test_deezer_error_envelope_is_filtered_and_direct_open_never_persists(
    client: AsyncClient, httpx_mock: HTTPXMock
) -> None:
    stale_id = "10002824"
    error_envelope = {"error": {"type": "DataException", "message": "no data", "code": 800}}
    httpx_mock.add_response(
        url="https://api.deezer.com/search/artist?q=stale+artist&limit=10",
        json={
            "data": [
                {
                    "id": int(stale_id),
                    "name": "Stale Artist",
                    "nb_fan": 9000,
                    "nb_album": 20,
                }
            ],
            "total": 1,
        },
    )
    httpx_mock.add_response(
        url=f"https://api.deezer.com/artist/{stale_id}/top?limit=5",
        json={"data": []},
    )
    httpx_mock.add_response(url=f"https://api.deezer.com/artist/{stale_id}", json=error_envelope)
    httpx_mock.add_response(url=f"https://api.deezer.com/artist/{stale_id}", json=error_envelope)

    search = await client.get("/search?q=stale+artist&provider=deezer")
    direct = await client.post(
        "/artists/catalog/open",
        data={
            "provider": "deezer",
            "provider_id": stale_id,
            "monitor": "true",
            "csrf_token": client.cookies.get("csrf", ""),
        },
        headers={"Accept": "application/json", "X-Requested-With": "fetch"},
    )

    assert search.status_code == 200
    assert f'data-provider-id="{stale_id}"' not in search.text
    assert direct.status_code == 422
    assert direct.json()["error"] == "invalid_artist_identity"
    factory = get_session_factory()
    async with factory() as db:
        assert await db.scalar(select(func.count(CatalogArtist.id))) == 0


async def test_search_card_monitor_retries_transient_sqlite_artist_open_lock(
    client: AsyncClient, monkeypatch
) -> None:
    async def fake_fetch(settings, provider_name: str, provider_id: str):
        del settings
        return ArtistDetail(
            provider=provider_name,
            provider_id=provider_id,
            name="Locked Search Artist",
            mbid=provider_id,
        )

    import app.routers.catalog as catalog_router

    original_upsert = catalog_router.upsert_catalog_artist
    attempts = 0

    async def flaky_upsert(db, detail):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError(
                "INSERT INTO catalog_artists", {}, Exception("database is locked")
            )
        return await original_upsert(db, detail)

    monkeypatch.setattr(catalog_router, "fetch_catalog_artist_detail", fake_fetch)
    monkeypatch.setattr(catalog_router, "upsert_catalog_artist", flaky_upsert)

    response = await client.post(
        "/artists/catalog/open",
        data={
            "provider": "musicbrainz",
            "provider_id": "locked-search-artist-mbid",
            "monitor": "true",
            "csrf_token": client.cookies.get("csrf", ""),
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert attempts == 2
    factory = get_session_factory()
    async with factory() as db:
        artist = (
            await db.scalars(
                select(CatalogArtist).where(CatalogArtist.name == "Locked Search Artist")
            )
        ).one()
        assert artist.monitored is True


async def test_artists_is_single_watchlist_page(client: AsyncClient) -> None:
    artist_id = await _seed_catalog()
    await client.post(
        f"/artists/catalog/{artist_id}/monitor",
        data={"quick": "1", "csrf_token": client.cookies.get("csrf", "")},
    )

    artists = await client.get("/artists", follow_redirects=False)
    library = await client.get("/library")
    monitored = await client.get("/artists/monitored", follow_redirects=False)
    wanted = await client.get("/wanted", follow_redirects=False)

    assert artists.status_code == 307
    assert artists.headers["location"] == "/library"
    assert library.status_code == 200
    assert "Daft Punk" in library.text
    assert "Watchlisted" in library.text
    assert "Monitored (" not in library.text
    assert monitored.status_code == 303
    assert monitored.headers["location"] == "/library"
    assert wanted.status_code == 200
    assert "Wanted" in wanted.text


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
                    title="MB Album",
                    release_type="Album",
                    providers_json='["musicbrainz"]',
                    track_count=1,
                ),
                CatalogAlbum(
                    title="DZ Album",
                    release_type="album",
                    providers_json='["deezer"]',
                    track_count=1,
                ),
                CatalogAlbum(
                    title="DZ Single",
                    release_type="Single / EP",
                    providers_json='["deezer"]',
                    track_count=1,
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
    assert 'name="watchlist_provider"' not in deezer.text
    assert "Configure watchlist" not in deezer.text
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


async def test_watchlist_defaults_monitor_all_release_types_and_upgrade_records(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        from app.models.catalog_entities import CatalogAlbumTrack
        from app.models.job import Job, JobStatus
        from app.models.release import Release
        from app.models.track import Track
        from app.models.workflow import AcquisitionState, ImportWorkflowState
        from app.settings_service import save_runtime_settings

        await save_runtime_settings(
            db,
            [{"name": "slskd", "enabled": True}],
            10,
            metadata_providers=[{"name": "deezer", "enabled": True}],
            primary_metadata_provider="deezer",
            default_watchlist_release_albums=True,
            default_watchlist_release_singles=True,
            default_watchlist_release_eps=True,
            default_watchlist_monitor_upgrades=True,
        )
        artist = CatalogArtist(
            name="Default Artist",
            monitored=False,
            watchlist_provider="deezer",
            last_enriched_at=datetime.now(tz=UTC),
        )
        db.add(artist)
        await db.flush()
        identity = CatalogArtistIdentity(
            artist_id=artist.id,
            provider="deezer",
            provider_artist_id="default-dz",
            name="Default Artist",
        )
        db.add(identity)
        await db.flush()
        releases = [
            ("Default Album", "album"),
            ("Default Single", "single"),
            ("Default EP", "ep"),
        ]
        for title, kind in releases:
            album = CatalogAlbum(artist=artist, title=title, release_type=kind, in_library=True)
            provider_release = CatalogAlbumProvider(
                artist_identity_id=identity.id,
                catalog_album=album,
                provider_album_id=f"{kind}-id",
                title=title,
                release_kind=kind,
            )
            catalog_track = CatalogAlbumTrack(
                album=album, position=1, disc=1, title=f"{title} Song"
            )
            job = Job(source="slskd", query=title, status=JobStatus.done, catalog_album=album)
            imported = Release(job=job, source="slskd", title=title, album_artist="Default Artist")
            Track(
                job=job,
                release=imported,
                catalog_album=album,
                catalog_track=catalog_track,
                source="slskd",
                title=f"{title} Song",
                file_format="mp3",
                acquisition_state=AcquisitionState.downloaded,
                import_state=ImportWorkflowState.imported,
            )
            db.add(provider_release)
        await db.commit()
        artist_id = artist.id

    response = await client.post(
        f"/artists/catalog/{artist_id}/monitor",
        data={"quick": "1", "provider": "deezer", "csrf_token": client.cookies.get("csrf", "")},
        follow_redirects=False,
    )

    assert response.status_code == 303
    async with factory() as db:
        refreshed = (
            await db.execute(
                select(CatalogArtist)
                .where(CatalogArtist.id == artist_id)
                .options(
                    selectinload(CatalogArtist.identities).selectinload(
                        CatalogArtistIdentity.releases
                    )
                )
            )
        ).scalar_one()
        provider_releases = refreshed.identities[0].releases
        upgrade_count = await db.scalar(select(func.count(MonitoringRecord.id)))

    assert refreshed.watchlist_release_albums is True
    assert refreshed.watchlist_release_singles is True
    assert refreshed.watchlist_release_eps is True
    assert refreshed.watchlist_monitor_upgrades is True
    assert {release.release_kind: release.monitored for release in provider_releases} == {
        "album": True,
        "single": True,
        "ep": True,
    }
    assert upgrade_count == 3


async def test_watchlist_upgrade_toggle_off_deactivates_monitoring_record(
    client: AsyncClient,
) -> None:
    from app.models.catalog_entities import CatalogAlbumTrack
    from app.models.job import Job, JobStatus
    from app.models.release import Release
    from app.models.track import Track
    from app.models.workflow import AcquisitionState, ImportWorkflowState

    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(
            name="Toggle Artist",
            monitored=True,
            watchlist_provider="deezer",
            watchlist_monitor_upgrades=True,
            last_enriched_at=datetime.now(tz=UTC),
        )
        album = CatalogAlbum(
            artist=artist, title="Toggle Album", release_type="Album", in_library=True
        )
        catalog_track = CatalogAlbumTrack(album=album, position=1, disc=1, title="Song")
        job = Job(source="slskd", query="Toggle Album", status=JobStatus.done, catalog_album=album)
        release = Release(
            job=job, source="slskd", title="Toggle Album", album_artist="Toggle Artist"
        )
        Track(
            job=job,
            release=release,
            catalog_album=album,
            catalog_track=catalog_track,
            source="slskd",
            title="Song",
            file_format="mp3",
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
        )
        record = MonitoringRecord(release=release, status=MonitoringStatus.active)
        db.add(record)
        await db.commit()
        album_id = album.id
        record_id = record.id

    response = await client.post(
        f"/albums/{album_id}/watch-upgrade",
        data={"monitor_for_upgrades": "false", "csrf_token": client.cookies.get("csrf", "")},
        follow_redirects=False,
    )

    assert response.status_code == 303
    async with factory() as db:
        refreshed = await db.get(MonitoringRecord, record_id)
    assert refreshed is not None
    assert refreshed.status == MonitoringStatus.paused


async def test_artist_page_renders_loading_state_for_queued_enrichment(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(
            name="Hydrating Artist",
            watchlist_provider="deezer",
            enrichment_state="queued",
        )
        identity = CatalogArtistIdentity(
            artist=artist,
            provider="deezer",
            provider_artist_id="hydrating-dz",
            name="Hydrating Artist",
        )
        db.add(identity)
        await db.commit()
        artist_id = artist.id

    response = await client.get(f"/artists/catalog/{artist_id}?provider=deezer")

    assert response.status_code == 200
    assert "Loading discography" in response.text
    assert 'data-artist-refresh="true"' in response.text


async def test_album_page_links_to_catalog_artist(client: AsyncClient) -> None:
    artist_id = await _seed_catalog()
    factory = get_session_factory()
    async with factory() as db:
        album = (
            await db.scalars(select(CatalogAlbum).where(CatalogAlbum.artist_id == artist_id))
        ).one()

    response = await client.get(f"/albums/{album.id}")

    assert response.status_code == 200
    assert f'href="/artists/catalog/{artist_id}"' in response.text


async def test_album_page_uses_one_responsive_track_list_for_all_release_sizes(
    client: AsyncClient,
) -> None:
    artist_id = await _seed_catalog()
    factory = get_session_factory()
    async with factory() as db:
        short_album = (
            await db.scalars(select(CatalogAlbum).where(CatalogAlbum.artist_id == artist_id))
        ).one()
        long_album = CatalogAlbum(
            artist_id=artist_id,
            title="Long Album",
            release_type="Album",
            track_count=4,
        )
        db.add(long_album)
        await db.flush()
        db.add_all(
            [
                CatalogAlbumTrack(
                    album_id=long_album.id,
                    position=position,
                    disc=1,
                    title=f"Long Track {position}",
                )
                for position in range(1, 5)
            ]
        )
        await db.commit()
        short_album_id = short_album.id
        long_album_id = long_album.id

    short_response = await client.get(f"/albums/{short_album_id}")
    long_response = await client.get(f"/albums/{long_album_id}")

    assert 'class="release-track-list"' in short_response.text
    assert 'class="release-track-list"' in long_response.text
    assert "album-track-table" not in short_response.text
    assert "album-track-table" not in long_response.text


async def test_artist_page_renders_one_discography_filter_bar(client: AsyncClient) -> None:
    artist_id = await _seed_catalog()

    response = await client.get(f"/artists/catalog/{artist_id}")

    assert response.status_code == 200
    assert response.text.count('class="discography-filters"') == 1


async def test_artist_page_keeps_section_heading_with_release_type_filter(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as db:
        artist = CatalogArtist(
            name="Filtered Artist",
            monitored=True,
            watchlist_provider="musicbrainz",
            last_enriched_at=datetime.now(tz=UTC),
        )
        db.add(artist)
        await db.flush()
        identity = CatalogArtistIdentity(
            artist_id=artist.id,
            provider="musicbrainz",
            provider_artist_id="filtered-artist",
            name="Filtered Artist",
        )
        db.add(identity)
        await db.flush()
        db.add(
            CatalogAlbumProvider(
                artist_identity_id=identity.id,
                provider_album_id="filtered-album",
                title="Filtered Album",
                release_kind="album",
                release_type_raw="Album",
                track_count=1,
            )
        )
        await db.commit()
        artist_id = artist.id

    response = await client.get(
        f"/artists/catalog/{artist_id}?provider=musicbrainz&release_type=Album"
    )

    assert response.status_code == 200
    assert "<h2>Album</h2>" in response.text
