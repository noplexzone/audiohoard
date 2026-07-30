from __future__ import annotations

from datetime import UTC, datetime

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

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
from app.models.monitoring import MonitoringRecord, MonitoringStatus
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
        jobs = list((await db.scalars(select(Job).where(Job.catalog_album_id == album_id))).all())
        assert [job.query for job in jobs] == ["Daft Punk One More Time", "Daft Punk Aerodynamic"]
        assert all(job.catalog_track_id is not None for job in jobs)


async def test_catalog_artist_page_skips_refresh_queue_when_sqlite_is_locked(
    client: AsyncClient, monkeypatch
) -> None:
    from sqlalchemy.exc import OperationalError

    from app.routers import catalog as catalog_router

    artist_id = await _seed_catalog()

    async def locked_queue(*args, **kwargs):
        del args, kwargs
        raise OperationalError("UPDATE catalog_artists", {}, Exception("database is locked"))

    monkeypatch.setattr(catalog_router, "_queue_artist_enrichment", locked_queue)

    response = await client.get(f"/artists/catalog/{artist_id}")

    assert response.status_code == 200
    assert "Discovery" in response.text
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


async def test_album_page_uses_list_for_three_tracks_or_fewer_and_table_for_more(
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

    assert 'class="album-track-list"' in short_response.text
    assert 'class="table-wrap album-track-table"' not in short_response.text
    assert 'class="table-wrap album-track-table"' in long_response.text


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
