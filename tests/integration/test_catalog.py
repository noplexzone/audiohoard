from __future__ import annotations

from datetime import UTC, datetime

import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

import app.database as db_module
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogAlbumTrack,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.import_plan import ImportPlan, LibraryFileState
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import FingerprintState, IdentityResolutionState, Track
from app.models.workflow import AcquisitionState, ImportWorkflowState


def _make_track(
    job_id: int,
    *,
    title: str = "Track",
    artist: str | None = "Artist",
    album_artist: str | None = None,
    album: str | None = "Album",
    year: str | None = "2020",
    source: str = "slskd",
    source_path: str | None = "/music/track.flac",
    duration_sec: int | None = 200,
    file_format: str | None = None,
    file_size_bytes: int | None = None,
    release_id: int | None = None,
) -> Track:
    track = Track(
        job_id=job_id,
        title=title,
        artist=artist,
        album_artist=album_artist,
        album=album,
        year=year,
        source=source,
        source_path=f"/staging/{title}.flac",
        acquisition_state=AcquisitionState.downloaded,
        import_state=ImportWorkflowState.imported,
        fingerprint_state=FingerprintState.pending,
        identity_state=IdentityResolutionState.pending,
        duration_sec=duration_sec,
        file_format=file_format,
        file_size_bytes=file_size_bytes,
        release_id=release_id,
    )
    track.import_plans.append(
        ImportPlan(
            release_id=release_id or 1,
            source_path=f"/staging/{title}.flac",
            destination_path=source_path or f"/music/{title}.flac",
            status=ImportWorkflowState.imported,
            file_state=LibraryFileState.present,
        )
    )
    return track


@pytest_asyncio.fixture
async def seeded_client(client: AsyncClient) -> AsyncClient:
    """Client fixture with realistic Track rows pre-seeded."""
    factory = db_module.get_session_factory()
    async with factory() as session:
        job = Job(source="slskd", query="test seed", status=JobStatus.done, result_json=None)
        session.add(job)
        await session.flush()
        release_a = Release(
            job_id=job.id,
            source="slskd",
            title="Great Album",
            album_artist="Album Artist A",
            year="2020",
            release_mbid="11111111-1111-1111-1111-111111111111",
            country="US",
            label="Example Records",
            catalog_number="EX-001",
        )
        release_b = Release(
            job_id=job.id,
            source="prowlarr",
            title="Solo Work",
            album_artist="Artist B",
            year="2021",
        )
        session.add_all([release_a, release_b])
        await session.flush()

        tracks = [
            _make_track(
                job.id,
                title="Song A",
                artist="Artist A",
                album_artist="Album Artist A",
                album="Great Album",
                year="2020",
                source="slskd",
                source_path="/music/artist_a/great_album/01_song_a.flac",
                duration_sec=300,
                file_format="flac",
                file_size_bytes=12_000_000,
                release_id=release_a.id,
            ),
            _make_track(
                job.id,
                title="Song B",
                artist="Artist A",
                album_artist="Album Artist A",
                album="Great Album",
                year="2020",
                source="youtube",
                source_path="/music/artist_a/great_album/02_song_b.mp3",
                duration_sec=240,
                file_format="mp3",
                file_size_bytes=8_000_000,
                release_id=release_a.id,
            ),
            _make_track(
                job.id,
                title="Song C",
                artist="Artist B",
                album_artist=None,
                album="Solo Work",
                year="2021",
                source="prowlarr",
                source_path="/music/artist_b/solo_work/01_song_c.flac",
                duration_sec=180,
                file_format="flac",
                file_size_bytes=9_000_000,
                release_id=release_b.id,
            ),
        ]
        session.add_all(tracks)
        catalog_artist_a = CatalogArtist(
            name="Album Artist A",
            monitored=True,
            artwork_url="https://images.example/artist-a.jpg",
        )
        catalog_album_a = CatalogAlbum(title="Great Album", release_type="Album")
        catalog_artist_a.albums.append(catalog_album_a)
        tracks[0].catalog_album = catalog_album_a
        catalog_artist_b = CatalogArtist(name="Artist B", monitored=True)
        catalog_artist_b.albums.append(CatalogAlbum(title="Solo Work", release_type="Album"))
        session.add_all([catalog_artist_a, catalog_artist_b])
        await session.commit()

    return client


# ── Auth guard ────────────────────────────────────────────────────────────────


async def test_library_requires_auth(unauthenticated_client: AsyncClient) -> None:
    resp = await unauthenticated_client.get("/library", follow_redirects=False)
    assert resp.status_code in (401, 302, 307)


async def test_artists_requires_auth(unauthenticated_client: AsyncClient) -> None:
    resp = await unauthenticated_client.get("/artists", follow_redirects=False)
    assert resp.status_code in (401, 302, 307)


async def test_artist_detail_requires_auth(unauthenticated_client: AsyncClient) -> None:
    resp = await unauthenticated_client.get("/artists/detail?name=Test", follow_redirects=False)
    assert resp.status_code in (401, 302, 307)


# ── Empty DB states ───────────────────────────────────────────────────────────


async def test_library_empty_db_returns_200(client: AsyncClient) -> None:
    resp = await client.get("/library")
    assert resp.status_code == 200
    assert "Library" in resp.text


async def test_library_empty_db_shows_zero_stats(client: AsyncClient) -> None:
    resp = await client.get("/library")
    assert resp.status_code == 200
    body = resp.text
    assert "0" in body


async def test_artists_empty_db_redirects_to_library(client: AsyncClient) -> None:
    resp = await client.get("/artists", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/library"


async def test_library_lists_only_eligible_catalog_artists(client: AsyncClient) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        watched = CatalogArtist(
            name="Watchlisted Artist",
            monitored=True,
            artwork_url="https://images.example/watchlisted.jpg",
        )
        session.add_all([watched, CatalogArtist(name="Hidden Artist", monitored=False)])
        await session.commit()
        watched_id = watched.id
    resp = await client.get("/library")
    assert resp.status_code == 200
    assert "Watchlisted Artist" in resp.text
    assert "Hidden Artist" not in resp.text
    assert f'href="/artists/catalog/{watched_id}"' in resp.text
    assert 'src="/artwork?url=https%3A//images.example/watchlisted.jpg"' in resp.text
    assert "primary-source releases complete" in resp.text
    assert "partial" not in resp.text.casefold()
    assert "ownership unknown" not in resp.text.casefold()


async def test_legacy_artist_routes_redirect_to_library(client: AsyncClient) -> None:
    for path in ("/artists/monitored",):
        resp = await client.get(path, follow_redirects=False)
        assert resp.status_code in (302, 303, 307, 308)
        assert resp.headers["location"] == "/library"


async def _seed_wanted_view_releases() -> dict[str, int]:
    factory = db_module.get_session_factory()
    async with factory() as session:
        job = Job(source="slskd", query="wanted view", status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Complete Wanted Album")
        watched = CatalogArtist(name="Wanted View Artist", monitored=True)
        complete = CatalogAlbum(
            title="Complete Wanted Album", year="2025", track_count=3, monitored=True
        )
        partial = CatalogAlbum(
            title="Partial Wanted Album", year="2026", track_count=2, monitored=True
        )
        second_partial = CatalogAlbum(
            title="Second Partial Wanted Album", year="2027", track_count=2, monitored=True
        )
        hidden_artist = CatalogArtist(name="Hidden Wanted Artist", monitored=False)
        hidden = CatalogAlbum(title="Nonwatchlisted Missing Album", year="2024", track_count=2)
        excluded = CatalogAlbum(
            title="Explicitly Unwatched Album", year="2023", track_count=2, monitored=False
        )
        watched.albums.extend([complete, partial, second_partial, excluded])
        hidden_artist.albums.append(hidden)
        complete_tracks = [
            CatalogAlbumTrack(
                album=complete, position=position, disc=1, title=f"Complete {position}"
            )
            for position in range(1, 3)
        ]
        partial_tracks = [
            CatalogAlbumTrack(
                album=partial, position=position, disc=1, title=f"Partial {position}"
            )
            for position in range(1, 3)
        ]
        second_partial_tracks = [
            CatalogAlbumTrack(
                album=second_partial, position=position, disc=1, title=f"Second Partial {position}"
            )
            for position in range(1, 3)
        ]
        session.add_all([job, release, watched, hidden_artist])
        await session.flush()
        for catalog_track in complete_tracks:
            imported = _make_track(
                job.id,
                title=catalog_track.title,
                artist=watched.name,
                album=complete.title,
                file_size_bytes=1024,
                release_id=release.id,
            )
            imported.catalog_album_id = complete.id
            imported.catalog_track_id = catalog_track.id
            session.add(imported)
        for album, catalog_track in (
            (partial, partial_tracks[0]),
            (second_partial, second_partial_tracks[0]),
        ):
            imported = _make_track(
                job.id,
                title=catalog_track.title,
                artist=watched.name,
                album=album.title,
                file_size_bytes=1024,
                release_id=release.id,
            )
            imported.catalog_album_id = album.id
            imported.catalog_track_id = catalog_track.id
            session.add(imported)
        await session.commit()
        return {
            "complete": complete.id,
            "partial": partial.id,
            "second_partial": second_partial.id,
            "partial_missing": partial_tracks[1].id,
            "second_partial_missing": second_partial_tracks[1].id,
        }


async def test_wanted_page_shows_partial_release_and_hides_fully_owned_release(
    client: AsyncClient,
) -> None:
    await _seed_wanted_view_releases()

    response = await client.get("/wanted")

    assert response.status_code == 200
    assert "Partial Wanted Album" in response.text
    assert "Second Partial Wanted Album" in response.text
    assert "Complete Wanted Album" not in response.text
    assert "Nonwatchlisted Missing Album" not in response.text
    assert "Explicitly Unwatched Album" not in response.text


async def test_wanted_queue_two_ids_queues_only_missing_tracks(
    client: AsyncClient, monkeypatch
) -> None:
    ids = await _seed_wanted_view_releases()
    dispatched: list[int] = []

    async def fake_dispatch(job_id: int) -> None:
        dispatched.append(job_id)

    monkeypatch.setattr("app.routers.catalog.job_dispatcher.dispatch", fake_dispatch)

    response = await client.post(
        "/wanted/queue",
        data={"catalog_album_ids": [ids["partial"], ids["second_partial"]]},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/downloads"
    factory = db_module.get_session_factory()
    async with factory() as session:
        jobs = list(
            (
                await session.execute(
                    select(Job).where(Job.id.in_(dispatched)).order_by(Job.catalog_album_id)
                )
            )
            .scalars()
            .all()
        )
    assert len(jobs) == 2
    assert {job.catalog_album_id for job in jobs} == {ids["partial"], ids["second_partial"]}
    assert {job.catalog_track_id for job in jobs} == {
        ids["partial_missing"],
        ids["second_partial_missing"],
    }
    assert all(job.catalog_track_id is not None for job in jobs)
    assert all(job.query != "Wanted View Artist Partial Wanted Album" for job in jobs)
    assert all(job.query != "Wanted View Artist Second Partial Wanted Album" for job in jobs)


async def test_wanted_queue_retry_does_not_expire_later_album_rows(
    client: AsyncClient, monkeypatch
) -> None:
    ids = await _seed_wanted_view_releases()
    dispatched: list[int] = []

    async def fake_dispatch(job_id: int) -> None:
        dispatched.append(job_id)

    original_flush = AsyncSession.flush
    injected_lock = False

    async def lock_first_job_flush(self, *args, **kwargs):
        nonlocal injected_lock
        if not injected_lock and any(isinstance(obj, Job) for obj in self.new):
            injected_lock = True
            raise OperationalError("INSERT INTO jobs", {}, Exception("database is locked"))
        return await original_flush(self, *args, **kwargs)

    monkeypatch.setattr("app.routers.catalog.job_dispatcher.dispatch", fake_dispatch)
    monkeypatch.setattr(AsyncSession, "flush", lock_first_job_flush)

    response = await client.post(
        "/wanted/queue",
        data={"catalog_album_ids": [ids["partial"], ids["second_partial"]]},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert injected_lock is True
    factory = db_module.get_session_factory()
    async with factory() as session:
        jobs = list(
            (
                await session.scalars(
                    select(Job).where(Job.id.in_(dispatched)).order_by(Job.catalog_album_id)
                )
            ).all()
        )
    assert {job.catalog_album_id for job in jobs} == {ids["partial"], ids["second_partial"]}


async def test_wanted_queue_all_enqueues_every_listed_release(
    client: AsyncClient, monkeypatch
) -> None:
    ids = await _seed_wanted_view_releases()
    page = await client.get("/wanted")
    assert page.status_code == 200
    assert f'name="catalog_album_ids" value="{ids["partial"]}"' in page.text
    assert f'name="catalog_album_ids" value="{ids["second_partial"]}"' in page.text
    assert f'name="catalog_album_ids" value="{ids["complete"]}"' not in page.text
    dispatched: list[int] = []

    async def fake_dispatch(job_id: int) -> None:
        dispatched.append(job_id)

    monkeypatch.setattr("app.routers.catalog.job_dispatcher.dispatch", fake_dispatch)

    response = await client.post(
        "/wanted/queue",
        data={"catalog_album_ids": [ids["partial"], ids["second_partial"]]},
        follow_redirects=False,
    )

    assert response.status_code == 303
    factory = db_module.get_session_factory()
    async with factory() as session:
        jobs = list((await session.execute(select(Job).where(Job.id.in_(dispatched)))).scalars())
    assert {job.catalog_album_id for job in jobs} == {ids["partial"], ids["second_partial"]}
    assert {job.catalog_track_id for job in jobs} == {
        ids["partial_missing"],
        ids["second_partial_missing"],
    }


async def test_settings_icon_is_a_conventional_gear(client: AsyncClient) -> None:
    resp = await client.get("/library")
    assert '<symbol id="i-settings"' in resp.text
    assert "M19.14,12.94" in resp.text


async def test_artist_detail_unknown_returns_200_empty(client: AsyncClient) -> None:
    resp = await client.get("/artists/detail?name=Nobody")
    assert resp.status_code == 200
    assert "Nobody" in resp.text


async def test_artist_detail_missing_name_returns_400(client: AsyncClient) -> None:
    resp = await client.get("/artists/detail")
    assert resp.status_code == 400


# ── Aggregate correctness ─────────────────────────────────────────────────────


async def test_library_stats_aggregate(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert resp.status_code == 200
    body = resp.text
    assert "3" in body  # track count
    assert "2" in body  # artist count


async def test_library_shows_all_eligible_artists(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert "Album Artist A" in resp.text
    assert "Artist B" in resp.text


async def test_library_shows_track_artist(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert "Album Artist A" in resp.text


async def test_library_shows_release_progress_counts(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert "primary-source releases complete" in resp.text
    assert "ownership unknown" not in resp.text.casefold()


async def test_library_uses_watchlisted_catalog_artists(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert resp.status_code == 200
    assert "Album Artist A" in resp.text
    assert "Artist B" in resp.text


async def test_artist_detail_renders_release_metadata(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/artists/detail?name=Album+Artist+A")
    assert resp.status_code == 200
    assert "Example Records" in resp.text
    assert "EX-001" in resp.text
    assert "11111111-1111-1111-1111-111111111111" in resp.text


async def test_artist_detail_fallback_finds_tracks(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/artists/detail?name=Artist+B")
    assert resp.status_code == 200
    body = resp.text
    assert "Artist B" in body
    assert "Song C" in body


# ── Filtering ─────────────────────────────────────────────────────────────────


async def test_library_text_filter(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library?q=Artist+B")
    assert resp.status_code == 200
    assert "Artist B" in resp.text
    assert "Album Artist A" not in resp.text


async def test_library_artist_search_filter(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library?q=Artist+B")
    assert resp.status_code == 200
    assert "Artist B" in resp.text
    assert "Album Artist A" not in resp.text


async def test_library_deterministic_sort_title(seeded_client: AsyncClient) -> None:
    r1 = await seeded_client.get("/library?sort=title")
    r2 = await seeded_client.get("/library?sort=title")
    assert r1.text == r2.text


async def test_library_deterministic_sort_name(seeded_client: AsyncClient) -> None:
    r1 = await seeded_client.get("/library?sort=name")
    r2 = await seeded_client.get("/library?sort=name")
    assert r1.text == r2.text


async def test_library_pagination_first_page(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library?per_page=1&sort=name")
    assert resp.status_code == 200
    assert "Page 1 of 2" in resp.text
    assert "Next" in resp.text


async def test_library_pagination_second_page(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library?per_page=1&page=2&sort=name")
    assert resp.status_code == 200
    assert "Page 2 of 2" in resp.text
    assert "Prev" in resp.text


async def test_library_pagination_beyond_bounds_shows_empty(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library?per_page=50&page=999")
    assert resp.status_code == 200


async def test_artists_pagination_query_is_preserved(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/artists?per_page=1&sort=name", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/library?per_page=1&sort=name"


async def test_library_page_too_large_returns_422(client: AsyncClient) -> None:
    resp = await client.get("/library?page=99999")
    assert resp.status_code == 422


async def test_artists_page_too_large_redirects_unchanged(client: AsyncClient) -> None:
    resp = await client.get("/artists?page=99999", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/library?page=99999"


async def test_artist_detail_page_too_large_returns_422(client: AsyncClient) -> None:
    resp = await client.get("/artists/detail?name=X&page=99999")
    assert resp.status_code == 422


# ── HTML content and structure ────────────────────────────────────────────────


async def test_library_html_has_artist_cards(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert "artist-card" in resp.text


async def test_library_tracks_view_renders_track_table(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library?view=tracks")

    assert resp.status_code == 200
    assert 'aria-label="Track list"' in resp.text
    assert "Song A" in resp.text


async def test_library_default_view_still_renders_artist_grid(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")

    assert resp.status_code == 200
    assert 'class="artist-grid"' in resp.text
    assert 'aria-label="Track list"' not in resp.text


async def test_library_html_has_filter_form(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert 'action="/library"' in resp.text
    assert 'name="q"' in resp.text


async def test_library_html_has_count_sort_filters(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert '<option value="downloaded"' in resp.text
    assert '<option value="wanted"' in resp.text


async def test_artists_redirects_instead_of_rendering_duplicate_cards(
    seeded_client: AsyncClient,
) -> None:
    resp = await seeded_client.get("/artists", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/library"


async def test_artist_detail_shows_album_section(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/artists/detail?name=Album+Artist+A")
    assert resp.status_code == 200
    body = resp.text
    assert "Great Album" in body
    assert "Song A" in body
    assert "Song B" in body


async def test_nav_includes_only_combined_library(client: AsyncClient) -> None:
    resp = await client.get("/library")
    assert resp.text.count('href="/library"') == 3
    assert "<span>Artists</span>" not in resp.text


async def test_library_does_not_leak_secret_key(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert "test-secret" not in resp.text


async def test_library_does_not_expose_db_url(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert "sqlite+aiosqlite" not in resp.text


async def test_artists_does_not_leak_secret_key(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/artists")
    assert "test-secret" not in resp.text


async def test_artists_redirects_to_library_preserving_query(client: AsyncClient) -> None:
    response = await client.get("/artists?q=AC%2FDC&sort=wanted&page=2", follow_redirects=False)
    assert response.status_code in (302, 303, 307, 308)
    assert response.headers["location"] == "/library?q=AC%2FDC&sort=wanted&page=2"


async def test_library_nav_has_no_separate_artists_item(client: AsyncClient) -> None:
    response = await client.get("/library")
    assert response.status_code == 200
    assert response.text.count('href="/library"') == 3
    assert "<span>Artists</span>" not in response.text


async def _seed_release_progress_artist() -> tuple[int, int, dict[str, int]]:
    factory = db_module.get_session_factory()
    async with factory() as session:
        job = Job(source="slskd", query="partial artist", status=JobStatus.done)
        release = Release(job=job, source="slskd", title="Partial Album")
        artist = CatalogArtist(
            name="Partial Artist",
            monitored=True,
            watchlist_provider="musicbrainz",
            last_enriched_at=datetime.now(tz=UTC),
        )
        partial = CatalogAlbum(title="Partial Album", release_type="Album", track_count=3)
        complete = CatalogAlbum(title="Complete Single", release_type="Single", track_count=1)
        known_unhydrated = CatalogAlbum(
            title="Known Unhydrated Album", release_type="Album", track_count=4
        )
        empty = CatalogAlbum(title="Unknown Empty Album", release_type="Album")
        artist.albums.extend([partial, complete, known_unhydrated, empty])
        identity = CatalogArtistIdentity(
            provider="musicbrainz",
            provider_artist_id="partial-artist",
            name=artist.name,
        )
        identity.releases.extend(
            [
                CatalogAlbumProvider(
                    provider_album_id="partial",
                    title="Partial Album",
                    release_kind="album",
                    catalog_album=partial,
                ),
                CatalogAlbumProvider(
                    provider_album_id="complete",
                    title="Complete Single",
                    release_kind="single",
                    catalog_album=complete,
                ),
                CatalogAlbumProvider(
                    provider_album_id="known-unhydrated",
                    title="Known Unhydrated Album",
                    track_count=4,
                    release_kind="album",
                    catalog_album=known_unhydrated,
                ),
                CatalogAlbumProvider(
                    provider_album_id="empty",
                    title="Unknown Empty Album",
                    release_kind="album",
                    catalog_album=empty,
                ),
            ]
        )
        artist.identities.append(identity)
        session.add_all([job, release, artist])
        await session.flush()
        catalog_tracks = {
            "partial imported": CatalogAlbumTrack(
                album_id=partial.id, position=1, disc=1, title="Imported File"
            ),
            "partial missing": CatalogAlbumTrack(
                album_id=partial.id, position=2, disc=1, title="Missing File"
            ),
            "partial other": CatalogAlbumTrack(
                album_id=partial.id, position=3, disc=1, title="Other Missing File"
            ),
            "complete": CatalogAlbumTrack(
                album_id=complete.id, position=1, disc=1, title="Complete File"
            ),
        }
        session.add_all(catalog_tracks.values())
        await session.flush()
        imported = _make_track(
            job.id,
            title="Imported File",
            artist=artist.name,
            album="Partial Album",
            source_path="/music/Partial Artist/Partial Album/01 Imported File.flac",
            file_size_bytes=1234,
            release_id=release.id,
        )
        imported.catalog_album_id = partial.id
        imported.catalog_track_id = catalog_tracks["partial imported"].id
        staging = _make_track(
            job.id,
            title="Staging File",
            artist=artist.name,
            album="Partial Album",
            source_path="/music/Partial Artist/Partial Album/02 Staging File.flac",
            file_size_bytes=1234,
            release_id=release.id,
        )
        staging.catalog_album_id = partial.id
        staging.catalog_track_id = catalog_tracks["partial missing"].id
        staging.import_state = ImportWorkflowState.discovered
        staging.import_plans[0].status = ImportWorkflowState.discovered
        failed = _make_track(
            job.id,
            title="Failed File",
            artist=artist.name,
            album="Partial Album",
            file_size_bytes=1234,
            release_id=release.id,
        )
        failed.catalog_album_id = partial.id
        failed.catalog_track_id = catalog_tracks["partial missing"].id
        failed.import_state = ImportWorkflowState.failed
        failed.import_plans[0].status = ImportWorkflowState.failed
        empty_destination = _make_track(
            job.id,
            title="Empty Destination",
            artist=artist.name,
            album="Partial Album",
            file_size_bytes=1234,
            release_id=release.id,
        )
        empty_destination.catalog_album_id = partial.id
        empty_destination.catalog_track_id = catalog_tracks["partial missing"].id
        empty_destination.import_plans[0].destination_path = "  "
        zero_byte = _make_track(
            job.id,
            title="Zero Byte",
            artist=artist.name,
            album="Partial Album",
            file_size_bytes=0,
            release_id=release.id,
        )
        zero_byte.catalog_album_id = partial.id
        zero_byte.catalog_track_id = catalog_tracks["partial missing"].id
        complete_file = _make_track(
            job.id,
            title="Complete File",
            artist=artist.name,
            album="Complete Single",
            file_size_bytes=4321,
            release_id=release.id,
        )
        complete_file.catalog_album_id = complete.id
        complete_file.catalog_track_id = catalog_tracks["complete"].id
        session.add_all([imported, staging, failed, empty_destination, zero_byte, complete_file])
        await session.commit()
        return (
            artist.id,
            partial.id,
            {name: track.id for name, track in catalog_tracks.items()},
        )


async def test_catalog_artist_unifies_release_progress_on_existing_cards(
    client: AsyncClient, monkeypatch
) -> None:
    artist_id, partial_id, _ = await _seed_release_progress_artist()
    refreshes = 0

    async def backfill_missing_counts(db, settings, artist, provider_name):
        nonlocal refreshes
        refreshes += 1
        identity = next(item for item in artist.identities if item.provider == provider_name)
        for release in identity.releases:
            if release.title == "Unknown Empty Album":
                release.track_count = 2
        await db.flush()
        return identity.releases

    monkeypatch.setattr("app.routers.catalog.fetch_and_store_discography", backfill_missing_counts)
    response = await client.get(f"/artists/catalog/{artist_id}")

    assert response.status_code == 200
    assert refreshes == 0
    assert 'data-section="downloaded-files"' not in response.text
    assert 'data-section="wanted-releases"' not in response.text
    assert "Downloaded files</h2>" not in response.text
    assert "Wanted releases</h2>" not in response.text
    assert "Albums" in response.text
    assert "Singles &amp; EPs" in response.text
    assert "1 of 3 tracks in library" in response.text
    assert "1 of 1 tracks in library" in response.text
    assert "Unknown Empty Album" in response.text
    assert "0 / 2 downloaded" not in response.text
    assert "0 / 0 downloaded" not in response.text
    assert f'href="/albums/{partial_id}"' in response.text


async def test_catalog_artist_enrich_queues_without_running_inline(
    client: AsyncClient, monkeypatch
) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name="Queued Artist", last_enriched_at=datetime.now(tz=UTC))
        session.add(artist)
        await session.commit()
        artist_id = artist.id

    calls = 0

    async def unexpected_enrichment(*args, **kwargs):
        nonlocal calls
        calls += 1

    queued_tasks = []

    def capture_task(self, func, *args, **kwargs):
        queued_tasks.append((func, args, kwargs))

    monkeypatch.setattr("app.routers.catalog.enrich_catalog_artist", unexpected_enrichment)
    monkeypatch.setattr("app.routers.catalog.BackgroundTasks.add_task", capture_task)
    page = await client.get(f"/artists/catalog/{artist_id}")
    csrf_token = page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]

    response = await client.post(
        f"/artists/catalog/{artist_id}/enrich",
        data={"csrf_token": csrf_token},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/library"
    assert calls == 0
    assert len(queued_tasks) == 1
    assert queued_tasks[0][0].__name__ == "_enrich_artist_task"
    assert queued_tasks[0][1][0] == artist_id


async def test_release_progress_does_not_treat_untracked_library_folder_as_owned(
    test_settings, db_session
) -> None:
    from app.services.catalog import get_release_progress

    artist = CatalogArtist(name="Juice WRLD")
    album = CatalogAlbum(title="Goodbye & Good Riddance", year="2018", track_count=15)
    artist.albums.append(album)
    album.tracks.extend(
        [
            CatalogAlbumTrack(position=position, title=f"Track {position}")
            for position in range(1, 16)
        ]
    )
    db_session.add(artist)
    await db_session.flush()
    release_folder = test_settings.library_root / "Juice WRLD" / "Goodbye & Good Riddance (2018)"
    release_folder.mkdir(parents=True)
    for position in range(1, 7):
        (release_folder / f"{position:02d} - Track {position}.flac").write_bytes(b"audio")

    progress = (
        await get_release_progress(db_session, [album.id], library_root=test_settings.library_root)
    )[album.id]

    assert progress.wanted_track_count == 15
    assert progress.downloaded_track_count == 0
    assert progress.downloaded_catalog_track_ids == frozenset()


async def test_release_progress_ignores_untracked_unknown_year_folder(
    test_settings, db_session
) -> None:
    from app.services.catalog import get_release_progress

    artist = CatalogArtist(name="Unknown Artist")
    album = CatalogAlbum(title="Unknown Album", year=None, track_count=1)
    album.tracks.append(CatalogAlbumTrack(disc=1, position=1, title="Track 1"))
    artist.albums.append(album)
    db_session.add(artist)
    await db_session.flush()
    release_folder = test_settings.library_root / "Unknown Artist" / "Unknown Album (0000)"
    release_folder.mkdir(parents=True)
    (release_folder / "001 - Track 1.flac").write_bytes(b"audio")

    progress = (
        await get_release_progress(db_session, [album.id], library_root=test_settings.library_root)
    )[album.id]

    assert progress.downloaded_track_count == 0
    assert progress.downloaded_catalog_track_ids == frozenset()


async def test_release_progress_ignores_untracked_multi_disc_files(
    test_settings, db_session
) -> None:
    from app.services.catalog import get_release_progress

    artist = CatalogArtist(name="Various Artist")
    album = CatalogAlbum(title="Double Album", year="2020", track_count=2)
    album.tracks.extend(
        [
            CatalogAlbumTrack(disc=1, position=1, title="Disc One"),
            CatalogAlbumTrack(disc=2, position=1, title="Disc Two"),
        ]
    )
    artist.albums.append(album)
    db_session.add(artist)
    await db_session.flush()
    release_folder = test_settings.library_root / "Various Artist" / "Double Album (2020)"
    release_folder.mkdir(parents=True)
    (release_folder / "1-01 - Disc One.flac").write_bytes(b"audio")
    (release_folder / "2-01 - Disc Two.flac").write_bytes(b"audio")

    progress = (
        await get_release_progress(db_session, [album.id], library_root=test_settings.library_root)
    )[album.id]

    assert progress.downloaded_track_count == 0
    assert progress.downloaded_catalog_track_ids == frozenset()


async def test_release_progress_rejects_intermediate_library_symlink(
    test_settings, db_session, tmp_path
) -> None:
    from app.services.catalog import get_release_progress

    artist = CatalogArtist(name="Juice WRLD")
    album = CatalogAlbum(title="WRLD ON DRUGS", year="2018", track_count=1)
    album.tracks.append(CatalogAlbumTrack(position=1, title="Track 1"))
    artist.albums.append(album)
    db_session.add(artist)
    await db_session.flush()

    outside = tmp_path / "outside"
    release_folder = outside / "WRLD ON DRUGS (2018)"
    release_folder.mkdir(parents=True)
    (release_folder / "01 - Track 1.flac").write_bytes(b"audio")
    test_settings.library_root.mkdir(parents=True, exist_ok=True)
    artist_link = test_settings.library_root / "Juice WRLD"
    artist_link.symlink_to(outside, target_is_directory=True)

    progress = (
        await get_release_progress(db_session, [album.id], library_root=test_settings.library_root)
    )[album.id]

    assert progress.downloaded_track_count == 0
    assert progress.downloaded_catalog_track_ids == frozenset()


async def test_catalog_album_shows_total_and_per_track_downloaded_wanted_states(
    client: AsyncClient, monkeypatch
) -> None:
    _, partial_id, catalog_track_ids = await _seed_release_progress_artist()

    provider_fetches = 0

    async def unexpected_provider_fetch(db, settings, album):
        nonlocal provider_fetches
        provider_fetches += 1
        raise AssertionError("a complete persisted manifest must not refetch provider metadata")

    monkeypatch.setattr("app.routers.catalog.fetch_and_store_album", unexpected_provider_fetch)
    response = await client.get(f"/albums/{partial_id}")

    assert response.status_code == 200
    assert provider_fetches == 0
    assert "1 of 3 tracks in library" in response.text
    imported_id = catalog_track_ids["partial imported"]
    missing_id = catalog_track_ids["partial missing"]
    imported_row = response.text.split(f'data-track-id="{imported_id}"', 1)[1].split("</li>", 1)[0]
    missing_row = response.text.split(f'data-track-id="{missing_id}"', 1)[1].split("</li>", 1)[0]
    assert 'aria-label="In library"' in imported_row
    assert "Wanted" not in imported_row
    assert f'action="/albums/{partial_id}/tracks/{imported_id}/download"' not in imported_row
    assert 'aria-label="Not in library"' in missing_row
    assert "Wanted" not in missing_row
    assert f'action="/albums/{partial_id}/tracks/{missing_id}/download"' in missing_row


async def test_album_download_does_not_trust_untracked_legacy_files(
    client: AsyncClient, test_settings, monkeypatch
) -> None:
    import app.routers.catalog as catalog_router

    dispatched: list[int] = []

    async def fake_dispatch(job_id: int):
        dispatched.append(job_id)

    monkeypatch.setattr(catalog_router.job_dispatcher, "dispatch", fake_dispatch)
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name="Juice WRLD")
        album = CatalogAlbum(title="Legacy Owned", year="2024", track_count=3)
        artist.albums.append(album)
        album.tracks.extend(
            [
                CatalogAlbumTrack(position=1, disc=1, title="Already Owned"),
                CatalogAlbumTrack(position=2, disc=1, title="Actually Missing"),
                CatalogAlbumTrack(position=3, disc=1, title="Also Missing"),
            ]
        )
        session.add(artist)
        await session.commit()
        album_id = album.id
    release_folder = test_settings.library_root / "Juice WRLD" / "Legacy Owned (2024)"
    release_folder.mkdir(parents=True)
    (release_folder / "01 - Already Owned.flac").write_bytes(b"audio")

    response = await client.post(f"/albums/{album_id}/download", follow_redirects=False)

    assert response.status_code == 303
    async with factory() as session:
        jobs = list(
            (
                await session.scalars(
                    select(Job).where(
                        Job.catalog_album_id == album_id,
                        Job.status == JobStatus.pending,
                    )
                )
            ).all()
        )
    assert [job.query for job in jobs] == [
        "Juice WRLD Already Owned",
        "Juice WRLD Actually Missing",
        "Juice WRLD Also Missing",
    ]
    assert all(job.catalog_track_id is not None for job in jobs)
    assert len(dispatched) == 3


async def test_album_download_queues_only_missing_catalog_tracks(
    client: AsyncClient, test_settings, monkeypatch
) -> None:
    import app.routers.catalog as catalog_router

    dispatched: list[int] = []

    async def fake_dispatch(job_id: int):
        dispatched.append(job_id)

    monkeypatch.setattr(catalog_router.job_dispatcher, "dispatch", fake_dispatch)
    factory = db_module.get_session_factory()
    release_folder = test_settings.library_root / "Juice WRLD" / "The Party Never Ends 2.0"
    release_folder.mkdir(parents=True)
    existing_path = release_folder / "01 - Already Owned.flac"
    existing_path.write_bytes(b"audio")
    async with factory() as session:
        artist = CatalogArtist(name="Juice WRLD")
        album = CatalogAlbum(title="The Party Never Ends 2.0", track_count=3)
        artist.albums.append(album)
        album.tracks.extend(
            [
                CatalogAlbumTrack(position=1, disc=1, title="Already Owned"),
                CatalogAlbumTrack(position=2, disc=1, title="Actually Missing"),
                CatalogAlbumTrack(position=3, disc=1, title="Also Missing"),
            ]
        )
        done_job = Job(source="slskd", query="old", status=JobStatus.done)
        release = Release(
            job=done_job, source="slskd", title=album.title, album_artist=artist.name
        )
        session.add_all([artist, done_job, release])
        await session.flush()
        owned = Track(
            job=done_job,
            release=release,
            source="slskd",
            title="Already Owned",
            catalog_album_id=album.id,
            catalog_track_id=album.tracks[0].id,
            acquisition_state=AcquisitionState.downloaded,
            import_state=ImportWorkflowState.imported,
            file_size_bytes=existing_path.stat().st_size,
        )
        owned.import_plans.append(
            ImportPlan(
                release=release,
                source_path=str(existing_path),
                destination_path=str(existing_path),
                status=ImportWorkflowState.imported,
                file_state=LibraryFileState.present,
            )
        )
        session.add(owned)
        await session.commit()
        album_id = album.id

    response = await client.post(f"/albums/{album_id}/download", follow_redirects=False)

    assert response.status_code == 303
    async with factory() as session:
        jobs = list(
            (
                await session.scalars(
                    select(Job).where(
                        Job.catalog_album_id == album_id,
                        Job.status == JobStatus.pending,
                    )
                )
            ).all()
        )
    assert [job.query for job in jobs] == [
        "Juice WRLD Actually Missing",
        "Juice WRLD Also Missing",
    ]
    assert all(job.catalog_track_id is not None for job in jobs)
    assert len(dispatched) == 2


async def test_album_download_fetch_returns_json_and_creates_job(
    client: AsyncClient, monkeypatch
) -> None:
    import app.routers.catalog as catalog_router

    dispatched: list[int] = []

    async def fake_dispatch(job_id: int):
        dispatched.append(job_id)

    monkeypatch.setattr(catalog_router.job_dispatcher, "dispatch", fake_dispatch)
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name="Fetch Artist")
        album = CatalogAlbum(title="Fetch Album", track_count=2)
        artist.albums.append(album)
        album.tracks.extend(
            [
                CatalogAlbumTrack(position=1, disc=1, title="One"),
                CatalogAlbumTrack(position=2, disc=1, title="Two"),
            ]
        )
        session.add(artist)
        await session.commit()
        album_id = album.id

    response = await client.post(
        f"/albums/{album_id}/download",
        headers={"X-Requested-With": "fetch"},
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert response.json() == {"queued": 2, "album_id": album_id}
    async with factory() as session:
        jobs = list(
            (
                await session.scalars(
                    select(Job).where(
                        Job.catalog_album_id == album_id,
                        Job.status == JobStatus.pending,
                    )
                )
            ).all()
        )
    assert [job.query for job in jobs] == ["Fetch Artist One", "Fetch Artist Two"]
    assert all(job.catalog_track_id is not None for job in jobs)
    assert dispatched == [job.id for job in jobs]


async def test_album_download_retries_transient_sqlite_writer_lock(
    client: AsyncClient, monkeypatch
) -> None:
    import app.routers.catalog as catalog_router

    async def fake_dispatch(job_id: int):
        return None

    monkeypatch.setattr(catalog_router.job_dispatcher, "dispatch", fake_dispatch)
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name="Lock Retry Artist")
        album = CatalogAlbum(title="Lock Retry Album", track_count=1)
        artist.albums.append(album)
        album.tracks.append(CatalogAlbumTrack(position=1, disc=1, title="Only"))
        session.add(artist)
        await session.commit()
        album_id = album.id

    original_flush = AsyncSession.flush
    attempts = 0

    async def lock_then_flush(self, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError("INSERT INTO jobs", {}, Exception("database is locked"))
        return await original_flush(self, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "flush", lock_then_flush)

    response = await client.post(f"/albums/{album_id}/download", follow_redirects=False)

    assert response.status_code == 303
    assert attempts >= 2
    async with factory() as session:
        jobs = list(
            (await session.scalars(select(Job).where(Job.catalog_album_id == album_id))).all()
        )
    assert len(jobs) == 1


async def test_library_artist_cards_use_primary_source_counts_and_copy(
    client: AsyncClient,
) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(
            name="Primary Count Artist",
            monitored=True,
            primary_metadata_provider="deezer",
            watchlist_provider="musicbrainz",
        )
        deezer_identity = CatalogArtistIdentity(
            provider="deezer", provider_artist_id="primary-deezer", name=artist.name
        )
        mb_identity = CatalogArtistIdentity(
            provider="musicbrainz", provider_artist_id="primary-mb", name=artist.name
        )
        deezer_album = CatalogAlbum(title="Primary Album", track_count=1, release_type="Album")
        secondary_album = CatalogAlbum(
            title="Secondary Album", track_count=1, release_type="Album"
        )
        deezer_album.tracks.append(CatalogAlbumTrack(position=1, disc=1, title="Primary Track"))
        secondary_album.tracks.append(
            CatalogAlbumTrack(position=1, disc=1, title="Secondary Track")
        )
        deezer_identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id="primary-album",
                title=deezer_album.title,
                release_kind="album",
                catalog_album=deezer_album,
            )
        )
        mb_identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id="secondary-album",
                title=secondary_album.title,
                release_kind="album",
                catalog_album=secondary_album,
            )
        )
        artist.albums.extend([deezer_album, secondary_album])
        artist.identities.extend([deezer_identity, mb_identity])
        session.add(artist)
        await session.commit()

    response = await client.get("/library")

    assert response.status_code == 200
    assert "0</strong> of 1 primary-source releases complete" in response.text
    assert "Deezer" in response.text
    assert "partial" not in response.text.casefold()
    assert "ownership unknown" not in response.text.casefold()


async def test_catalog_artist_primary_source_selection_changes_display_provider(
    client: AsyncClient,
) -> None:
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name="Switch Source Artist", monitored=True)
        deezer_identity = CatalogArtistIdentity(
            provider="deezer", provider_artist_id="switch-deezer", name=artist.name
        )
        mb_identity = CatalogArtistIdentity(
            provider="musicbrainz", provider_artist_id="switch-mb", name=artist.name
        )
        deezer_album = CatalogAlbum(title="Deezer Album", track_count=1, release_type="Album")
        mb_album = CatalogAlbum(title="MusicBrainz Album", track_count=1, release_type="Album")
        deezer_identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id="deezer-album",
                title=deezer_album.title,
                release_kind="album",
                catalog_album=deezer_album,
            )
        )
        mb_identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id="mb-album",
                title=mb_album.title,
                release_kind="album",
                catalog_album=mb_album,
            )
        )
        artist.albums.extend([deezer_album, mb_album])
        artist.identities.extend([deezer_identity, mb_identity])
        session.add(artist)
        await session.commit()
        artist_id = artist.id

    page = await client.get(f"/artists/catalog/{artist_id}")
    csrf_token = page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]
    response = await client.post(
        f"/artists/catalog/{artist_id}/primary-source",
        data={"csrf_token": csrf_token, "primary_metadata_provider": "musicbrainz"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert (
        response.headers["location"]
        == f"/artists/catalog/{artist_id}?provider=musicbrainz&sort=desc"
    )
    async with factory() as session:
        refreshed = await session.get(CatalogArtist, artist_id)
        assert refreshed is not None
        assert refreshed.primary_metadata_provider == "musicbrainz"
    switched = await client.get(f"/artists/catalog/{artist_id}")
    assert "Primary: MusicBrainz" in switched.text
    assert "MusicBrainz Album" in switched.text
    assert "Deezer Album" not in switched.text


async def test_album_download_without_fetch_header_still_redirects(
    client: AsyncClient, monkeypatch
) -> None:
    import app.routers.catalog as catalog_router

    async def fake_dispatch(job_id: int):
        return None

    monkeypatch.setattr(catalog_router.job_dispatcher, "dispatch", fake_dispatch)
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name="Redirect Artist")
        album = CatalogAlbum(title="Redirect Album", track_count=1)
        artist.albums.append(album)
        album.tracks.append(CatalogAlbumTrack(position=1, disc=1, title="Only"))
        session.add(artist)
        await session.commit()
        album_id = album.id

    response = await client.post(f"/albums/{album_id}/download", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/downloads"


async def test_artist_download_monitored_queues_only_missing_partial_album_tracks(
    client: AsyncClient, test_settings, monkeypatch
) -> None:
    import app.routers.catalog as catalog_router

    dispatched: list[int] = []

    async def fake_dispatch(job_id: int):
        dispatched.append(job_id)

    monkeypatch.setattr(catalog_router.job_dispatcher, "dispatch", fake_dispatch)
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(
            name="Bulk Partial Artist",
            monitored=True,
            watchlist_provider="deezer",
            last_enriched_at=datetime.now(tz=UTC),
        )
        complete = CatalogAlbum(title="Complete Monitored Album", track_count=12, in_library=True)
        partial = CatalogAlbum(title="Partial Monitored Album", track_count=12, in_library=False)
        complete.tracks.extend(
            CatalogAlbumTrack(position=index, disc=1, title=f"Complete Track {index:02d}")
            for index in range(1, 13)
        )
        partial.tracks.extend(
            CatalogAlbumTrack(position=index, disc=1, title=f"Partial Track {index:02d}")
            for index in range(1, 13)
        )
        identity = CatalogArtistIdentity(
            provider="deezer", provider_artist_id="bulk-partial", name=artist.name
        )
        identity.releases.extend(
            [
                CatalogAlbumProvider(
                    provider_album_id="complete-monitored",
                    title=complete.title,
                    release_kind="album",
                    monitored=True,
                    catalog_album=complete,
                ),
                CatalogAlbumProvider(
                    provider_album_id="partial-monitored",
                    title=partial.title,
                    release_kind="album",
                    monitored=True,
                    catalog_album=partial,
                ),
            ]
        )
        artist.albums.extend([complete, partial])
        artist.identities.append(identity)
        done_job = Job(source="slskd", query="old", status=JobStatus.done)
        release = Release(
            job=done_job, source="slskd", title=partial.title, album_artist=artist.name
        )
        session.add_all([artist, done_job, release])
        await session.flush()

        release_folder = test_settings.library_root / artist.name / partial.title
        release_folder.mkdir(parents=True)
        for index, catalog_track in enumerate(partial.tracks[:10], start=1):
            existing_path = release_folder / f"{index:02d} - {catalog_track.title}.flac"
            existing_path.write_bytes(b"audio")
            track = Track(
                job=done_job,
                release=release,
                source="slskd",
                title=catalog_track.title,
                catalog_album_id=partial.id,
                catalog_track_id=catalog_track.id,
                acquisition_state=AcquisitionState.downloaded,
                import_state=ImportWorkflowState.imported,
                file_size_bytes=existing_path.stat().st_size,
            )
            track.import_plans.append(
                ImportPlan(
                    release=release,
                    source_path=str(existing_path),
                    destination_path=str(existing_path),
                    status=ImportWorkflowState.imported,
                    file_state=LibraryFileState.present,
                )
            )
            session.add(track)
        await session.commit()
        artist_id = artist.id
        partial_id = partial.id
        partial_track_ids = [track.id for track in partial.tracks]

    response = await client.post(
        f"/artists/catalog/{artist_id}/download-monitored",
        headers={"X-Requested-With": "fetch"},
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert response.json() == {"queued": 2, "artist_id": artist_id}
    async with factory() as session:
        jobs = list(
            (
                await session.scalars(
                    select(Job).where(Job.status == JobStatus.pending).order_by(Job.id)
                )
            ).all()
        )
    assert [job.query for job in jobs] == [
        "Bulk Partial Artist Partial Track 11",
        "Bulk Partial Artist Partial Track 12",
    ]
    assert [job.catalog_album_id for job in jobs] == [partial_id, partial_id]
    assert [job.catalog_track_id for job in jobs] == partial_track_ids[10:]
    assert all(job.query != "Bulk Partial Artist Partial Monitored Album" for job in jobs)
    assert len(dispatched) == 2


async def test_artist_download_monitored_fetch_returns_json_and_stays_put(
    client: AsyncClient, monkeypatch
) -> None:
    import app.routers.catalog as catalog_router

    dispatched: list[int] = []

    async def fake_dispatch(job_id: int):
        dispatched.append(job_id)

    monkeypatch.setattr(catalog_router.job_dispatcher, "dispatch", fake_dispatch)
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(
            name="Fetch Bulk Artist",
            monitored=True,
            watchlist_provider="deezer",
            last_enriched_at=datetime.now(tz=UTC),
        )
        album = CatalogAlbum(title="Fetch Bulk Single", track_count=1, in_library=False)
        album.tracks.append(CatalogAlbumTrack(position=1, disc=1, title="Fetch Bulk Single"))
        identity = CatalogArtistIdentity(
            provider="deezer", provider_artist_id="fetch-bulk", name=artist.name
        )
        identity.releases.append(
            CatalogAlbumProvider(
                provider_album_id="fetch-bulk-single",
                title=album.title,
                release_kind="single",
                monitored=True,
                catalog_album=album,
            )
        )
        artist.albums.append(album)
        artist.identities.append(identity)
        session.add(artist)
        await session.commit()
        artist_id = artist.id

    response = await client.post(
        f"/artists/catalog/{artist_id}/download-monitored",
        headers={"X-Requested-With": "fetch"},
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert response.json() == {"queued": 1, "artist_id": artist_id}
    async with factory() as session:
        jobs = list(
            (
                await session.scalars(
                    select(Job).where(
                        Job.catalog_album_id == album.id,
                        Job.status == JobStatus.pending,
                    )
                )
            ).all()
        )
    assert len(jobs) == 1
    assert dispatched == [jobs[0].id]
