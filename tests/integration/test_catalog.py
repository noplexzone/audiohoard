from __future__ import annotations

from datetime import UTC, datetime

import pytest_asyncio
from httpx import AsyncClient

import app.database as db_module
from app.models.catalog_entities import (
    CatalogAlbum,
    CatalogAlbumProvider,
    CatalogArtist,
    CatalogArtistIdentity,
)
from app.models.import_plan import ImportPlan
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
        catalog_artist_a.albums.append(CatalogAlbum(title="Great Album", release_type="Album"))
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
    assert 'src="https://images.example/watchlisted.jpg"' in resp.text
    assert "Downloaded files" in resp.text
    assert "Wanted releases" in resp.text


async def test_legacy_artist_routes_redirect_to_library(client: AsyncClient) -> None:
    for path in ("/artists/monitored", "/wanted"):
        resp = await client.get(path, follow_redirects=False)
        assert resp.status_code in (302, 303, 307, 308)
        assert resp.headers["location"] == "/library"


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


async def test_library_shows_file_and_wanted_counts(seeded_client: AsyncClient) -> None:
    resp = await seeded_client.get("/library")
    assert "Downloaded files" in resp.text
    assert "Wanted releases" in resp.text


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
    assert resp.text.count('href="/library"') == 2
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
    assert response.text.count('href="/library"') == 2
    assert "<span>Artists</span>" not in response.text


async def test_catalog_artist_separates_downloaded_files_and_wanted_releases(
    client: AsyncClient,
) -> None:
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
        partial = CatalogAlbum(title="Partial Album", in_library=False)
        complete = CatalogAlbum(title="Complete Album", in_library=True)
        artist.albums.extend([partial, complete])
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
                    title="Complete Album",
                    release_kind="album",
                    catalog_album=complete,
                ),
            ]
        )
        artist.identities.append(identity)
        session.add_all([job, release, artist])
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
        staging.import_state = ImportWorkflowState.discovered
        staging.import_plans[0].status = ImportWorkflowState.discovered
        session.add_all([imported, staging])
        await session.commit()
        artist_id = artist.id

    response = await client.get(f"/artists/catalog/{artist_id}")

    assert response.status_code == 200
    assert "Downloaded files" in response.text
    assert "Wanted releases" in response.text
    downloaded = response.text.split('data-section="downloaded-files"', 1)[1].split(
        'data-section="wanted-releases"', 1
    )[0]
    wanted = response.text.split('data-section="wanted-releases"', 1)[1].split("</section>", 1)[0]
    assert "Imported File.flac" in downloaded
    assert "/music/Partial Artist/Partial Album/01 Imported File.flac" in downloaded
    assert "Staging File" not in downloaded
    assert "Partial Album" in wanted
    assert "Complete Album" not in wanted
