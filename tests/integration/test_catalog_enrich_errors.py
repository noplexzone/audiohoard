from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

import app.database as db_module
from app.models.catalog_entities import CatalogArtist


async def _seed_artist(name: str = "Test Artist") -> int:
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = CatalogArtist(name=name, mbid="test-provider-id")
        session.add(artist)
        await session.commit()
        await session.refresh(artist)
        return artist.id


@pytest.mark.asyncio
async def test_manual_enrich_failure_returns_303_not_500(client: AsyncClient) -> None:
    artist_id = await _seed_artist()
    with patch(
        "app.routers.catalog.enrich_catalog_artist",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Provider timeout"),
    ):
        resp = await client.post(f"/artists/catalog/{artist_id}/enrich", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers["location"]
    assert "sql:" not in location.lower()
    assert location == f"/artists/catalog/{artist_id}?enrichment=failed"


@pytest.mark.asyncio
async def test_manual_enrich_failure_persists_sanitized_error(client: AsyncClient) -> None:
    artist_id = await _seed_artist()
    with patch(
        "app.routers.catalog.enrich_catalog_artist",
        new_callable=AsyncMock,
        side_effect=ValueError("Bad response\nSecond line"),
    ):
        await client.post(f"/artists/catalog/{artist_id}/enrich", follow_redirects=False)

    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = await session.get(CatalogArtist, artist_id)
        assert artist is not None
        provenance = json.loads(artist.provenance_json or "{}")
        err = provenance.get("last_enrichment_error", {})
        assert err.get("message") == "ValueError: Bad response"
        assert len(err["message"]) <= 200


@pytest.mark.asyncio
async def test_manual_enrich_failure_redirect_has_no_sql_fragment(client: AsyncClient) -> None:
    artist_id = await _seed_artist()
    with patch(
        "app.routers.catalog.enrich_catalog_artist",
        new_callable=AsyncMock,
        side_effect=Exception("SELECT * FROM secrets"),
    ):
        resp = await client.post(f"/artists/catalog/{artist_id}/enrich", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers["location"]
    assert "sql:" not in location.lower()
    assert "SELECT" not in location


@pytest.mark.asyncio
async def test_manual_enrich_success_redirects_to_surviving_artist(client: AsyncClient) -> None:
    original_id = await _seed_artist("Duplicate Artist")
    survivor_id = await _seed_artist("Surviving Artist")

    with patch(
        "app.routers.catalog.enrich_catalog_artist",
        new_callable=AsyncMock,
        return_value={"status": "ok", "artist_id": survivor_id},
    ):
        resp = await client.post(f"/artists/catalog/{original_id}/enrich", follow_redirects=False)

    assert resp.status_code == 303
    assert resp.headers["location"] == f"/artists/catalog/{survivor_id}?enrichment=ok"


@pytest.mark.asyncio
async def test_manual_enrich_success_clears_prior_error(client: AsyncClient) -> None:
    artist_id = await _seed_artist()
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = await session.get(CatalogArtist, artist_id)
        assert artist is not None
        artist.provenance_json = json.dumps(
            {"last_enrichment_error": {"at": "2024-01-01T00:00:00+00:00", "message": "old error"}}
        )
        await session.commit()

    with patch(
        "app.routers.catalog.enrich_catalog_artist",
        new_callable=AsyncMock,
        return_value={"status": "ok"},
    ):
        resp = await client.post(f"/artists/catalog/{artist_id}/enrich", follow_redirects=False)
    assert resp.status_code == 303
    assert "failed" not in resp.headers["location"]

    async with factory() as session:
        artist = await session.get(CatalogArtist, artist_id)
        assert artist is not None
        provenance = json.loads(artist.provenance_json or "{}")
        assert "last_enrichment_error" not in provenance


@pytest.mark.asyncio
async def test_artist_page_shows_enrichment_failed_banner(client: AsyncClient) -> None:
    artist_id = await _seed_artist()
    with patch(
        "app.routers.catalog.enrich_catalog_artist",
        new_callable=AsyncMock,
        side_effect=RuntimeError("boom"),
    ):
        await client.post(f"/artists/catalog/{artist_id}/enrich", follow_redirects=False)

    resp = await client.get(f"/artists/catalog/{artist_id}?enrichment=failed")
    assert resp.status_code == 200
    assert 'role="alert"' in resp.text
    assert "Last enrichment failed" in resp.text


@pytest.mark.asyncio
async def test_artist_page_shows_provenance_error_banner(client: AsyncClient) -> None:
    artist_id = await _seed_artist()
    factory = db_module.get_session_factory()
    async with factory() as session:
        artist = await session.get(CatalogArtist, artist_id)
        assert artist is not None
        artist.provenance_json = json.dumps(
            {"last_enrichment_error": {"at": "2024-01-01T00:00:00+00:00", "message": "boom"}}
        )
        await session.commit()

    resp = await client.get(f"/artists/catalog/{artist_id}")
    assert resp.status_code == 200
    assert 'role="alert"' in resp.text
    assert "boom" in resp.text
