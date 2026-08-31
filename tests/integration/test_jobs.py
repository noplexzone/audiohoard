from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.jobs.dispatcher import job_dispatcher


async def test_create_job_returns_201(client: AsyncClient) -> None:
    resp = await client.post(
        "/jobs",
        json={"source": "youtube", "query": "Beethoven Moonlight Sonata"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["source"] == "youtube"
    assert data["query"] == "Beethoven Moonlight Sonata"
    assert data["status"] == "pending"
    assert "id" in data


async def test_create_job_accepts_tidal_and_rejects_unknown_sources(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def do_not_execute_source_job(_job_id: int) -> None:
        return None

    monkeypatch.setattr(job_dispatcher, "dispatch", do_not_execute_source_job)
    tidal = await client.post(
        "/jobs",
        json={"source": "tidal", "query": "https://tidal.com/browse/track/123"},
    )
    assert tidal.status_code == 201
    unknown = await client.post("/jobs", json={"source": "unknown", "query": "test track"})
    assert unknown.status_code == 422


async def test_list_jobs(client: AsyncClient) -> None:
    await client.post("/jobs", json={"source": "slskd", "query": "test track"})
    resp = await client.get("/jobs")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 1


async def test_get_job_by_id(client: AsyncClient) -> None:
    create = await client.post("/jobs", json={"source": "prowlarr", "query": "Miles Davis"})
    job_id = create.json()["id"]
    resp = await client.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == job_id


async def test_get_job_not_found(client: AsyncClient) -> None:
    resp = await client.get("/jobs/99999")
    assert resp.status_code == 404


async def test_list_tracks_empty(client: AsyncClient) -> None:
    resp = await client.get("/tracks")
    assert resp.status_code == 200
    assert resp.json() == []
