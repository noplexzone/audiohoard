from __future__ import annotations

from unittest.mock import AsyncMock

from httpx import AsyncClient

from app.database import get_session_factory
from app.models.job import Job
from app.routers import jobs


async def test_create_job_dispatches_without_request_scoped_session(
    client: AsyncClient, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    dispatch = AsyncMock()
    mp.setattr(jobs.job_dispatcher, "dispatch", dispatch)

    response = await client.post("/jobs", json={"source": "youtube", "query": "test"})

    assert response.status_code == 201
    dispatch.assert_awaited_once_with(response.json()["id"])


async def test_dispatcher_can_read_committed_job_before_response_finishes(
    client: AsyncClient, monkeypatch: object
) -> None:
    from pytest import MonkeyPatch

    mp = monkeypatch
    assert isinstance(mp, MonkeyPatch)
    seen: list[bool] = []

    async def probe_dispatch(job_id: int) -> None:
        factory = get_session_factory()
        async with factory() as session:
            seen.append(await session.get(Job, job_id) is not None)

    mp.setattr(jobs.job_dispatcher, "dispatch", probe_dispatch)

    response = await client.post("/jobs", json={"source": "youtube", "query": "test"})

    assert response.status_code == 201
    assert seen == [True]
