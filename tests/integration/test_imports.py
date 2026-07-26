from __future__ import annotations

import re
from pathlib import Path

from httpx import AsyncClient

from app.database import get_session_factory
from app.models.import_plan import ImportPlan
from app.models.job import Job, JobStatus
from app.models.release import Release
from app.models.track import Track
from app.models.workflow import ImportWorkflowState


async def test_import_plans_endpoint_starts_empty(client: AsyncClient) -> None:
    resp = await client.get("/imports/plans")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_import_review_page_renders(client: AsyncClient) -> None:
    resp = await client.get("/imports/ui/review")
    assert resp.status_code == 200
    assert "Import review" in resp.text
    assert "No import plans yet" in resp.text


async def test_import_review_form_actions_use_post_redirect_get(
    client: AsyncClient, tmp_path: Path, monkeypatch
) -> None:
    staged = tmp_path / "staged.mp3"
    staged.write_bytes(b"staged-audio")  # noqa: ASYNC240
    factory = get_session_factory()
    async with factory() as session:
        job = Job(source="slskd", query="album", status=JobStatus.done)
        release = Release(
            job=job,
            source="slskd",
            title="Album",
            album_artist="Artist",
            year="2000",
            import_state=ImportWorkflowState.ready,
        )
        track = Track(
            job=job,
            release=release,
            source="slskd",
            title="Song",
            artist="Artist",
            album_artist="Artist",
            album="Album",
            year="2000",
            disc=1,
            track_no=1,
            staging_path=str(staged),
            source_path=str(staged),
            import_state=ImportWorkflowState.ready,
        )
        session.add_all([job, release, track])
        await session.commit()
        release_id = release.id

    unplanned_review = await client.get("/imports/ui/review")
    assert "Plan and review first" in unplanned_review.text
    assert f'action="/imports/ui/releases/{release_id}/execute"' not in unplanned_review.text
    unplanned_execute = await client.post(f"/imports/releases/{release_id}/execute")
    assert unplanned_execute.status_code == 409
    assert unplanned_execute.json()["detail"] == "release has no reviewed import plan"

    csrf_header = client.headers.pop("X-CSRF-Token")
    for action in ("plan", "execute"):
        rejected = await client.post(
            f"/imports/ui/releases/{release_id}/{action}", follow_redirects=False
        )
        assert rejected.status_code == 403

    planned = await client.post(
        f"/imports/ui/releases/{release_id}/plan",
        data={"csrf_token": client.cookies["csrf"]},
        follow_redirects=False,
    )
    client.headers["X-CSRF-Token"] = csrf_header
    assert planned.status_code == 303
    assert planned.headers["location"] == "/imports/ui/review"
    review = await client.get(planned.headers["location"])
    assert review.status_code == 200
    assert "Album" in review.text
    assert "Artist" in review.text
    assert "Import 1 reviewed file" in review.text

    unconfirmed = await client.post(
        f"/imports/ui/releases/{release_id}/execute", follow_redirects=False
    )
    assert unconfirmed.status_code == 303
    assert unconfirmed.headers["location"].endswith("error=confirmation_required")

    from app.routers import imports as imports_router

    async def fake_execute(db, release, *, library_root):
        del db, release, library_root
        return []

    monkeypatch.setattr(imports_router, "execute_release_import", fake_execute)
    confirmed_plan_ids = re.findall(r'name="confirmed_plan_id" value="(\d+)"', review.text)
    assert len(confirmed_plan_ids) == 1
    stale = await client.post(
        f"/imports/ui/releases/{release_id}/execute",
        data={"confirm_import": "yes", "confirmed_plan_id": "999999"},
        follow_redirects=False,
    )
    assert stale.status_code == 303
    assert stale.headers["location"].endswith("error=plan_changed")

    executed = await client.post(
        f"/imports/ui/releases/{release_id}/execute",
        data={"confirm_import": "yes", "confirmed_plan_id": confirmed_plan_ids},
        follow_redirects=False,
    )
    assert executed.status_code == 303
    assert executed.headers["location"] == "/imports/ui/review"
    final_page = await client.get(executed.headers["location"])
    assert final_page.status_code == 200
    assert "Import review" in final_page.text

    template = final_page.text
    assert f'action="/imports/ui/releases/{release_id}/plan"' in template
    assert f'action="/imports/ui/releases/{release_id}/execute"' in template


async def test_import_review_confirmation_includes_every_plan_for_visible_release(
    client: AsyncClient,
) -> None:
    factory = get_session_factory()
    async with factory() as session:
        job = Job(source="slskd", query="large album", status=JobStatus.done)
        release = Release(
            job=job,
            source="slskd",
            title="Large Album",
            album_artist="Artist",
            import_state=ImportWorkflowState.ready,
        )
        session.add_all([job, release])
        await session.flush()
        session.add_all(
            ImportPlan(
                release=release,
                source_path=f"/staging/{index}.flac",
                destination_path=f"/library/{index}.flac",
                status=ImportWorkflowState.ready,
            )
            for index in range(201)
        )
        await session.commit()

    review = await client.get("/imports/ui/review")
    assert review.status_code == 200
    assert "Import 201 reviewed files" in review.text
