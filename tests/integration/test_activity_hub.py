from __future__ import annotations

from httpx import AsyncClient

_ACTIVITY_TABS = (
    ('href="/wanted"', "Wanted"),
    ('href="/downloads"', "Downloads"),
    ('href="/review"', "Review"),
    ('href="/blocklist"', "Rejected Sources"),
)


async def test_activity_hub_renders_counts_tabs_and_empty_guidance(client: AsyncClient) -> None:
    response = await client.get("/activity")

    assert response.status_code == 200
    assert "Acquisition activity" in response.text
    assert 'aria-label="Activity sections"' in response.text
    for href, label in _ACTIVITY_TABS:
        assert href in response.text
        assert label in response.text
    for count_name in (
        "wanted",
        "active-downloads",
        "acquisition-issues",
        "awaiting-review",
        "rejected-sources",
    ):
        assert f'data-activity-count="{count_name}">0<' in response.text
    assert "Nothing needs your attention" in response.text


async def test_desktop_and_mobile_navigation_use_task_destinations(client: AsyncClient) -> None:
    response = await client.get("/activity")
    body = response.text

    assert body.count('data-nav-destination="home"') == 2
    assert body.count('data-nav-destination="discover"') == 2
    assert body.count('data-nav-destination="library"') == 2
    assert body.count('data-nav-destination="activity"') == 2
    assert body.count('data-nav-destination="settings"') == 1
    assert body.count('href="/activity"') >= 2
    assert "<span>Discover</span>" in body
    assert "<span>Activity</span>" in body
    assert "<span>Search</span>" not in body
    assert "<span>Blocklist</span>" not in body
    assert 'href="#i-home"' in body
    assert 'href="#i-discover"' in body
    assert 'href="#i-library"' in body
    assert 'href="#i-activity"' in body
    assert 'href="#i-settings"' in body
    assert 'aria-label="Settings"' in body


async def test_activity_attention_badge_only_appears_for_actionable_items(
    client: AsyncClient,
) -> None:
    from app.database import get_session_factory
    from app.models.job import Job, JobStatus
    from app.models.source_candidate_block import SourceCandidateBlock

    async with get_session_factory()() as db:
        db.add(Job(source="slskd", query="failed", status=JobStatus.failed))
        db.add(
            SourceCandidateBlock(
                provider="slskd", peer="peer", filename="track.flac", reason="denied"
            )
        )
        await db.commit()

    response = await client.get("/activity")

    assert 'data-activity-attention="1"' in response.text
    assert response.text.count('aria-label="1 activity item needs attention"') == 2
    assert 'data-activity-count="rejected-sources">1<' in response.text


async def test_activity_tabs_are_shared_by_legacy_pages(client: AsyncClient) -> None:
    for path in ("/wanted", "/downloads", "/review", "/blocklist"):
        response = await client.get(path)
        assert response.status_code == 200, path
        assert 'aria-label="Activity sections"' in response.text, path
        assert f'href="{path}" class="active" aria-current="page"' in response.text, path


async def test_legacy_routes_deep_links_and_forms_remain_compatible(client: AsyncClient) -> None:
    for path in (
        "/wanted?q=missing&sort=artist&page=1",
        "/downloads?status=failed",
        "/review?after=1",
        "/blocklist",
        "/search?q=test",
    ):
        response = await client.get(path)
        assert response.status_code == 200, path

    # Existing POST contracts retain their dedicated integration coverage; this
    # smoke test verifies that direct GET bookmarks and query strings still render.


async def test_activity_hub_degrades_to_an_accessible_error_state(
    client: AsyncClient, monkeypatch
) -> None:
    async def fail_summary(_db):
        raise RuntimeError("aggregate unavailable")

    monkeypatch.setattr("app.main.get_activity_summary", fail_summary)

    response = await client.get("/activity")

    assert response.status_code == 200
    assert 'role="alert"' in response.text
    assert "Activity counts are temporarily unavailable" in response.text
    assert 'aria-label="Activity sections"' in response.text
