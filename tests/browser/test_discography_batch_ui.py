from __future__ import annotations

from pathlib import Path

from playwright.sync_api import Page

SCREENSHOT_ROOT = Path("/opt/data")


def test_discography_batch_direct_queue_desktop_and_mobile(
    authenticated_page: Page, browser_base_url: str
) -> None:
    page = authenticated_page
    page.set_viewport_size({"width": 1440, "height": 900})
    page.goto(f"{browser_base_url}/wanted")
    page.get_by_role("checkbox", name="Select Browser Context Album").check()
    page.get_by_role("button", name="Queue selected").click()
    page.wait_for_url("**/activity?notice=queued")

    assert page.get_by_role("heading", name="Activity").is_visible()
    assert page.get_by_text("Downloads queued", exact=False).is_visible()
    assert page.locator('[data-activity-count="active-downloads"]').inner_text() != "0"
    assert page.get_by_role("heading", name="Batch status").count() == 0
    assert page.evaluate(
        "document.documentElement.scrollWidth <= document.documentElement.clientWidth"
    )
    assert page.evaluate("document.querySelectorAll('article.batch-item').length") == 0
    page.screenshot(
        path=str(SCREENSHOT_ROOT / "audiohoard-queue-activity-desktop.png"), full_page=True
    )

    page.set_viewport_size({"width": 390, "height": 844})
    page.reload()
    assert page.get_by_role("heading", name="Activity").is_visible()
    assert page.get_by_text("Downloads queued", exact=False).is_visible()
    assert page.evaluate(
        "document.documentElement.scrollWidth <= document.documentElement.clientWidth"
    )
    page.screenshot(
        path=str(SCREENSHOT_ROOT / "audiohoard-queue-activity-mobile.png"), full_page=True
    )
