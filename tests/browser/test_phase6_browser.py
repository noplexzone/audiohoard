from __future__ import annotations

import re

from playwright.sync_api import Page, expect


def test_configure_and_test_provider_and_unsaved_feedback(
    authenticated_page: Page, browser_base_url: str
) -> None:
    page = authenticated_page
    page.goto(f"{browser_base_url}/settings/acquisition")

    prowlarr = page.locator('[data-provider-card="prowlarr"]')
    prowlarr.locator('input[name="prowlarr_url"]').fill("http://draft-prowlarr:9696")
    prowlarr.locator('input[name="prowlarr_api_key"]').fill("draft-key")
    expect(prowlarr.locator(".unsaved-indicator")).to_be_visible()
    expect(prowlarr.locator(".unsaved-indicator")).to_have_text("Unsaved changes")

    slskd = page.locator('[data-provider-card="slskd"]')
    slskd.locator('input[name="slskd_url"]').fill("http://browser-slskd:5030")
    slskd.locator('input[name="slskd_api_key"]').fill("browser-secret")
    slskd.get_by_role("button", name="Save and test").click()
    expect(page).to_have_url(re.compile(r"/settings/acquisition\?saved=1"))
    expect(page.locator(".alert.ok")).to_contain_text("Settings saved")
    expect(page.locator("#settings-feedback")).to_contain_text("slskd: Connected")


def test_search_monitor_artist_and_open_contextual_manual_search(
    authenticated_page: Page, browser_base_url: str
) -> None:
    page = authenticated_page
    page.goto(f"{browser_base_url}/search")
    page.get_by_label("Artist query").fill("Browser Search Artist")
    page.get_by_role("button", name="Search artists").click()
    card = page.locator('[data-provider-id="browser-artist-42"]')
    expect(card.get_by_role("heading", name="Browser Search Artist")).to_be_visible()
    card.get_by_role("button", name="Add to watchlist").click()
    expect(card.locator("[data-watchlist-status]")).to_have_text("Watched")
    expect(card.get_by_role("button", name="Watched")).to_be_disabled()

    page.goto(f"{browser_base_url}/wanted")
    page.get_by_role("link", name="Browser Context Album", exact=True).click()
    expect(page.get_by_role("heading", name="Browser Context Album")).to_be_visible()
    page.get_by_role("link", name="Manual search").click()
    expect(page).to_have_url(
        re.compile(
            r"/search\?tab=advanced&artist=Browser(?:%20|\+)Wanted(?:%20|\+)Artist&album=Browser(?:%20|\+)Context(?:%20|\+)Album$"
        )
    )
    expect(page.get_by_label("Artist")).to_have_value("Browser Wanted Artist")
    expect(page.get_by_label("Album")).to_have_value("Browser Context Album")


def test_queue_wanted_filter_page(authenticated_page: Page, browser_base_url: str) -> None:
    page = authenticated_page
    page.goto(f"{browser_base_url}/wanted?status=needs-search")
    expect(page.get_by_label("State", exact=True)).to_have_value("needs-search")
    expect(page.get_by_text("Browser Context Album", exact=True)).to_be_visible()
    page.get_by_role("button", name="Queue this page").click()
    page.wait_for_url("**/discography-batches/*?notice=queued")
    expect(page.get_by_role("heading", name="Batch status")).to_be_visible()
    expect(page.get_by_text("Browser Context Album", exact=True)).to_be_visible()
    expect(page.get_by_role("button", name="Confirm and queue")).to_have_count(0)


def test_review_skip_approve_deny_and_no_itunes_reference(
    authenticated_page: Page, browser_base_url: str
) -> None:
    page = authenticated_page
    page.goto(f"{browser_base_url}/review")
    expect(page.get_by_text("No verified reference available", exact=True)).to_be_visible()
    expect(
        page.get_by_text("No verified comparison clip is available.", exact=False)
    ).to_be_visible()
    expect(page.locator("body")).not_to_contain_text("iTunes")
    first_title = page.locator(".review-deck-heading h2").inner_text()

    page.get_by_role("link", name="Skip", exact=True).click()
    expect(page.locator(".review-deck-heading h2")).not_to_have_text(first_title)
    page.get_by_role("button", name="Approve", exact=False).click()
    expect(page).to_have_url(f"{browser_base_url}/review?notice=approved")

    page.once("dialog", lambda dialog: dialog.accept())
    page.get_by_role("button", name="Deny", exact=False).click()
    expect(page).to_have_url(f"{browser_base_url}/review?notice=source_blocked")
    expect(page.get_by_text("exact provider source was blocked", exact=False)).to_be_visible()
    expect(page.get_by_text("tracks remaining", exact=False)).to_be_visible()


def test_restore_rejected_source(authenticated_page: Page, browser_base_url: str) -> None:
    page = authenticated_page
    page.goto(f"{browser_base_url}/blocklist")
    expect(page.get_by_text("Browser Rejected/track.flac", exact=True)).to_be_visible()
    page.once("dialog", lambda dialog: dialog.accept())
    rejected_row = page.locator("tbody tr").filter(has_text="Browser Rejected/track.flac")
    rejected_row.get_by_role("button", name="Allow again").click()
    expect(page).to_have_url(f"{browser_base_url}/blocklist?allowed=1")
    expect(page.get_by_role("status")).to_contain_text("Source allowed again")
    expect(page.get_by_text("Browser Rejected/track.flac", exact=True)).to_have_count(0)


def test_activity_tabs_and_mobile_navigation(
    authenticated_page: Page, browser_base_url: str
) -> None:
    page = authenticated_page
    page.goto(f"{browser_base_url}/activity")
    tabs = page.get_by_role("navigation", name="Activity sections")
    for label, path in (
        ("Wanted", "/wanted"),
        ("Downloads", "/downloads"),
        ("Review", "/review"),
        ("Rejected Sources", "/blocklist"),
    ):
        tabs.get_by_role("link", name=label).click()
        expect(page).to_have_url(f"{browser_base_url}{path}")
        tabs = page.get_by_role("navigation", name="Activity sections")

    page.set_viewport_size({"width": 390, "height": 844})
    page.goto(f"{browser_base_url}/activity")
    mobile = page.get_by_role("navigation", name="Mobile navigation")
    expect(mobile).to_be_visible()
    expect(page.locator(".sidebar")).to_be_hidden()
    for destination in ("Home", "Discover", "Library", "Activity"):
        expect(mobile.get_by_role("link", name=destination, exact=False)).to_be_visible()
    expect(page.get_by_role("link", name="Settings", exact=True)).to_be_visible()
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
