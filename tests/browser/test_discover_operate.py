from __future__ import annotations

import base64
import re
from collections import Counter
from pathlib import Path

from playwright.sync_api import Page, Route, expect

SCREENSHOT_ROOT = Path("/opt/data")
_FEEDS = ("popular", "genres", "new", "trending")
_PIXEL = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


def _assert_no_document_overflow(page: Page) -> None:
    assert page.evaluate(
        "document.documentElement.scrollWidth <= document.documentElement.clientWidth"
    )


def _assert_each_fragment_once(requests: list[str]) -> None:
    counts = Counter(url.rsplit("/", 1)[-1] for url in requests)
    assert counts == Counter({feed: 1 for feed in _FEEDS})


def test_discover_poster_first_workflows_desktop_mobile_and_recovery(
    authenticated_page: Page, browser_base_url: str
) -> None:
    page = authenticated_page
    fragment_requests: list[str] = []
    console_errors: list[str] = []
    failed_requests: list[str] = []
    expected_failed_requests: list[str] = []
    page.on(
        "request",
        lambda request: (
            fragment_requests.append(request.url)
            if "/discover/fragments/" in request.url
            else None
        ),
    )
    page.on(
        "console",
        lambda message: console_errors.append(message.text) if message.type == "error" else None,
    )
    page.on("requestfailed", lambda request: failed_requests.append(request.url))
    page.route(
        re.compile(r".*/artwork\?url=.*"),
        lambda route: route.fulfill(status=200, content_type="image/png", body=_PIXEL),
    )
    page.emulate_media(reduced_motion="reduce")

    # Desktop landing: independent feeds, exact identities, safe links, and native forms.
    page.set_viewport_size({"width": 1440, "height": 900})
    page.goto(f"{browser_base_url}/search")
    for feed in _FEEDS:
        expect(page.locator(f"#discovery-{feed}")).to_have_attribute(
            "data-discover-state", "ready"
        )
    _assert_each_fragment_once(fragment_requests)
    _assert_no_document_overflow(page)
    expect(page.get_by_role("link", name="Manual search", exact=True)).to_be_visible()
    expect(page.get_by_text("Browser New Release", exact=True)).to_be_visible()
    expect(page.get_by_text("Browser Trending Release", exact=True)).to_be_visible()
    expect(page.get_by_role("link", name="Explore Browser Jazz genre").last).to_have_attribute(
        "href", "/discover/genres/132"
    )
    namesakes = page.locator('[data-provider-id^="browser-popular-"]')
    expect(namesakes).to_have_count(2)
    assert namesakes.evaluate_all("cards => cards.map(card => card.dataset.providerId)") == [
        "browser-popular-1",
        "browser-popular-2",
    ]
    expect(page.locator('[data-provider-id="browser-popular-1"]')).to_contain_text("North America")
    expect(page.locator('[data-provider-id="browser-popular-2"]')).to_contain_text("Europe")
    expect(page.locator(".discover-poster-missing").first).to_be_visible()
    assert page.locator('a[href*="/artists/catalog/open?"]').count() == 0
    native_forms = page.locator('form[method="post"][action="/artists/catalog/open"]')
    assert native_forms.count() >= 5
    assert native_forms.locator('input[name="csrf_token"]').count() == native_forms.count()
    assert page.locator(".discover-primary").evaluate_all(
        "items => items.every(item => getComputedStyle(item).visibility === 'visible')"
    )
    page.screenshot(
        path=str(SCREENSHOT_ROOT / "audiohoard-discover-operate-desktop.png"),
        full_page=True,
    )

    # Watch the exact injected identity through the keyboard-accessible native form enhancement.
    watched_card = page.locator('[data-provider-id="browser-popular-1"]')
    watch_button = watched_card.get_by_role("button", name="Add to watchlist")
    watch_button.focus()
    page.keyboard.press("Enter")
    expect(watched_card.locator("[data-watchlist-status]")).to_have_text("Watched")
    expect(watched_card).to_have_attribute("data-watched", "true")
    dialog = watched_card.locator("[data-watchlist-dialog]")
    if dialog.count():
        expect(dialog).to_be_visible()
        page.keyboard.press("Escape")
        expect(dialog).not_to_be_visible()
        expect(watch_button).to_be_focused()

    # Dedicated routes preserve breadcrumbs, exact labels/IDs, and explicit continuation.
    dedicated = {
        "/discover/popular": "Popular artists",
        "/discover/genres": "Genres",
        "/discover/new": "New releases",
        "/discover/trending": "Trending releases",
    }
    for path, title in dedicated.items():
        page.goto(f"{browser_base_url}{path}")
        expect(page.get_by_role("navigation", name="Breadcrumb")).to_be_visible()
        expect(page.get_by_role("heading", name=title, level=1)).to_be_visible()
        _assert_no_document_overflow(page)
    expect(page.get_by_text("Actions apply to the artist.", exact=True)).to_be_visible()
    expect(page.get_by_role("button", name="Watch artist")).to_be_visible()

    page.goto(f"{browser_base_url}/discover/popular")
    expect(page.get_by_role("link", name="Next")).to_have_attribute("href", "?page=2")
    page.get_by_role("link", name="Next").click()
    expect(page).to_have_url(f"{browser_base_url}/discover/popular?page=2")
    expect(page.get_by_role("link", name="Previous")).to_have_attribute("href", "?page=1")
    expect(page.get_by_role("link", name="Next")).to_have_count(0)

    page.goto(f"{browser_base_url}/discover/genres/132")
    expect(page.get_by_role("heading", name="Browser Jazz", level=1)).to_be_visible()
    breadcrumb = page.get_by_role("navigation", name="Breadcrumb")
    expect(breadcrumb.get_by_role("link", name="Genres")).to_have_attribute(
        "href", "/discover/genres"
    )
    expect(page.locator('[data-provider-id="browser-genre-artist"]')).to_be_visible()

    # One failed fragment is isolated and retry fetches only that fragment.
    page.goto(f"{browser_base_url}/search")
    for feed in _FEEDS:
        expect(page.locator(f"#discovery-{feed}")).to_have_attribute(
            "data-discover-state", "ready"
        )
    fragment_requests.clear()
    failed_pattern = "**/discover/fragments/new"

    def abort_expected(route: Route) -> None:
        expected_failed_requests.append(route.request.url)
        route.abort()

    page.route(failed_pattern, abort_expected)
    page.reload()
    failed = page.locator("#discovery-new")
    expect(failed).to_have_attribute("data-discover-state", "error")
    expect(failed.get_by_role("alert")).to_contain_text("This discovery feed could not be loaded")
    expect(page.locator("#discovery-popular")).to_have_attribute("data-discover-state", "ready")
    _assert_each_fragment_once(fragment_requests)
    page.unroute(failed_pattern)
    fragment_requests.clear()
    failed.get_by_role("link", name="Retry this section").click()
    expect(failed).to_have_attribute("data-discover-state", "ready")
    assert [url.rsplit("/", 1)[-1] for url in fragment_requests] == ["new"]
    assert all("Failed to load resource" in message for message in console_errors)
    console_errors.clear()

    # A full page containing a matching-looking section is still rejected.
    def wrong_fragment(route: Route) -> None:
        route.fulfill(
            status=200,
            content_type="text/html",
            body=(
                '<!doctype html><html><body><section id="discovery-popular" '
                'data-discover-section data-discover-state="ready" '
                'data-discover-fragment-url="/discover/fragments/popular">'
                "wrong full page</section></body></html>"
            ),
        )

    page.route("**/discover/fragments/popular", wrong_fragment)
    page.reload()
    expect(page.locator("#discovery-popular")).to_have_attribute("data-discover-state", "error")
    expect(page.locator("#discovery-genres")).to_have_attribute("data-discover-state", "ready")
    page.unroute("**/discover/fragments/popular")

    # Mobile: real two-column layout, touch target geometry, wrapping, and reachable tail controls.
    page.set_viewport_size({"width": 390, "height": 844})
    page.goto(f"{browser_base_url}/search")
    _assert_no_document_overflow(page)
    assert (
        page.locator("#discovery-popular .discover-poster-grid").evaluate(
            "grid => getComputedStyle(grid).gridTemplateColumns.split(' ').length"
        )
        == 2
    )
    primary_sizes = page.locator(".discover-primary").evaluate_all(
        "items => items.map(item => { const r = item.getBoundingClientRect(); "
        "return [r.width, r.height]; })"
    )
    assert primary_sizes and all(width >= 44 and height >= 44 for width, height in primary_sizes)
    long_heading = page.get_by_role(
        "heading",
        name="Browser Artist With An Exceptionally Long Name That Must Wrap Without Overflow",
    )
    assert long_heading.evaluate("node => node.scrollWidth <= node.clientWidth")
    last_control = page.locator("#discovery-trending .discover-primary").last
    last_control.scroll_into_view_if_needed()
    assert last_control.is_visible()
    nav_top = page.locator(".mobile-nav").evaluate("nav => nav.getBoundingClientRect().top")
    control_bottom = last_control.evaluate("node => node.getBoundingClientRect().bottom")
    assert control_bottom <= nav_top
    page.locator('[data-provider-id="browser-long-artist"]').scroll_into_view_if_needed()
    page.screenshot(path=str(SCREENSHOT_ROOT / "audiohoard-discover-operate-mobile.png"))

    # 200% zoom equivalent and keyboard focus-visible behavior under reduced motion.
    page.set_viewport_size({"width": 720, "height": 450})
    page.reload()
    _assert_no_document_overflow(page)
    manual_search = page.get_by_role("link", name="Manual search", exact=True)
    manual_search.focus()
    assert manual_search.evaluate("node => node.matches(':focus-visible')")
    assert manual_search.evaluate("node => getComputedStyle(node).outlineStyle") != "none"
    assert page.evaluate("matchMedia('(prefers-reduced-motion: reduce)').matches")

    unexpected_failures = [url for url in failed_requests if url not in expected_failed_requests]
    assert unexpected_failures == []
    assert console_errors == []
