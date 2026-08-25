from __future__ import annotations

from collections import Counter

from playwright.sync_api import Page, expect


def test_discover_progressive_fragments_retry_and_identity_validation(
    authenticated_page: Page, browser_base_url: str
) -> None:
    page = authenticated_page
    requests: list[str] = []
    page.on(
        "request",
        lambda request: (
            requests.append(request.url) if "/discover/fragments/" in request.url else None
        ),
    )

    page.goto(f"{browser_base_url}/search")
    for feed in ("popular", "genres", "new", "trending"):
        expect(page.locator(f"#discovery-{feed}")).to_have_attribute(
            "data-discover-state", "ready"
        )
    counts = Counter(url.rsplit("/", 1)[-1] for url in requests)
    assert counts == Counter({feed: 1 for feed in ("popular", "genres", "new", "trending")})

    card = page.locator('[data-provider-id="browser-discovery-artist"]')
    expect(card.get_by_role("heading", name="Browser Discovery Artist")).to_be_visible()
    card.get_by_role("button", name="Add to watchlist").click()
    expect(card.locator("[data-watchlist-status]")).to_have_text("Watched")

    failed_pattern = "**/discover/fragments/new"
    page.route(failed_pattern, lambda route: route.abort())
    page.goto(f"{browser_base_url}/search")
    failed = page.locator("#discovery-new")
    expect(failed).to_have_attribute("data-discover-state", "error")
    expect(failed.get_by_role("alert")).to_contain_text("This discovery feed could not be loaded")
    retry = failed.get_by_role("link", name="Retry this section")
    expect(retry).to_have_attribute("href", "/search#discovery-new")
    expect(page.locator("#discovery-popular")).to_have_attribute("data-discover-state", "ready")
    page.unroute(failed_pattern)
    retry.click()
    expect(page).to_have_url(f"{browser_base_url}/search")
    expect(page.locator("#discovery-new")).to_have_attribute("data-discover-state", "ready")

    def wrong_fragment(route) -> None:
        route.fulfill(
            status=200,
            content_type="text/html",
            body=(
                '<section id="discovery-genres" data-discover-section '
                'data-discover-state="ready" '
                'data-discover-fragment-url="/discover/fragments/genres">wrong</section>'
            ),
        )

    page.route("**/discover/fragments/popular", wrong_fragment)
    page.goto(f"{browser_base_url}/search")
    popular = page.locator("#discovery-popular")
    expect(popular).to_have_attribute("data-discover-state", "error")
    expect(popular.get_by_role("alert")).to_contain_text("This discovery feed could not be loaded")
    expect(page.locator("#discovery-genres")).to_have_count(1)
