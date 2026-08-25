from __future__ import annotations

from collections import Counter

from playwright.sync_api import Page, expect


def test_discover_fragments_load_once_and_injected_watchlist_works(
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


def test_discover_fragment_network_failure_is_isolated(
    authenticated_page: Page, browser_base_url: str
) -> None:
    page = authenticated_page
    page.route("**/discover/fragments/new", lambda route: route.abort())
    page.goto(f"{browser_base_url}/search")

    failed = page.locator("#discovery-new")
    expect(failed).to_have_attribute("data-discover-state", "error")
    expect(failed.get_by_role("alert")).to_contain_text("This discovery feed could not be loaded")
    expect(page.locator("#discovery-popular")).to_have_attribute("data-discover-state", "ready")
