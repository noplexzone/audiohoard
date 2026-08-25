# Audiohoard Discover Operate Surface Implementation Plan

> **For Hermes:** Execute task-by-task with TDD, fixed-commit independent review, and one writer per worktree.

**Goal:** Replace the blocking, generic Discover pages with a fast, truthful, poster-first server-rendered Operate surface that supports safe navigation and in-context watchlisting.

**Architecture:** Keep FastAPI, Jinja, native forms, and vanilla progressive enhancement. Establish safe read-only provider navigation and a batched local-state projection first; then return the Discover shell immediately and hydrate independent authenticated feed fragments; finally converge artist, release, and genre cards on shared poster-first components. Manual search remains the advanced escape hatch.

**Tech Stack:** FastAPI, async SQLAlchemy, Jinja2, vanilla JavaScript, CSS, pytest, Playwright, Impeccable detector.

## Global Constraints

- Preserve authentication, CSRF, native no-JS forms, stable provider identities, `/search?tab=advanced`, and existing catalog artist URLs.
- GET navigation must not commit database rows or start background enrichment.
- No SPA, frontend framework, horizontal carousel, or nested swipe region.
- Artwork and title lead; primary actions remain visible on touch/mobile and keyboard reachable.
- Pending is distinct from empty. Empty is rendered only after a successful ready response with zero items.
- Provider failures are isolated by section and sanitized; stale/global fallback is labeled truthfully.
- No per-card database query or unbounded provider fan-out.
- Same-name artists from different provider identities must remain distinct.
- Native actions return only to allowlisted same-origin Discover paths and stable card anchors.
- No production database, library, container, or runtime mutation during implementation and review.

---

### Task 1: Safe provider navigation and batched card state

**Objective:** Supply every Discover card with safe navigation and truthful monitored/library/progress state without N+1 queries or mutating GETs.

**Files:**
- Modify: `app/metadata/base.py`
- Modify: `app/services/discovery.py`
- Modify: `app/routers/search.py`
- Modify: `app/routers/catalog.py`
- Modify: `app/templates/partials/_artist_card.html`
- Modify: `app/templates/partials/_release_card.html`
- Test: `tests/unit/test_discovery.py`
- Test: `tests/integration/test_discovery.py`
- Test: focused catalog route tests

**Interfaces:**
- Produces a card-state projection keyed by `(provider, provider_id)` with catalog ID, monitored state, local-file/library state, and release progress when known.
- Produces a read-only provider artist/release preview URL, or an explicit CSRF POST for operations that persist identity; ordinary GETs perform zero writes and schedule zero tasks.
- Produces an allowlisted Discover return-path validator shared by native watch actions.

**TDD:**
1. Add tests proving a Discover View GET causes zero inserts/updates and starts no enrichment task.
2. Add monitored, persisted-unmonitored, local-only, complete, partial, and unknown fixtures; assert one bounded projection and no relationship lazy loads.
3. Add same-name/different-provider and unsafe return-path tests.
4. Implement the minimal state DTO, grouped query, safe route, and native return contract.
5. Run focused discovery/catalog tests, Ruff, format check, mypy, and `git diff --check`.
6. Commit: `feat(discover): add safe stateful card contracts`.

### Task 2: Progressive independent feed fragments

**Objective:** Render `/search` without waiting for provider enrichment and load each feed independently with truthful pending, ready-empty, stale, and error states.

**Files:**
- Modify: `app/services/discovery.py`
- Modify: `app/routers/search.py`
- Modify: `app/templates/search.html`
- Create: `app/templates/partials/_discover_section.html`
- Modify: `app/static/js/discovery.js`
- Test: `tests/unit/test_discovery.py`
- Test: `tests/integration/test_discovery.py`

**Interfaces:**
- `GET /search` returns a shell and cached ready sections within a bounded local-only budget.
- Authenticated fragment routes return one complete section with explicit `pending|ready|stale|error`, `has_next`, sanitized message, and card states.
- Genre sections retain exact ID and resolved name.

**TDD:**
1. Add a delayed-provider integration test proving the shell returns before enrichment completes.
2. Assert pending never renders empty, one feed failure does not block siblings, stale data stays usable, and fragments require auth.
3. Assert polling/fetch stops at ready/error, aborts on navigation, pauses while hidden, and binds once.
4. Implement cached-shell selection, fragment routes, partial rendering, and bounded progressive enhancement.
5. Run discovery unit/integration tests and quality gates.
6. Commit: `feat(discover): load independent feed sections`.

### Task 3: Poster-first landing and dedicated feed pages

**Objective:** Converge landing and dedicated pages on reusable artwork-led artist, release, and genre cards with direct, truthful controls.

**Files:**
- Modify: `app/templates/search.html`
- Modify: `app/templates/discover_list.html`
- Modify: `app/templates/partials/_artist_card.html`
- Modify: `app/templates/partials/_release_card.html`
- Create: `app/templates/partials/_genre_card.html`
- Create: `app/templates/partials/_discover_state.html`
- Modify: `app/static/css/pages.css`
- Modify: `app/static/js/discovery.js`
- Test: `tests/integration/test_discovery.py`
- Test: `tests/browser/test_discover_operate.py`

**Interfaces:**
- Shared cards expose safe poster/title links, provider/disambiguation evidence, monitored/library state, one visible primary action, and branded missing-art fallback.
- Dedicated genre pages show resolved genre names and breadcrumbs.
- Every feed uses explicit continuation rather than item-count inference.

**TDD:**
1. Add structural tests for card links, native CSRF forms, image dimensions/lazy loading, provider labels, state badges, exact genre heading, and explicit pagination.
2. Implement shared partials and responsive CSS without horizontal scrollers or hover-only controls.
3. Run `npx --yes impeccable detect` on changed templates/CSS/JS and resolve relevant findings.
4. Run focused integration tests and commit: `feat(discover): build poster-first operate surface`.

### Task 4: Browser acceptance and hardening

**Objective:** Prove the complete Discover workflow at desktop and mobile widths, then fix all material defects in one bounded pass.

**Files:**
- Create/modify: `tests/browser/test_discover_operate.py`
- Modify only confirmed implementation defects from the visual pass.

**Verification:**
1. Exercise 1440×900 and 390×844: landing, each feed type, genre, loading, ready-empty, stale, and error.
2. Assert no document overflow; 44px touch targets; visible actions without hover; long names and missing art; keyboard dialog/Escape/focus return; 200% zoom and reduced motion.
3. Verify console and failed network requests; preserve Manual search regression coverage.
4. Run one confirmation visual pass only.
5. Run focused browser/integration suites, Ruff, format, mypy, Impeccable detector, and diff checks.
6. Commit: `test(discover): verify poster-first workflows`.
7. Independent fixed-range review covers GET safety, CSRF/return URL handling, async state truth, N+1/fan-out, identity separation, accessibility, and responsive behavior.
