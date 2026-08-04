# Discover and Artist Flow Implementation Plan

> **For Hermes:** Implement task-by-task with Claude Code or subagent-driven development. Light owns implementation, L owns independent review, and Jarvis owns integration, verification, publication, and release decisions.

**Goal:** Make `/search` a region-aware discovery surface with reliable artist search/watchlisting and a primary-provider-first progressive discography.

**Architecture:** Add provider-neutral discovery DTOs and Deezer-backed adapters with bounded validation, region-aware caching, and section-level failures. Keep Jinja forms usable without JavaScript, then progressively enhance card watchlisting and discography refresh. Split fast release summaries from deferred release-detail and cross-provider enrichment to remove the initial N+1 path.

**Tech Stack:** Python 3.12+, FastAPI, async SQLAlchemy/SQLite, httpx, Jinja, vanilla JavaScript, pytest/pytest-httpx, Ruff, mypy, Docker/GitHub Actions.

## Global Constraints

- `/search` is Discover with artist search at the top; Advanced source search remains.
- Landing sections: Popular artists, Genres, New releases, Trending releases; 12 cards each plus dedicated deeper pages.
- Persist a Discovery region setting, default `US`; prefer the selected region and visibly label global fallback.
- Deezer search results first by descending fans; fanless Deezer rows after counted rows; other providers retain native order afterward.
- Bounded identity validation filters unusable rows. Never substitute or merge same-name artists without provider-specific evidence. Deezer ID `10002824` must not render actionable or 500.
- Artist cards offer View discography and in-place Add to watchlist. Apply saved defaults immediately, mark Watched, prevent duplicates, then open an optional dialog for albums/singles/EPs/upgrade monitoring. Closing keeps defaults.
- New/trending release cards open artist discography and offer Watch artist only; no direct download.
- Initial artist-page shell target under one second; primary-provider releases progressively visible within five seconds.
- Initial grid cannot block on per-release detail, UPC, content rating, cross-provider matching, or full enrichment.
- Unwatched artists load secondary providers only on explicit demand. Watched artists may background-enrich enabled secondary providers after primary storage.
- Provider failures are section/card scoped with bounded timeout/concurrency and truthful stale/fallback status.
- Preserve authentication, CSRF, safe redirects, provider-native identity, SQLite whole-operation retry, and no-JS fallback.
- Update `CHANGELOG.md` under `Unreleased`. No version bump, stable tag, `latest`, production service lifecycle action, or production mutation.
- Acceptance artifact is only `noplexzone/audiohoard:develop`, published through CI and digest-verified.

## Ownership and Gates

- Workspace: `/mnt/user/appdata/dev/_scratch/audiohoard-discovery`
- Branch: `jarvis/discovery-artist-flow` from `origin/main` at `ed5f087`.
- Light/Claude is sole implementation writer. L reviews a stable commit range read-only. Sequence: implementation → review → remediation → Jarvis verification/publish.

## Task 1: Validate and rank artist search

**Files:** `app/metadata/base.py`, `app/metadata/deezer.py`, `app/services/catalog_metadata.py`, `app/routers/search.py`, `app/templates/search.html`, `tests/unit/test_deezer.py`, `tests/unit/test_catalog_metadata_v060.py`, `tests/integration/test_catalog_v030.py`.

1. RED: test the actual malformed Deezer error-envelope detail for ID `10002824`; assert filtering and no persistence/500.
2. RED: test counted Deezer hits descending, fanless Deezer afterward, then unchanged MusicBrainz/iTunes order.
3. Implement bounded validation using short provider-detail/evidence budgets. Reject HTTP-200 error envelopes, missing/mismatched IDs, and invalid artist types at row scope.
4. Harden direct open GET/POST to return safe HTML/JSON errors instead of uncaught `ValueError`.
5. Render valid evidence-rich cards only.
6. Run focused provider/catalog tests.
7. Commit `fix(catalog): validate and rank artist search results`.

## Task 2: Add in-place idempotent watchlisting

**Files:** `app/routers/catalog.py`, `app/templates/search.html`, new `app/templates/partials/_artist_card.html`, new `app/static/js/discovery.js`, `app/static/css/pages.css`, relevant script registration/package tests, `tests/integration/test_catalog_v030.py`.

1. RED: test fetch watchlisting, duplicate idempotency, defaults, SQLite retry, malformed identity error, and dialog updates.
2. RED: render watched state by provider-native identity, never name only.
3. Keep provider HTTP outside the DB write window; rerun the entire DB mutation under lock retry. Return JSON for fetch and 303 fallback for native form POST.
4. Build a shared artist-card partial and progressive enhancement: pending/disabled state, inline error, Watched state, optional accessible `<dialog>`, focus return, Escape/close, and ARIA-live status.
5. Dialog saves only the selected artist; closing retains defaults.
6. Run focused integration/package tests.
7. Commit `feat(catalog): add in-place artist watchlisting`.

## Task 3: Make discography primary-first and progressive

**Files:** `app/metadata/base.py`, `app/metadata/deezer.py`, `app/services/catalog_metadata.py`, `app/routers/catalog.py`, `app/templates/catalog_artist.html`, `app/static/js/artist-watchlist.js`, provider/catalog integration tests.

1. RED: initial Deezer discography summary makes one artist-albums request and zero per-album detail requests.
2. RED: delayed provider fixture proves the shell does not await provider I/O; primary grid appears while secondary remains pending; unwatched artists do not start secondary enrichment; watched artists do.
3. Persist list-response summaries with deferred fields unknown. Move detail enrichment behind an explicit deferred path.
4. Add authenticated provider state/fragment endpoint and independent primary/secondary states with sanitized errors. Keep network I/O outside SQLite writes.
5. Add explicit on-demand secondary-provider action.
6. Replace fixed full-page polling with bounded fragment/state polling that stops on ready/failed and pauses while hidden.
7. Add controlled timing assertions matching the one-second shell/five-second progressive requirements.
8. Run focused tests and commit `perf(catalog): load primary discographies progressively`.

## Task 4: Add region-aware discovery and setting

**Files:** `app/metadata/base.py`, `app/metadata/deezer.py`, new `app/services/discovery.py`, `app/settings_service.py`, `app/routers/settings.py`, `app/templates/settings.html`, `app/routers/search.py`, `app/templates/search.html`, new `app/templates/discover_list.html`, shared card partials, `app/static/js/discovery.js`, `app/static/css/pages.css`, settings/discovery/search tests.

1. RED settings tests: US default, valid region persistence, invalid handling, unrelated-section preservation, and cache partition/invalidation.
2. RED discovery tests: chart artists, genres, new/trending releases, requested/effective region metadata, global fallback, stale-if-error, and independent section failure.
3. Add `RuntimeSettings.discovery_region` with a maintained code/label allowlist. Preserve it on unrelated settings saves.
4. Add provider-neutral artist/release/genre/section DTOs carrying provider IDs, artist routing identity, artwork, rank/date, requested/effective region, fallback, and sanitized state.
5. Implement bounded Deezer feeds and short-TTL cache keyed by provider/feed/region/page. Do not persist volatile chart rows merely for rendering.
6. Add Metadata setting UI with United States default and fallback explanation.
7. Empty `/search` renders four 12-card sections. Queried search remains above/replaces discovery consistently; Advanced remains unchanged.
8. Add `/discover/popular`, `/discover/genres`, `/discover/genres/{id}`, `/discover/new`, `/discover/trending` with bounded pagination.
9. Release cards route to artist discography and reuse in-place Watch artist; no download.
10. Commit `feat(discovery): add regional artist and release feeds`.

## Task 5: Integrate, review, and verify

1. Update `CHANGELOG.md` under `Unreleased`.
2. Run `git diff --check`; inspect for secrets, unsafe URLs/rendering, CSRF bypass, unbounded calls, and DB transactions spanning network I/O.
3. Run:
   - `/mnt/user/appdata/dev/audiohoard/.venv/bin/pytest`
   - `/mnt/user/appdata/dev/audiohoard/.venv/bin/ruff check .`
   - `/mnt/user/appdata/dev/audiohoard/.venv/bin/ruff format --check .`
   - `/mnt/user/appdata/dev/audiohoard/.venv/bin/mypy app`
   - `uv build`
4. L reviews `origin/main..HEAD` for specification, then correctness/security/performance. Route critical/important findings back through remediation and scoped re-review.
5. Jarvis inspects the real diff and reruns configured checks.

## Task 6: Push and produce acceptance artifact

1. Push feature branch, open/update PR, and verify CI against the intended commit.
2. Merge only after review/CI approval through the repository’s normal integration path.
3. Manually dispatch the existing Release workflow on `main`; verify it emits only approved `develop` for acceptance and no stable/`latest` publication.
4. Verify Docker Hub manifest digest and OCI revision label against the intended main commit.
5. Smoke a disposable fresh-data container on a distinct name/port: setup/login, Discover, Playboi search, in-place watchlisting/dialog, progressive discography, region change/fallback, and deep links. Use no production mounts.
6. Remove only session-created disposable resources. Do not alter production service lifecycle or production data.
7. Handoff literal: `Pull noplexzone/audiohoard:develop (sha256:...)`.
