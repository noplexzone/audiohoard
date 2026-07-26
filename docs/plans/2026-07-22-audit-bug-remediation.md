# Audiohoard Audit Bug Remediation Plan

> **For Hermes:** Implement task-by-task with Claude Code, then independent specification and quality review.

**Goal:** Correct every concrete functional, reliability, security, accessibility, and documentation defect verified by the 2026-07-22 audit without expanding into roadmap features.

**Architecture:** Keep FastAPI/SQLAlchemy/SQLite/Jinja. Add a durable in-process dispatcher backed by persisted job state, restart reconciliation, isolated monitoring transactions, and source-specific completion handling. Reuse existing workflow/import abstractions; no external broker.

**Scope boundary:** This fixes verified defects. It does not add a full library scanner, recommendations, player, playlists, multi-user quotas, broad quality-profile UI, or new providers.

## Acceptance criteria

1. slskd and Prowlarr/SAB jobs stay active after enqueue, poll to terminal state, locate completed files, and cannot report `done` merely because enqueue succeeded.
2. Pending/running jobs reconcile after restart and are idempotently claimed so duplicate workers cannot process one job.
3. Auto-download monitoring dispatches jobs; one artist/provider failure cannot terminate the scheduler or roll back other artists.
4. YouTube/TIDAL outputs and importer-supported formats agree; unsupported formats fail early with actionable errors.
5. Album requests are represented/verified at release level, not as one catalog track per Prowlarr NZB/arbitrary result.
6. Successful verified imports update catalog ownership so Wanted converges.
7. Track identity requires track evidence; album MBID alone does not resolve a track.
8. Browser auth failures redirect to login with a safe return path; APIs retain JSON 401.
9. Queue UI shows partial/progress/stage/errors/retry/cancel/refresh, with dashboard links.
10. Import execution requires a current valid plan; browser failures render actionable HTML.
11. Monitoring labels describe actions; settings priority can be reordered; settings tests are unambiguous.
12. Search/download fields are labeled, key targets are 44px, mobile retains all destinations, and semantics/loading/error states are accessible.
13. Public liveness is cheap; detailed cached readiness is authenticated; Docker health reflects readiness.
14. Version/docs/Compose/CLAUDE constraints match v0.6. CSS token/contrast defects are fixed.
15. Behavior changes have regression tests; full CI gates, package build, Docker build, and fresh-state smoke pass.

## Task 1: Durable dispatcher and restart recovery

**Files:** `app/main.py`, `app/jobs/runner.py`, `app/routers/jobs.py`, `app/routers/catalog.py`, job models/migration if required, tests.

- Add a single in-process dispatcher that atomically claims persisted jobs and tracks active tasks.
- Reconcile stale running/pending jobs on startup; repeated dispatch is idempotent.
- Add valid cancel/retry service and router operations.
- Preserve structured failure/retry details.
- TDD restart recovery, duplicate prevention, cancel, and retry.

## Task 2: External-client completion lifecycle

**Files:** runner, source adapters, workflow services/models, tests.

- Separate enqueue from completion.
- Poll slskd and SAB/Prowlarr with bounded interval/timeout and terminal-state mapping.
- Resolve real completed paths under staging roots and reject traversal/out-of-root paths.
- Keep job/track state truthful; do not mark done until artifacts are present and ready for import planning.
- Test success, failure, timeout, missing artifact, cancellation, and partial completion.

## Task 3: Release-level acquisition and identity

**Files:** runner, release/candidate services/models, tests.

- Keep strict track requests separate from album requests.
- Treat album provider results as release candidates; enumerate completed release files and match the expected catalog manifest.
- Remove album-MBID-as-track-resolution shortcut.
- Preserve album MBID/year/track count in Release records.
- Test NZB/release handling, incomplete releases, and unresolved tracks.

## Task 4: Format compatibility and ownership convergence

**Files:** YouTube/TIDAL adapters, `library_import.py`, catalog/import services, tests.

- Define one supported-audio-extension contract.
- Make acquisition output and import tag/readback support agree through safe native support, normalization, or early rejection.
- After successful import, reconcile `CatalogAlbum.in_library`/completeness from linked tracks.
- Test each accepted extension and Wanted disappearance.

## Task 5: Monitoring resilience

**Files:** `artist_monitoring.py`, lifecycle wiring, tests.

- One bounded session/transaction per artist.
- Persist/log per-artist failures and keep the loop alive.
- Dispatch auto-download jobs after commit through the durable dispatcher.
- Deduplicate wanted jobs.
- Test failing artist followed by success, task survival, dispatch, and dedupe.

## Task 6: Authentication and health

**Files:** auth/main/health routers, Compose, tests.

- HTML-aware auth redirect with validated local `next`; preserve API JSON 401.
- Cheap `/health/live`; authenticated cached readiness/provider diagnostics with meaningful status codes.
- Docker healthcheck uses correct semantics.
- Add safe expired-session cleanup.
- Test open-redirect rejection, browser/API split, liveness, diagnostics, and degraded readiness.

## Task 7: Queue/dashboard/import/monitoring UI

**Files:** templates/CSS and job/import/catalog routers/tests.

- Render `partial`, auto-fit dashboard state layout, link counts/rows.
- Queue details: error, stage, progress, update time, retry/cancel, polling/refresh and `aria-live`.
- Gate Import on current valid plan; show file/collision/tag summary; confirm mutation; redirect failures with alerts.
- Rename active monitor action to **Stop monitoring** and fake refresh to **View discography** unless made real.
- Add sign-out control.
- Add route/form/render regression tests first.

## Task 8: Settings and accessibility

**Files:** settings/search/download/base templates, CSS, settings routers/services, tests.

- Reorder source priority with no-JS up/down controls.
- Test entered connection values or clearly enforce save-before-test.
- Label all fields; add pending/duplicate-submit protection and inline setup errors.
- Keep Artists/Monitored/Wanted/Imports discoverable on mobile.
- Improve mobile queue/import/search layouts.
- Add `aria-current`, table scopes, breadcrumbs, alerts, 44px targets, valid tokens, and contrast.
- Add focused HTML/form tests.

## Task 9: Documentation drift

**Files:** README, Compose, base template, CLAUDE.md, CHANGELOG, version rendering.

- Render package version dynamically.
- Correct stale v0.1/v0.5 constraints and examples to current v0.6 behavior.
- Document queue recovery/health changes and add all fixes under Unreleased.

## Task 10: Review and verification

- Independent specification review, then quality/security review.
- Remediate critical/important findings and re-review.
- Run separately: `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy app`, `uv build`, Docker build.
- Fresh-state disposable smoke: setup, login, queue, settings, health, restart recovery.
- Conventional commits, push, green CI, merge, then CI-publish and verify `noplexzone/audiohoard:develop` before handoff.
