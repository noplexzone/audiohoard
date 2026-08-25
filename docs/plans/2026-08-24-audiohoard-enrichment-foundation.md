# Audiohoard Enrichment Foundation Implementation Plan

> **For Hermes:** Execute slice-by-slice with implementation, independent review, and Jarvis verification/publication.

**Goal:** Close compilation-credit, denial, bulk-acquisition, genre-discovery, and Discover-UX defects in that priority order, then publish an acceptance-ready `develop` image.

**Architecture:** Preserve one acquisition pipeline. Use one catalog-to-library credit projection; persist exact denial identity before continuation; expand durable bulk batches through ordinary jobs; derive genre artists from semantically scoped chart tracks; rebuild Discover as a poster-first Operate surface.

**Tech Stack:** Python 3.12, FastAPI, async SQLAlchemy, SQLite/Alembic, Jinja2, vanilla JS/CSS, pytest, Playwright, Ruff, mypy, Docker/GitHub Actions.

## Global Constraints

- Workspace: `/mnt/user/appdata/dev/_scratch/audiohoard-enrichment-20260824`, branch `feat/enrichment-foundation`.
- Production container/database remain read-only. No restart or production mutation without separate approval.
- Backups precede destructive library/database work; no automatic destructive legacy repair.
- Denials block exact provider identity, never title alone. slskd identity is provider + peer + normalized remote path.
- Verified tracks import immediately; completeness controls continuation.
- Bulk work uses the existing dispatcher and configured concurrency bound.
- Preserve authentication, CSRF, native forms, escaped data, stable URLs, and mobile navigation.
- TDD every behavior change; independent review every slice.
- Frontend work requires Impeccable detector and bounded desktop/mobile Playwright verification.
- Publish only `noplexzone/audiohoard:develop` plus immutable commit artifact. Never `latest` or stable tags.

## Task 1 — Compilation artist-credit closure

**Files:** `app/services/catalog_artist_credits.py`, `app/services/library_import.py`, runner/adoption only if required, compilation and album-retag tests, new real-file tag tests, `CHANGELOG.md`.

Create one projection where track artist prefers `CatalogAlbumTrack.artist_name`, album artist prefers `CatalogAlbum.album_artist_name`, and catalog-owner fallback is conservative. Make import synchronization, tag writing, retagging, destination rendering, and legacy discovery consume it.

**Acceptance:** three distinct compilation credits survive real `plan_release_import()`; FLAC/MP3/Ogg tags read back correctly; retag preserves unrelated metadata/artwork; ordinary albums remain unchanged; existing files are not auto-mutated.

## Task 2 — Durable denial and exact blocking

**Files:** staging router, source-candidate-block service, runner selection, truthful review/blocklist UI as needed, staging/acquisition/blocklist tests, changelog.

Canonicalize exact candidate identity across provenance, attempt rows, blocks, and selection. Commit denial plus block before continuation. Distinguish `source_blocked` from `source_identity_unavailable`.

**Acceptance:** exact slskd artifact cannot be reselected; separator/root variants normalize safely; blocked member excludes whole album folder; missing provenance is truthful; allowing one source removes only its explicit block.

## Task 3 — High-volume admission and artifact-missing stability

**Files:** runner, download-queue presentation/policy, focused SQLite/slskd tests, changelog.

Transient pending-to-running SQLite contention must leave/requeue work with bounded backoff, not create non-retryable terminal failure. Repeated `artifact_missing` for one target must stop at a small bound and surface a structured provider/staging diagnostic rather than burning 8–10 candidates.

**Acceptance:** deterministic file-backed contention test proves eventual admission without replay; non-lock failures remain terminal; missing-artifact churn is bounded; later retry resumes safely.

## Task 4 — Durable scoped discography batches

**Files:** migration/models, catalog service/router, artist template, focused JS/CSS, migration/unit/integration/browser tests, changelog.

Persist batch scope and batch items. Preview matching, complete, active, hydration-required, missing, skipped, and estimated jobs. Expand idempotently through ordinary dispatcher jobs.

**Acceptance:** preview before queue; no duplicate active track work; truthful batch states; pause/cancel pending only; scoped retry; Wanted page/all-matching semantics preserved.

## Task 5 — Genre discovery semantics

**Files:** Deezer adapter, discovery service/router if needed, discovery tests, changelog.

Fetch `/editorial/{genre_id}/charts`; derive artists from `tracks.data`; dedupe by artist ID preserving order; validate artists; reject unknown taxonomy IDs; retain bounded cache/stale behavior.

**Acceptance:** Pop/Rap sequences differ; nonexistent genres do not return global artists; dedupe/pagination are stable; HTTP-200 errors and semantic collapse show unavailable state.

## Task 6 — Poster-first Discover redesign

**Files:** establish `PRODUCT.md`/`DESIGN.md`; Discover templates/partials; page CSS; discovery JS; integration/browser tests; changelog.

Compact search; Trending, Genres, New releases, Popular artists; Advanced/manual search separate. Cards show artwork, identity, one state, View/Watch. Each section owns loading/stale/empty/error/retry states.

**Acceptance:** no nested-card/action clutter; desktop/mobile edge states pass; forms/CSRF/no-JS/routes preserved; region copy truthful; Impeccable detector clean.

## Task 7 — Integration and acceptance artifact

Run full non-browser/browser tests, Ruff lint/format, mypy, package build, Impeccable detector, disposable fresh-state Docker smoke, and independent whole-branch review. Push through PR/CI, verify `noplexzone/audiohoard:develop` revision/digest, and provide the literal pull line. Production restart remains permission-gated.
