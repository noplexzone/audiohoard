# Library Release Progress and Queue Closure Implementation Plan

> **For Hermes:** Use the `claude-code` skill to implement this plan task-by-task.

**Goal:** Replace duplicate artist-detail library sections with release-level ownership progress, group one album's acquisition attempts into one operational download, and remove completed/timed-out work from both Audiohoard and slskd queues automatically.

**Architecture:** Treat `CatalogAlbum` as the stable release identity shared by catalog cards, album details, tracks, and jobs. Compute release progress from catalog track manifests plus successfully imported track artifacts, and project it into existing Albums / Singles & EPs cards rather than maintaining separate downloaded-file and wanted-release lists. Project jobs into catalog-album groups for queue display while preserving individual job rows for audit/retry internals; use non-destructive `queue_hidden` for automatic Audiohoard cleanup and idempotent provider cancellation for slskd cleanup.

**Tech Stack:** FastAPI, async SQLAlchemy 2.x, Jinja2, SQLite/Alembic, pytest, Ruff, mypy.

## Global Constraints

- Remove the redundant artist-detail sections for downloaded files and wanted albums/releases.
- Existing Albums and Singles & EPs release cards show `downloaded / wanted` track progress.
- Selecting an album opens its album detail page.
- Album detail shows release progress and per-track downloaded/wanted state.
- Downloads related to the same catalog album release are presented as one group even when separate searches/jobs were needed.
- Completed and timed-out work disappears automatically from Audiohoard's active download queue.
- Completed and timed-out slskd transfers are removed idempotently from slskd.
- Preserve durable metadata, audit rows, retry state, imported files, review-required work, and non-terminal jobs; do not cascade-delete job/release/track history.
- Library ownership requires a committed imported artifact with a non-empty destination and positive file size.
- Work only in `/mnt/user/appdata/dev/_scratch/audiohoard-library-overhaul` on `feature/library-overhaul`.
- Do not push, publish, tag, delete unrelated files, modify running containers, or print secrets during implementation.

---

### Task 1: Release ownership projection and unified artist cards

**Objective:** Provide one reusable release-progress projection and render it on the existing Albums / Singles & EPs cards while deleting the duplicate downloaded-files and wanted-releases sections.

**Files:** `app/services/catalog.py`, `app/routers/catalog.py`, `app/templates/catalog_artist.html`, optional `app/static/css/style.css`, and `tests/integration/test_catalog.py`.

**Interfaces:** Produce release progress containing expected/wanted track count, imported/downloaded track count, and truthful state for each displayed catalog release. Consume `CatalogAlbumTrack`, `Track.catalog_album_id`, `Track.catalog_track_id`, and the successful-artifact predicate.

**Steps:**
1. Add failing tests proving partial, complete, and zero-manifest releases report correct progress and staging-only files do not count.
2. Implement a set-based or bounded projection that avoids async lazy loads and N+1 provider calls.
3. Remove `downloaded-files` and `wanted-releases` sections from artist detail.
4. Render progress on existing Albums / Singles & EPs cards and retain direct album-detail links.
5. Run focused tests and Ruff on touched Python.

### Task 2: Album detail progress and per-track status

**Objective:** Make album selection open a detail page that shows total progress and every wanted track's imported/downloaded state.

**Files:** `app/routers/catalog.py`, `app/templates/catalog_album.html`, optional CSS, and `tests/integration/test_catalog.py`.

**Interfaces:** Consume Task 1 progress; produce `/albums/{album_id}` total progress and a catalog-track-to-imported-artifact status map.

**Steps:**
1. Add failing route tests for partial totals, imported/missing indicators, and card navigation.
2. Explicitly load catalog tracks and imported artifact state without lazy loads.
3. Render concise total progress and per-track state while preserving download actions for missing tracks.
4. Run focused tests.

### Task 3: Release-grouped download queue

**Objective:** Present every job/continuation belonging to one album release as one queue item, including attempts and aggregate progress.

**Files:** `app/routers/jobs.py`, `app/templates/downloads.html`, an optional small projection module, `tests/integration/test_downloads_ui.py`, and relevant dispatcher unit tests.

**Interfaces:** Group by `catalog_album_id` when present, otherwise root parent chain, otherwise individual job ID. Aggregate distinct wanted catalog tracks and distinct imported/downloaded identities while retaining attempts for diagnostics.

**Steps:**
1. Add failing tests with two independent jobs sharing one `catalog_album_id` plus a continuation and prove one queue group renders.
2. Confirm unrelated jobs remain separate and actions target an appropriate attempt.
3. Implement deterministic grouping/status/progress without mutating history.
4. Render one album-level item with expandable attempts.
5. Run focused tests.

### Task 4: Automatic terminal queue cleanup

**Objective:** Automatically hide completed and timed-out jobs in Audiohoard and remove their slskd transfers idempotently after durable state commits.

**Files:** `app/jobs/runner.py`, `app/jobs/dispatcher.py` and/or `app/services/acquisition_cleanup.py`, `app/sources/slskd.py` only if needed, plus acquisition/dispatcher/slskd/download tests.

**Interfaces:** Audiohoard cleanup sets `Job.queue_hidden=True` only after terminal completion/timeout is durable. slskd cleanup treats already-absent transfers as success. Review-required, retryable ordinary failures, and active continuations remain visible.

**Steps:**
1. Add failing tests proving completion and timeout hide entries only after commit while ordinary failure/review remains visible.
2. Prove completion/timeout removes slskd transfer rows idempotently.
3. Implement post-commit hiding/provider cleanup without holding SQLite transactions during provider I/O.
4. Ensure startup recovery retries durable cleanup debt without starvation.
5. Run focused tests.

### Task 5: Documentation and full verification

**Objective:** Record and prove the behavior.

**Files:** `CHANGELOG.md` under `Unreleased`; `README.md` only if existing documentation becomes false.

**Steps:**
1. Document unified progress, grouped acquisitions, and automatic queue cleanup.
2. Run `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy app`, and `uv build`.
3. Run `git diff --check`, inspect the full diff, and obtain independent specification/quality review.
4. Remediate blockers, rerun checks, commit, push, merge through the normal path, publish `noplexzone/audiohoard:develop`, and verify its manifest digest before handoff.
