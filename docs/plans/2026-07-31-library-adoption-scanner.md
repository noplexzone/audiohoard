# Library Adoption Scanner Implementation Plan

> **For Hermes:** Use Claude Code to implement task-by-task; Jarvis independently inspects, tests, reviews, publishes, and verifies.

**Goal:** Adopt high-confidence audio already under `/music`, restore missing database links, expose full-library/artist/release actions, and stop showing staging provenance as the current path.

**Architecture:** Preserve comparison-only scanning and add persisted scan/candidate models, a recoverable runner, Mutagen evidence extraction, fail-closed catalog/imported-record matching, and snapshot-verified Track/ImportPlan adoption. Authenticated server-rendered routes enqueue scoped scans; files are never modified.

**Tech Stack:** Python 3.12+, FastAPI, async SQLAlchemy 2.x, SQLite, Mutagen, Jinja, pytest, Ruff, mypy.

## Global Constraints

- Exact actions: **Scan full library**, **Scan artist**, **Scan release**.
- Albums, singles, and EPs share the release scanner.
- Scanning never moves, renames, retags, overwrites, or deletes audio.
- Never follow symlinks or accept paths outside `library_root`.
- Adopt only high-confidence matches; ambiguous/contradictory files remain orphans.
- `ImportPlan.destination_path` is current location; staging/source paths are provenance only.
- Scans are idempotent, serialized, and make no external metadata calls.
- Worktree: `/mnt/user/appdata/dev/_scratch/audiohoard-library-adoption-scanner`.
- Baseline at `f129f0fdb0d1827976156123914f554a8b6be61d`: 908 tests passed.

### Task 1: Core adoption scanner

**Files:** add `app/models/library_adoption.py`, `app/metadata/audio_file.py`, `app/services/library_adoption.py`, `app/services/library_adoption_runner.py`, and migration `0023`; modify `app/services/library_scan.py`; test `tests/unit/test_library_adoption.py` and schema parity.

Add tagged audio fixtures and RED tests for exact adoption, lost-plan repair, idempotence, artist/album scope isolation, complete-album truth, duplicate release ambiguity, contradictory MBID/title/position, unknown release, and symlink/outside-root rejection. Implement safe evidence extraction, deterministic release/track matching, hidden completed library jobs/releases only when needed, present imported plans, and catalog truth recomputation. Run focused tests. Commit `feat(library): adopt matched library files during scans`.

### Task 2: Full and scoped actions

**Files:** modify `app/routers/maintenance.py`, `app/templates/index.html`, `app/templates/maintenance.html`, `app/templates/catalog_artist.html`, `app/templates/catalog_album.html`, `app/templates/artist_detail.html`, `app/services/maintenance_state.py`; test `tests/integration/test_dashboard.py`, `tests/integration/test_maintenance.py`, `tests/unit/test_artist_release_ui_contracts.py`.

Add RED tests for auth, CSRF, background dispatch arguments, safe redirects, exact button copy, and scope fields. Implement full, catalog-artist, catalog-album, and imported-artist POST routes. Add the three exact actions and maintenance adopted/ambiguous reporting. Run focused tests. Commit `feat(ui): add full and scoped library scan actions`.

### Task 3: Authoritative path presentation

**Files:** modify `app/services/catalog.py`, `app/routers/tracks.py`, `app/templates/library_tracks.html`, `app/templates/track.html`; test catalog and track integration suites.

Add RED tests proving a staged source without a present plan never becomes Library path. Remove source fallback from `_track_file_path`; project the newest present destination into track detail; label **Library path** and **Original source**. Run focused tests. Commit `fix(library): show authoritative library file paths`.

### Task 4: Review, release, and artifact gates

Update `CHANGELOG.md`. Run `uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run mypy app`, `git diff --check`, and inspect the full diff. Obtain independent review and remediate Critical/Important findings. Build/run a disposable container with tagged orphan files and browser-test full/artist/release scans plus path display. Push branch, open PR, wait for CI, merge preserving history. Per `docs/VERSIONING.md`, this feature requires the next MINOR release. Push the annotated tag, wait for release CI, pull `noplexzone/audiohoard:develop`, and verify revision/version labels and manifest digest. Do not restart production without separate approval.
