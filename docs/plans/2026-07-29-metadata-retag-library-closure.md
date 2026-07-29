# Audiohoard Metadata Retag and Library Closure Bug Fix Plan

> **For Hermes:** Implement directly with TDD, then run quality gates and publish `noplexzone/audiohoard:develop` through CI.

**Goal:** Fix the live issues found in the Audiohoard/Navidrome audit: missing embedded artwork, stale grouping tags, legacy files invisible to retag, stale review cards, and whole-album duplicate Download Missing jobs.

**Architecture:** Keep the import/retag pipeline transactional and backup-first. Extend canonical tag writing to include artwork and album-grouping cleanup; add read-only legacy library discovery for retagging files that lack Audiohoard import rows; move review-card visibility to actionable items only; and restrict album download jobs to catalog tracks not already imported in Audiohoard.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy async, SQLite, Mutagen, pytest/ruff/mypy, Docker/GitHub Actions.

## Global Constraints

- Checkout: `/mnt/user/appdata/dev/audiohoard`.
- Running production container: `audiohoard`, image `noplexzone/audiohoard:develop`, app port `8001:8000`.
- Production database/media paths are not to be mutated during implementation unless Caleb explicitly approves a deployment/repair step.
- Caleb approved: embed/overwrite Audiohoard canonical artwork during import/repair, and cancel the duplicate The Party Never Ends 2.0 job if possible.
- Container restart still requires explicit approval; this plan only builds and publishes a new acceptance image.
- Testable handoff must publish `noplexzone/audiohoard:develop` via CI and report verified digest.

---

### Task 1: Extend canonical tag writing to artwork and Navidrome grouping cleanup

**Objective:** Imports and Repair Metadata overwrite canonical artwork and remove stale grouping tags that cause Navidrome album splits.

**Files:**
- Modify: `app/services/library_import.py`
- Test: `tests/unit/test_album_retag.py`

**Interfaces:**
- Produces: `CanonicalArtwork`/tag writer support for optional artwork bytes and MIME.
- Consumes: `CatalogAlbum.artwork_url`, `Release`/`Track` tag data.

### Task 2: Let album retag adopt filesystem-matched legacy library files

**Objective:** Repair Metadata can retag existing album files even when Audiohoard did not import them.

**Files:**
- Modify: `app/services/library_import.py`
- Test: `tests/unit/test_album_retag.py`

### Task 3: Remove stale non-actionable import-review cards

**Objective:** Downloads page only shows Import Review cards when there are pending review items or a concrete release action.

**Files:**
- Modify: `app/routers/jobs.py`
- Modify: `app/services/auto_import.py`
- Test: `tests/integration/test_downloads_ui.py`

### Task 4: Restrict album Download Missing to truly missing catalog identities

**Objective:** A new album download job must not redownload catalog tracks that are already imported with existing destination files.

**Files:**
- Modify: `app/routers/catalog.py`
- Modify: `app/jobs/runner.py`
- Test: `tests/unit/test_acquisition_closure.py` or router tests.

### Task 5: Verify, publish, and hand off

**Objective:** Ship a Caleb-testable acceptance image with focused tests, full checks, commit, push, CI publish, and verified Docker digest.
