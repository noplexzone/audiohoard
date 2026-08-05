# Library Navigation and Deezer Release Condensation Plan

> **For Hermes:** Implement directly because Claude Code authentication is expired; preserve TDD and independent review gates.

**Goal:** Make the artist Library page fast and collapse updated Deezer snapshots into the richest single release without merging real editions.

**Architecture:** Replace per-artist correlated scalar queries with reusable grouped aggregate subqueries joined to a paginated artist identity set. Add query-aligned indexes in one Alembic migration. Normalize same-provider Deezer discography snapshots before upsert and add an idempotent startup reconciliation that keeps the largest-track release while safely re-homing canonical album state.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy async ORM, SQLite/WAL, Alembic, pytest.

## Global Constraints

- Preserve Library filtering, sorting, pagination, legacy artist rows, provider selection, and all card counts.
- Condense Deezer rows only when artist identity, normalized title, year, release kind, edition markers, and content rating identify the same release family.
- Keep clean and explicit releases distinct.
- Keep Deluxe, Remaster, Anniversary, Expanded, and other title-distinct editions separate.
- Keep providers separate and prefer the highest non-null track count within a Deezer family.
- Existing production data must repair idempotently; future refreshes must not recreate smaller snapshots.
- Preserve imported tracks, jobs, monitoring state, provider state, and the richer manifest.
- Do not modify the running container or production database during implementation.

### Task 1: Query regression tests and indexes

Add production-scale Library tests and migration upgrade/downgrade tests for indexes on catalog album artist, catalog track album, and import-plan track/state lookups.

### Task 2: Aggregate Library artist query

Build aggregate subqueries once, form catalog and legacy artist identity projections, count that lightweight union, paginate it, then join card aggregates for only the selected rows. Run focused catalog tests and compare all existing count semantics.

### Task 3: Deezer snapshot condensation

Add a pure family key and discography compactor for future Deezer fetches. Add idempotent database reconciliation for existing same-family Deezer rows, selecting the largest manifest/release and using the canonical album merge machinery without over-merging ratings, providers, or edition titles. Invoke reconciliation during startup and after Deezer refreshes.

### Task 4: Verification and delivery

Update `CHANGELOG.md`, run the full quality gate, independently review, commit/push the feature branch, merge after green CI, and publish the test artifact through the repository workflow without touching the running container.
