# Download Queue Lifecycle Hardening Implementation Plan

> **For Hermes:** Use Claude Code as the focused implementer; Jarvis owns review, verification, publication, and runtime safety.

**Goal:** Make the Downloads page resilient to historical failure metadata, close completed album groups correctly, and prevent SQLite contention from permanently stranding source cleanup work.

**Architecture:** Normalize persisted failure metadata at the service boundary before rendering, reconcile queue visibility from durable album-level import completion rather than one attempt's status, and centralize cleanup reconciliation behind serialization/retry controls. Harden every SQLite connection with foreign keys, WAL, and a busy timeout. Keep historical orphan repair explicit, backup-first, and opt-in rather than silently destructive during startup.

**Tech Stack:** FastAPI, Jinja2, SQLAlchemy 2.x async, aiosqlite, Alembic, pytest, Docker.

## Global Constraints

- Do not restart, update, or mutate the running production `audiohoard` container during implementation.
- Do not modify production music or staging files.
- Preserve historical job records; completed album attempts should be hidden, not deleted.
- Close an album group only after every canonical catalog track has a durable imported track.
- Legacy malformed errors must never make Downloads fail.
- Cleanup must be idempotent, serialized, retryable after SQLite contention, and periodically reconciled.
- Orphan repair must be explicit, backup-first, and opt-in.
- Never expose credentials or connection strings.
- Publish only `noplexzone/audiohoard:develop`; never `latest` or a stable tag.

### Task 1: Normalize Downloads failure metadata

Modify `app/services/download_queue.py`, `app/jobs/runner.py`, and `app/templates/partials/_downloads_queue.html`; add regression tests. Accept dicts, JSON-encoded dicts/strings, bare strings, malformed JSON, and null/unknown values. Normalize at the service boundary, emit structured new failures, and remove unsafe template parsing.

### Task 2: Close fully imported album groups

Modify `app/services/acquisition_cleanup.py` and tests. Evaluate scoped albums across all attempts. Hide terminal jobs only when every `CatalogAlbumTrack.id` has an imported `Track.catalog_track_id`. Cover partial-root/successful-continuation completion plus missing and downloaded-only negatives.

### Task 3: Harden SQLite and cleanup reconciliation

Modify `app/database.py`, cleanup services, and lifecycle wiring. Enable foreign keys, bounded busy timeout, and WAL where supported. Serialize cleanup, retry transient SQLite contention with bounded backoff, and periodically revisit uncompleted durable cleanup records. Ensure network waits do not retain write transactions.

### Task 4: Add explicit backup-first orphan repair

Add an operator maintenance command and isolated tests. Dry-run by default; explicit apply required; require safe exclusion from a running writer; create and verify a timestamped SQLite backup; delete only orphan staging-review rows; run integrity checks and report counts. Document that production repair requires stopping Audiohoard and separate approval.

### Task 5: Verify and publish

Run focused/full quality gates, migration and fresh-state Docker smoke tests, independent blocking review, PR merge, CI publication, pushed digest/revision verification, and fresh-state runtime health checks. Do not restart production; provide the verified `develop` pull line and separate repair gate.
