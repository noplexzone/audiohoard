# Durable Batch Runaway Recovery Implementation Plan

> **For Hermes:** Use Claude Code or subagent-driven development to implement this plan task-by-task.

**Goal:** Prevent all-matching discography batches from losing hydration identity, implicitly reacquiring the same track repeatedly, creating concurrent duplicate work, or leaving thousands of cancelled rows in the ordinary Activity queue.

**Architecture:** Preserve immutable provider hydration context on each batch item, and treat each explicit item execution as a durable generation. Jobs linked during one generation are attempted at most once: terminal work without a verified artifact fails the item, while an explicit operator retry advances the generation and may create a replacement. Equivalent active batch scopes are rejected server-side. Cancellation remains non-destructive: pending batch-created jobs are cancelled and hidden, running and observed jobs continue normally, and all links remain historical evidence.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2 async, SQLite/WAL, Alembic, pytest, Ruff, mypy.

## Global Constraints

- Work only in `/mnt/user/appdata/dev/_scratch/audiohoard-batch-runaway` on `fix/batch-runaway-recovery`.
- Do not mutate the production database, running containers, provider queues, staging files, or music library from the implementation worktree.
- Never delete durable job, attempt, batch, link, provider, import, or review history as part of reconciliation.
- Running and observed jobs must survive pause/cancel controls; only pending jobs created by the controlled batch may be cancelled.
- Hydration provider selection must be exact, persisted before provider I/O, and revalidated behind the item lease. Do not infer provider identity from fuzzy title/artist matching.
- A terminal job is not acquisition success; only a verified present imported artifact closes a target.
- No provider HTTP, filesystem work, sleeps, or dispatch may occur while a SQLite transaction is open.
- Duplicate protection must hold across ordinary queueing, durable batches, startup recovery, watchdog recovery, explicit retry, and concurrent SQLite sessions.
- Explicit retry may create replacement work; automatic reconciliation may not silently create another attempt for a track already attempted in the current generation.
- Cancelled batch-created jobs should disappear from the ordinary Activity/download queue without deleting their rows or batch links.
- Add schema only through Alembic; prove upgrade/downgrade/upgrade and schema parity.
- Update `CHANGELOG.md` under `[Unreleased]`. Do not bump `0.26.0`, create a stable tag, publish `latest`, or mutate CI workflows.
- Required sequence: RED tests → minimal implementation → focused tests → full quality gate → independent review → Jarvis publication of `noplexzone/audiohoard:develop`.

---

### Task 1: Persist exact wanted-batch hydration context

**Objective:** Ensure every wanted batch item that requires hydration either carries an exact persisted provider release/artist identity or fails at materialization with an actionable non-retry loop state.

**Files:**
- Modify: `app/services/discography_batches.py`
- Modify if needed: `app/services/catalog_metadata.py`
- Modify if needed: `app/models/discography_batch.py`
- Create if schema is needed: `alembic/versions/0034_batch_attempt_fencing.py`
- Test: `tests/unit/test_discography_batches.py`
- Test: `tests/unit/test_discography_batch_runner.py`
- Test: `tests/unit/test_migration_0034.py`
- Test: `tests/unit/test_schema_parity.py`

**Steps:**
1. Add a failing test where `wanted_selected`/`wanted_all_matching` materializes an incomplete catalog album with an exact persisted provider release. Assert the item stores that provider release and `hydrate_discography_batch_item` reaches the exact provider rather than raising `batch hydration identity is no longer available`.
2. Add a failing test for an incomplete album with no exact persisted hydration source. Assert queue materialization does not create a retrying hydration item; it records a bounded actionable reason such as `hydration_provider_unavailable` and creates no jobs/provider calls.
3. Implement one batched provider-context selection query for all wanted album IDs. Prefer an exact provider-native snapshot already owned by the canonical album; rank deterministically by the artist/catalog primary provider contract and stable ID. Do not use title matching or per-row queries.
4. If existing columns cannot stamp the exact identity for all supported cases, add the minimum nullable immutable snapshot columns in migration `0034`; preserve old rows and fail closed when legacy context is insufficient.
5. Keep hydration provider I/O outside sessions and revalidate the exact lease, batch state, catalog album, provider namespace, artist identity, and provider-native album ID before storing metadata.
6. Run the focused tests and migration round trip.

**Proof:** The production-shaped wanted item follows the hydration path with exact identity, while an unresolvable item becomes one bounded actionable failure and cannot spin.

---

### Task 2: Fence implicit retries with durable item generations

**Objective:** Guarantee that one batch item generation attempts each exact catalog track at most once, while preserving explicit operator retry.

**Files:**
- Modify: `app/models/discography_batch.py`
- Modify: `app/services/catalog.py`
- Modify: `app/services/discography_batch_runner.py`
- Modify: `app/services/discography_batches.py`
- Modify: `alembic/versions/0034_batch_attempt_fencing.py`
- Test: `tests/unit/test_catalog_queue_expansion.py`
- Test: `tests/unit/test_discography_batch_runner.py`
- Test: `tests/unit/test_discography_batch_controls.py`
- Test: `tests/unit/test_migration_0034.py`

**Steps:**
1. Add a RED regression that models a multi-track item: one linked job becomes terminal without an artifact while sibling jobs remain active. Re-running the batch runner must not create another job for the terminal track; after siblings settle, the item must fail truthfully.
2. Add a RED file-backed SQLite regression with concurrent expansion/reconciliation and restart recovery. Require one active job per `(catalog_album_id, catalog_track_id)` and one created attempt per `(item, generation, catalog_track)`.
3. Add generation columns/defaults and indexes/constraints through migration `0034`. Backfill existing links into generation `1` (or the chosen initial generation) without deleting history.
4. Stamp created and observed links with the item’s current generation. During expansion, load links for target tracks in that generation before claim takeover: active linked work is observed; terminal linked work is treated as attempted and is never replaced automatically.
5. Reconcile item state using verified artifacts, current-generation active links, attempted terminal links, and unattempted targets. Only unattempted targets may be materialized. Terminal attempted targets without artifacts yield a stable failure reason.
6. Explicit `retry_discography_batch_items` advances the selected item generation and resets only that item. Pause/resume advances generation only for items whose undelivered created jobs were cancelled by the pause fence; normal restart recovery does not advance it.
7. Prove that repeating runner ticks, watchdog cycles, and startup recovery cannot create replacements in the same generation, while one explicit retry can create exactly one replacement.

**Proof:** A fixture equivalent to the incident’s repeated target ends with one created job per generation, zero concurrent duplicate active jobs, preserved historical links, and a successful explicitly authorized retry.

---

### Task 3: Reject equivalent active scopes and close cancellation visibility

**Objective:** Prevent duplicate active all-matching batches and keep cancelled batch work out of the ordinary Activity queue without deleting history.

**Files:**
- Modify: `app/models/discography_batch.py`
- Modify: `app/services/discography_batches.py`
- Modify: `app/services/activity.py` only if projection needs adjustment
- Modify: `alembic/versions/0034_batch_attempt_fencing.py`
- Test: `tests/unit/test_discography_batches.py`
- Test: `tests/unit/test_discography_batch_controls.py`
- Test: `tests/integration/test_discography_batch_ui.py`
- Test: `tests/unit/test_activity.py`

**Steps:**
1. Add RED concurrent-session tests that queue the same `scope_hash` twice. Require at most one batch in `queued`/`running`/`paused`; the loser receives a bounded domain result rather than creating another materialized batch.
2. Enforce the invariant at the database boundary where SQLite supports it, with service-level handling that returns the existing active batch safely. Do not rely on a pre-insert SELECT alone.
3. Update cancellation so pending jobs created by the batch become `cancelled` and `queue_hidden=true` in the same transaction. Running created jobs and every observed job remain untouched and visible under their ordinary lifecycle.
4. Keep item/job links and batch detail rows intact so cancellation remains auditable and selected retry remains possible.
5. Add Activity regressions proving thousands of cancelled batch-created jobs do not inflate active/issues counts or the ordinary queue, while batch detail still renders history.

**Proof:** Two concurrent equivalent submissions produce one active batch; cancelling it hides only its pending created jobs, preserves running/observed work, and leaves database history intact.

---

### Task 4: Production-shaped repair reporting and release verification

**Objective:** Provide a read-only incident classifier and prove the fixed code against a consistent production-shaped database copy before publishing.

**Files:**
- Create or modify a narrowly scoped maintenance/report module under `app/maintenance/` only if an existing report path cannot express the classification.
- Modify: `CHANGELOG.md`
- Test: corresponding unit/integration maintenance tests.

**Steps:**
1. Add a read-only report mode that uses SQLite URI `mode=ro`, emits stable aggregate counts, and never prints provider URLs, credentials, filenames, or secrets.
2. Create a consistent online backup of the live database only through the already approved maintenance boundary; mount it read-only with network disabled for dry-run proof.
3. Require `quick_check=ok`, `foreign_key_check` empty, zero duplicate active targets after simulated reconciliation, zero implicit same-generation replacements, and unchanged imported/present ownership counts.
4. Run focused tests, then `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, and `uv run mypy app`.
5. Run an independent fixed-diff review for spec compliance, data-loss/provider-cleanup risk, transaction boundaries, migration safety, and restart races. Resolve every critical/important finding.
6. Commit coherent Conventional Commits, push the branch, open/merge after green CI, and publish only `noplexzone/audiohoard:develop` through CI. Verify image revision and manifest digest, then smoke a disposable container with a fresh writable data mount. Do not restart production.

**Proof:** Full checks and independent review pass; the published `develop` image matches the intended commit and starts healthy on a fresh database.