# Import Pipeline Reconciliation Implementation Plan

> **For Hermes:** Execute task-by-task in the isolated worktree with one writer and fixed-commit review between slices.

**Goal:** Repair Audiohoard's bulk import pipeline so acquisition concurrency is truthful, slskd polling is bounded/coalesced, SQLite transitions recover safely, artifacts are checkpointed and cleaned exactly once, and retained downloads can be reconciled without weakening identity or ownership rules.

**Architecture:** A dispatcher permit owns one complete job through provider queue/polling and terminal work; provider pressure is reduced independently by a configuration-keyed slskd download-snapshot single-flight with bounded 429 backoff. Later slices add short rollback-safe DB transitions, exact artifact fencing, dry-run-first historical identity reconciliation, and validated same-catalog ownership repair. The complete repair must pass copied-production-database proof with read-only media before publication.

**Tech Stack:** Python 3.12+, asyncio, FastAPI, SQLAlchemy 2.x async, aiosqlite/SQLite, httpx, pytest/pytest-httpx, Ruff, mypy, Alembic.

## Global Constraints

- Sole worktree: `/mnt/user/appdata/dev/_scratch/audiohoard-import-pipeline`, branch `fix/import-pipeline-reconciliation`, base `c8b7a5395ef234a3b0efce985364f4cdbda5fba4`.
- Keep `max_parallel_acquisitions` configurable across 1–16. Never hard-code/recommend 2 or clamp valid values.
- The value bounds complete in-flight jobs, including provider queue/poll time. Increases wake waiters; decreases cancel nothing and admit nothing until active usage is below the new limit.
- Coalesced slskd snapshots never cross endpoint/credential configurations and preserve exact transfer-ID plus username+filename fallback matching.
- HTTP 429 is sanitized/retryable with bounded exponential backoff and jitter. Never log credentials, query parameters, or unsanitized bodies.
- Provider HTTP, sleeps, fingerprinting, filesystem probes, and expensive file work stay outside SQLite transactions.
- MusicBrainz recording identity and strict title+duration evidence remain authoritative; ambiguous, contradictory, mismatched, or unavailable evidence stays review-gated.
- Never overwrite a destination owned by another catalog identity/edition.
- Do not modify production/live DB, access Docker, restart containers, push, publish, tag, bump versions, delete files, or print secrets in implementation slices.
- Later publication requires Jarvis verification; only CI `develop` may publish then. No stable tag without Caleb approval.

## File Responsibility Map

- `app/jobs/dispatcher.py`: whole-job resizable concurrency.
- `app/jobs/runner.py`: provider lifecycle, checkpoints, artifact outcomes, reconciliation entry points.
- `app/sources/slskd.py`: keyed polling coalescing, exact lookup, sanitized 429 backoff.
- `app/services/acquisition_cleanup.py`: active-reference cleanup fencing.
- `app/services/acquisition_recovery.py` or a dedicated service: bounded historical reconciliation.
- `app/models/*`, `alembic/versions/*`: durable state only if existing schema cannot express it.
- Focused unit/integration tests named in each task; `CHANGELOG.md` records only implemented behavior.

---

### Task 1: Truthful Whole-Job Concurrency

**Objective:** Hold one dispatcher permit for the entire job, including slskd queue/polling, while preserving runtime 1–16 resizing.

**Files:** Modify `app/jobs/dispatcher.py`, `app/jobs/runner.py`; test `tests/unit/test_job_dispatcher.py` and relevant runner polling tests.

**Interfaces:** Consume `dispatch()`, `_run_with_limit()`, `set_max_concurrent_jobs()`, `_poll_slskd_transfer()`; produce one permit from runner entry to terminal completion with no queued-state yield.

**Steps:**
1. Replace the yielding test with a failing regression: at limit 1, first job enters queue/polling, second does not start, then starts only after first terminal completion.
2. Run it and record RED showing current code admits job two.
3. Preserve/add increase/decrease assertions: increase wakes immediately; decrease cancels nothing and blocks waiters while active usage is at/above the new limit.
4. Remove queued release/reacquire behavior and obsolete context-local lease helpers; `_run_with_limit()` remains sole permit owner.
5. Run the regression and dispatcher concurrency tests GREEN, then focused runner tests.

**Commit:** `fix(jobs): bound complete acquisition concurrency`

### Task 2: Coalesced slskd Snapshots and 429 Backoff

**Objective:** Share one short-TTL slskd download snapshot per endpoint/credential configuration and retry 429 with deterministic-testable bounded exponential backoff/jitter.

**Files:** Modify `app/sources/slskd.py`; test `tests/unit/test_slskd.py`; preserve `tests/unit/test_slskd_transfers.py`.

**Interfaces:** `downloads()`/`status()` produce a process-local cache keyed by normalized URL plus non-reversible credential discriminator, one in-flight fetch per key, success-only TTL, and injected/private clock/sleep/jitter seams.

**Steps:**
1. Add concurrent status tests with a blocked mocked GET: same-config callers receive distinct exact/fallback matches from one upstream request.
2. Test reuse within TTL, refresh after injected-clock expiry, and isolation across endpoint/API-key configurations.
3. Test an upstream failure reaches current callers but is not cached; a later call makes a fresh successful request.
4. Test 429 then success with injected sleep/jitter, no real delay, bounded exponential delays, and no key/query/body secret in errors.
5. Implement narrow-lock single-flight election; followers await outside the lock; cache only successful flattened snapshots; remove failed in-flight work.
6. Implement bounded transfer-list 429 handling and sanitized retryable exhaustion. Preserve exact UUID and fallback matching.
7. Ensure destructive `cancel()` uses a fresh/bypassed snapshot or invalidation so stale cache cannot target replacement work.
8. Run new tests RED/GREEN plus existing UUID/fallback, envelope, cancellation, and compound-terminal coverage.

**Commit:** `fix(slskd): coalesce transfer polling with backoff`

### Task 3: Changelog and First-Slice Gate

**Objective:** Document only Tasks 1–2 and verify the exact first slice.

**Files:** Modify `CHANGELOG.md` under `[Unreleased]`.

**Steps:**
1. Add `Fixed` entries for whole-job limits and coalesced/backed-off slskd transfer polling; do not bump versions.
2. Run the exact focused pytest, Ruff lint, Ruff format, and mypy commands from the task brief.
3. Run `git diff --check`, inspect the full base-to-HEAD diff for secrets/debug/out-of-scope work, require a clean tree.

**Commit:** Fold changelog into Task 2 or use `docs: record import pipeline polling fixes`.

### Task 4: Rollback-Safe SQLite Retries (Later Slice)

**Objective:** Retry runner/watchdog/cleanup short state transitions without reusing failed transactions or duplicating external work.

**Files:** `app/jobs/runner.py`, `app/jobs/dispatcher.py`, `app/services/acquisition_cleanup.py`, shared SQLite helper if present; tests in job and SQLite suites.

**Steps:**
1. Inventory pending→running, progress, terminal, watchdog, and cleanup commits and define idempotency guards.
2. Write lock-injection tests: first commit fails, rollback occurs, rows reload in a fresh transaction/session, intended state commits once.
3. Implement bounded jittered DB-only retries; never retry provider enqueue/DELETE or file deletion as part of the DB loop.
4. Prove transactions close before HTTP/sleep and errors stay truthful after exhaustion.
5. Commit `fix(database): retry acquisition state transitions safely`.

### Task 5: Exact Artifact Checkpoint and Cleanup Fencing (Later Slice)

**Objective:** Persist exact staged paths and one durable `artifact_missing` outcome/block per provider artifact while cleanup cannot touch active references.

**Files:** `app/jobs/runner.py`, `app/services/acquisition_cleanup.py`, models/migration only if required; runner, cleanup, slskd integration tests.

**Steps:**
1. Add crash/restart tests around enqueue ID, exact staged discovery, verification/import handoff, and cleanup.
2. Checkpoint the exact contained regular staged path in a short transaction before fingerprint/import work.
3. Define immutable provider artifact identity and an idempotent uniqueness/merge rule for one missing outcome/block.
4. Atomically claim cleanup and query running tracks, reviews, and import plans; retain obligation when referenced.
5. Recheck claim/version after I/O; repeated reconciliation becomes a no-op after durable success.
6. Prove replacement transfers/referenced files are untouched; commit `fix(import): checkpoint and fence staged artifacts`.

### Task 6: Historical `no_expected_mbid` Reconciliation (Later Slice)

**Objective:** Add bounded starvation-free dry-run-first repair that re-evaluates retained files using current strict evidence and executes only exact plans selected by that run.

**Files:** `app/services/acquisition_recovery.py` or dedicated service, authenticated maintenance surface, import planner/executor; focused service/route tests.

**Steps:**
1. Scope safe retained regular files; missing/unsafe files receive unavailable outcomes, never approval.
2. Add a two-run starvation regression with an unreconcilable low-ID row before a valid row and a smaller batch bound.
3. Load candidates then close DB work; fingerprint/requery outside transactions. Accept only high-confidence recording results whose titles all normalize to target and measured duration is sane.
4. Persist evidence/outcome in short retries. Mismatch, ties, ambiguity, low confidence, duration outliers, and provider unavailability remain review-gated.
5. Apply only an explicitly approved dry-run set and execute exact newly eligible plan IDs, never every ready release plan.
6. Prove idempotent second run and immediate per-track import independent of album completeness.
7. Commit `fix(import): reconcile retained identity reviews safely`.

### Task 7: Same-Catalog Destination Ownership (Later Slice)

**Objective:** Resolve collisions only when a present contained destination is valid and belongs to the same catalog track; preserve different identities/editions.

**Files:** import planning/execution and ownership/adoption services selected by inspection; collision/adoption/catalog tests.

**Steps:**
1. Test same-track/same-edition present destination, missing/changed destination, different track, clean-vs-explicit sibling, and same-title different release.
2. Validate containment, regular-file presence, supported audio, and catalog identity before mutation.
3. Reconcile current ownership only for exact same identity; retain provenance separately.
4. Send different identities/editions and untracked files to review/version-aware planning; never overwrite.
5. Prove idempotency; commit `fix(import): reconcile validated destination ownership`.

### Task 8: Copied-Production Proof and Independent Review (Later Gate)

**Objective:** Prove the complete repair without changing live state, then gate any push/`develop` publication on Jarvis.

**Steps:**
1. With later explicit approval, create a consistent copied DB including WAL; never open live source read-write. Record sanitized source integrity metadata.
2. Mount staging/library read-only and disable provider cleanup/network mutation; run quick-check and repair dry run on the copy.
3. Apply to another disposable copy, compare predicted/actual categories, rerun and require no duplicate side effects.
4. Verify live source unchanged and zero provider cleanup requests.
5. Run `uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run mypy app`.
6. Obtain independent fixed-commit review for races, key isolation, retry idempotency, cleanup safety, identity evidence, and edition boundaries; remediate and rerun.
7. Only after Jarvis verification may the branch be pushed and CI publish `develop`. No local publication or stable tag without Caleb approval.

## Exact First-Slice Verification

```bash
uv run pytest tests/unit/test_job_dispatcher.py tests/unit/test_job_runner.py tests/unit/test_slskd.py tests/unit/test_slskd_transfers.py tests/unit/test_database_sqlite.py tests/integration/test_slskd_import_pipeline.py -q
uv run ruff check app/jobs/dispatcher.py app/jobs/runner.py app/sources/slskd.py tests/unit/test_job_dispatcher.py tests/unit/test_job_runner.py tests/unit/test_slskd.py
uv run ruff format --check app/jobs/dispatcher.py app/jobs/runner.py app/sources/slskd.py tests/unit/test_job_dispatcher.py tests/unit/test_job_runner.py tests/unit/test_slskd.py
uv run mypy app
git diff --check c8b7a5395ef234a3b0efce985364f4cdbda5fba4..HEAD
```

Also inspect status, base-to-HEAD stat/full diff, and commit list. Completion requires a clean tree, exact RED/GREEN evidence, all checks passing, and exact Conventional Commit IDs. Do not push or publish.
