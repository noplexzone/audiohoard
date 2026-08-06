# Evidence-Based Import Review Automation

## Goal

Reduce Audiohoard's approval-heavy import-review queue without allowing AcoustID score alone to override target identity.

## First implementation slice

This slice activates only the strongest new rule: an exact Deezer catalog-track preview must acoustically match the downloaded file. It also persists automation decisions for later policy evaluation. AcoustID-only conflict overrides remain shadow evidence and do not authorize import in this slice.

## Global constraints

- A high AcoustID score alone never overrides an expected-MBID conflict.
- Only pending `mismatch` and `no_expected_mbid` review items with a catalog-bound track and non-empty observed AcoustID MBIDs are eligible.
- Exact Deezer identity comes from the catalog album's `deezer_id` plus authoritative disc/position hydration. The fuzzy `Track.deezer_id` enrichment field must not authorize approval.
- The exact provider title, album artist, and selected provider-track artist must match the catalog/target identity after punctuation normalization and stripping only non-identity descriptors. Live, acoustic, remix/mix-year, instrumental, featured-artist, clean/explicit, and similar identity-bearing qualifiers must remain significant.
- Provider duration and downloaded fingerprint duration must each be within four seconds of the catalog target when those durations are known.
- Every observed MBID considered for track assignment must have its own persisted score strictly above the current configured AcoustID threshold; an aggregate score from another recording cannot qualify it. All automatic verification, preview approval, and legacy backlog recovery use one strict parser that rejects the entire payload when any member is malformed, any MBID is invalid or duplicated, any score is boolean, non-numeric, non-finite, overflowing, or outside `[0, 1]`, or the canonical MBID keyset differs from `observed_acoustid_mbids_json`.
- Preview alignment must return `confidence == "high"`; medium, ambiguous, missing, or failed alignment never approves.
- Provider HTTP, preview download, and fingerprint subprocess work occurs outside a database write transaction.
- Transient provider/alignment failures are bounded and retryable. Identity contradictions are terminal review decisions, not retries.
- Every claim creates an append-only sanitized attempt record; stale claims are closed as abandoned. Never persist or log signed preview URLs.
- On approval, preserve the original expected MBID and evidence revision. Bind one observed MBID only when exactly one independently qualified MBID remains; if several remain, clear the conflicting track-level MBID rather than choosing arbitrarily. Never rewrite `catalog_album_tracks.recording_mbid` in this slice.
- Revalidate the staging-contained source digest/stat/path and catalog album/track identity immediately before approval. Copy the source through the hashing descriptor into a sealed Linux `memfd`, run acoustic alignment through the immutable snapshot's `/proc/<pid>/fd/<fd>` view, and revalidate the original pathname again before committing approval. This fences both pathname replacement and same-inode overwrite/restore ABA races.
- Manual approval/denial and automation application serialize through short SQLite `BEGIN IMMEDIATE` transactions so a committed operator decision cannot be overwritten by stale automation.
- Approval atomically creates durable import-dispatch work; manual approval hashes the reviewed staging artifact before committing and never invokes the importer directly. Dispatch claims use ownership tokens, a bounded importer timeout, a longer stale lease, and token-fenced completion. Immediately before import, revalidate the approved source digest/stat/path, restrict the release importer to that review's track, and pass the approved path/hash into planning and descriptor-pinned copy verification so sibling tracks or substituted bytes cannot ride the approval. Cancellation runs the same pinned-filesystem rollback as execution failure before propagating, preventing untracked published files. Dispatch failures retry on later scheduler cycles with bounded backoff and append-only outcomes.
- Keep approved review rows as audit history while removing them from the pending queue.
- Import only through the existing verified `try_auto_import_release` path.
- Process existing and future pending reviews through a cancellable background scheduler. Use short per-row claims/commits and bounded work per cycle; do not block app startup.
- Do not mutate the production database during development or verification.
- Publish only `noplexzone/audiohoard:develop`; do not restart the running Audiohoard container without Caleb's approval.

## Architecture

1. Add an exact catalog-position Deezer resolver that hydrates `CatalogAlbum.deezer_id`, selects one authoritative `(disc, position)` track, validates identity and duration, and returns its current preview without fuzzy search.
2. Add durable automation claim/evidence/import-dispatch fields to `staging_review_items` plus append-only `review_automation_attempts` through Alembic revision `0026`.
3. Add a review-automation service that claims one row, snapshots versioned database and source evidence, releases the transaction, performs provider/alignment I/O, then applies the result with a pending-state/token/evidence/catalog/source fence.
4. A high-confidence exact-preview match marks the review approved, updates track evidence and safe track-level MBID handling, and atomically queues durable import dispatch. Existing auto-import then runs outside that write transaction and is retried periodically when needed.
5. Add a small cancellable scheduler to process bounded batches immediately after startup and periodically thereafter. Transient retries use persisted attempt counts and next-attempt timestamps.
6. Extend AcoustID evidence with sanitized per-recording score/title/artist information so strict AcoustID-consensus candidates can be logged in shadow mode for future evaluation, but cannot approve in this slice.

## File responsibility map

- `app/services/reference_audio.py`: exact catalog-position Deezer reference resolution.
- `app/services/acoustid_verification.py`: persist sanitized high-confidence recording evidence; existing verification behavior remains unchanged.
- `app/models/staging_review.py`: durable automation fields/properties.
- `alembic/versions/0026_review_automation.py`: schema migration and indexes.
- `app/services/review_automation.py`: claims, pure evidence checks, provider/alignment execution, retry/final decisions, safe MBID override, auto-import dispatch, scheduler.
- `app/main.py`: scheduler lifecycle wiring only.
- `tests/unit/test_reference_audio.py`: exact resolver behavior and rejection cases.
- `tests/unit/test_acquisition_closure.py`: enriched sanitized evidence without changing existing decisions.
- `tests/unit/test_review_automation.py`: RED/GREEN tests for approval, fail-closed behavior, retries, claims, audit evidence, MBID handling, shadow output, and scheduler cancellation.
- migration/schema tests as required by the repository's existing conventions.
- `CHANGELOG.md`: Unreleased entry.

## Acceptance tests

1. A fuzzy `Track.deezer_id` without a catalog-album Deezer ID cannot approve.
2. Exact album/disc/position, matching identity, score above threshold, close duration, and high-confidence alignment approves and invokes existing auto-import.
3. Medium or ambiguous alignment remains pending for human review.
4. Provider-title or identity-qualifier contradiction remains pending and does not run alignment when detectable before it.
5. Duration contradiction remains pending and does not run alignment.
6. Transient provider/alignment failure records bounded retry state without approving.
7. A unique observed MBID replaces only the track-level conflicting MBID and records the original.
8. Multiple observed MBIDs clear the track-level conflicting MBID and do not alter catalog MBID.
9. A manual decision or changed claim cannot be overwritten by a stale automation result.
10. Existing no-expected-MBID behavior, M4A/MP4 review previews, mobile FLAC review previews, and manual approve/deny behavior remain green.
11. Migration upgrades a legacy SQLite database and downgrades cleanly.
12. Full pytest, Ruff lint, Ruff format check, mypy, build, CI, and published-container smoke checks pass.
13. Mixed per-MBID scores and threshold equality fail closed; evidence refresh invalidates an in-flight claim.
14. A true two-session manual-denial race leaves the denial authoritative.
15. Source/catalog swaps fail closed, and a failed import succeeds on a later scheduler cycle without restart or duplicate dispatch.
