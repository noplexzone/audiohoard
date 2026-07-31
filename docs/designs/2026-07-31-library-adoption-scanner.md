# Library Adoption Scanner Design

## Problem

Audiohoard stores acquisition provenance in `Track.source_path`/`Track.staging_path`, while `ImportPlan.destination_path` is the authoritative current library file. The tracks UI can fall back to acquisition provenance when a present plan is absent. The existing scanner only reports orphan and missing paths; it cannot rebuild a lost connection or adopt a valid file already under `/music`.

## Required outcome

- Dashboard: **Scan full library**.
- Catalog and imported-only artist pages: **Scan artist**.
- Album, single, and EP pages: **Scan release**.
- Scans restore high-confidence database ownership links for safe regular audio beneath `library_root`.
- Ambiguous/contradictory files remain orphans; scanning never guesses, moves, retags, overwrites, or deletes.
- UI labels `ImportPlan.destination_path` as **Library path** and retains staging only as **Original source** provenance.

## Architecture

Keep `app/services/library_scan.py` as comparison-only verification and add `app/services/library_adoption.py` for adoption. Scopes cover the full library, a catalog artist ID, a catalog album ID (including singles/EPs), and an imported-only artist optionally narrowed by release title/year.

Filesystem work runs before short database writes: skip already-owned destinations, enumerate supported files without following symlinks, resolve beneath `library_root`, read tags/duration and hash candidates in a worker thread, and retain folder/filename evidence. A process-local async lock serializes scans. Persisted queued/running states recover interrupted work, and every insertion rechecks file snapshots and destination ownership for idempotence.

## Matching

Catalog adoption fails closed.

Release identity, strongest first:
1. Exact MusicBrainz release/release-group ID.
2. Exact normalized artist + album, with year agreeing whenever both sides provide it.
3. Exact canonical artist/release folder identity when tags are incomplete.

Track identity, strongest first:
1. Exact recording MBID within the matched release.
2. Exact disc/position plus normalized title.
3. Unique normalized title with no contradictory supplied position.
4. Exact canonical filename position/title evidence.

Conflicting MBID, title, position, artist, album, or year evidence is ambiguous. For imported-only scopes, adoption is restricted to an existing Track/Release identity; arbitrary folders do not create unknown releases.

## Persistence

Persist `LibraryAdoptionScan` and `LibraryAdoptionCandidate` rows so progress and unresolved evidence survive restarts. For a high-confidence catalog match, prefer an existing matching Track, especially one whose plan was lost. Otherwise create one hidden completed `Job(source="library_adoption")` and imported Release per catalog release batch, then create Tracks. Create imported/present ImportPlans whose source and destination are the safe library path and whose staging path is null. Synchronize catalog IDs, identity, metadata, size/format, and recompute album `in_library` truth. Candidate commits are bounded and snapshot-verified; rollback affects only database rows because files are never modified.

Results include scanned, adopted, review, unmatched, stale, and error counts with persisted candidate details.

## HTTP and UI

Authenticated, CSRF-protected background POST actions:

- `POST /maintenance/scan`
- `POST /maintenance/scan/artists/{artist_id}`
- `POST /maintenance/scan/albums/{album_id}`
- `POST /maintenance/scan/imported`

Redirects return to the originating page with queued status; complete details remain on Maintenance.

## Safety and verification

No filesystem mutation, external metadata calls, symlink following, outside-root paths, or ambiguous adoption. Tests discriminate exact, ambiguous, contradictory, idempotent, and scoped behavior. Full pytest/Ruff/mypy, independent review, Docker runtime, and browser smoke gates apply before release.
