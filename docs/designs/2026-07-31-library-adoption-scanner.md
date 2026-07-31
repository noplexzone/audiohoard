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

Extend `app/services/library_scan.py` into filesystem discovery plus transactional adoption. Scopes cover the full library, a catalog artist ID, a catalog album ID (including singles/EPs), and an imported-only artist optionally narrowed by release title/year.

Filesystem work runs before database writes: enumerate supported files without following symlinks, resolve beneath `library_root`, read tags/duration via Mutagen in a worker thread, and retain folder/filename evidence. A process-local async lock serializes scans. Every insertion rechecks destination ownership for idempotence.

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

For a high-confidence catalog match, prefer an existing matching Track, especially one whose plan was lost. Otherwise create a hidden completed `Job(source="library")`, imported Release, and Track. Create an imported/present ImportPlan whose source and destination are the safe library path and whose staging path is null. Synchronize catalog IDs, identity, metadata, size/format, and recompute album `in_library` truth. Commit one scan atomically; rollback affects only database rows because files are never modified.

Results include matched, adopted, ambiguous, orphan, missing, and scanned counts plus bounded details.

## HTTP and UI

Authenticated, CSRF-protected background POST actions:

- `POST /maintenance/scan`
- `POST /maintenance/scan/artists/{artist_id}`
- `POST /maintenance/scan/albums/{album_id}`
- `POST /maintenance/scan/imported-artist`

Redirects return to the originating page with queued status; complete details remain on Maintenance.

## Safety and verification

No filesystem mutation, external metadata calls, symlink following, outside-root paths, or ambiguous adoption. Tests discriminate exact, ambiguous, contradictory, idempotent, and scoped behavior. Full pytest/Ruff/mypy, independent review, Docker runtime, and browser smoke gates apply before release.
