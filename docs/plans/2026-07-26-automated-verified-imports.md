# Automated Verified Imports

**Branch:** `feat/automated-verified-imports`
**Date:** 2026-07-26

## Problem summary

1. **slskd file cap bug**: 108 files across 7 peers were flattened to individual `SearchResult` records; the 10-result cap then sliced only tracks 1–10 from the first MP3 folder.
2. **Catalog matching failure**: `_catalog_track_for_result` used exact casefolded equality. Filenames like `01 Intro`, `10 Karma` did not bind to catalog titles `Intro`, `Karma (skit)`.
3. **No automatic continuation**: After a partial job there was no follow-up; manual retry resubmitted the full search and hit the same 10-file cap.
4. **No quality profile**: Only TIDAL had a quality setting; no global format preference or bitrate floor.
5. **Manual import only**: Users had to visit the Import tab to plan and execute after download.
6. **AcoustID not gating import**: Fingerprint ran but never compared against expected recording MBID before import.

## Architecture

Single durable acquisition→verification→import state machine. No parallel manual path.

```
Job (acquisition) → per-folder candidate scoring → enqueue best album folder
                 → per-track: download → fingerprint → AcoustID verify
                 → if all tracks verified: auto plan + execute import
                 → if mismatch / unavailable: StagingReviewItem (pending)
                 → if partial: continuation job for remaining catalog tracks
```

## New models

### `StagingReviewItem` (new table: `staging_review_items`)
- `id`, `track_id → tracks`, `release_id → releases`
- `expected_recording_mbid`, `expected_title`
- `observed_acoustid_mbids_json` (list of MBIDs from lookup)
- `fingerprint_duration_sec`, `acoustid_score` (best confidence score)
- `verification_reason`: `"mismatch"`, `"unavailable"`, `"no_fingerprint"`
- `review_state`: `"pending"`, `"approved"`, `"denied"`
- `reviewed_at`, `created_at`

### Additions to existing models
- `Track.acoustid_verification_state`: `"pending"`, `"verified"`, `"mismatch"`, `"unavailable"`, `"approved"`, `"denied"`
- `Track.acoustid_evidence_json`: full AcoustID response snapshot
- `Job.parent_job_id → jobs` (nullable FK): links continuation jobs
- `Job.partial_attempt`: int (retry counter, default 0)

### New `ImportWorkflowState` values
- `verifying` — fingerprint + AcoustID in progress
- `verified` — AcoustID confirmed (or approved by deterministic evidence)
- `review_needed` — sent to StagingReviewItem

## Key changes by file

### `app/metadata/filename_parse.py`
Add `normalize_for_catalog_match(title: str) -> str`:
- Strip leading track-number prefix (same regex as `_TRACK_PREFIX_RE`)
- NFKD Unicode normalize, strip combining diacritics
- Replace fancy apostrophes → `'`, smart quotes → `"`
- Casefold, collapse whitespace

Add `strip_non_identity_descriptor(title: str) -> str`:
- Remove bracketed suffixes that are pure descriptors: `(skit)`, `(interlude)`, `(outro)`, `(intro)`, `(skit)`, `(prod. …)`, `(feat. …)`
- Do NOT strip descriptors that distinguish identity (e.g., `(live)`, `(acoustic)`, `(demo)` — these change the recording)

### `app/sources/slskd.py`
Add `AlbumFolder` dataclass: `username, parent_dir, format, files: list[SlskdFile]`

Add `search_album_folders(query, catalog_album) -> list[AlbumFolder]`:
- Call existing search endpoint with `fileLimit=500`
- Group results by `(username, backslash-normalized parent directory, audio_extension)`
- Drop non-audio files (artwork, cue, log, etc.)
- Return sorted by score (see scoring below)

### `app/services/slskd_scoring.py` (new)
`score_album_folder(folder, catalog_album, quality_profile) -> float`:
- +30 release identity: folder name contains artist + album (normalized)
- +25 completeness: `len(folder.files) == catalog_track_count`
- +10 format preference: matches top preference in quality profile
- +8 bitrate floor: avg bitrate >= `min_mp3_bitrate` (for MP3)
- -5 per missing track vs expected count
- -2 if folder is partial (fewer files than expected)

### `app/jobs/runner.py`
- Replace `results[:limit]` with folder-grouped selection when `catalog_album_id` is set.
- Update `_catalog_track_for_result` to use normalized + descriptor-stripped matching; fall back to position-based matching.
- After download: call `_run_fingerprint` (already done) then new `_verify_acoustid(track, cfg)`.
- After all tracks: if all `acoustid_verification_state in {verified, approved}` → call `_auto_import_release(release, db, cfg)`.
- If `partial`: record `missing_catalog_track_ids_json` in `job.result_json`, spawn continuation `Job` with `parent_job_id`.

### `app/services/acoustid_verification.py` (new)
`verify_against_catalog(track, acoustid_mbids) -> tuple[state, reason]`:
- If `track.mbid` set and acoustid_mbids contains `track.mbid` → `"verified"`
- If `track.mbid` set but no matching MBID → `"mismatch"`
- If no acoustid result but `track.identity_state == resolved` and `track.mbid` set → `"unavailable"` (needs review unless flag set)
- If no acoustid result and no expected MBID → `"unavailable"` (needs review)

### `app/services/auto_import.py` (new)
`auto_import_release(db, release, cfg)`:
- Call `plan_release_import(db, release, library_root=cfg.library_root, ...)`
- If all plans are `ready` → call `execute_release_import(db, release, library_root=cfg.library_root)`
- If any plan `needs_review` → leave release in `needs_review` state
- Update release `import_state` accordingly

### `app/settings_service.py`
Add `quality_profile_json` to `RuntimeSettings`:
- `format_preference: list[str]` default `["flac", "mp3", "m4a", "aac"]`
- `min_mp3_bitrate: int` default `192`
- `allow_lower_quality_fallback: bool` default `True`
- Stored as `quality_profile` key in `app_settings`

### `app/routers/settings.py` + `app/templates/settings.html`
Add `"quality"` section to `SETTINGS_SECTIONS`.

### `app/routers/imports.py`
Remove UI routes: `/ui/review`, `/ui/releases/{id}/plan`, `/ui/releases/{id}/execute`.
Keep JSON API routes: `GET /plans`, `POST /releases/{id}/plan`, `POST /releases/{id}/execute`.

### `app/routers/staging.py` (new)
`GET /staging/audio/{item_id}` — authenticated, validates containment under `staging_root`, rejects symlinks, serves with `Range` support.
`POST /staging/review/{item_id}/approve` — approve a `StagingReviewItem`.
`POST /staging/review/{item_id}/deny` — deny.

### `app/templates/base.html`
Remove Imports nav link (`/imports/ui/review`) from sidebar and mobile nav.

### `app/templates/downloads.html`
Add "Pending review" section below the downloads table. Show:
- Release + expected track title + artist
- Expected MBID vs observed AcoustID MBIDs
- Audio player (`<audio controls src="/staging/audio/{item_id}">`)
- Approve / Deny forms

## Migrations

`alembic/versions/0014_automated_verified_imports.py`:
- `ALTER TABLE tracks ADD COLUMN acoustid_verification_state VARCHAR(32) NOT NULL DEFAULT 'pending'`
- `ALTER TABLE tracks ADD COLUMN acoustid_evidence_json TEXT`
- `ALTER TABLE jobs ADD COLUMN parent_job_id INTEGER REFERENCES jobs(id) ON DELETE SET NULL`
- `ALTER TABLE jobs ADD COLUMN partial_attempt INTEGER NOT NULL DEFAULT 0`
- `CREATE TABLE staging_review_items (...)` (all non-null with good defaults)
- Non-destructive; existing rows get defaults.

## Execution order

1. Migration (schema foundation)
2. `normalize_for_catalog_match` + catalog matching fix (AC3)
3. `slskd` folder grouping (AC1) + scoring service (AC2)
4. Quality profile in `settings_service` + settings UI (AC5)
5. `acoustid_verification.py` service + `Track` verification fields (AC7/8)
6. `auto_import.py` service (AC11)
7. Partial continuation via continuation jobs (AC4)
8. `staging.py` router — audio serving + review mutations (AC9/10)
9. UI: remove Import tab, add review section to downloads (AC6/13)
10. Tests (AC14)
11. CHANGELOG (AC15)

## Test strategy

`tests/unit/test_slskd_folder_grouping.py`:
- 108-file response → grouped → top FLAC folder selected → no 10-file truncation
- All 15 catalog IDs bound correctly using normalized matching
- Non-audio files ignored

`tests/unit/test_catalog_title_normalize.py`:
- `"01 Intro"` normalizes to match `"Intro"`
- `"06 Betrayal"` normalizes to match `"Betrayal (skit)"` (strip descriptor)
- Unsafe fuzzy (different track titles) do NOT match

`tests/unit/test_acoustid_verification.py`:
- Match → `verified`
- MBID mismatch → `mismatch`, StagingReviewItem created
- No result + known MBID → `unavailable`, StagingReviewItem created

`tests/unit/test_auto_import_pipeline.py`:
- All tracks verified → auto plan + execute called
- Mismatch track → release stays in `needs_review`
- Approve → resumes import
- Deny → file stays staged, track `denied`
- No continuation job spawned for already-verified tracks

`tests/unit/test_staging_audio.py`:
- Valid item → serves with Range support
- Traversal attempt → 400
- Symlink → 400
- Non-audio extension → 400
- Unauthenticated → 401
