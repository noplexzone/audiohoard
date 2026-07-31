# Library UI, Playback, and File Management Design

**Status:** Approved for implementation

## Outcome

Audiohoard's Library becomes an artwork-first, direct-manipulation catalog. Artist cards truthfully distinguish complete, partial, and unknown release ownership. Artist and album/single/EP pages share one visual system. Imported tracks can be auditioned without Navidrome, removed safely, and reconciled when removed externally.

## Product contract

1. Overhaul `/library`, `/artists/catalog/{id}`, `/albums/{id}`, and legacy imported-only artist detail.
2. Artist cards show complete downloaded releases versus total known releases. A release is complete only when every expected catalog track is locally present. Partial releases are shown separately. Unknown denominators stay explicitly unknown.
3. Release headers use a compact toolbar: **Download missing** primary; **Repair metadata** and **Clean quality duplicates** grouped as maintenance; **Monitor for upgrades** autosaves without a Save button. Destructive actions retain confirmation.
4. Imported tracks play through a fixed global bottom player with play/pause, seek, previous/next, title, artist, and artwork. Playback is uninterrupted across internal Audiohoard navigation.
5. Compatible formats use authenticated byte-range streaming. Unsupported formats are transcoded on demand by ffmpeg into a bounded, seekable cache.
6. Each imported track exposes file format, size, source/state, and confirmation-gated permanent removal. Whole-release bulk removal is required. No rename/move UI.
7. Any authenticated user may play/remove. Mutations retain existing CSRF enforcement; no new admin role.
8. In-app removal updates filesystem, persisted state, counts, progress, and UI as one user-visible action. External deletion is detected by a watcher plus startup/periodic reconciliation.
9. Removing the last file preserves catalog metadata and makes the track/release reacquirable.
10. Desktop and 360px layouts must remain accessible, responsive, and visually direct rather than CRUD-heavy.

## UI

Artist cards remain artwork-first and show only artist name, `N of M releases`, optional `P partial`, a thin ownership bar, and a small watchlist/library badge. Imported-only artists show `N local releases · total unknown` rather than a fabricated percentage.

Artist pages use one hero and discography grid for catalog-backed and imported-only artists. Catalog-only controls appear only when a catalog entity exists. Release cards show year/type and complete/partial/missing state.

Album/single/EP pages use a cover-led hero and compact action toolbar. The track list is the main interaction surface: present rows have Play and file details/removal; missing rows have Download. Bulk removal is visually separated and confirmation-gated.

## Uninterrupted navigation

The `<audio>` element and player live outside `#main-content` in `base.html`. A progressive History API controller intercepts only safe, unmodified same-origin HTML GET links, fetches the next document, parses it, replaces `#main-content`, updates title/nav/badges/history, and dispatches `audiohoard:page-unload` / `audiohoard:page-load`. Page scripts expose idempotent initializers and cleanup hooks.

It preserves query strings, fragments, popstate, focus, live announcements, and scroll behavior; aborts superseded requests; and hard-navigates on auth, fetch, parse, or shell-contract failure. It never intercepts forms, media/download endpoints, external/new-tab/modifier clicks, or links marked `data-hard-navigation`. Native navigation/forms remain the fallback.

## Persisted file truth

Add persisted import-plan file state: `unknown`, `present`, `missing`, `removed`, plus last-check/removal metadata. Existing imported rows migrate to `unknown`; bounded reconciliation verifies them. Successful imports write `present`. Library queries, playback, and ownership require a latest imported plan in `present` state. Historical Track/Catalog entities remain when state becomes missing/removed.

Ownership aggregates are manifest-aware: complete when a known denominator is fully present, partial when present count is between zero and denominator, missing when zero, and unknown when no trustworthy denominator exists. Page reads use bounded database queries, not library-wide filesystem walks.

## Streaming

`GET|HEAD /library/tracks/{track_id}/audio` accepts a track ID only and resolves the latest present destination server-side. Validation requires an absolute regular file under the effective library root, rejects every symlink component/final symlink, rejects unsupported extensions/empty files, and never discloses host paths.

Direct responses support one strict byte range, 206/416 semantics, `Accept-Ranges`, `Content-Length`, `Content-Range`, ETag, MIME, HEAD, and descriptor cleanup. Invalid/multi-range requests return 416 rather than silently becoming full responses.

For `?transcode=mp3`, the cache key includes canonical file identity, size, mtime, and profile. ffmpeg writes a bounded temporary MP3 then atomically renames. Per-key locks prevent duplicate work; a global semaphore bounds concurrent transcodes; timeout/cancellation removes temporary files. LRU cleanup bounds count and bytes. Completed MP3s use the same range service for seeking. Cache lives under app data, never the music library. Existing staging playback keeps its behavior while reusing safe helpers where practical.

## Permanent removal and crash consistency

Use a durable deletion-operation journal. Validate every selected path before any bulk mutation. For each operation:

1. Commit a `prepared` journal row with import-plan ID and original/generated same-directory temporary paths.
2. Under a path lock, atomically rename to the hidden temporary name and fsync the directory.
3. In one DB transaction mark plans `removed`, record reason/time, recompute catalog ownership, and mark journal `committed`.
4. After commit unlink and fsync, then mark `finalized`.
5. On DB failure restore original names. If final unlink fails, user-visible state remains removed and cleanup retries.

Startup recovery idempotently restores moved files when DB state is still present, or finalizes committed removals. Bulk release removal uses one operation group and restores every precommit rename on failure. No operation follows symlinks or leaves the library root.

## External removal detection

Use `watchfiles` on the effective library root. Debounce/coalesce delete and move-out events, ignore Audiohoard temporary/in-flight paths, map paths to present plans in bounded queries, mark confirmed absences `missing`, and recompute affected ownership in short transactions. Watcher failure degrades without stopping the app and cancellation propagates cleanly.

The watcher is an accelerator. Startup and periodic sweeps use durable checkpoint/last-check ordering, bounded batches, and per-batch commits so unreconcilable low-ID rows cannot starve later files.

## Security and acceptance

- Existing authentication protects media/library routes; existing mutation dependency protects removal/autosave.
- User errors contain no literal paths, ffmpeg stderr, or secrets.
- Navigation receiving login HTML hard-navigates instead of inserting it into the shell.
- Confirmation names track/release and file count, not host paths.
- Database lock retries are bounded and journal evidence remains truthful.

Acceptance requires accurate complete/partial/unknown counts; compact autosaving actions; seekable direct/transcoded playback; uninterrupted playback through navigation/back/forward; safe single/bulk deletion with rollback/crash recovery; prompt external-deletion reflection; preserved/reacquirable catalog entries; native fallback; and clean desktop/360px browser console, network, focus, and overflow checks.
