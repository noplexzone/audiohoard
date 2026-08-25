# Changelog

All notable changes to Audiohoard are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed

- Derive Deezer genre discovery artists from exact genre radio tracks instead of provider endpoints that silently return the same global chart for every genre, with strict provider ID parsing and truthful pagination after exact artist validation.
- Defer SQLite-locked job admissions without crossing the provider boundary, and retain exact completed slskd transfers for retry when their staged artifact is temporarily missing.
- Canonicalize namespace-aware exact slskd peer/path identities so review denial durably blocks the provider artifact before re-acquisition, legacy denied provenance is backfilled once, containing album folders are excluded, and Allow or Allow and retry truthfully makes the exact source selectable without rewriting denied history.
- Serialize startup database maintenance, catalog ownership, library reconciliation, and each scheduler's initial cycle before recovering queued jobs, preventing post-update SQLite writer storms from failing recovered work.
- Skip expensive orphan-history pruning when startup finds queued or interrupted acquisitions, allowing serialized job recovery to resume downloads promptly after an update.
- Invalidate cached slskd download snapshots after a successful enqueue so accepted transfers are not immediately misclassified as missing and redundantly downloaded from every candidate.
- Retry callers awaiting a displaced slskd snapshot generation so concurrent queue mutations cannot return a stale pre-enqueue result.
- Preserve provider-native album and track artist credits for compilation releases through import planning, destination naming, tag write/readback, retagging, and conservative legacy-folder repair while leaving existing folders in place.

### Added

- Add a shared poster-first Discover surface with responsive artist, release, and exact-genre cards, truthful state badges, safe native watch controls, and explicit feed continuation.
- Add read-only provider artist previews, exact provider-identity Discover card state, and safe in-context native watchlist returns without mutating navigation GETs.
- Load Discover landing feeds as independent authenticated fragments with truthful cached, pending, empty, stale, and error states so provider latency no longer blocks the page shell.
- Add durable scoped discography batch previews and native status controls for artist watchlists and Wanted selections, pages, and server-rerun matching scopes while reusing ordinary bounded acquisition jobs.
- Add Wanted-page queue-all-matching bulk dispatch and quality-profile enabled-format controls for strict MP3-320-or-better acquisition.

### Fixed

- Hide automatic Retry controls for non-retryable download failures and show explicit restart/watchdog recovery notices in the Downloads queue.
- Run imported-source cleanup from startup in the background so stale cleanup obligations cannot keep the WebUI port closed during application startup.
- Defer startup cleanup scheduling until the app has finished starting so cleanup debt cannot run before the WebUI becomes reachable.
- Delay startup imported-source cleanup work briefly after readiness so it cannot monopolize the event loop before Uvicorn starts serving.
- Stop scheduling imported-source cleanup during the startup lifespan; periodic cleanup handles it after the WebUI is live.
- Run startup library-file reconciliation in the background so large libraries cannot block the WebUI port.
- Run startup database maintenance and job recovery in background tasks so they cannot block WebUI readiness.

## [0.25.0] - 2026-08-10

### Added

- Add deterministic Playwright coverage for provider setup, discovery and monitoring, contextual Manual search, Wanted queueing, import review decisions, rejected-source recovery, Activity navigation, and mobile layout.

- Turn Wanted into a state-aware acquisition work queue with server-side filtering, persistent attempt/failure/review context, contextual Manual search links, and an accurately bounded **Queue this page** action.
- Rename the blocklist UI to Rejected Sources, add pagination and operational context, safe allow/retry controls, and reversible temporary-failure cooldown fields with capped exponential retry delays.
- Add a task-oriented Activity hub with aggregate Wanted, download, review, and rejected-source counts plus shared tabs and actionable navigation badges.
- Add a Settings overview with actionable readiness warnings, task-oriented sections, reusable provider cards, bounded Save and test, configured-root permission checks, and progressive unsaved/inline feedback.
- Add Discover and contextual Manual search with explicit source controls, deterministic evidence scoring, safe stable-ID grouping, and monitored-artist discovery context.

### Changed

- Restrict Settings UI and mutations to administrators, constrain provider submissions to provider-specific fields, reject unsafe provider URL forms, avoid unauthenticated Activity aggregation, and use current acquisition/rejection/review state in Wanted and Manual search.
- Restrict import-review comparison audio to exact Deezer track or album-position identities, persist complete cache provenance, refresh expired exact references, and make the review UI explicit when no verified clip exists.
- Delete denied staged files only after same-directory quarantine and a successful database commit, restoring the file when the transaction fails.

## [0.24.0] - 2026-08-10

### Added

- Persist every slskd candidate attempt with canonical transfer UUID and content-bound artifact metadata, and suppress redundant exact-release acquisitions without conflating other editions.
- Add disabled-by-default, explicitly mounted slskd complete/incomplete empty-directory sweeping with live-transfer and age safeguards, plus a bounded report-only JSON reconciliation command.

- Add provider-scoped release-family monitoring with durable per-edition overrides, explicit-first defaults, unknown fallback, clean-off defaults, and startup reconciliation for existing artists.
- Automatically approve import-review mismatches only when the exact catalog-position Deezer preview produces a high-confidence acoustic match, with per-MBID score evidence, source/catalog revalidation, operator-precedence fencing, append-only audit attempts, and safe track-level MBID overrides.
- Persist and retry approved auto-import dispatches with bounded backoff and duplicate-dispatch claims so transient failures recover without an application restart.
- Record strict AcoustID-only consensus decisions in shadow mode so future automation can be evaluated without allowing confidence score alone to authorize an import.

### Changed

- Hide the redundant Unknown badge and edition chooser on release cards that have no explicit or clean edition choice.
- Group compatible provider release editions into one artist-discography card with native family-scoped Explicit, Clean, Not explicit, and Unknown controls plus a per-family reset to defaults.
- Apply monitoring after complete discography refreshes, preserve edition choices across outer watchlist gates, and prevent stale sibling editions from creating duplicate acquisition or quality-upgrade work.
- Count compatible rating editions once in local library, watchlist, and provider-state release totals while keeping ownership and progress tied to each exact canonical edition.
- Default import-review preview alignment to the common 47.926-second source offset when an exact acoustic match is unavailable.

### Fixed

- Make slskd enqueue, adoption, provider cleanup, staged-file cleanup, and partial-file cleanup crash-, cancellation-, and replacement-safe using durable ownership fences and fail-closed reconciliation.
- Retry provider and artifact cleanup independently without starving later obligations, preserve review/import/recovery artifacts, and retain content-erased tombstones rather than path-unlinking an unverified replacement.
- Persist the slskd download timeout when saving the Behavior settings form.
- Refresh expired signed Deezer reference previews before rendering import review so comparison audio and alignment remain usable.
- Serve cached MP3 review audio for FLAC files on mobile browsers so matched timestamps seek to the correct audible passage without changing the downloaded source.

## [0.23.0] - 2026-08-05

### Fixed

- Replace repeated correlated Library artist-card scans with grouped catalog aggregates and query-aligned indexes, removing multi-second navigation stalls on production-sized libraries.
- Condense same-release Deezer discography snapshots to the richer track list while preserving distinct editions, years, release kinds, providers, and content ratings; reconcile existing duplicates without losing linked library state.

## [0.22.0] - 2026-08-05

### Added

- Add non-destructive Skip navigation to the import-review deck, including wraparound queue traversal and an `N` keyboard shortcut.

### Changed

- Remember independent browser-local volume levels for downloaded and reference review audio across approve, deny, skip, progressive navigation, and page reloads.

## [0.21.0] - 2026-08-05

### Changed

- Automatically align and seek the idle downloaded player to the matched or explicitly estimated reference passage when each import-review card opens, without autoplay or interrupting playback started before analysis completes.

### Fixed

- Retry import-review approval state writes during transient SQLite contention, avoid premature query-triggered autoflush, and preserve a successful approval when follow-up auto-import encounters a separate writer lock.

## [0.20.0] - 2026-08-05

### Added

- Align Deezer reference previews to the equivalent passage in downloaded import-review files with bounded transient Chromaprint analysis, confidence reporting, A/B switching, and manual timing nudges.

### Changed

- Replace the inaccurate generic midpoint assumption with an explicitly labelled centered-preview estimate when exact acoustic alignment is unavailable; iTunes fallback previews remain independent and are never fetched for synchronization.

## [0.19.2] - 2026-08-04

### Fixed

- Recover import-review execution from transient SQLite writer contention without reusing a rollback-only session, repeating filesystem work, or surfacing an internal server error.
- Replace the horizontally scrolling tag-comparison table with responsive field comparisons that remain readable inside the mobile swipe card.

## [0.19.1] - 2026-08-04

### Fixed

- Compact import review on mobile so track identity, both audio players, deny/approve actions, swipe guidance, and a collapsed tag/file-details disclosure fit within one phone viewport.
- Keep all mobile navigation destinations in one safe-area-aware row and reserve global-player space only after a library track is selected.
- Handle iOS touch gestures directly for reliable swipe-right approval and swipe-left denial while preserving vertical scrolling, native audio controls, and the existing destructive confirmation.
- Show visible directional swipe feedback instead of leaving its approval and denial labels hidden.

### Removed

- Remove the unused downloaded-file midpoint control and its JavaScript behavior.

## [0.19.0] - 2026-08-04

### Added

- Show persisted slskd username and remote-folder provenance in import review, and add mobile swipe-right approval and swipe-left denial with destructive confirmation preserved.

## [0.18.1] - 2026-08-04

### Fixed

- Bound crash-recovery cleanup quarantine filenames independently of source-name length while preserving legacy claim recovery and rejecting malformed claims.

## [0.18.0] - 2026-08-04

### Added

- Add a dedicated authenticated import-review deck with independent downloaded/reference audio, Deezer-first and iTunes-fallback preview badges, midpoint seeking, keyboard actions, file-vs-catalog tag differences, and verification details.

### Changed

- Move pending import review out of the Downloads rail into `/review`, retain acquisition source and original filename details, and replace the rail with a compact queue link while keeping Downloads full-width.

## [0.17.3] - 2026-08-04

### Changed

- Import-review Match details now show the acquisition source and original pre-staging filename, including exact persisted slskd remote filenames.

## [0.17.2] - 2026-08-04

### Fixed

- Default import-review audio previews to maximum volume and remember the browser's selected volume across subsequent review items.

## [0.17.1] - 2026-08-04

### Fixed

- Keep each configured acquisition permit for the complete provider queue and polling lifecycle, preserving truthful runtime increase and decrease bounds.
- Coalesce concurrent slskd download polling into short-lived endpoint- and credential-isolated snapshots, with bounded jittered backoff, sanitized retryable HTTP 429 failures, and fresh exact-transfer reads before destructive cleanup.
- Retry startup and watchdog job recovery as complete rollback-safe SQLite transactions, using conditional stale-state claims so concurrent heartbeats or terminal updates cannot be overwritten or dispatched twice.
- Persist runner job-envelope transitions through atomic conditional claims and rollback-safe retries so concurrent workers cannot execute the same provider job, queued cancellation remains durable, and concurrent terminal advances remain authoritative.
- Serialize idempotent continuation creation before duplicate checks and dispatch only newly committed child jobs, preventing lock retries or concurrent callers from creating or launching duplicates.
- Fence provider and staged-artifact cleanup against reassigned transfers, paths, active destination owners, and replaced filesystem inodes by durably quarantining the owned inode before unlink and checkpointing exact provider cleanup.
- Add a dry-run-first backlog reconciliation command that strictly rechecks historical fingerprint evidence, closes only byte-identical same-catalog destination projections, dismisses only explicit duplicate rollbacks with no surviving source, reports unresolved precedence buckets, and retries complete apply transactions under SQLite contention.

## [0.17.0] - 2026-08-03

### Added

- Add region-selectable discovery with bounded popular artist, genre, new-release, and trending feeds, dedicated paginated pages, global-fallback labels, and stale cache recovery.
- Add in-place, idempotent artist watchlisting from shared catalog cards with saved release defaults, watched-state feedback, and an optional accessible configuration dialog.
- Show provider IDs, fan/release counts, top-track previews, missing-image states, and external provider links on catalog artist search cards to support duplicate-artist disambiguation.

### Changed

- Load catalog artist discographies progressively from the primary metadata provider, defer secondary providers until requested or watched, and avoid per-album Deezer detail requests during the initial release list.

### Fixed

- Validate provider-native artist identities before rendering or opening search results, filter malformed provider responses, and rank Deezer artists by fan count without reordering other providers.
- Keep exact-name catalog artists with distinct provider-native IDs separate during enrichment, so opening a Deezer artist cannot be merged away by a same-name MusicBrainz match and leave the discography URL returning 404.
- Retry transient SQLite writer locks when opening or watchlisting a catalog artist so background reconciliation does not surface as a 500.

## [0.16.0] - 2026-08-03

### Added

- Add a source blocklist page for viewing and removing blocked provider artifacts.

### Changed

- Polish the Downloads queue presentation with counted friendly error chips, stronger progress bars, corrected source casing, quieter bulk-clear actions, and a full-width queue when import review is empty.

### Fixed

- Count MP3 imports as upgrade-eligible when the quality profile prefers FLAC.

## [0.15.1] - 2026-08-02

### Fixed

- Retry bounded human-style slskd targeted queries after strict artist/title misses, including title-only and first significant artist-token variants, while preserving post-result catalog identity verification and source-attempt provenance.
- Back off slskd search-state polling during long acquisition searches to reduce burst pressure and 429 amplification.

## [0.15.0] - 2026-08-02

### Changed

- Collapse artist watchlist settings into a hero Configure dialog with immediate release-type/source updates and remove redundant preset/save controls.
- Quick-watchlisted artists now default their watchlist catalog source to the artist primary source when one is set.
- Rework Downloads with status tabs, mobile card-style queue rows, and full-width expandable attempt details.

### Fixed

- Embed canonical artwork in Ogg/Vorbis imports and metadata repairs, and clear album-artist alias tags that can split Navidrome albums.
- Prefer Deezer album manifests when hydrating hybrid Deezer/MusicBrainz catalog rows so monitored releases stay on the selected Deezer-backed edition.
- Auto-verify catalog-bound downloads when AcoustID returns multiple high-confidence recording MBIDs whose titles all match the requested track, while still sending duration outliers to review.
- Reject targeted slskd candidates before acquisition when provider duration metadata materially contradicts the catalog track duration.
- Keep grouped download status/source truthful while provider-backed priority jobs are actively searching or downloading, and freeze elapsed time once a download reaches a terminal state.
- Stack the Downloads import-review rail above the queue on mobile and make review controls/details full-width so cards no longer render as a crushed side column.

## [0.14.0] - 2026-07-31

### Changed

- Move artist metadata enrichment into the hero actions, show enrichment state inline, align hero action controls, and tidy watchlist release-type toggles.

### Fixed

- Render artist-page release progress bars with CSP-safe native progress elements and remove the JS inline width update.
- Remove the global player's backdrop blur to avoid sitewide hover and audio-control repaint lag.
- Prefer the runtime primary metadata provider for artist library cards when no explicit artist primary or watchlist identity is set, keeping provider counts accurate.

## [0.13.1] - 2026-07-31

### Fixed

- Cap interactive source searches to short provider-specific timeouts so the longer slskd bulk-acquisition budget does not hold web/API search requests open for minutes.

## [0.13.0] - 2026-07-31

### Fixed

- Wait for slskd compound terminal search states and use a long enough bulk-search timeout so queued albums do not fail as `sources_exhausted` while slskd is still finding results.

### Changed

- Raise the configurable source-search budget ceiling to 15 minutes for high-volume slskd bulk downloads.

## [0.12.0] - 2026-07-31

### Changed

- Polish the artist detail page with a wider layout, denser album grid, per-release progress bars, artist-level completion rollups, quieter album-card badges, and grouped primary/maintenance actions.

## [0.11.3] - 2026-07-31

### Fixed

- Show the loading-discography state immediately when opening a watchlisted artist from search so the page auto-refreshes after enrichment finishes.

## [0.11.2] - 2026-07-31

### Changed

- Limit artist-card release counts to the selected primary metadata source, simplify card wording, and expose per-artist primary source selection.

### Fixed

- Queue background enrichment when watchlisting an artist before its selected-provider discography has hydrated.
- Preserve search-page watchlist defaults through enrichment so newly added artists monitor albums, singles, EPs, and upgrade checks according to Behavior settings.
- Keep artist and release-page download buttons on the current page by giving download forms a global fetch-submit fallback.
- Add MusicBrainz recording-credit collaborators to targeted slskd searches and candidate guards so bare catalog titles like `Miami` and `Heartless` can still require their remix/featured artists.
- Match slskd targeted filenames by full basename identity so promo-library suffixes and `1/24` separator variants do not hide otherwise valid single-track files.
- Keep wanted and catalog release download actions on the current page while queueing work via fetch.

## [0.11.1] - 2026-07-31

### Fixed

- Preserve featured-artist text during slskd targeted matching so plain and featured/remix singles no longer collapse to the same candidate.

## [0.11.0] - 2026-07-31

### Added

- Add persisted full-library, artist, and album/single/EP scanners that safely adopt existing music from embedded identity or canonical folder evidence, repair lost import-plan links, and retain ambiguous files for review.
- Backfill import-plan source fingerprints during scans so already-imported files can be reconciled without rewriting library audio.
- Add bulk auto-approve controls for exact and folder+title adoption matches, plus review filters for existing staged import actions.
- Surface adoption reasons and existing destination paths in review cards so retained/linked files are auditable before approval.
- Add a dry-run endpoint for the scanner so full-library jobs can preview scope and candidate counts before queuing.
- Add adoption lifecycle tests covering missing-plan repair, destination conflicts, retained staged files, and review approval/decline transitions.
- Add direct in-app playback for imported local files with poster-card controls, continuous playlist playback, and a sticky mini-player that survives page navigation.
- Add release-level library actions for rescanning, reconciling, deleting files, and removing release rows while preserving staged data.
- Add persisted track file state, size, mtime, and content hash fields for external-file reconciliation.
- Add safe imported-file deletion with trash-first fallback, missing-file marking, and release cleanup guards.
- Add metadata-driven reconciliation that detects moved, missing, changed, and duplicate imported files during library scans.
- Add focused integration and unit coverage for playback authorization, file deletion, and scan reconciliation workflows.

### Changed

- Redesign artist and release pages around poster-first cards, prominent playback actions, and direct library-management controls.
- Show imported track paths, status badges, and direct play/delete/rescan controls from the release page.
- Allow release-level removal of imported library rows only when file deletion is explicit or files are already missing.

### Fixed

- Add targeted single-track candidate identity checks before slskd download/import planning so plain album cuts, remixes, featured versions, and other bracketed recording variants cannot satisfy the wrong catalog track.
- Build targeted single-track searches from artist and track identity instead of repeating same-title album names, normalize smart punctuation for providers, and retry bounded edition-suffix variants such as acoustic and live titles.
- Preserve per-source and per-query attempt provenance when all configured acquisition sources are exhausted.

## [0.8.9] - 2026-07-30

### Fixed
- Release quality-monitoring in-process claims if the initial database checkpoint fails, allowing later checks to retry normally.
- Re-fetch each album during wanted and monitored-artist bulk queue operations so a transient SQLite retry rollback cannot expire retained ORM rows and skip or crash later albums.

## [0.8.8] - 2026-07-30

### Fixed
- Commit each quality-monitoring status checkpoint before provider search and polling so the scheduler cannot hold SQLite's writer lock across sequential 60-second searches.
- Persist quality-monitoring failure and cancellation states after provider I/O so records cannot remain permanently stuck as `checking`.
- Retry album download job creation after transient SQLite writer contention without duplicating jobs or reusing expired ORM state.

## [0.8.7] - 2026-07-30

### Fixed
- Remove legacy `DISC`/`DISCC` and `TRACK`/`TRACKC` aliases during import and Repair Metadata so Navidrome cannot prefer stale global positions over canonical multi-disc tags.
- Remove Picard `MUSICBRAINZ_ALBUMCOMMENT` metadata, including MP3 TXXX variants, so Navidrome does not derive a stale `albumversion` that splits otherwise-identical tracks into duplicate albums.
- Commit acquisition provenance checkpoints before provider fallback and polling so background searches release SQLite's writer lock before network I/O and cannot block orphan-review dismissal.

## [0.8.6] - 2026-07-30

### Fixed
- Clear stale `ALBUMVERSION` metadata during import and Repair Metadata so otherwise-identical tracks no longer split into duplicate Navidrome albums.
- Emit a final scanner-visible file change after batch metadata repair so Navidrome rescans the completed album instead of retaining a mixed mid-repair snapshot.
- Commit auto-import planning before artwork/provider I/O so background imports do not hold SQLite writer locks long enough to break orphan-review dismissal.

## [0.8.5] - 2026-07-30

### Fixed
- Kept catalog artist pages read-only so GET navigation no longer queues provider refresh jobs that can hold browser requests behind SQLite writer locks.
- Shortened SQLite busy waits on retryable UI writes so dismissing stale import-review cards retries quickly instead of appearing to load indefinitely.

## [0.8.4] - 2026-07-30

### Fixed
- Prevent artist pages and actionless review dismissal from returning 500 errors when background jobs briefly hold the SQLite writer lock.

## [0.8.3] - 2026-07-30

### Fixed
- Write FLAC/Ogg disc numbers as plain `DISCNUMBER` plus separate `DISCTOTAL`/`TOTALDISCS` fields so Navidrome parses multi-disc repairs correctly.
- Keep retagged library files group/world-readable for downstream scanners after metadata repair.
- Add a release-level dismiss action for actionless import-review cards with no per-track Deny button.

## [0.8.2] - 2026-07-30

### Fixed
- Metadata repair no longer lazy-loads import plans from the filesystem retag thread, fixing the greenlet error seen while repairing multi-disc albums.
- Denying stale import-review items now clears the review even when the staged path is unsafe or outside the staging root; unsafe files are not deleted.

## [0.8.1] - 2026-07-30

### Fixed
- Import planning and metadata repair now apply catalog multi-disc totals before rendering paths, so imported and repaired files use `{disc}-{track}` names such as `3-09` when Deezer/catalog metadata identifies multi-disc releases.
- Metadata repair now renames repaired multi-disc album files to the canonical naming-template filename while retagging them, preventing duplicate per-disc track numbers from splitting albums in Navidrome.
- Removed the API Docs link from the sidebar and kept it only in Settings → About.

## [0.8.0] - 2026-07-29

### Added
- Added persisted artist watchlist defaults for Albums, Singles, EPs, and quality-upgrade monitoring, with runtime defaults applied when watchlisting artists.
- Added reversible quality-upgrade monitoring toggles on watchlist and album pages.
- Added bulk Wanted-page queue actions for selected or all listed incomplete releases.
- Added a bounded job dispatcher concurrency setting to keep download bursts from exhausting the SQLite connection pool.
- Added a dashboard library-quality card that groups imported tracks by quality tier and reports tracks below the runtime quality profile.
- Added an on-demand background quality-upgrade scan trigger and dashboard workflow buttons for quality-upgrade and duplicate-cleanup scans.
- Store provider release and track content ratings/UPCs and show rating labels on catalog release pages.
- Download buttons on catalog album pages and artist release cards now queue work in place with a brief confirmation while preserving native form redirects without JavaScript.

### Changed
- Album downloads now queue missing catalog tracks and imported tracks that are positively known to be below the configured quality profile, while skipping already acceptable or unknown-bitrate imports.
- Rebuilt the dashboard with a compact recent-jobs summary and expanded recent library activity with album artwork thumbnails.
- Settings navigation is reorganized into clearer sections, and the naming-template field now shows the documented default template as its placeholder when unset.
- Wanted release counts now prefer hydrated catalog track manifests so fully owned releases stay off `/wanted`.

### Fixed
- Backfill Deezer release explicitness/UPC from album summaries and stop filesystem-only progress from crediting same-title clean/explicit sibling releases.
- Bind catalog-scoped one-track releases to their sole catalog track so approved single imports count as complete.
- Bulk watchlisted artist downloads now queue only missing or sub-quality tracks for partial albums instead of reacquiring the full album.
- Catalog artist pages now show a loading discography state while enrichment/discography hydration is queued or running, then refresh the release section in place.
- Combine sibling `CD1`/`CD2` slskd album folders and preserve multi-disc totals in import/metadata-repair tags.
- Filesystem release-progress caching now notices new files inside nested disc folders even when directory timestamp resolution is too coarse.
- Ignore `.lrc` lyrics files from slskd search/acquisition so lyric sidecars cannot enter import review as zero-duration tracks.
- Keep artist-page monitored-download submissions on the current page for fetch requests.
- Keep clean and explicit provider releases as separate catalog entries so one imported file does not credit both versions.
- Reconcile partial catalog album jobs after continuation imports complete the album.
- Treat denied slskd review artifacts as blocked candidates so the same peer/file is not downloaded again after denial.
- Wanted bulk queueing reuses album download per-track acquisition so partial releases never enqueue a whole-album job.

## [0.7.2] - 2026-07-29

### Added
- Wired quality-upgrade monitoring into album watch actions, maintenance approval, and a disabled-by-default scheduled check interval.
- Added a background-backed Maintenance page with library filesystem verification, duplicate dry-run summaries, safe tie-free cleanup, and scheduling controls.
- Added quality-duplicate cleanup that follows the Settings quality profile, permanently removes only clear same-folder lower-quality duplicates, and leaves ambiguous matches for review.
- Added an explicit backup-first maintenance command for auditing and repairing historical orphaned staging-review records.
- Added a confirmed album-detail action that transactionally retags every imported file in a release folder from Audiohoard's stored canonical metadata without changing database records or filenames.

### Fixed
- Schedulers now run their first cycle at startup instead of waiting a full interval on freshly booted containers.
- Dashboard provider readiness now uses cached health snapshots instead of live YouTube/TIDAL probes, and the staged-review quick action points directly at the import review rail.
- Dashboard listening time now shows hour and minute remainders together.
- Repair Metadata now handles flat multi-disc album folders where files are named with per-disc track numbers, preventing albums like `The Select (Deluxe)` from failing with unlinked-audio errors.
- Import tagging now writes Navidrome-compatible `releasedate` metadata immediately during auto-import, not only during later metadata repair.
- Auto-import now embeds catalog album artwork and scene-style three-digit track prefixes like `101-`/`210-` bind to the correct disc/track before AcoustID review.
- Metadata repair now writes a Navidrome-compatible `releasedate` tag for FLAC/Vorbis album files so mixed-format albums do not split from MP3 release-date frames.
- Metadata repair now handles mixed imported/legacy album folders and clears additional Navidrome grouping tags such as original year and MusicBrainz status/type leftovers.
- Import Review denial can remove non-audio staged artifacts that were incorrectly surfaced for AcoustID review.
- Metadata repair now clears stale FLAC `year` tags so Navidrome does not split tracks whose canonical date was repaired.
- Metadata repair now follows safe Cover Art Archive redirects and clears stale Navidrome grouping tags that split albums.
- Metadata repair now embeds canonical cover art, clears stale Navidrome grouping tags, and can retag matching legacy library files that were not originally imported by Audiohoard.
- Import Review no longer shows stale actionless cards after every pending review item has already been approved or imported.
- Album Download Missing now skips catalog tracks already imported with existing destination files and queues only genuinely missing tracks.
- Made Downloads tolerate malformed historical errors, close fully imported album attempts across continuations, and periodically retry serialized source cleanup.
- Enabled SQLite WAL, foreign-key enforcement, and a bounded busy timeout to reduce concurrent job contention and prevent new orphaned review rows.
- Made album retagging durable across crashes, safe against duplicate or concurrently changed files, compatible with legacy unmapped imports, and non-blocking for unrelated web requests.

## [0.7.1] - 2026-07-28

### Added
- Restored the track-table library view alongside the artist library.
- Added a global Wanted page for incomplete releases from watchlisted artists.
- Added restrictive security headers to HTML responses with CSP exemptions for API documentation.
- Cache allowlisted remote artist artwork locally behind the authenticated artwork proxy.

### Changed
- Split the application stylesheet into layered tokens, base, components, and page assets.
- Reworked Downloads with incremental queue updates, release artwork and progress, and a pending-review rail.
- Reworked library artist cards into poster-style release progress cards.
- Rebuilt catalog album pages with cover art, metadata, progress, and responsive track layouts.

- Right-size artist artwork requests and defer image decoding in artist views.
- Artist enrichment and discography refreshes now leave catalog page requests non-blocking and expose persisted enrichment state.
- Filesystem release evidence is cached by album folder modification time to avoid repeated library walks during catalog rendering.
- Added indexes for catalog ownership, workflow state, and job relationship columns used on request hot paths.
- Track review now offers only approve or deny; denying removes the review row and staged artifact, current `{Album} ({Year})` library folders contribute truthful release progress, and completed slskd transfers are removed through the supported transfer-ID endpoint.
- Library release cards and album details now show downloaded-versus-wanted track progress, replacing duplicate downloaded-file and wanted-release sections.
- Downloads now group all acquisition attempts for the same release and automatically clear completed or timed-out work from Audiohoard and slskd queues after durable state is saved.
- Verified tracks now import immediately without waiting for the rest of an album, while missing tracks remain available for targeted continuation searches.
- The default album directory format is now `{album} ({year})`, while track filenames retain their disc and track prefixes.

### Fixed
- Reorganized catalog artist controls and added a persistent save bar for unsaved watchlist selections.

- Catalog hydration now rejects and repairs overfull manifests instead of retaining phantom tracks.
- Existing full-count catalog manifests with duplicate or invalid disc/track positions are rehydrated in place, preserving linked acquisition identities and corrected numbering.
- Provider-edition album titles now fall back to canonical MusicBrainz titles during recording reconciliation; Deezer hydration follows every authoritative tracklist page and rejects unpositioned fallbacks before automated mapping.
- Deezer artist discographies now retain release track totals even though the artist-albums API omits them, with a bounded non-retrying count lookup during provider outages; fully stored album pages avoid redundant refreshes while unknown-count partial manifests still hydrate.
- Download queue grouping now retains completed release progress without re-exposing hidden attempts, bounds relationship loading to selected groups, and records successful slskd cleanup so startup does not repeat historical transfer deletions.
- Successful imports now remove empty acquisition directories below the configured staging root while preserving non-empty directories and the staging root itself.
- Matching expected recording MBIDs above the configured AcoustID threshold now auto-verify even when the fingerprint result lists alternate recordings, and stale review rows are cleared.
- Startup reconciliation removes terminal acquisition rows that no longer have a staged or imported file, preventing old jobs from affecting later acquisitions.
- Partial imports preserve committed plans and process only newly eligible tracks instead of replanning or rolling back the entire release.

## [0.7.0] - 2026-07-27

### Added
- Added configurable AcoustID auto-acceptance and slskd timeout controls, plus a durable timed-out candidate blocklist and alternate-source retries.
- Added source-independent quality profiles, coherent slskd album-folder scoring, bounded missing-track continuation, AcoustID verification, and authenticated staged-audio review controls.
- Added non-destructive queue controls to hide individual terminal downloads, clear failed downloads, or clear all finished downloads while preserving library metadata.
- Added provider-native artist identities and discography snapshots, with per-artist watchlist-provider selection and MusicBrainz, Deezer, and iTunes discography switching.
- Added an Alembic-to-model schema parity gate and mocked slskd-to-staging integration coverage for verified auto-import and review outcomes.
- Added persisted import-review reasons with non-destructive Dismiss controls and bounded Re-acquire handling for missing staged sources.

### Changed
- Replaced the manual Import tab with automatic transactional import after catalog completeness and fingerprint verification gates pass.
- Replaced every application icon and favicon with the new Audiohoard artwork.
- Consolidated Artists into one filterable watchlist with artwork and release-type counts.
- Library views now include only successfully downloaded files and display their paths and metadata.
- Replaced user-facing monitoring terminology with watchlist terminology and restored a conventional settings gear icon.
- Exposed the ordered Quality profile in Settings and enforced format-family, minimum MP3 bitrate, and lower-quality fallback rules during slskd selection.

### Fixed
- Album downloads now hydrate catalog track manifests before dispatch, report incomplete slskd folders as partial with targeted continuations, and release SQLite write locks before provider waits.
- Approved legacy albums now reconcile catalog tracks and resume transactional import; committed imports remove staging inputs and completed slskd transfers.
- Library and dashboard projections now exclude staging-only downloads and require committed import destinations.
- Album acquisitions no longer flatten coherent slskd folders into a ten-file cap, and normalized catalog matching now reports only genuinely missing tracks.
- Reconciled slskd transfers by peer and filename when enqueue responses omit the provider UUID, and restored compatibility with the legacy staging mount when the new default path is absent.
- Provider-specific release kinds no longer contaminate one another, and Albums-only views and watchlist policies now exclude Singles and EPs, including iTunes `- Single` and `- EP` releases.
- Corrected slskd download enqueue payloads to use the required request array, preserved validation details, and handled nested transfer responses.
- Made pull-request, main, and tag quality checks authoritative and uniquely named so release checks cannot satisfy branch protection accidentally.
- Persisted and rendered AcoustID mismatch, verification-unavailable, track-count, planning, missing-source, and execution failure reasons without deleting retained source or audit records on dismissal.

## [0.6.1] - 2026-07-26

### Added
- Added restart-safe in-process job dispatch with startup recovery, retry, cancellation, and monitored-album scheduling.
- Added terminal-state polling and artifact verification for slskd and SABnzbd acquisitions.
- Added download queue details, live refresh, actionable errors, and browser retry/cancel controls.
- Added visible account logout controls and direct Artists/Imports navigation on mobile.
- Added a dispatcher watchdog that recovers orphaned active jobs once and then fails recurring losses with a structured `dispatch_lost` reason.
- Added filterable dashboard and download status links, source-priority reordering controls, and library/staging path diagnostics.

### Changed
- Prowlarr album search results are handled as alternative release candidates instead of individual tracks.
- Audio discovery and import now share one verified format contract for FLAC, MP3, M4A/MP4, Ogg Vorbis, and Opus.
- Browser settings validation now tests entered provider credentials before saving and reports Ready, Degraded, and Disabled states consistently.
- Search and settings screens now use clearer labels, table semantics, contrast, and accessible target sizes.
- The release workflow can publish the moving `develop` image on demand without creating a version tag.
- Job state and per-result progress now commit in short transactions so active downloads expose elapsed time, last activity, transfer source, and track-level progress while work is running.

### Fixed
- Prevented external acquisitions from completing before a usable staged artifact exists.
- Removed arbitrary positional catalog-track assignment and album-level MBID promotion to resolved track identity.
- Successful complete album imports now update catalog ownership; incomplete and rolled-back imports remain Wanted.
- Monitoring refresh failures are isolated per artist and no longer terminate the scheduler.
- Public liveness/readiness checks no longer probe or expose provider diagnostics, and failed readiness returns HTTP 503.
- Expired browser sessions are deleted and HTML requests redirect to login while API clients retain JSON 401 responses.
- Import execution now requires a persisted, reviewed ready plan instead of silently planning and importing in one action.
- Dashboard status counts include partial jobs and remain responsive with all six terminal/active states.
- Runtime version labels, README examples, Compose image tags, and health checks now match v0.6.1.
- Login redirects now preserve safe deep links, show the signed-in username, and reject open redirects.
- Import and settings form failures now redirect with browser-readable errors instead of returning server errors.
- Invalid behavior, metadata, and naming values are rejected without partially saving settings.
- Guarded job startup and dispatcher exception observation prevent provider or settings failures from leaving jobs invisibly pending.
- Artist enrichment and startup repair merge duplicate provider identities without losing monitoring state or albums, including legacy rows where only one normalized-name match has an MBID.
- Manual and background enrichment failures now render concise sanitized banners and redirect cleanly instead of exposing SQL details or returning a raw HTTP 500.
- Main-branch protection now requires the full `Quality checks` and Docker build checks; tag publication remains dependent on the release workflow’s full quality job.

## [0.6.0] - 2026-07-22

### Added
- Rebuilt catalog artist pages with artwork hero headers, identity chips, grouped discography cards, filter chips, compact monitor controls, and native form actions.
- Added catalog album duplicate reconciliation for legacy punctuation-normalization collisions.

### Changed
- Cached PBKDF2-derived Fernet settings keys per process, removing repeated 200,000-iteration derivation from request-time secret decrypts.
- Regenerated favicons with border-connected background keying to transparent alpha while keeping launcher icons opaque.
- Enrichment fills missing artist artwork from Deezer, backfills album track counts from providers without overwriting opened tracklists, and schedules first-open background enrichment.

### Fixed
- Scoped global form-control CSS so checkboxes and radios no longer render as full-width 42px controls.
- Normalized Unicode apostrophes, quotes, and dashes during album matching so curly/straight punctuation variants dedupe correctly.

## [0.5.0] - 2026-07-22

### Changed
- Project renamed to Audiohoard; all visible strings, branding, and packaging updated.
- Added `display_name()` helper and Jinja2 filter/global for provider and source labels.
- Generated branding assets (favicon, apple-touch-icon, PWA icons, webmanifest).
- MusicBrainz default app name and version updated to `audiohoard`/`0.5.0`.
- Docker image, container name, database path, and staging root updated to audiohoard.

## [0.4.1] - 2026-07-22

### Fixed
- Fixed changelog page rendering for markdown links.

## [0.4.0] - 2026-07-22

### Added
- Artist monitoring, wanted-album views, per-album monitor controls, and in-app discography refresh settings.
- Cross-provider artist enrichment with conservative matching, provenance, provider badges, and manual enrichment.
- Primary metadata provider selection and catalog search defaulting to the primary provider.
- Sectioned settings pages and an About changelog page.

### Changed
- Catalog and free-text downloads use shared source-priority fallback and record attempted-to-served provenance.

## [0.3.0] - 2026-07-21

### Added
- Metadata catalog search, catalog artist pages, catalog albums, and catalog-driven acquisition entry points.

## [0.2.1] - 2026-07-20

### Fixed
- Restored native form submission behavior in the v0.2 UI.

## [0.2.0] - 2026-07-20

### Added
- Dashboard and server-rendered v0.2 application shell with library, artist, downloads, imports, and settings navigation.

## [0.1.3] - 2026-07-19

### Added
- TIDAL acquisition support and provider settings improvements.

## [0.1.2] - 2026-07-19

### Fixed
- Database URL handling during migrations.

## [0.1.1] - 2026-07-18

### Fixed
- Initial release hardening fixes after v0.1.0.

## [0.1.0] - 2026-07-18

### Added
- Initial self-hosted music acquisition workflow, jobs, source adapters, settings, and Docker packaging.