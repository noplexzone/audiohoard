# Release Edition Grouping and Monitoring Implementation Plan

> **For Hermes:** Use Claude Code to implement this plan task-by-task, followed by independent review and Jarvis verification.

**Goal:** Present clean, explicit, and unknown storefront editions as one release while preserving each provider edition internally and monitoring explicit editions by default, falling back to unknown only when no explicit edition exists, with clean editions always off by default.

**Architecture:** Keep `CatalogAlbum` and `CatalogAlbumProvider` edition identities separate so ownership, provider IDs, UPCs, track manifests, and clean/explicit acquisition targets remain truthful. Introduce a provider-scoped release-family projection for display/counting and apply a durable per-provider-release default-versus-user-override monitoring policy. Existing installations receive a schema migration plus database-only startup reconciliation; future discography refreshes use the same pure policy.

**Tech Stack:** FastAPI, SQLAlchemy 2 async, SQLite/Alembic, Jinja, vanilla JavaScript, pytest, Ruff, mypy, Docker/GitHub Actions.

## Global Constraints

- Guest appearances remain in artist discographies.
- One visible release card represents compatible clean, explicit, not-explicit, and unknown editions from the selected provider.
- Preserve every concrete provider release and canonical catalog album internally; do not merge clean audio ownership into explicit ownership.
- Never group releases with incompatible normalized titles, years, release kinds, concrete track counts, or identity-changing version descriptors.
- Default monitoring policy everywhere: explicit editions on; unknown editions on only when the family has no explicit edition; clean and not-explicit editions off.
- If a family contains only clean/not-explicit editions, it remains unmonitored.
- The artist's album/single/EP watchlist switches remain the outer gate.
- Users can override edition monitoring per grouped release. Explicit user overrides survive refreshes and new provider rows.
- Within duplicate provider snapshots of the same rating, monitor at most one deterministic representative; retain the others as alternate provider records.
- Existing monitored artists are reconciled on upgrade without provider HTTP and without touching files, imports, ownership, jobs, or running containers.
- GET routes remain read-only. Provider HTTP remains outside database transactions.
- Update `CHANGELOG.md` under Unreleased. This is a user-visible feature and will release as the next minor version under `docs/VERSIONING.md`.
- Do not restart or modify the production AudioHoard container or live database during development/release verification.

---

### Task 1: Define release-family identity and edition preference

**Objective:** Produce a pure, provider-scoped grouping and deterministic preferred-edition policy shared by UI, counts, refreshes, and repair.

**Files:**
- Modify: `app/services/catalog_metadata.py` or create a focused `app/services/release_editions.py`
- Test: `tests/unit/test_release_editions.py`

**Interfaces:**
- Produce a stable release-family key that ignores content rating/provider album ID but preserves normalized title, year, exact normalized release kind, edition/version marker, and compatible concrete track count.
- Produce grouped family/edition view models with one representative per rating and a preferred display/download edition.
- Preferred rating order: explicit, unknown, clean/not-explicit. Monitoring eligibility differs: clean/not-explicit never default on.
- Deterministically rank duplicate same-rating snapshots by complete track count/metadata/artwork, then stable ID.

**TDD proof:** Fixtures must distinguish clean versus explicit siblings, explicit versus unknown snapshots, only-clean families, incompatible track counts, alternate/version bundles, years, and release kinds.

### Task 2: Persist user overrides and reconcile existing/future defaults

**Objective:** Distinguish policy-managed monitoring from explicit user choices and apply the approved policy idempotently.

**Files:**
- Modify: `app/models/catalog_entities.py`
- Create: `alembic/versions/0027_release_edition_monitoring.py`
- Modify: `app/services/catalog_metadata.py` and/or the release-edition service
- Modify: `app/main.py` only if startup wiring is required
- Test: `tests/unit/test_migration_0027.py`
- Test: `tests/unit/test_release_editions.py`
- Test: `tests/unit/test_main_startup.py`
- Test: `tests/unit/test_schema_parity.py`

**Interfaces:**
- Add nullable `monitor_override` to `catalog_album_providers`: null means policy-managed; true/false means user-selected.
- Reconciliation accepts an artist identity plus release-type gates and sets only provider-release monitoring state. It must be idempotent and select at most one same-rating snapshot.
- A family with any explicit edition defaults to one explicit representative; unknown/clean/not-explicit are off. Without explicit, one unknown representative is on. Clean-only remains off.
- Any family containing explicit per-release overrides respects those exact choices and does not auto-enable newly arriving siblings.
- Existing rows start policy-managed and are reconciled once after migration/startup using database state only.
- Startup work must not block readiness on external I/O and must be serialized/idempotent.

**TDD proof:** Upgrade an existing database, run reconciliation twice, verify exact rows and zero second-run changes; verify clean-only, explicit+unknown, explicit+clean, manual clean override, release-type disabled, unmonitored artist, and non-watchlist provider behavior.

### Task 3: Apply policy to refresh, bulk watchlist, and manual edition selection

**Objective:** Ensure every future artist and every monitoring mutation uses the same edition-aware behavior.

**Files:**
- Modify: `app/routers/catalog.py`
- Modify: `app/services/catalog_metadata.py` if needed
- Test: `tests/integration/test_catalog_v030.py`
- Test: `tests/unit/test_catalog_metadata_v060.py`

**Interfaces:**
- After all provider summaries are upserted, reconcile the complete provider family set once; do not decide defaults row-by-row before siblings are known.
- Bulk `all`, `albums_only`, `singles_off`, and `none` operations preserve edition policy and clear/set only appropriate states.
- Manual grouped-release submission records explicit overrides for every edition in the submitted family so refresh cannot silently reverse the choice.
- Monitoring/acquisition continues to consume concrete `CatalogAlbumProvider.monitored` rows and therefore targets the selected explicit/unknown/clean canonical album only.

**TDD proof:** New explicit+clean artist defaults explicit only; explicit arriving after unknown shifts a policy-managed family to explicit; a manual clean choice survives refresh; bulk none disables all; re-enabling applies explicit-first policy; provider source changes do not credit or monitor another provider's edition.

### Task 4: Render one card per release family with edition controls

**Objective:** Replace duplicate edition cards with one poster-first card while allowing edition-level monitoring decisions.

**Files:**
- Modify: `app/routers/catalog.py`
- Modify: `app/templates/partials/_discography.html`
- Modify: `app/static/css/pages.css` or the actual catalog stylesheet
- Modify: `app/static/js/artist-watchlist.js` only if existing immediate-submit behavior needs edition-family support
- Test: `tests/integration/test_catalog_v030.py`
- Test: `tests/unit/test_artist_release_ui_contracts.py`

**Interfaces:**
- Card title/artwork/link/download/progress use the preferred edition: explicit, otherwise unknown, otherwise a clean display fallback.
- Show one concise edition summary, such as `Explicit + Clean`, and an expandable native `<details>` control listing available editions and their monitoring checkboxes.
- Preserve native form behavior, CSRF, no-JavaScript submission, accessibility labels, and progressive fragment replacement.
- Count release families, not provider rows, in artist-page filter counts and provider state counts.
- Keep guest appearances unchanged.

**TDD proof:** One card renders for explicit+clean and explicit+unknown siblings; both edition choices remain visible; explicit is checked by default; clean is unchecked; only-clean is unchecked; manual POST updates exact edition rows; fragments and normal page render identically.

### Task 5: Make library/search counts edition-family aware

**Objective:** Remove clean/explicit inflation from every local catalog count without changing provider-reported external discovery metadata.

**Files:**
- Modify: `app/services/catalog.py`
- Test: `tests/unit/test_catalog_service.py`
- Test: relevant library integration tests

**Interfaces:**
- Primary-provider library artist release counts use distinct compatible release-family identity rather than raw provider-release or canonical-album row count.
- Downloaded/owned progress remains canonical-edition-specific and is never cross-credited.
- External provider search/discovery counts remain clearly provider-reported and are not rewritten as local grouped counts.

**TDD proof:** Explicit+clean siblings count once; different years, release kinds, track counts, and version descriptors count separately; legacy rows continue to work.

### Task 6: Production-shaped migration and behavioral verification

**Objective:** Prove the change against a copied live database and the complete application gate without mutating production.

**Files:**
- Modify: `CHANGELOG.md`
- Add tests/fixtures only if production-shape probing exposes a gap

**Steps:**
1. Use SQLite online backup/read-only source access to create a disposable production-shaped database copy.
2. Run migration 0027 and the reconciliation against the copy.
3. Verify Kodak Black and Tee Grizzley family/card counts decrease, explicit representatives are monitored, clean/not-explicit rows are off, unknown is used only for families without explicit, and guest appearances remain.
4. Verify second-run idempotency and that imported ownership/job/file paths are byte-for-byte unchanged in relevant tables.
5. Run focused tests, full pytest, Ruff lint, Ruff format check, mypy, package build, and `git diff --check`.
6. Obtain independent specification and quality review, remediate findings, and rerun the affected gates.
7. Commit coherent Conventional Commits and push the feature branch. Open a PR and require CI success before merge.

### Task 7: Release and published-image proof

**Objective:** Publish the exact verified feature through Audiohoard's required release path.

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `pyproject.toml`
- Modify: `docker/Dockerfile`

**Steps:**
1. Re-read `docs/VERSIONING.md`.
2. Release the next minor version, expected `0.24.0`, because this adds schema and user-visible behavior.
3. Merge preserving history, create and push annotated `v0.24.0`, and confirm the release workflow is running.
4. Wait for release quality/publish success.
5. Pull and smoke-test the published image against disposable data; verify migration, readiness, and grouped-edition behavior without touching production.
6. Verify Docker Hub digest and OCI labels for the exact merge/tag commit.
7. Report the literal pull line for `noplexzone/audiohoard:develop@sha256:…`; do not change `latest` and do not restart production.
