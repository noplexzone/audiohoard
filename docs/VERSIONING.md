# Audiohoard Versioning Policy

Audiohoard releases follow this document as the single authoritative versioning policy. Before cutting any future version, re-read this file and follow the release checklist exactly. A release is not complete until the version tag is pushed to origin and the release workflow is running.

## Scheme

Audiohoard follows [Semantic Versioning 2.0.0](https://semver.org): `MAJOR.MINOR.PATCH`.

Audiohoard is pre-1.0 and remains in `0.x` until the 1.0 criteria below are met. While the project is in `0.x`, `MINOR` carries the weight that `MAJOR` normally would: both new features and breaking changes bump `MINOR`; only backward-compatible fixes bump `PATCH`.

## Bump Rules for 0.x

- `PATCH` (for example, `0.7.2 -> 0.7.3`): bug fixes only. No new capability, no schema-adding migration, no new route, and no new settings field. A batch containing only `fix()` commits is a `PATCH`.
- `MINOR` (for example, `0.7.x -> 0.8.0`): any new feature, new route, new settings field, new scheduler, migration that adds schema, or user-noticeable behaviour change. Reset `PATCH` to `0`.
- `MAJOR` (`0.x -> 1.0.0`): the 1.0 milestone only, using the criteria below. Never bump `MAJOR` as part of routine work.

## Release Granularity

Tag a release when a coherent, shippable unit of work is complete: one feature-set or one related group of fixes. Do not release on a fixed cadence, and never batch unrelated work into a single version.

Never let more than roughly five unreleased commits accumulate on `main` without cutting a version. If `[Unreleased]` has grown past about five commits and the work is shippable, release before starting new work.

When merging a branch that contained several independently shippable milestones, tag each milestone as its own `MINOR` rather than collapsing them into one version.

Historical context only: `v0.7.0` bundled 16 commits, `v0.7.1` bundled 12, and `v0.7.2` bundled 17, each flattening several tag-worthy milestones. Those tags are published and must not be retroactively changed.

## Release Checklist

Follow these mechanical steps in order for every release:

1. Ensure `[Unreleased]` in `CHANGELOG.md` contains every change since the last tag, sorted into `Added`, `Changed`, and `Fixed`.
2. Choose the bump: any `Added` entry or user-noticeable `Changed` entry means `MINOR`; fixed-only means `PATCH`.
3. Rename `[Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`; add a new empty `[Unreleased]` section above it.
4. Set `X.Y.Z` in `pyproject.toml` (`version`) and `docker/Dockerfile` (`org.opencontainers.image.version`). The packaging test enforces the Dockerfile label.
5. Run the full gate: `uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run mypy app`. All commands must pass.
6. Commit `release: vX.Y.Z`; merge to `main` preserving history. Do not squash.
7. Create an annotated tag: `git tag -a vX.Y.Z -m "Audiohoard vX.Y.Z"`. Push `main` and the tag to origin.
8. The tag push triggers `.github/workflows/release.yml`, which runs the quality gate and publishes `noplexzone/audiohoard:X.Y.Z` and `noplexzone/audiohoard:develop` to Docker Hub. Do not run `docker push` locally, and do not consider the release finished until the tag is pushed and that workflow is running.

## Tag Immutability

Once a version tag is pushed, it is never moved, deleted, or reused. A mistake in a released version is fixed by releasing the next version, never by editing the old tag.

## 1.0 Criteria

Do not release 1.0 until all of the following hold. The 1.0 release is a deliberate maintainer decision, never automatic.

- Core workflows (`search -> acquire -> import -> library -> monitor`) are feature-complete for the maintainer's daily use, with no major reshaping expected.
- The database schema is stable: no migrations are expected that alter or drop existing columns or tables in a breaking way.
- There are no known data-loss or corruption bugs in acquisition, import, or maintenance paths.
- The public surface, including routes, settings, naming template, and Docker contract, is stable enough that changing it would itself be breaking.

These criteria are not yet met. Audiohoard remains a `0.x` project.
