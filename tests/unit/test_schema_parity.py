"""Verify ORM model registration and Alembic migration parity with Base.metadata."""

from __future__ import annotations

import pprint
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext

import app.models  # noqa: F401
from alembic import command
from app.database import Base

# Encode the full expected table set explicitly so a missing import is caught clearly.
EXPECTED_TABLES: frozenset[str] = frozenset(
    {
        "app_settings",
        "app_users",
        "auth_sessions",
        "catalog_album_providers",
        "catalog_album_tracks",
        "catalog_albums",
        "catalog_artist_identities",
        "catalog_artists",
        "deletion_operations",
        "import_plans",
        "jobs",
        "monitoring_records",
        "path_previews",
        "provider_settings",
        "release_candidates",
        "releases",
        "source_candidate_blocks",
        "staging_review_items",
        "tracks",
    }
)


def test_model_registration_includes_all_tables() -> None:
    registered = set(Base.metadata.tables.keys())
    missing = EXPECTED_TABLES - registered
    extra = registered - EXPECTED_TABLES
    assert not missing, (
        f"Tables missing from Base.metadata (model import dropped?): {sorted(missing)}"
    )
    assert not extra, (
        f"Unexpected tables in Base.metadata (update EXPECTED_TABLES): {sorted(extra)}"
    )
    # Belt-and-suspenders for the historically missing table.
    assert "import_plans" in registered


def test_migration_parity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_file = tmp_path / "parity.db"
    async_url = f"sqlite+aiosqlite:///{db_file}"
    sync_url = f"sqlite:///{db_file}"

    # alembic/env.py gives DATABASE_URL precedence over alembic.ini. Never let
    # this test migrate an ambient application database.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", async_url)
    command.upgrade(cfg, "head")

    engine = sa.create_engine(sync_url)
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn, opts={"compare_server_default": True})
            raw_diffs = compare_metadata(ctx, Base.metadata)
    finally:
        engine.dispose()

    # Filter the alembic_version bookkeeping table that lives in the DB but not in Base.metadata.
    diffs = [
        d
        for d in raw_diffs
        if not (
            isinstance(d, tuple)
            and d[0] == "remove_table"
            and getattr(d[1], "name", None) == "alembic_version"
        )
    ]

    assert not diffs, (
        f"Schema drift detected between Alembic head and Base.metadata "
        f"({len(diffs)} difference(s)):\n{pprint.pformat(diffs)}"
    )


def test_0016_normalizes_legacy_verification_state_before_sqlite_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_file = tmp_path / "legacy-state.db"
    async_url = f"sqlite+aiosqlite:///{db_file}"
    sync_url = f"sqlite:///{db_file}"
    monkeypatch.delenv("DATABASE_URL", raising=False)
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", async_url)
    command.upgrade(cfg, "0015")

    engine = sa.create_engine(sync_url)
    with engine.begin() as conn:
        conn.execute(sa.text("INSERT INTO jobs (source, query) VALUES ('slskd', 'legacy')"))
        conn.execute(
            sa.text(
                "INSERT INTO tracks (job_id, source, acoustid_verification_state) "
                "VALUES (1, 'slskd', 'legacy_custom')"
            )
        )
    engine.dispose()

    command.upgrade(cfg, "head")

    engine = sa.create_engine(sync_url)
    try:
        with engine.connect() as conn:
            state = conn.scalar(sa.text("SELECT acoustid_verification_state FROM tracks"))
            temporary = conn.scalar(
                sa.text(
                    "SELECT count(*) FROM sqlite_master "
                    "WHERE type='table' AND name='_alembic_tmp_tracks'"
                )
            )
    finally:
        engine.dispose()
    assert state == "pending"
    assert temporary == 0
