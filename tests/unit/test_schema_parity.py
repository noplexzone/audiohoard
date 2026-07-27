"""Verify ORM model registration and Alembic migration parity with Base.metadata."""

from __future__ import annotations

import pprint
from pathlib import Path

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


def test_migration_parity(tmp_path: Path) -> None:
    db_file = tmp_path / "parity.db"
    async_url = f"sqlite+aiosqlite:///{db_file}"
    sync_url = f"sqlite:///{db_file}"

    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", async_url)
    command.upgrade(cfg, "head")

    engine = sa.create_engine(sync_url)
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
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
