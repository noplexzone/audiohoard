from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa


def _load_migration():
    module_path = (
        Path(__file__).parents[2] / "alembic" / "versions" / "0018_enrichment_state_and_indexes.py"
    )
    spec = importlib.util.spec_from_file_location("migration_0018", module_path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _create_pre_migration_schema(connection) -> None:
    connection.execute(sa.text("CREATE TABLE catalog_artists (id INTEGER PRIMARY KEY)"))
    connection.execute(
        sa.text(
            "CREATE TABLE tracks (id INTEGER PRIMARY KEY, catalog_album_id INTEGER, "
            "catalog_track_id INTEGER, import_state VARCHAR(32) NOT NULL, "
            "acquisition_state VARCHAR(32) NOT NULL, job_id INTEGER NOT NULL)"
        )
    )
    connection.execute(
        sa.text(
            "CREATE TABLE jobs (id INTEGER PRIMARY KEY, status VARCHAR(16) NOT NULL, "
            "parent_job_id INTEGER, catalog_album_id INTEGER)"
        )
    )


def _run_upgrade(migration, monkeypatch, connection) -> None:
    monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
    monkeypatch.setattr(
        migration.op,
        "add_column",
        lambda table, column: connection.execute(
            sa.text(
                f"ALTER TABLE {table} ADD COLUMN {column.name} VARCHAR(16) NOT NULL DEFAULT 'idle'"
            )
        ),
    )
    monkeypatch.setattr(
        migration.op,
        "create_index",
        lambda name, table, columns, unique=False: connection.execute(
            sa.text(f"CREATE INDEX {name} ON {table} ({', '.join(columns)})")
        ),
    )
    migration.upgrade()


def test_migration_0018_adds_enrichment_state_with_idle_default(
    monkeypatch, tmp_path: Path
) -> None:
    migration = _load_migration()
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    with engine.begin() as connection:
        _create_pre_migration_schema(connection)
        _run_upgrade(migration, monkeypatch, connection)
        columns = {
            column["name"]: column
            for column in sa.inspect(connection).get_columns("catalog_artists")
        }

    assert "enrichment_state" in columns
    assert columns["enrichment_state"]["nullable"] is False
    assert str(columns["enrichment_state"]["default"]).strip("'\"()") == "idle"


def test_migration_0018_adds_all_hot_path_indexes(monkeypatch, tmp_path: Path) -> None:
    migration = _load_migration()
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    with engine.begin() as connection:
        _create_pre_migration_schema(connection)
        _run_upgrade(migration, monkeypatch, connection)
        inspector = sa.inspect(connection)
        index_names = {
            index["name"] for table in ("tracks", "jobs") for index in inspector.get_indexes(table)
        }

    assert index_names == {
        "ix_tracks_catalog_album_id",
        "ix_tracks_catalog_track_id",
        "ix_tracks_import_state",
        "ix_tracks_acquisition_state",
        "ix_tracks_job_id",
        "ix_jobs_status",
        "ix_jobs_parent_job_id",
        "ix_jobs_catalog_album_id",
    }
