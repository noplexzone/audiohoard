from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa


def _load_migration():
    path = (
        Path(__file__).parents[2] / "alembic" / "versions" / "0027_release_edition_monitoring.py"
    )
    spec = importlib.util.spec_from_file_location("migration_0027", path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_migration_0027_adds_nullable_monitor_override(monkeypatch, tmp_path: Path) -> None:
    migration = _load_migration()
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "CREATE TABLE catalog_album_providers "
                "(id INTEGER PRIMARY KEY, monitored BOOLEAN NOT NULL DEFAULT 0)"
            )
        )
        connection.execute(
            sa.text("INSERT INTO catalog_album_providers (id, monitored) VALUES (1, 1)")
        )
        monkeypatch.setattr(
            migration.op,
            "add_column",
            lambda table, column: connection.execute(
                sa.text(f"ALTER TABLE {table} ADD COLUMN {column.name} BOOLEAN NULL")
            ),
        )
        migration.upgrade()
        columns = {
            column["name"]: column
            for column in sa.inspect(connection).get_columns("catalog_album_providers")
        }
        row = connection.execute(
            sa.text("SELECT monitored, monitor_override FROM catalog_album_providers")
        ).one()

    assert migration.revision == "0027"
    assert migration.down_revision == "0026"
    assert columns["monitor_override"]["nullable"] is True
    assert row == (1, None)
