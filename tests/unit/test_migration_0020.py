from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa


def _load_migration():
    module_path = Path(__file__).parents[2] / "alembic" / "versions" / "0020_watchlist_defaults.py"
    spec = importlib.util.spec_from_file_location("migration_0020", module_path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_migration_0020_adds_watchlist_defaults(monkeypatch, tmp_path: Path) -> None:
    migration = _load_migration()
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    with engine.begin() as connection:
        connection.execute(sa.text("CREATE TABLE catalog_artists (id INTEGER PRIMARY KEY)"))
        monkeypatch.setattr(
            migration.op,
            "add_column",
            lambda table, column: connection.execute(
                sa.text(
                    f"ALTER TABLE {table} ADD COLUMN {column.name} BOOLEAN NOT NULL "
                    f"DEFAULT {column.server_default.arg!s}"
                )
            ),
        )
        migration.upgrade()
        columns = {
            column["name"]: column
            for column in sa.inspect(connection).get_columns("catalog_artists")
        }

    assert columns["watchlist_release_albums"]["nullable"] is False
    assert str(columns["watchlist_release_albums"]["default"]).strip("'\"()") == "1"
    assert str(columns["watchlist_release_singles"]["default"]).strip("'\"()") == "0"
    assert str(columns["watchlist_release_eps"]["default"]).strip("'\"()") == "0"
    assert str(columns["watchlist_monitor_upgrades"]["default"]).strip("'\"()") == "0"
