from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa


def test_migration_0013_adds_queue_hidden(monkeypatch, tmp_path: Path) -> None:
    module_path = (
        Path(__file__).parents[2] / "alembic" / "versions" / "0013_job_queue_visibility.py"
    )
    spec = importlib.util.spec_from_file_location("migration_0013", module_path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    engine = sa.create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "CREATE TABLE jobs (id INTEGER PRIMARY KEY, source VARCHAR(64) NOT NULL, "
                "query TEXT NOT NULL, status VARCHAR(9) NOT NULL)"
            )
        )
        monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
        monkeypatch.setattr(
            migration.op,
            "add_column",
            lambda table, column: connection.execute(
                sa.text(f"ALTER TABLE {table} ADD COLUMN {column.name} BOOLEAN NOT NULL DEFAULT 0")
            ),
        )
        migration.upgrade()
        columns = {column["name"] for column in sa.inspect(connection).get_columns("jobs")}

    assert "queue_hidden" in columns
