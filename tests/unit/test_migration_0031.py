from __future__ import annotations

import importlib.util
from contextlib import contextmanager
from pathlib import Path

import sqlalchemy as sa


def _load_migration():
    path = Path(__file__).parents[2] / "alembic" / "versions" / "0031_compilation_track_artists.py"
    spec = importlib.util.spec_from_file_location("migration_0031", path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_migration_0031_backfills_compilation_from_canonical_and_provider_kind(
    monkeypatch, tmp_path: Path
) -> None:
    migration = _load_migration()
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'migration.db'}")

    class Batch:
        def add_column(self, column: sa.Column) -> None:
            return None

    @contextmanager
    def batch_alter_table(table_name: str):
        yield Batch()

    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "CREATE TABLE catalog_albums "
                "(id INTEGER PRIMARY KEY, release_type TEXT, "
                "is_compilation BOOLEAN NOT NULL DEFAULT 0)"
            )
        )
        connection.execute(
            sa.text(
                "CREATE TABLE catalog_album_providers "
                "(id INTEGER PRIMARY KEY, catalog_album_id INTEGER NOT NULL, release_kind TEXT)"
            )
        )
        connection.execute(sa.text("CREATE TABLE catalog_album_tracks (id INTEGER PRIMARY KEY)"))
        connection.execute(
            sa.text(
                "INSERT INTO catalog_albums (id, release_type) VALUES "
                "(1, 'compilation'), (2, 'album'), (3, 'album')"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO catalog_album_providers (id, catalog_album_id, release_kind) "
                "VALUES (1, 2, 'compilation'), (2, 3, 'album')"
            )
        )
        monkeypatch.setattr(migration.op, "batch_alter_table", batch_alter_table)
        monkeypatch.setattr(migration.op, "execute", connection.execute)

        migration.upgrade()
        rows = connection.execute(
            sa.text("SELECT id, is_compilation FROM catalog_albums ORDER BY id")
        ).all()

    assert migration.revision == "0031"
    assert migration.down_revision == "0030"
    assert rows == [(1, 1), (2, 1), (3, 0)]
