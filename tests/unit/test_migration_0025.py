from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa
from alembic.config import Config

from alembic import command

EXPECTED_INDEXES = {
    "catalog_albums": "ix_catalog_albums_artist_id",
    "catalog_album_tracks": "ix_catalog_album_tracks_album_id",
    "import_plans": "ix_import_plans_track_id",
}


def _config(database: Path) -> Config:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database}")
    return cfg


def _indexes(connection: sa.Connection, table: str) -> set[str]:
    return {
        str(index["name"])
        for index in sa.inspect(connection).get_indexes(table)
        if index.get("name")
    }


def test_0025_upgrade_downgrade_and_reupgrade_sqlite(tmp_path: Path) -> None:
    database = tmp_path / "migration-0025.db"
    cfg = _config(database)
    command.upgrade(cfg, "head")

    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        with engine.connect() as connection:
            for table, index_name in EXPECTED_INDEXES.items():
                assert index_name in _indexes(connection, table)
    finally:
        engine.dispose()

    command.downgrade(cfg, "0024")
    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        with engine.connect() as connection:
            for table, index_name in EXPECTED_INDEXES.items():
                assert index_name not in _indexes(connection, table)
    finally:
        engine.dispose()

    command.upgrade(cfg, "head")
