from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from alembic.config import Config

from alembic import command


def _cfg(db_path: Path) -> Config:
    os.environ.setdefault("SECRET_KEY", "test-secret")
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{db_path}")
    return cfg


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_0012_watchlist_provider_upgrade_backfill_and_downgrade(tmp_path: Path) -> None:
    db_path = tmp_path / "watchlist_provider.db"
    cfg = _cfg(db_path)
    command.upgrade(cfg, "0011")
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO catalog_artists (name) VALUES ('Existing')")
        conn.commit()

    command.upgrade(cfg, "0012")
    command.upgrade(cfg, "0012")
    with sqlite3.connect(db_path) as conn:
        assert "watchlist_provider" in _columns(conn, "catalog_artists")
        assert conn.execute(
            "SELECT watchlist_provider FROM catalog_artists WHERE name = 'Existing'"
        ).fetchone() == (None,)
        assert "catalog_artist_identities" in {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "catalog_album_providers" in {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

    command.downgrade(cfg, "0011")
    with sqlite3.connect(db_path) as conn:
        assert "watchlist_provider" not in _columns(conn, "catalog_artists")
