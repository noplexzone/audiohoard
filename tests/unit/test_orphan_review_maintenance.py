from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.maintenance import orphan_reviews
from app.maintenance.orphan_reviews import repair_orphan_reviews, sqlite_path


def _database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            PRAGMA foreign_keys=OFF;
            CREATE TABLE tracks (id INTEGER PRIMARY KEY);
            CREATE TABLE releases (id INTEGER PRIMARY KEY);
            CREATE TABLE staging_review_items (
                id INTEGER PRIMARY KEY,
                track_id INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
                release_id INTEGER NOT NULL REFERENCES releases(id) ON DELETE CASCADE
            );
            INSERT INTO tracks(id) VALUES (1);
            INSERT INTO releases(id) VALUES (1);
            INSERT INTO staging_review_items(id, track_id, release_id) VALUES
                (1, 1, 1), (2, 99, 1), (3, 1, 99);
            """
        )


def test_orphan_repair_dry_run_never_mutates(tmp_path: Path) -> None:
    database = tmp_path / "audiohoard.db"
    _database(database)

    result = repair_orphan_reviews(database)

    assert result.orphan_count == 2
    assert result.removed_count == 0
    assert result.backup_path is None
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM staging_review_items").fetchone() == (3,)


def test_orphan_repair_requires_stopped_confirmation(tmp_path: Path) -> None:
    database = tmp_path / "audiohoard.db"
    _database(database)

    with pytest.raises(ValueError, match="confirm-stopped"):
        repair_orphan_reviews(database, apply=True)


def test_orphan_repair_creates_verified_backup_and_removes_only_orphans(
    tmp_path: Path,
) -> None:
    database = tmp_path / "audiohoard.db"
    backup = tmp_path / "audiohoard.pre-repair.db"
    _database(database)

    result = repair_orphan_reviews(database, apply=True, confirm_stopped=True, backup_path=backup)

    assert result == result.__class__(2, 2, backup)
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT id FROM staging_review_items").fetchall() == [(1,)]
        assert connection.execute("PRAGMA quick_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    with sqlite3.connect(backup) as connection:
        assert connection.execute("SELECT count(*) FROM staging_review_items").fetchone() == (3,)
        assert connection.execute("PRAGMA quick_check").fetchone() == ("ok",)


def test_orphan_repair_aborts_if_database_changes_during_backup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "audiohoard.db"
    backup = tmp_path / "audiohoard.pre-repair.db"
    _database(database)
    original_backup = orphan_reviews._backup_database

    def backup_then_write(source: sqlite3.Connection, destination: sqlite3.Connection) -> None:
        original_backup(source, destination)
        with sqlite3.connect(database) as writer:
            writer.execute("INSERT INTO tracks(id) VALUES (2)")
            writer.commit()

    monkeypatch.setattr(orphan_reviews, "_backup_database", backup_then_write)

    with pytest.raises(RuntimeError, match="changed while its backup was captured"):
        repair_orphan_reviews(
            database,
            apply=True,
            confirm_stopped=True,
            backup_path=backup,
        )

    assert not backup.exists()
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM staging_review_items").fetchone() == (3,)
        assert connection.execute("SELECT count(*) FROM tracks WHERE id = 2").fetchone() == (1,)


def test_sqlite_path_rejects_non_sqlite_urls(tmp_path: Path) -> None:
    assert sqlite_path(str(tmp_path / "db.sqlite")) == (tmp_path / "db.sqlite").resolve()
    with pytest.raises(ValueError, match="file-backed SQLite"):
        sqlite_path("postgresql://localhost/audiohoard")
