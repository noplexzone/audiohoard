from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

from app.models.import_plan import _ADOPTION_CLAIM_INSERT_TRIGGER


def test_begin_immediate_serializes_adoption_against_ordinary_import(tmp_path: Path) -> None:
    database = tmp_path / "claim-race.db"
    scanner = sqlite3.connect(database, timeout=5, check_same_thread=False)
    importer = sqlite3.connect(database, timeout=5, check_same_thread=False)
    for connection in (scanner, importer):
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
    scanner.execute(
        """
        CREATE TABLE import_plans (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            file_state TEXT NOT NULL,
            destination_path TEXT NOT NULL,
            planned_operations_json TEXT
        )
        """
    )
    scanner.execute(_ADOPTION_CLAIM_INSERT_TRIGGER)
    scanner.commit()

    scanner.execute("BEGIN IMMEDIATE")
    assert (
        scanner.execute(
            "SELECT COUNT(*) FROM import_plans WHERE destination_path = ?",
            ("/music/song.flac",),
        ).fetchone()[0]
        == 0
    )

    attempted = threading.Event()
    result: list[str] = []

    def ordinary_import() -> None:
        attempted.set()
        try:
            importer.execute(
                """
                INSERT INTO import_plans
                    (status, file_state, destination_path, planned_operations_json)
                VALUES ('ready', 'present', '/music/song.flac', '{}')
                """
            )
            importer.commit()
            result.append("committed")
        except sqlite3.IntegrityError:
            importer.rollback()
            result.append("claim_rejected")

    contender = threading.Thread(target=ordinary_import)
    contender.start()
    assert attempted.wait(timeout=1)
    time.sleep(0.05)
    assert contender.is_alive(), "ordinary import was not serialized by BEGIN IMMEDIATE"

    scanner.execute(
        """
        INSERT INTO import_plans
            (status, file_state, destination_path, planned_operations_json)
        VALUES (
            'imported',
            'present',
            '/music/song.flac',
            '{"operation": "adopt_in_place"}'
        )
        """
    )
    scanner.commit()
    contender.join(timeout=2)

    assert not contender.is_alive()
    assert result == ["claim_rejected"]
    assert scanner.execute("SELECT COUNT(*) FROM import_plans").fetchone()[0] == 1
    importer.close()
    scanner.close()
