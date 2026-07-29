from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote


@dataclass(frozen=True)
class RepairResult:
    orphan_count: int
    removed_count: int
    backup_path: Path | None


def sqlite_path(database: str) -> Path:
    prefixes = ("sqlite+aiosqlite:///", "sqlite:///")
    for prefix in prefixes:
        if database.startswith(prefix):
            database = database[len(prefix) :]
            break
    if "://" in database or database == ":memory:":
        raise ValueError("maintenance requires a file-backed SQLite database")
    return Path(unquote(database)).expanduser().resolve()


def _orphans(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT count(*)
        FROM staging_review_items AS review
        WHERE NOT EXISTS (SELECT 1 FROM tracks WHERE tracks.id = review.track_id)
           OR NOT EXISTS (SELECT 1 FROM releases WHERE releases.id = review.release_id)
        """
    ).fetchone()
    return int(row[0] if row else 0)


def _quick_check(connection: sqlite3.Connection) -> None:
    rows = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
    if rows != ["ok"]:
        raise RuntimeError(f"SQLite quick_check failed: {', '.join(rows)}")


def repair_orphan_reviews(
    database: Path,
    *,
    apply: bool = False,
    confirm_stopped: bool = False,
    backup_path: Path | None = None,
    lock_timeout: float = 1.0,
) -> RepairResult:
    database = database.expanduser().resolve()
    if not database.is_file():
        raise FileNotFoundError(database)
    with sqlite3.connect(database, timeout=lock_timeout, isolation_level=None) as connection:
        _quick_check(connection)
        orphan_count = _orphans(connection)
        if not apply or orphan_count == 0:
            return RepairResult(orphan_count, 0, None)
        if not confirm_stopped:
            raise ValueError("refusing apply without --confirm-stopped")
        if backup_path is None:
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            backup_path = database.with_name(f"{database.name}.backup-{stamp}")
        backup_path = backup_path.expanduser().resolve()
        if backup_path.exists():
            raise FileExistsError(backup_path)
        with sqlite3.connect(backup_path) as backup:
            connection.backup(backup)
            _quick_check(backup)
        try:
            connection.execute("BEGIN EXCLUSIVE")
        except sqlite3.OperationalError as exc:
            raise RuntimeError(
                "could not acquire an exclusive SQLite write lock; stop Audiohoard and retry"
            ) from exc
        try:
            cursor = connection.execute(
                """
                DELETE FROM staging_review_items AS review
                WHERE NOT EXISTS (SELECT 1 FROM tracks WHERE tracks.id = review.track_id)
                   OR NOT EXISTS (SELECT 1 FROM releases WHERE releases.id = review.release_id)
                """
            )
            removed = max(cursor.rowcount, 0)
            _quick_check(connection)
            violations = list(connection.execute("PRAGMA foreign_key_check"))
            if violations:
                raise RuntimeError(
                    f"foreign_key_check still reports {len(violations)} violation(s); rolled back"
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return RepairResult(orphan_count, removed, backup_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit or repair orphaned Audiohoard staging-review records."
    )
    parser.add_argument("database", help="SQLite path or sqlite+aiosqlite:/// URL")
    parser.add_argument(
        "--apply", action="store_true", help="Apply the repair; default is dry-run"
    )
    parser.add_argument(
        "--confirm-stopped",
        action="store_true",
        help="Confirm that the Audiohoard container/process is stopped",
    )
    parser.add_argument("--backup", type=Path, help="Explicit backup destination")
    args = parser.parse_args(argv)
    try:
        result = repair_orphan_reviews(
            sqlite_path(args.database),
            apply=args.apply,
            confirm_stopped=args.confirm_stopped,
            backup_path=args.backup,
        )
    except (FileNotFoundError, FileExistsError, RuntimeError, ValueError, sqlite3.Error) as exc:
        parser.error(str(exc))
    if not args.apply:
        print(f"dry-run: {result.orphan_count} orphan review record(s); no changes made")
    elif result.removed_count == 0:
        print("apply: no orphan review records found; no backup required")
    else:
        print(
            f"apply: removed {result.removed_count} orphan review record(s); "
            f"backup={result.backup_path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
