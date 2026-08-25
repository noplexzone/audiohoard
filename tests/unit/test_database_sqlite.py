from __future__ import annotations

from pathlib import Path

from sqlalchemy.exc import OperationalError

from app.database import _make_engine, is_sqlite_database_locked


def test_sqlite_lock_classifier_finds_nested_operational_error() -> None:
    try:
        raise OperationalError("UPDATE jobs", {}, Exception("database is locked"))
    except OperationalError as exc:
        wrapped = RuntimeError("admission failed")
        wrapped.__cause__ = exc

    assert is_sqlite_database_locked(wrapped) is True


def test_sqlite_lock_classifier_ignores_nested_non_lock_and_wrapper_text() -> None:
    try:
        raise OperationalError("UPDATE jobs", {}, Exception("disk I/O error"))
    except OperationalError as exc:
        wrapped = RuntimeError("database is locked")
        wrapped.__cause__ = exc

    assert is_sqlite_database_locked(wrapped) is False


def test_sqlite_lock_classifier_is_cycle_safe() -> None:
    first = RuntimeError("database is locked")
    second = RuntimeError("still locked")
    first.__cause__ = second
    second.__context__ = first

    assert is_sqlite_database_locked(first) is False


async def test_file_sqlite_connections_enable_integrity_and_concurrency_pragmas(
    tmp_path: Path,
) -> None:
    engine = _make_engine(f"sqlite+aiosqlite:///{tmp_path / 'pragmas.db'}")
    try:
        async with engine.connect() as connection:
            foreign_keys = await connection.exec_driver_sql("PRAGMA foreign_keys")
            busy_timeout = await connection.exec_driver_sql("PRAGMA busy_timeout")
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode")
            assert foreign_keys.scalar_one() == 1
            assert busy_timeout.scalar_one() == 30_000
            assert journal_mode.scalar_one().casefold() == "wal"
    finally:
        await engine.dispose()


def test_file_sqlite_pool_covers_max_runtime_acquisition_limit(tmp_path: Path) -> None:
    engine = _make_engine(f"sqlite+aiosqlite:///{tmp_path / 'pool.db'}")
    try:
        assert engine.pool.size() >= 16  # type: ignore[attr-defined]
    finally:
        engine.sync_engine.dispose()
