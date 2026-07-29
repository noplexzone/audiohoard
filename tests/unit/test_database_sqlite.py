from __future__ import annotations

from pathlib import Path

from app.database import _make_engine


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
