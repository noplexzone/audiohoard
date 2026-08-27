from __future__ import annotations

import asyncio
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database import _make_engine, is_sqlite_database_locked, run_with_sqlite_lock_retry


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


async def test_lock_retry_recovers_from_real_sqlite_writer_contention(
    tmp_path: Path,
) -> None:
    engine = _make_engine(f"sqlite+aiosqlite:///{tmp_path / 'real-lock.db'}")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE TABLE values_table (value INTEGER)"))
        async with factory() as holder, factory() as contender:
            await holder.execute(text("INSERT INTO values_table VALUES (1)"))

            async def commit_contender() -> None:
                await contender.execute(text("INSERT INTO values_table VALUES (2)"))
                await contender.commit()

            retry_task = asyncio.create_task(
                run_with_sqlite_lock_retry(
                    contender, commit_contender, attempts=3, delay_seconds=0.01
                )
            )
            await asyncio.sleep(1.1)
            await holder.commit()
            await retry_task

        async with engine.connect() as connection:
            count = await connection.scalar(text("SELECT count(*) FROM values_table"))
            assert count == 2
    finally:
        await engine.dispose()


async def test_lock_retry_commit_restores_busy_timeout_before_pool_reuse(
    tmp_path: Path,
) -> None:
    engine = _make_engine(f"sqlite+aiosqlite:///{tmp_path / 'retry-pragmas.db'}")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE TABLE values_table (value INTEGER)"))
        async with factory() as session:

            async def commit_value() -> None:
                await session.execute(text("INSERT INTO values_table VALUES (1)"))
                await session.commit()

            await run_with_sqlite_lock_retry(session, commit_value)
            connection = await session.connection()
            busy_timeout = await connection.exec_driver_sql("PRAGMA busy_timeout")
            assert busy_timeout.scalar_one() == 30_000
    finally:
        await engine.dispose()


def test_file_sqlite_pool_covers_max_runtime_acquisition_limit(tmp_path: Path) -> None:
    engine = _make_engine(f"sqlite+aiosqlite:///{tmp_path / 'pool.db'}")
    try:
        assert engine.pool.size() >= 16  # type: ignore[attr-defined]
    finally:
        engine.sync_engine.dispose()
