from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable

from sqlalchemy import event
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session

from app.config import MAX_PARALLEL_ACQUISITIONS, get_settings

logger = logging.getLogger(__name__)


def is_sqlite_database_locked(exc: BaseException) -> bool:
    return isinstance(exc, OperationalError) and "database is locked" in str(exc).casefold()


async def run_with_sqlite_lock_retry(
    session: AsyncSession,
    operation: Callable[[], Awaitable[None]],
    *,
    attempts: int = 4,
    delay_seconds: float = 0.25,
) -> None:
    """Run a small DB write operation, retrying transient SQLite writer locks.

    A failed SQLite write rolls back the transaction, so callers pass the complete
    operation instead of just retrying ``commit()`` with expired pending changes.
    """
    last_locked: OperationalError | None = None
    for attempt in range(max(1, attempts)):
        try:
            connection = await session.connection()
            await connection.exec_driver_sql("PRAGMA busy_timeout=1000")
            try:
                await operation()
            finally:
                with contextlib.suppress(Exception):
                    await connection.exec_driver_sql("PRAGMA busy_timeout=30000")
            return
        except OperationalError as exc:
            if not is_sqlite_database_locked(exc):
                raise
            last_locked = exc
            await session.rollback()
            if attempt < attempts - 1:
                await asyncio.sleep(delay_seconds * (attempt + 1))
    assert last_locked is not None
    raise last_locked


class Base(DeclarativeBase):
    pass


_AFTER_COMMIT_KEY = "audiohoard_after_commit"
_AFTER_ROLLBACK_KEY = "audiohoard_after_rollback"


def register_transaction_callbacks(
    session: AsyncSession,
    *,
    after_commit: Callable[[], None],
    after_rollback: Callable[[], None],
) -> None:
    """Register filesystem work that follows the surrounding DB transaction."""
    sync_session = session.sync_session
    sync_session.info.setdefault(_AFTER_COMMIT_KEY, []).append(after_commit)
    sync_session.info.setdefault(_AFTER_ROLLBACK_KEY, []).append(after_rollback)


@event.listens_for(Session, "after_commit")
def _run_after_commit_callbacks(session: Session) -> None:
    callbacks = list(session.info.pop(_AFTER_COMMIT_KEY, []))
    session.info.pop(_AFTER_ROLLBACK_KEY, None)
    for callback in callbacks:
        try:
            callback()
        except Exception:
            logger.exception("after-commit transaction callback failed")


@event.listens_for(Session, "after_rollback")
def _run_after_rollback_callbacks(session: Session) -> None:
    callbacks = list(reversed(session.info.pop(_AFTER_ROLLBACK_KEY, [])))
    session.info.pop(_AFTER_COMMIT_KEY, None)
    for callback in callbacks:
        try:
            callback()
        except Exception:
            logger.exception("after-rollback transaction callback failed")


def _make_engine(url: str | None = None) -> AsyncEngine:
    db_url = url or get_settings().database_url
    if not db_url.startswith("sqlite"):
        return create_async_engine(db_url, echo=False)

    is_memory = ":memory:" in db_url or "mode=memory" in db_url
    engine_kwargs = {
        "echo": False,
        "connect_args": {"check_same_thread": False, "timeout": 30.0},
    }
    if not is_memory:
        max_concurrent = max(get_settings().max_concurrent_jobs, MAX_PARALLEL_ACQUISITIONS)
        engine_kwargs["pool_size"] = max(8, max_concurrent + 4)
        engine_kwargs["max_overflow"] = max(4, max_concurrent)
    engine = create_async_engine(db_url, **engine_kwargs)

    @event.listens_for(engine.sync_engine, "connect")
    def _configure_sqlite(dbapi_connection: object, _connection_record: object) -> None:
        cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=30000")
            if not is_memory:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.fetchone()
        finally:
            cursor.close()

    return engine


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = _make_engine()
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def reset_engine(url: str | None = None) -> None:
    """Replace engine/factory — used in tests to point at in-memory SQLite."""
    global _engine, _session_factory
    _engine = _make_engine(url)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
