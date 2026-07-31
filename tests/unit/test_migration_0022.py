from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa
from alembic.config import Config

from alembic import command


def _config(database: Path) -> Config:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database}")
    return cfg


def _state_check_sql(inspector: sa.Inspector, table: str, column: str) -> str:
    return " ".join(
        str(constraint.get("sqltext") or "")
        for constraint in inspector.get_check_constraints(table)
        if column in str(constraint.get("sqltext") or "")
    )


def test_0022_upgrade_downgrade_and_reupgrade_sqlite(tmp_path: Path) -> None:
    database = tmp_path / "migration-0022.db"
    cfg = _config(database)
    command.upgrade(cfg, "0022")

    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        inspector = sa.inspect(engine)
        for table, column in (
            ("tracks", "import_state"),
            ("releases", "import_state"),
            ("import_plans", "status"),
        ):
            checks = _state_check_sql(inspector, table, column)
            assert column in checks
            assert "removed" in checks
        deletion_columns = {
            column["name"] for column in inspector.get_columns("deletion_operations")
        }
        assert {"expected_device", "expected_inode", "file_was_missing"} <= deletion_columns
    finally:
        engine.dispose()

    command.downgrade(cfg, "0021")
    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        inspector = sa.inspect(engine)
        for table, column in (
            ("tracks", "import_state"),
            ("releases", "import_state"),
            ("import_plans", "status"),
        ):
            assert "removed" not in _state_check_sql(inspector, table, column)
        deletion_columns = {
            column["name"] for column in inspector.get_columns("deletion_operations")
        }
        assert not {"expected_device", "expected_inode", "file_was_missing"} & deletion_columns
    finally:
        engine.dispose()

    command.upgrade(cfg, "head")
