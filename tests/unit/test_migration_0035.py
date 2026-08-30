from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa
from alembic.config import Config

from alembic import command


def _config(database: Path) -> Config:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database}")
    return cfg


def _engine(database: Path) -> sa.Engine:
    return sa.create_engine(f"sqlite:///{database}")


def test_0035_execution_leases_preserve_rows_and_roundtrip(tmp_path: Path) -> None:
    database = tmp_path / "migration-0035.db"
    cfg = _config(database)
    command.upgrade(cfg, "0034")
    engine = _engine(database)
    with engine.begin() as connection:
        job_id = connection.execute(
            sa.text(
                "INSERT INTO jobs(source, query, status) VALUES ('test', 'existing', 'running')"
            )
        ).lastrowid
    engine.dispose()

    command.upgrade(cfg, "head")
    engine = _engine(database)
    inspector = sa.inspect(engine)
    columns = {column["name"]: column for column in inspector.get_columns("jobs")}
    indexes = {index["name"]: index for index in inspector.get_indexes("jobs")}
    assert columns["execution_token"]["nullable"] is True
    assert isinstance(columns["execution_token"]["type"], sa.String)
    assert columns["execution_token"]["type"].length == 36
    assert columns["execution_lease_expires_at"]["nullable"] is True
    assert "ix_jobs_status_execution_lease_expires_at" in indexes
    assert indexes["ix_jobs_status_execution_lease_expires_at"]["column_names"] == [
        "status",
        "execution_lease_expires_at",
    ]
    with engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT status, execution_token, execution_lease_expires_at "
                "FROM jobs WHERE id=:job_id"
            ),
            {"job_id": job_id},
        ).one()
    assert tuple(row) == ("running", None, None)
    engine.dispose()

    command.downgrade(cfg, "0034")
    engine = _engine(database)
    downgraded = {column["name"] for column in sa.inspect(engine).get_columns("jobs")}
    assert "execution_token" not in downgraded
    assert "execution_lease_expires_at" not in downgraded
    with engine.connect() as connection:
        assert (
            connection.execute(
                sa.text("SELECT status FROM jobs WHERE id=:job_id"), {"job_id": job_id}
            ).scalar_one()
            == "running"
        )
    engine.dispose()

    command.upgrade(cfg, "head")
    engine = _engine(database)
    reupgraded = {column["name"] for column in sa.inspect(engine).get_columns("jobs")}
    assert {"execution_token", "execution_lease_expires_at"} <= reupgraded
    engine.dispose()
