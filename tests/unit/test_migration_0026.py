from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa
from alembic.config import Config

from alembic import command

EXPECTED_COLUMNS = {
    "automation_state",
    "automation_attempt_count",
    "automation_claim_token",
    "automation_claimed_at",
    "automation_next_attempt_at",
    "automation_last_attempted_at",
    "automation_decision_json",
    "observed_acoustid_evidence_json",
    "evidence_revision",
    "import_dispatch_state",
    "import_dispatch_claim_token",
    "import_dispatch_attempt_count",
    "import_dispatch_next_attempt_at",
    "import_dispatch_claimed_at",
    "import_dispatch_outcome_json",
}
EXPECTED_INDEXES = {
    "ix_staging_review_automation_candidates",
    "ix_staging_review_automation_claims",
}


def _config(database: Path) -> Config:
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database}")
    return cfg


def test_0026_upgrade_downgrade_and_reupgrade_sqlite(tmp_path: Path) -> None:
    database = tmp_path / "migration-0026.db"
    cfg = _config(database)
    command.upgrade(cfg, "0025")

    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    "INSERT INTO jobs (source, query, status, created_at, updated_at) "
                    "VALUES ('slskd', 'x', 'done', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                )
            )
            job_id = connection.execute(sa.text("SELECT id FROM jobs")).scalar_one()
            connection.execute(
                sa.text(
                    "INSERT INTO releases "
                    "(job_id, source, import_state, created_at, updated_at) "
                    "VALUES (:job_id, 'slskd', 'needs_review', "
                    "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                ),
                {"job_id": job_id},
            )
            release_id = connection.execute(sa.text("SELECT id FROM releases")).scalar_one()
            connection.execute(
                sa.text(
                    "INSERT INTO tracks "
                    "(job_id, release_id, source, acquisition_state, import_state, "
                    "identity_state, fingerprint_state, acoustid_verification_state) "
                    "VALUES (:job_id, :release_id, 'slskd', 'downloaded', 'needs_review', "
                    "'resolved', 'done', 'mismatch')"
                ),
                {"job_id": job_id, "release_id": release_id},
            )
            track_id = connection.execute(sa.text("SELECT id FROM tracks")).scalar_one()
            connection.execute(
                sa.text(
                    "INSERT INTO staging_review_items "
                    "(track_id, release_id, verification_reason, review_state, created_at) "
                    "VALUES (:track_id, :release_id, 'mismatch', 'pending', CURRENT_TIMESTAMP)"
                ),
                {"track_id": track_id, "release_id": release_id},
            )
    finally:
        engine.dispose()

    command.upgrade(cfg, "head")
    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        with engine.connect() as connection:
            inspector = sa.inspect(connection)
            columns = {
                str(column["name"]) for column in inspector.get_columns("staging_review_items")
            }
            indexes = {
                str(index["name"])
                for index in inspector.get_indexes("staging_review_items")
                if index.get("name")
            }
            row = connection.execute(
                sa.text(
                    "SELECT automation_state, automation_attempt_count FROM staging_review_items"
                )
            ).one()
            assert columns >= EXPECTED_COLUMNS
            assert indexes >= EXPECTED_INDEXES
            assert row == ("pending", 0)
            assert "review_automation_attempts" in inspector.get_table_names()
            attempt_columns = {
                str(column["name"])
                for column in inspector.get_columns("review_automation_attempts")
            }
            assert attempt_columns >= {
                "review_item_id",
                "track_id",
                "release_id",
                "attempt_number",
                "evidence_revision",
                "claim_token",
                "state",
                "claimed_at",
                "completed_at",
                "input_json",
                "decision_json",
                "import_outcome_json",
            }
    finally:
        engine.dispose()

    command.downgrade(cfg, "0025")
    engine = sa.create_engine(f"sqlite:///{database}")
    try:
        with engine.connect() as connection:
            inspector = sa.inspect(connection)
            columns = {
                str(column["name"]) for column in inspector.get_columns("staging_review_items")
            }
            indexes = {
                str(index["name"])
                for index in inspector.get_indexes("staging_review_items")
                if index.get("name")
            }
            assert EXPECTED_COLUMNS.isdisjoint(columns)
            assert EXPECTED_INDEXES.isdisjoint(indexes)
            assert "review_automation_attempts" not in inspector.get_table_names()
    finally:
        engine.dispose()

    command.upgrade(cfg, "head")
