"""Add durable import-review automation claims and audit decisions.

Revision ID: 0026
Revises: 0025
Create Date: 2026-08-05
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("staging_review_items") as batch:
        batch.add_column(
            sa.Column(
                "automation_state", sa.String(length=32), server_default="pending", nullable=False
            )
        )
        batch.add_column(
            sa.Column("automation_attempt_count", sa.Integer(), server_default="0", nullable=False)
        )
        batch.add_column(sa.Column("automation_claim_token", sa.String(length=36), nullable=True))
        batch.add_column(
            sa.Column("automation_claimed_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(
            sa.Column("automation_next_attempt_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(
            sa.Column("automation_last_attempted_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(sa.Column("automation_decision_json", sa.Text(), nullable=True))
        batch.add_column(sa.Column("observed_acoustid_evidence_json", sa.Text(), nullable=True))
        batch.add_column(
            sa.Column("evidence_revision", sa.Integer(), server_default="1", nullable=False)
        )
        batch.add_column(
            sa.Column(
                "import_dispatch_state",
                sa.String(length=32),
                server_default="none",
                nullable=False,
            )
        )
        batch.add_column(
            sa.Column("import_dispatch_claim_token", sa.String(length=36), nullable=True)
        )
        batch.add_column(
            sa.Column(
                "import_dispatch_attempt_count", sa.Integer(), server_default="0", nullable=False
            )
        )
        batch.add_column(
            sa.Column("import_dispatch_next_attempt_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(
            sa.Column("import_dispatch_claimed_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(sa.Column("import_dispatch_outcome_json", sa.Text(), nullable=True))
        batch.create_index(
            "ix_staging_review_automation_candidates",
            ["review_state", "automation_state", "automation_next_attempt_at", "id"],
            unique=False,
        )
        batch.create_index(
            "ix_staging_review_automation_claims",
            ["automation_state", "automation_claimed_at"],
            unique=False,
        )
        batch.create_index(
            "ix_staging_review_items_import_dispatch_claim_token",
            ["import_dispatch_claim_token"],
            unique=False,
        )
    op.create_table(
        "review_automation_attempts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("review_item_id", sa.Integer(), nullable=True),
        sa.Column("track_id", sa.Integer(), nullable=False),
        sa.Column("release_id", sa.Integer(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("evidence_revision", sa.Integer(), nullable=False),
        sa.Column("claim_token", sa.String(length=36), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("input_json", sa.Text(), nullable=True),
        sa.Column("decision_json", sa.Text(), nullable=True),
        sa.Column("import_outcome_json", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["review_item_id"], ["staging_review_items.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("claim_token"),
    )
    op.create_index(
        "ix_review_automation_attempt_review",
        "review_automation_attempts",
        ["review_item_id", "attempt_number"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_review_automation_attempt_review", table_name="review_automation_attempts")
    op.drop_table("review_automation_attempts")
    with op.batch_alter_table("staging_review_items") as batch:
        batch.drop_index("ix_staging_review_items_import_dispatch_claim_token")
        batch.drop_index("ix_staging_review_automation_claims")
        batch.drop_index("ix_staging_review_automation_candidates")
        batch.drop_column("import_dispatch_outcome_json")
        batch.drop_column("import_dispatch_claimed_at")
        batch.drop_column("import_dispatch_claim_token")
        batch.drop_column("import_dispatch_next_attempt_at")
        batch.drop_column("import_dispatch_attempt_count")
        batch.drop_column("import_dispatch_state")
        batch.drop_column("evidence_revision")
        batch.drop_column("observed_acoustid_evidence_json")
        batch.drop_column("automation_decision_json")
        batch.drop_column("automation_last_attempted_at")
        batch.drop_column("automation_next_attempt_at")
        batch.drop_column("automation_claimed_at")
        batch.drop_column("automation_claim_token")
        batch.drop_column("automation_attempt_count")
        batch.drop_column("automation_state")
