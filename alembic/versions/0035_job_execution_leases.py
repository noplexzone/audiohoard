"""Add durable job execution leases.

Revision ID: 0035
Revises: 0034
Create Date: 2026-08-30
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0035"
down_revision = "0034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("execution_token", sa.String(length=36), nullable=True))
    op.add_column(
        "jobs",
        sa.Column("execution_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_jobs_status_execution_lease_expires_at",
        "jobs",
        ["status", "execution_lease_expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_status_execution_lease_expires_at", table_name="jobs")
    with op.batch_alter_table("jobs") as batch:
        batch.drop_column("execution_lease_expires_at")
        batch.drop_column("execution_token")
