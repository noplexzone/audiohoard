"""Add non-destructive job queue visibility.

Revision ID: 0013
Revises: 0012
Create Date: 2026-07-26
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("jobs")}
    if "queue_hidden" not in columns:
        op.add_column(
            "jobs",
            sa.Column(
                "queue_hidden",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            ),
        )


def downgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("jobs")}
    if "queue_hidden" in columns:
        with op.batch_alter_table("jobs") as batch_op:
            batch_op.drop_column("queue_hidden")
