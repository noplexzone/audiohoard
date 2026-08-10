"""Add temporary rejected-source cooldown state.

Revision ID: 0030
Revises: 0029
Create Date: 2026-08-10
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0030"
down_revision = "0029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("source_candidate_blocks") as batch_op:
        batch_op.add_column(sa.Column("blocked_until", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(
            sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0")
        )
        batch_op.add_column(
            sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True)
        )
    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            UPDATE source_candidate_blocks
               SET retry_count = 1,
                   last_failure_at = created_at,
                   blocked_until = datetime(created_at, '+15 minutes')
             WHERE lower(replace(reason, '-', '_')) IN
                   ('timeout', 'transfer_timeout', 'provider_timeout')
            """
        )
    )


def downgrade() -> None:
    with op.batch_alter_table("source_candidate_blocks") as batch_op:
        batch_op.drop_column("last_failure_at")
        batch_op.drop_column("retry_count")
        batch_op.drop_column("blocked_until")
