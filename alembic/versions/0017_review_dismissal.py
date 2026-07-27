"""Persist release review dismissal state.

Revision ID: 0017
Revises: 0016
Create Date: 2026-07-27
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("releases", sa.Column("review_dismissed_at", sa.DateTime(), nullable=True))
    op.execute(
        sa.text(
            """
            UPDATE releases
            SET error_detail = 'review required: legacy release has no recorded reason'
            WHERE import_state IN ('needs_review', 'failed', 'rolled_back')
              AND error_detail IS NULL
              AND rollback_detail IS NULL
            """
        )
    )


def downgrade() -> None:
    op.drop_column("releases", "review_dismissed_at")
