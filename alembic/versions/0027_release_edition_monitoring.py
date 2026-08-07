"""Add per-provider-release monitoring overrides.

Revision ID: 0027
Revises: 0026
Create Date: 2026-08-06
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "catalog_album_providers",
        sa.Column("monitor_override", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("catalog_album_providers", "monitor_override")
