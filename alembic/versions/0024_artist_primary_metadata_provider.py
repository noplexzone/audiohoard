"""Persist per-artist primary metadata provider.

Revision ID: 0024
Revises: 0023
Create Date: 2026-07-31
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "catalog_artists",
        sa.Column("primary_metadata_provider", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("catalog_artists", "primary_metadata_provider")
