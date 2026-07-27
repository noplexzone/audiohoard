"""Durable source candidate blocklist.

Revision ID: 0015
Revises: 0014
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "releases" in tables:
        release_columns = {column["name"] for column in inspector.get_columns("releases")}
        if "recovery_attempted_at" not in release_columns:
            op.add_column(
                "releases", sa.Column("recovery_attempted_at", sa.DateTime(), nullable=True)
            )
    if "import_plans" in tables:
        plan_columns = {column["name"] for column in inspector.get_columns("import_plans")}
        if "cleanup_attempted_at" not in plan_columns:
            op.add_column(
                "import_plans", sa.Column("cleanup_attempted_at", sa.DateTime(), nullable=True)
            )
    if "source_candidate_blocks" not in tables:
        op.create_table(
            "source_candidate_blocks",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("provider", sa.String(64), nullable=False),
            sa.Column("peer", sa.String(255), nullable=False),
            sa.Column("filename", sa.Text, nullable=False),
            sa.Column("reason", sa.String(64), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.UniqueConstraint(
                "provider", "peer", "filename", name="uq_source_candidate_block_identity"
            ),
        )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "source_candidate_blocks" in tables:
        op.drop_table("source_candidate_blocks")
    if "import_plans" in tables:
        plan_columns = {column["name"] for column in inspector.get_columns("import_plans")}
        if "cleanup_attempted_at" in plan_columns:
            with op.batch_alter_table("import_plans") as batch_op:
                batch_op.drop_column("cleanup_attempted_at")
    if "releases" in tables:
        release_columns = {column["name"] for column in inspector.get_columns("releases")}
        if "recovery_attempted_at" in release_columns:
            with op.batch_alter_table("releases") as batch_op:
                batch_op.drop_column("recovery_attempted_at")
