"""Persist library file state and deletion journal.

Revision ID: 0021
Revises: 0020
Create Date: 2026-07-30
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("import_plans") as batch_op:
        batch_op.add_column(
            sa.Column(
                "file_state",
                sa.Enum(
                    "unknown",
                    "present",
                    "missing",
                    "removed",
                    name="libraryfilestate",
                    native_enum=False,
                    create_constraint=True,
                ),
                server_default="unknown",
                nullable=False,
            )
        )
        batch_op.add_column(sa.Column("file_checked_at", sa.DateTime(timezone=True)))
        batch_op.add_column(sa.Column("file_removed_at", sa.DateTime(timezone=True)))
        batch_op.add_column(sa.Column("file_removal_reason", sa.String(length=64)))
        batch_op.create_index("ix_import_plans_file_state", ["file_state"])
    op.create_table(
        "deletion_operations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("group_id", sa.String(length=36), nullable=False),
        sa.Column("import_plan_id", sa.Integer(), nullable=False),
        sa.Column("original_path", sa.Text(), nullable=False),
        sa.Column("temporary_path", sa.Text(), nullable=False),
        sa.Column("state", sa.String(length=9), server_default="prepared", nullable=False),
        sa.Column("error_detail", sa.Text()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("finalized_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "state IN ('prepared', 'committed', 'finalized')",
            name="deletionoperationstate",
        ),
        sa.ForeignKeyConstraint(["import_plan_id"], ["import_plans.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_deletion_operations_group_id", "deletion_operations", ["group_id"])
    op.create_index("ix_deletion_operations_state", "deletion_operations", ["state"])


def downgrade() -> None:
    op.drop_index("ix_deletion_operations_state", table_name="deletion_operations")
    op.drop_index("ix_deletion_operations_group_id", table_name="deletion_operations")
    op.drop_table("deletion_operations")
    with op.batch_alter_table("import_plans") as batch_op:
        batch_op.drop_index("ix_import_plans_file_state")
        batch_op.drop_column("file_removal_reason")
        batch_op.drop_column("file_removed_at")
        batch_op.drop_column("file_checked_at")
        batch_op.drop_column("file_state")
