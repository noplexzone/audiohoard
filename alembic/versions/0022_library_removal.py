"""Add permanent-removal workflow state and deletion inode evidence.

Revision ID: 0022
Revises: 0021
Create Date: 2026-07-30
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None

_OLD_IMPORT_STATE = sa.Enum(
    "discovered",
    "staged",
    "matching",
    "needs_review",
    "ready",
    "importing",
    "imported",
    "failed",
    "rolled_back",
    name="importworkflowstate",
    native_enum=False,
    create_constraint=True,
)
_NEW_IMPORT_STATE = sa.Enum(
    "discovered",
    "staged",
    "matching",
    "needs_review",
    "ready",
    "importing",
    "imported",
    "removed",
    "failed",
    "rolled_back",
    name="importworkflowstate",
    native_enum=False,
    create_constraint=True,
)


def upgrade() -> None:
    for table, column in (
        ("tracks", "import_state"),
        ("releases", "import_state"),
        ("import_plans", "status"),
    ):
        with op.batch_alter_table(table) as batch_op:
            batch_op.alter_column(
                column,
                existing_type=_OLD_IMPORT_STATE,
                type_=_NEW_IMPORT_STATE,
                existing_nullable=False,
            )
    with op.batch_alter_table("deletion_operations") as batch_op:
        batch_op.add_column(sa.Column("expected_device", sa.BigInteger(), nullable=True))
        batch_op.add_column(sa.Column("expected_inode", sa.BigInteger(), nullable=True))
        batch_op.add_column(
            sa.Column("file_was_missing", sa.Boolean(), server_default="0", nullable=False)
        )


def downgrade() -> None:
    for table, column in (
        ("tracks", "import_state"),
        ("releases", "import_state"),
        ("import_plans", "status"),
    ):
        op.execute(
            sa.text(f"UPDATE {table} SET {column} = 'needs_review' WHERE {column} = 'removed'")
        )
    with op.batch_alter_table("deletion_operations") as batch_op:
        batch_op.drop_column("file_was_missing")
        batch_op.drop_column("expected_inode")
        batch_op.drop_column("expected_device")
    for table, column in (
        ("tracks", "import_state"),
        ("releases", "import_state"),
        ("import_plans", "status"),
    ):
        with op.batch_alter_table(table) as batch_op:
            batch_op.alter_column(
                column,
                existing_type=_NEW_IMPORT_STATE,
                type_=_OLD_IMPORT_STATE,
                existing_nullable=False,
            )
