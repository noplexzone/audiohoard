"""Fix schema parity: rename cap index; convert acoustid_verification_state to Enum.

Revision ID: 0016
Revises: 0015
Create Date: 2026-07-27
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None

_OLD_IDX = "ix_catalog_album_providers_identity_id"
_NEW_IDX = "ix_catalog_album_providers_artist_identity_id"

_ACOUSTID_ENUM = sa.Enum(
    "pending",
    "verified",
    "mismatch",
    "unavailable",
    "approved",
    "denied",
    name="acoustidverificationstate",
    native_enum=False,
    create_constraint=True,
)


def upgrade() -> None:
    # 0014 stored this state as an unrestricted VARCHAR. Normalize legacy or
    # hand-written values before adding the model CHECK constraint so SQLite's
    # batch copy cannot fail halfway through and leave a temporary table behind.
    op.execute(
        sa.text(
            "UPDATE tracks SET acoustid_verification_state = 'pending' "
            "WHERE acoustid_verification_state NOT IN "
            "('pending', 'verified', 'mismatch', 'unavailable', 'approved', 'denied')"
        )
    )

    inspector = sa.inspect(op.get_bind())

    # 1. Rename index on catalog_album_providers.artist_identity_id.
    existing_idx = {idx["name"] for idx in inspector.get_indexes("catalog_album_providers")}
    if _OLD_IDX in existing_idx:
        op.drop_index(_OLD_IDX, table_name="catalog_album_providers")
    if _NEW_IDX not in existing_idx:
        op.create_index(_NEW_IDX, "catalog_album_providers", ["artist_identity_id"])

    # 2. Convert tracks.acoustid_verification_state from VARCHAR(32) to non-native Enum.
    #    Batch rebuild is required for SQLite to apply the CHECK constraint.
    with op.batch_alter_table("tracks") as batch_op:
        batch_op.alter_column(
            "acoustid_verification_state",
            type_=_ACOUSTID_ENUM,
            existing_type=sa.String(32),
            existing_nullable=False,
            existing_server_default="pending",
        )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())

    # 2. Restore acoustid_verification_state to plain VARCHAR(32).
    with op.batch_alter_table("tracks") as batch_op:
        batch_op.alter_column(
            "acoustid_verification_state",
            type_=sa.String(32),
            existing_type=_ACOUSTID_ENUM,
            existing_nullable=False,
            existing_server_default="pending",
        )

    # 1. Restore old index name.
    existing_idx = {idx["name"] for idx in inspector.get_indexes("catalog_album_providers")}
    if _NEW_IDX in existing_idx:
        op.drop_index(_NEW_IDX, table_name="catalog_album_providers")
    if _OLD_IDX not in existing_idx:
        op.create_index(_OLD_IDX, "catalog_album_providers", ["artist_identity_id"])
