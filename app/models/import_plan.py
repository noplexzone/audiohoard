from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import (
    DDL,
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    String,
    Text,
    event,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.workflow import ImportWorkflowState

if TYPE_CHECKING:
    from app.models.release import Release
    from app.models.track import Track


class CollisionState(StrEnum):
    unchecked = "unchecked"
    clear = "clear"
    duplicate = "duplicate"
    conflict = "conflict"
    needs_review = "needs_review"


class TagVerificationState(StrEnum):
    pending = "pending"
    verified = "verified"
    failed = "failed"
    skipped = "skipped"


class LibraryFileState(StrEnum):
    unknown = "unknown"
    present = "present"
    missing = "missing"
    removed = "removed"


class DeletionOperationState(StrEnum):
    prepared = "prepared"
    committed = "committed"
    finalized = "finalized"


class ImportPlan(Base):
    __tablename__ = "import_plans"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    release_id: Mapped[int] = mapped_column(
        ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    track_id: Mapped[int | None] = mapped_column(
        ForeignKey("tracks.id", ondelete="CASCADE"), nullable=True
    )
    source_path: Mapped[str] = mapped_column(Text, nullable=False)
    staging_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    destination_path: Mapped[str] = mapped_column(Text, nullable=False)
    destination_temp_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    planned_operations_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    collision_state: Mapped[CollisionState] = mapped_column(
        Enum(CollisionState, native_enum=False, create_constraint=True),
        nullable=False,
        default=CollisionState.unchecked,
        server_default=CollisionState.unchecked.value,
    )
    tag_verification_state: Mapped[TagVerificationState] = mapped_column(
        Enum(TagVerificationState, native_enum=False, create_constraint=True),
        nullable=False,
        default=TagVerificationState.pending,
        server_default=TagVerificationState.pending.value,
    )
    status: Mapped[ImportWorkflowState] = mapped_column(
        Enum(ImportWorkflowState, native_enum=False, create_constraint=True),
        nullable=False,
        default=ImportWorkflowState.discovered,
        server_default=ImportWorkflowState.discovered.value,
    )
    file_state: Mapped[LibraryFileState] = mapped_column(
        Enum(LibraryFileState, native_enum=False, create_constraint=True),
        nullable=False,
        default=LibraryFileState.unknown,
        server_default=LibraryFileState.unknown.value,
        index=True,
    )
    file_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    file_removed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    file_removal_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    cleanup_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    rollback_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    release: Mapped[Release] = relationship("Release", back_populates="import_plans")
    track: Mapped[Track | None] = relationship("Track", back_populates="import_plans")
    deletion_operations: Mapped[list[DeletionOperation]] = relationship(
        "DeletionOperation", back_populates="import_plan", cascade="all, delete-orphan"
    )


_ADOPTION_CLAIM_INSERT_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_import_plans_adoption_claim_insert
BEFORE INSERT ON import_plans
WHEN (
    (NEW.status IN ('ready', 'importing') AND NEW.file_state != 'removed')
    OR (NEW.status = 'imported' AND NEW.file_state = 'present')
 )
 AND EXISTS (
    SELECT 1 FROM import_plans AS existing
    WHERE (
        existing.destination_path = NEW.destination_path
        OR (NEW.track_id IS NOT NULL AND existing.track_id = NEW.track_id)
      )
      AND (
        (existing.status IN ('ready', 'importing') AND existing.file_state != 'removed')
        OR (existing.status = 'imported' AND existing.file_state = 'present')
      )
      AND (
        json_extract(NEW.planned_operations_json, '$.operation') = 'adopt_in_place'
        OR json_extract(existing.planned_operations_json, '$.operation') = 'adopt_in_place'
      )
 )
BEGIN
    SELECT RAISE(ABORT, 'adopted library destination already claimed');
END
"""

_ADOPTION_CLAIM_UPDATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_import_plans_adoption_claim_update
BEFORE UPDATE OF track_id, source_path, destination_path, planned_operations_json,
    status, file_state
ON import_plans
WHEN (
    (NEW.status IN ('ready', 'importing') AND NEW.file_state != 'removed')
    OR (NEW.status = 'imported' AND NEW.file_state = 'present')
 )
 AND EXISTS (
    SELECT 1 FROM import_plans AS existing
    WHERE existing.id != OLD.id
      AND (
        existing.destination_path = NEW.destination_path
        OR (NEW.track_id IS NOT NULL AND existing.track_id = NEW.track_id)
      )
      AND (
        (existing.status IN ('ready', 'importing') AND existing.file_state != 'removed')
        OR (existing.status = 'imported' AND existing.file_state = 'present')
      )
      AND (
        json_extract(NEW.planned_operations_json, '$.operation') = 'adopt_in_place'
        OR json_extract(existing.planned_operations_json, '$.operation') = 'adopt_in_place'
      )
 )
BEGIN
    SELECT RAISE(ABORT, 'adopted library destination already claimed');
END
"""

event.listen(
    ImportPlan.__table__,
    "after_create",
    DDL(_ADOPTION_CLAIM_INSERT_TRIGGER).execute_if(dialect="sqlite"),  # type: ignore[no-untyped-call]
)
event.listen(
    ImportPlan.__table__,
    "after_create",
    DDL(_ADOPTION_CLAIM_UPDATE_TRIGGER).execute_if(dialect="sqlite"),  # type: ignore[no-untyped-call]
)


class DeletionOperation(Base):
    __tablename__ = "deletion_operations"
    __table_args__ = (
        Index("ix_deletion_operations_group_id", "group_id"),
        Index("ix_deletion_operations_state", "state"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[str] = mapped_column(String(36), nullable=False)
    import_plan_id: Mapped[int] = mapped_column(
        ForeignKey("import_plans.id", ondelete="CASCADE"), nullable=False
    )
    original_path: Mapped[str] = mapped_column(Text, nullable=False)
    temporary_path: Mapped[str] = mapped_column(Text, nullable=False)
    expected_device: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    expected_inode: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    file_was_missing: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    state: Mapped[DeletionOperationState] = mapped_column(
        Enum(DeletionOperationState, native_enum=False, create_constraint=True),
        nullable=False,
        default=DeletionOperationState.prepared,
        server_default=DeletionOperationState.prepared.value,
    )
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    import_plan: Mapped[ImportPlan] = relationship(
        "ImportPlan", back_populates="deletion_operations"
    )
