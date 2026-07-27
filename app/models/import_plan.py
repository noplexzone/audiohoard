from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum, ForeignKey, Text, func
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
    )
    tag_verification_state: Mapped[TagVerificationState] = mapped_column(
        Enum(TagVerificationState, native_enum=False, create_constraint=True),
        nullable=False,
        default=TagVerificationState.pending,
    )
    status: Mapped[ImportWorkflowState] = mapped_column(
        Enum(ImportWorkflowState, native_enum=False, create_constraint=True),
        nullable=False,
        default=ImportWorkflowState.discovered,
    )
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
