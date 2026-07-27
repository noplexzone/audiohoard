from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum, ForeignKey, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.release import Release
    from app.models.release_candidate import ReleaseCandidate


class MonitoringStatus(StrEnum):
    active = "active"
    paused = "paused"
    checking = "checking"
    candidate_found = "candidate_found"
    failed = "failed"


class MonitoringRecord(Base):
    __tablename__ = "monitoring_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    release_id: Mapped[int] = mapped_column(
        ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    candidate_id: Mapped[int | None] = mapped_column(
        ForeignKey("release_candidates.id", ondelete="SET NULL"), nullable=True
    )
    status: Mapped[MonitoringStatus] = mapped_column(
        Enum(MonitoringStatus, native_enum=False, create_constraint=True),
        nullable=False,
        default=MonitoringStatus.active,
        server_default=MonitoringStatus.active.value,
    )
    desired_quality_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    history_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    release: Mapped[Release] = relationship("Release", back_populates="monitoring_records")
    candidate: Mapped[ReleaseCandidate | None] = relationship("ReleaseCandidate")
