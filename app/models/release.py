from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.workflow import ImportWorkflowState

if TYPE_CHECKING:
    from app.models.import_plan import ImportPlan
    from app.models.job import Job
    from app.models.monitoring import MonitoringRecord
    from app.models.release_candidate import ReleaseCandidate
    from app.models.track import Track


class Release(Base):
    __tablename__ = "releases"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    album_artist: Mapped[str | None] = mapped_column(Text, nullable=True)
    year: Mapped[str | None] = mapped_column(String(4), nullable=True)
    release_mbid: Mapped[str | None] = mapped_column(String(36), nullable=True)
    country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    label: Mapped[str | None] = mapped_column(Text, nullable=True)
    catalog_number: Mapped[str | None] = mapped_column(String(128), nullable=True)
    barcode: Mapped[str | None] = mapped_column(String(64), nullable=True)
    track_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    staging_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    import_state: Mapped[ImportWorkflowState] = mapped_column(
        Enum(ImportWorkflowState, native_enum=False, create_constraint=True),
        nullable=False,
        default=ImportWorkflowState.discovered,
        server_default=ImportWorkflowState.discovered.value,
    )
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    rollback_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    recovery_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    job: Mapped[Job] = relationship("Job", back_populates="releases")
    tracks: Mapped[list[Track]] = relationship("Track", back_populates="release", lazy="selectin")
    candidates: Mapped[list[ReleaseCandidate]] = relationship(
        "ReleaseCandidate", back_populates="release", cascade="all, delete-orphan", lazy="selectin"
    )
    import_plans: Mapped[list[ImportPlan]] = relationship(
        "ImportPlan", back_populates="release", cascade="all, delete-orphan", lazy="selectin"
    )
    monitoring_records: Mapped[list[MonitoringRecord]] = relationship(
        "MonitoringRecord", back_populates="release", cascade="all, delete-orphan", lazy="selectin"
    )
