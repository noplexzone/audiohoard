from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String, Text, false, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.catalog_entities import CatalogAlbum, CatalogAlbumTrack
    from app.models.release import Release
    from app.models.track import Track


class JobStatus(StrEnum):
    pending = "pending"
    running = "running"
    done = "done"
    failed = "failed"
    partial = "partial"
    cancelled = "cancelled"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus),
        nullable=False,
        index=True,
        default=JobStatus.pending,
        server_default=JobStatus.pending.value,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected_result_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    queue_hidden: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    parent_job_id: Mapped[int | None] = mapped_column(
        ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    partial_attempt: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    catalog_album_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_albums.id", ondelete="SET NULL"), nullable=True, index=True
    )
    catalog_track_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_album_tracks.id", ondelete="SET NULL"), nullable=True
    )

    tracks: Mapped[list[Track]] = relationship(
        "Track", back_populates="job", cascade="all, delete-orphan"
    )
    releases: Mapped[list[Release]] = relationship(
        "Release", back_populates="job", cascade="all, delete-orphan"
    )
    catalog_album: Mapped[CatalogAlbum | None] = relationship(
        "CatalogAlbum", back_populates="jobs"
    )
    catalog_track: Mapped[CatalogAlbumTrack | None] = relationship(
        "CatalogAlbumTrack", back_populates="jobs"
    )
