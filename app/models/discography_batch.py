from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.catalog_entities import CatalogAlbum, CatalogAlbumProvider
    from app.models.job import Job


class DiscographyScopeKind(StrEnum):
    artist = "artist"
    wanted_selected = "wanted_selected"
    wanted_page = "wanted_page"
    wanted_all_matching = "wanted_all_matching"


class DiscographyBatchState(StrEnum):
    preview = "preview"
    queued = "queued"
    running = "running"
    paused = "paused"
    completed = "completed"
    completed_with_failures = "completed_with_failures"
    cancelled = "cancelled"


class DiscographyBatchItemState(StrEnum):
    preview = "preview"
    pending = "pending"
    hydrating = "hydrating"
    expanding = "expanding"
    waiting = "waiting"
    complete = "complete"
    skipped = "skipped"
    failed = "failed"
    cancelled = "cancelled"


class DiscographyJobOwnership(StrEnum):
    created = "created"
    observed = "observed"


class DiscographyBatch(Base):
    __tablename__ = "discography_batches"
    __table_args__ = (
        Index("ix_discography_batches_state", "state"),
        Index("ix_discography_batches_created_at", "created_at"),
        CheckConstraint("matching_count >= 0", name="ck_matching_count_nonnegative"),
        CheckConstraint("complete_count >= 0", name="ck_complete_count_nonnegative"),
        CheckConstraint("active_count >= 0", name="ck_active_count_nonnegative"),
        CheckConstraint(
            "hydration_required_count >= 0", name="ck_hydration_required_count_nonnegative"
        ),
        CheckConstraint("missing_count >= 0", name="ck_missing_count_nonnegative"),
        CheckConstraint("skipped_count >= 0", name="ck_skipped_count_nonnegative"),
        CheckConstraint("estimated_job_count >= 0", name="ck_estimated_job_count_nonnegative"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    scope_kind: Mapped[DiscographyScopeKind] = mapped_column(
        Enum(DiscographyScopeKind, native_enum=False, create_constraint=True), nullable=False
    )
    scope_json: Mapped[str] = mapped_column(Text, nullable=False)
    scope_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[DiscographyBatchState] = mapped_column(
        Enum(DiscographyBatchState, native_enum=False, create_constraint=True),
        nullable=False,
        default=DiscographyBatchState.preview,
        server_default=DiscographyBatchState.preview.value,
    )
    matching_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    complete_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    active_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    hydration_required_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    missing_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    skipped_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    estimated_job_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    items: Mapped[list[DiscographyBatchItem]] = relationship(
        "DiscographyBatchItem", back_populates="batch", cascade="all, delete-orphan"
    )


class DiscographyBatchItem(Base):
    __tablename__ = "discography_batch_items"
    __table_args__ = (
        CheckConstraint(
            "trim(release_identity) <> ''",
            name="ck_discography_batch_item_identity",
        ),
        UniqueConstraint(
            "batch_id", "release_identity", name="uq_discography_batch_items_release_identity"
        ),
        CheckConstraint("target_count >= 0", name="ck_target_count_nonnegative"),
        CheckConstraint("active_count >= 0", name="ck_active_count_nonnegative"),
        CheckConstraint("skipped_count >= 0", name="ck_skipped_count_nonnegative"),
        CheckConstraint("estimated_job_count >= 0", name="ck_estimated_job_count_nonnegative"),
        CheckConstraint("attempt_count >= 0", name="ck_attempt_count_nonnegative"),
        Index(
            "uq_discography_batch_items_provider_release",
            "batch_id",
            "provider_release_id",
            unique=True,
            sqlite_where=text("provider_release_id IS NOT NULL"),
        ),
        Index(
            "uq_discography_batch_items_catalog_album",
            "batch_id",
            "catalog_album_id",
            unique=True,
            sqlite_where=text(
                "provider_release_id IS NULL AND catalog_album_id IS NOT NULL "
                "AND release_identity LIKE 'catalog_album:%'"
            ),
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    batch_id: Mapped[int] = mapped_column(
        ForeignKey("discography_batches.id", ondelete="CASCADE"), nullable=False
    )
    release_identity: Mapped[str] = mapped_column(String(256), nullable=False)
    provider_release_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_album_providers.id", ondelete="SET NULL"), nullable=True
    )
    catalog_album_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_albums.id", ondelete="SET NULL"), nullable=True
    )
    artist_name: Mapped[str] = mapped_column(Text, nullable=False)
    release_title: Mapped[str] = mapped_column(Text, nullable=False)
    release_year: Mapped[str | None] = mapped_column(String(4), nullable=True)
    release_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    provider: Mapped[str | None] = mapped_column(String(32), nullable=True)
    state: Mapped[DiscographyBatchItemState] = mapped_column(
        Enum(DiscographyBatchItemState, native_enum=False, create_constraint=True),
        nullable=False,
        default=DiscographyBatchItemState.preview,
        server_default=DiscographyBatchItemState.preview.value,
    )
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    target_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    active_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    skipped_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    estimated_job_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    batch: Mapped[DiscographyBatch] = relationship("DiscographyBatch", back_populates="items")
    provider_release: Mapped[CatalogAlbumProvider | None] = relationship("CatalogAlbumProvider")
    catalog_album: Mapped[CatalogAlbum | None] = relationship("CatalogAlbum")
    job_links: Mapped[list[DiscographyBatchItemJob]] = relationship(
        "DiscographyBatchItemJob", back_populates="item", cascade="all, delete-orphan"
    )


class DiscographyBatchItemJob(Base):
    __tablename__ = "discography_batch_item_jobs"
    __table_args__ = (
        CheckConstraint("ownership IN ('created', 'observed')", name="discographyjobownership"),
        UniqueConstraint("item_id", "job_id", name="uq_discography_batch_item_job"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    item_id: Mapped[int] = mapped_column(
        ForeignKey("discography_batch_items.id", ondelete="CASCADE"), nullable=False
    )
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="RESTRICT"), nullable=False)
    ownership: Mapped[DiscographyJobOwnership] = mapped_column(
        Enum(DiscographyJobOwnership, native_enum=False, create_constraint=False), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    item: Mapped[DiscographyBatchItem] = relationship(
        "DiscographyBatchItem", back_populates="job_links"
    )
    job: Mapped[Job] = relationship("Job")
