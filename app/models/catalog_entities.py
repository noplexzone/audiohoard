from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.job import Job
    from app.models.track import Track


class CatalogArtist(Base):
    __tablename__ = "catalog_artists"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    mbid: Mapped[str | None] = mapped_column(String(36), nullable=True, unique=True)
    deezer_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    itunes_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    artwork_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    provenance_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    monitored: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    monitor_policy: Mapped[str] = mapped_column(
        String(32), nullable=False, default="all", server_default="all"
    )
    watchlist_provider: Mapped[str | None] = mapped_column(String(32), nullable=True)
    last_enriched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_refreshed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    enrichment_state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="idle", server_default="idle"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    albums: Mapped[list[CatalogAlbum]] = relationship(
        "CatalogAlbum", back_populates="artist", cascade="all, delete-orphan", lazy="selectin"
    )
    identities: Mapped[list[CatalogArtistIdentity]] = relationship(
        "CatalogArtistIdentity",
        back_populates="artist",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class CatalogAlbum(Base):
    __tablename__ = "catalog_albums"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    artist_id: Mapped[int] = mapped_column(
        ForeignKey("catalog_artists.id", ondelete="CASCADE"), nullable=False
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    year: Mapped[str | None] = mapped_column(String(4), nullable=True)
    release_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    mbid: Mapped[str | None] = mapped_column(String(36), nullable=True, unique=True)
    deezer_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    itunes_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    artwork_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    track_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_rating: Mapped[str] = mapped_column(
        String(16), nullable=False, default="unknown", server_default="unknown"
    )
    upc: Mapped[str | None] = mapped_column(String(64), nullable=True)
    providers_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    provenance_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    monitored: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    in_library: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    artist: Mapped[CatalogArtist] = relationship("CatalogArtist", back_populates="albums")
    provider_releases: Mapped[list[CatalogAlbumProvider]] = relationship(
        "CatalogAlbumProvider", back_populates="catalog_album", lazy="selectin"
    )
    tracks: Mapped[list[CatalogAlbumTrack]] = relationship(
        "CatalogAlbumTrack",
        back_populates="album",
        cascade="all, delete-orphan",
        order_by="CatalogAlbumTrack.disc, CatalogAlbumTrack.position",
    )
    jobs: Mapped[list[Job]] = relationship("Job", back_populates="catalog_album")
    library_tracks: Mapped[list[Track]] = relationship("Track", back_populates="catalog_album")


class CatalogArtistIdentity(Base):
    __tablename__ = "catalog_artist_identities"
    __table_args__ = (
        UniqueConstraint("provider", "provider_artist_id", name="uq_artist_identity_provider_id"),
        UniqueConstraint("artist_id", "provider", name="uq_artist_identity_artist_provider"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    artist_id: Mapped[int] = mapped_column(
        ForeignKey("catalog_artists.id", ondelete="CASCADE"), nullable=False, index=True
    )
    provider: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    provider_artist_id: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    artwork_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    provenance_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_enriched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_discography_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    artist: Mapped[CatalogArtist] = relationship("CatalogArtist", back_populates="identities")
    releases: Mapped[list[CatalogAlbumProvider]] = relationship(
        "CatalogAlbumProvider",
        back_populates="artist_identity",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class CatalogAlbumProvider(Base):
    __tablename__ = "catalog_album_providers"
    __table_args__ = (
        UniqueConstraint(
            "artist_identity_id", "provider_album_id", name="uq_album_provider_identity_release"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    artist_identity_id: Mapped[int] = mapped_column(
        ForeignKey("catalog_artist_identities.id", ondelete="CASCADE"), nullable=False, index=True
    )
    catalog_album_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_albums.id", ondelete="SET NULL"), nullable=True, index=True
    )
    provider_album_id: Mapped[str] = mapped_column(String(128), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    year: Mapped[str | None] = mapped_column(String(4), nullable=True)
    artwork_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    track_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    release_kind: Mapped[str] = mapped_column(
        String(32), nullable=False, default="unknown", server_default="unknown"
    )
    release_type_raw: Mapped[str | None] = mapped_column(String(128), nullable=True)
    content_rating: Mapped[str] = mapped_column(
        String(16), nullable=False, default="unknown", server_default="unknown"
    )
    upc: Mapped[str | None] = mapped_column(String(64), nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    monitored: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    artist_identity: Mapped[CatalogArtistIdentity] = relationship(
        "CatalogArtistIdentity", back_populates="releases"
    )
    catalog_album: Mapped[CatalogAlbum | None] = relationship(
        "CatalogAlbum", back_populates="provider_releases", lazy="joined"
    )


class CatalogAlbumTrack(Base):
    __tablename__ = "catalog_album_tracks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    album_id: Mapped[int] = mapped_column(
        ForeignKey("catalog_albums.id", ondelete="CASCADE"), nullable=False
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    disc: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    title: Mapped[str] = mapped_column(Text, nullable=False)
    duration_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)
    recording_mbid: Mapped[str | None] = mapped_column(String(36), nullable=True)
    content_rating: Mapped[str] = mapped_column(
        String(16), nullable=False, default="unknown", server_default="unknown"
    )

    album: Mapped[CatalogAlbum] = relationship("CatalogAlbum", back_populates="tracks")
    jobs: Mapped[list[Job]] = relationship("Job", back_populates="catalog_track")
    library_tracks: Mapped[list[Track]] = relationship("Track", back_populates="catalog_track")
