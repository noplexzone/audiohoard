from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class AppSetting(Base):
    __tablename__ = "app_settings"
    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class ProviderSetting(Base):
    """Persistent key/value store for provider and library settings.

    Secrets are stored in *value_encrypted* (Fernet-authenticated, derived from SECRET_KEY).
    Plain config is stored in *value_plain*.
    Exactly one of the two value columns is non-null per row.
    """

    __tablename__ = "provider_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value_plain: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    value_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
