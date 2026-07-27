from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from app.config import Settings


def test_secret_key_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SECRET_KEY", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_explicit_default_staging_path_falls_back_to_existing_legacy_mount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_exists = Path.exists

    def fake_exists(path: Path) -> bool:
        if path == Path("/staging/audiohoard"):
            return False
        if path == Path("/staging/music-manager"):
            return True
        return original_exists(path)

    monkeypatch.setattr(Path, "exists", fake_exists)

    settings = Settings(
        _env_file=None,
        secret_key="test",
        staging_root=Path("/staging/audiohoard"),
    )

    assert settings.staging_root == Path("/staging/music-manager")


def test_custom_staging_path_is_not_replaced(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Path, "exists", lambda path: path == Path("/staging/music-manager"))

    settings = Settings(_env_file=None, secret_key="test", staging_root=Path("/custom/staging"))

    assert settings.staging_root == Path("/custom/staging")
