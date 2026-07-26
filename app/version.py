from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    APP_VERSION = version("audiohoard")
except PackageNotFoundError:  # pragma: no cover - source tree without installation
    APP_VERSION = "0.0.0+dev"
