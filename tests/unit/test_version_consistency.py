from __future__ import annotations

import re
import tomllib
from pathlib import Path


def test_release_version_is_consistent_across_packaging_and_docs() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    version = project["project"]["version"]

    lock = Path("uv.lock").read_text(encoding="utf-8")
    package_match = re.search(r'\[\[package\]\]\nname = "audiohoard"\nversion = "([^"]+)"', lock)
    assert package_match is not None
    assert package_match.group(1) == version

    dockerfile = Path("docker/Dockerfile").read_text(encoding="utf-8")
    assert f'org.opencontainers.image.version="{version}"' in dockerfile

    readme = Path("README.md").read_text(encoding="utf-8")
    assert f"noplexzone/audiohoard:{version}" in readme

    changelog = Path("CHANGELOG.md").read_text(encoding="utf-8")
    assert f"## [{version}]" in changelog
