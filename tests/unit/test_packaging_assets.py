from __future__ import annotations

import tomllib
from pathlib import Path


def test_setuptools_includes_web_assets_in_built_distributions() -> None:
    data = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    setuptools_config = data["tool"]["setuptools"]
    package_data = setuptools_config["package-data"]

    assert setuptools_config["include-package-data"] is True
    assert "app" in package_data
    assert "templates/*.html" in package_data["app"]
    assert "static/css/*.css" in package_data["app"]
    assert "static/branding/*" in package_data["app"]
    data_files = setuptools_config["data-files"]
    assert "." in data_files
    assert "CHANGELOG.md" in data_files["."]


def test_branding_assets_exist() -> None:
    branding = Path("app/static/branding")
    for name in (
        "favicon.ico",
        "favicon-16.png",
        "favicon-32.png",
        "icon-32.png",
        "apple-touch-icon.png",
        "icon-192.png",
        "icon-512.png",
        "site.webmanifest",
    ):
        assert (branding / name).exists(), f"Missing branding asset: {name}"


def test_webmanifest_has_audiohoard_name() -> None:
    import json

    manifest = json.loads(Path("app/static/branding/site.webmanifest").read_text(encoding="utf-8"))
    assert manifest["name"] == "Audiohoard"
    assert manifest["short_name"] == "Audiohoard"


def test_settings_forms_are_native_and_not_double_submitted() -> None:
    templates = Path("app/templates")
    base = (templates / "base.html").read_text()
    setup = (templates / "setup.html").read_text()
    settings = (templates / "settings.html").read_text()

    assert 'document.addEventListener("submit"' not in base
    assert 'id="setup-form"' in setup and 'data-custom-submit="true"' in setup
    assert 'id="settings-form"' not in settings
    assert 'data-custom-submit="true"' not in settings
    assert 'headers: {"Content-Type": "application/json"' not in settings
    assert 'method="post" action="/settings/save"' in settings
    assert '"tidal_config_path","tidal_session_path","tidal_quality"' in setup


def test_favicon_png_has_transparent_corners_and_touch_icon_is_opaque() -> None:
    from PIL import Image

    favicon = Image.open("app/static/branding/favicon-32.png").convert("RGBA")
    assert favicon.getpixel((0, 0))[3] == 0
    assert favicon.getpixel((31, 31))[3] == 0

    touch = Image.open("app/static/branding/apple-touch-icon.png").convert("RGBA")
    assert touch.getpixel((0, 0))[3] == 255


def test_favicon_ico_entries_carry_alpha() -> None:
    from PIL import Image

    ico = Image.open("app/static/branding/favicon.ico")
    for index in range(getattr(ico, "n_frames", 1)):
        ico.seek(index)
        image = ico.convert("RGBA")
        assert image.getpixel((0, 0))[3] == 0


def test_dockerfile_version_and_healthcheck_match_current_runtime_contract() -> None:
    dockerfile = Path("docker/Dockerfile").read_text(encoding="utf-8")
    assert 'org.opencontainers.image.version="0.6.2"' in dockerfile
    assert "http://localhost:8000/health/ready" in dockerfile


def test_manual_develop_publish_is_restricted_to_main() -> None:
    workflow = Path(".github/workflows/release.yml").read_text(encoding="utf-8")
    assert "workflow_dispatch:" in workflow
    assert "if: github.event_name == 'push' || github.ref == 'refs/heads/main'" in workflow
