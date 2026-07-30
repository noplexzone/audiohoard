from __future__ import annotations

import re
import tomllib
from pathlib import Path


def test_setuptools_includes_web_assets_in_built_distributions() -> None:
    data = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    setuptools_config = data["tool"]["setuptools"]
    package_data = setuptools_config["package-data"]

    assert setuptools_config["include-package-data"] is True
    assert "app" in package_data
    assert "templates/*.html" in package_data["app"]
    assert "templates/partials/*.html" in package_data["app"]
    assert "static/css/*.css" in package_data["app"]
    assert "static/js/*.js" in package_data["app"]
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
    setup_js = Path("app/static/js/setup.js").read_text()

    assert 'document.addEventListener("submit"' not in base
    assert 'id="setup-form"' in setup and 'data-custom-submit="true"' in setup
    assert 'id="settings-form"' not in settings
    assert 'data-custom-submit="true"' not in settings
    assert 'headers: {"Content-Type": "application/json"' not in settings
    assert 'method="post" action="/settings/save"' in settings
    assert '"tidal_config_path", "tidal_session_path", "tidal_quality"' in setup_js


def test_branding_sources_are_the_requested_artwork() -> None:
    import hashlib

    expected_sha256 = "1fd2198b6b6dbf556eeb9b1e713332e21b9a53c32be13d400bc06c883cca8bb8"
    branding = Path("app/static/branding")
    for name in ("source-app-icon.png", "source-favicon.png"):
        assert hashlib.sha256((branding / name).read_bytes()).hexdigest() == expected_sha256


def test_generated_png_icons_use_the_requested_artwork() -> None:
    from PIL import Image, ImageChops

    branding = Path("app/static/branding")
    for name, size, source_name in (
        ("favicon-16.png", 16, "source-favicon.png"),
        ("favicon-32.png", 32, "source-favicon.png"),
        ("icon-32.png", 32, "source-app-icon.png"),
        ("apple-touch-icon.png", 180, "source-app-icon.png"),
        ("icon-192.png", 192, "source-app-icon.png"),
        ("icon-512.png", 512, "source-app-icon.png"),
    ):
        source = Image.open(branding / source_name).convert("RGBA")
        actual = Image.open(branding / name).convert("RGBA")
        expected = source.resize((size, size), Image.Resampling.LANCZOS)
        assert ImageChops.difference(actual, expected).getbbox() is None


def test_favicon_ico_uses_the_requested_artwork_at_every_size() -> None:
    from PIL import Image, ImageChops

    source = Image.open("app/static/branding/source-favicon.png").convert("RGBA")
    ico = Image.open("app/static/branding/favicon.ico")
    assert ico.ico.sizes() == {(16, 16), (32, 32), (48, 48)}
    for size in ico.ico.sizes():
        actual = ico.ico.getimage(size).convert("RGBA")
        expected = source.resize(size, Image.Resampling.LANCZOS)
        assert ImageChops.difference(actual, expected).getbbox() is None


def test_dockerfile_version_and_healthcheck_match_current_runtime_contract() -> None:
    dockerfile = Path("docker/Dockerfile").read_text(encoding="utf-8")
    assert 'org.opencontainers.image.version="0.8.5"' in dockerfile
    assert "http://localhost:8000/health/ready" in dockerfile


def test_manual_develop_publish_is_restricted_to_main() -> None:
    workflow = Path(".github/workflows/release.yml").read_text(encoding="utf-8")
    assert "workflow_dispatch:" in workflow
    assert "if: github.event_name == 'push' || github.ref == 'refs/heads/main'" in workflow


def test_templates_are_compatible_with_the_html_content_security_policy() -> None:
    templates = Path("app/templates")
    for template in templates.rglob("*.html"):
        text = template.read_text(encoding="utf-8")
        assert not re.search(r"\b(?:style|on[a-z]+)\s*=", text, re.IGNORECASE), template
        for script in re.findall(r"<script\b[^>]*>", text, re.IGNORECASE):
            assert re.search(r"\bsrc\s*=", script, re.IGNORECASE), (template, script)


def test_api_docs_link_is_about_only_not_sidebar() -> None:
    templates = Path("app/templates")
    base = (templates / "base.html").read_text()
    settings = (templates / "settings.html").read_text()

    assert "/api/docs" not in base
    assert '<a class="btn secondary" href="/api/docs">API docs</a>' in settings
