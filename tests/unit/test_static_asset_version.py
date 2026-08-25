from pathlib import Path

from app.main import _static_asset_version


def test_static_asset_version_changes_with_asset_content(tmp_path: Path) -> None:
    css = tmp_path / "css"
    js = tmp_path / "js"
    css.mkdir()
    js.mkdir()
    stylesheet = css / "pages.css"
    stylesheet.write_text(".card { display: block; }")
    (js / "app.js").write_text('console.log("ready");')

    initial = _static_asset_version(tmp_path)
    stylesheet.write_text(".card { display: grid; }")

    assert _static_asset_version(tmp_path) != initial


def test_templates_cache_bust_static_assets_independently_of_app_version() -> None:
    templates = Path("app/templates")
    rendered_sources = "\n".join(path.read_text() for path in templates.rglob("*.html"))

    assert "/static/" in rendered_sources
    assert "?v={{ asset_version }}" in rendered_sources
    assert "?v={{ app_version }}" not in rendered_sources
