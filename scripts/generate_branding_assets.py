#!/usr/bin/env python3
"""Generate Audiohoard branding assets from source images.

Usage:
    uvx --with pillow python scripts/generate_branding_assets.py
    # or in a venv with Pillow installed:
    python scripts/generate_branding_assets.py

Reads:
    app/static/branding/source-app-icon.png  — full-size app icon (square)
    app/static/branding/source-favicon.png   — favicon source (square, may be smaller)

Writes under app/static/branding/:
    favicon.ico              — multi-size ICO (16, 32, 48)
    favicon-16.png
    favicon-32.png
    apple-touch-icon.png     — 180×180
    icon-32.png              — nav brand mark
    icon-192.png             — PWA icon
    icon-512.png             — PWA icon
    site.webmanifest         — PWA manifest with Audiohoard names and sampled colors
"""

from __future__ import annotations

import json
import struct
from pathlib import Path

try:
    from PIL import Image
except ImportError as err:
    raise SystemExit(
        "Pillow is required. Run with:\n"
        "  uvx --with pillow python scripts/generate_branding_assets.py"
    ) from err

BRANDING = Path("app/static/branding")
SOURCE_ICON = BRANDING / "source-app-icon.png"
SOURCE_FAVICON = BRANDING / "source-favicon.png"


def _sample_theme_color(img: Image.Image) -> str:
    small = img.convert("RGB").resize((1, 1), Image.LANCZOS)
    r, g, b = small.getpixel((0, 0))
    return f"#{r:02x}{g:02x}{b:02x}"


def _sample_bg_color(img: Image.Image) -> str:
    rgba = img.convert("RGBA").resize((1, 1), Image.LANCZOS)
    r, g, b, a = rgba.getpixel((0, 0))
    if a < 128:
        return "#000000"
    return f"#{r:02x}{g:02x}{b:02x}"


def _corner_background(img: Image.Image, tolerance: int = 10) -> tuple[int, int, int] | None:
    rgba = img.convert("RGBA")
    coords = [(0, 0), (rgba.width - 1, 0), (0, rgba.height - 1), (rgba.width - 1, rgba.height - 1)]
    corners = [rgba.getpixel(coord) for coord in coords]
    if any(pixel[3] < 250 for pixel in corners):
        return None
    rgb = tuple(int(round(sum(pixel[i] for pixel in corners) / 4)) for i in range(3))
    if any(sum(abs(pixel[i] - rgb[i]) for i in range(3)) > tolerance for pixel in corners):
        return None
    return rgb


def _resize_square(img: Image.Image, size: int) -> Image.Image:
    return img.convert("RGBA").resize((size, size), Image.LANCZOS)


def _write_png_bytes(img: Image.Image) -> bytes:
    import io

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _build_ico(images: list[Image.Image]) -> bytes:
    """Build a minimal .ico from a list of RGBA images."""
    count = len(images)
    header = struct.pack("<HHH", 0, 1, count)
    png_blobs: list[bytes] = [_write_png_bytes(img) for img in images]

    dir_entries = b""
    offset = 6 + count * 16
    for img, blob in zip(images, png_blobs, strict=True):
        w = img.width if img.width < 256 else 0
        h = img.height if img.height < 256 else 0
        size = len(blob)
        dir_entries += struct.pack("<BBBBHHII", w, h, 0, 0, 1, 32, size, offset)
        offset += size

    return header + dir_entries + b"".join(png_blobs)


def main() -> None:
    if not SOURCE_ICON.exists():
        raise SystemExit(f"Missing source image: {SOURCE_ICON}")
    if not SOURCE_FAVICON.exists():
        raise SystemExit(f"Missing source image: {SOURCE_FAVICON}")

    icon_src = Image.open(SOURCE_ICON)
    favicon_src = Image.open(SOURCE_FAVICON)
    favicon_artwork = favicon_src.convert("RGBA")

    theme_color = _sample_theme_color(icon_src)
    detected_icon_bg = _corner_background(icon_src)
    bg_color = (
        f"#{detected_icon_bg[0]:02x}{detected_icon_bg[1]:02x}{detected_icon_bg[2]:02x}"
        if detected_icon_bg
        else _sample_bg_color(icon_src)
    )

    sizes: dict[str, tuple[Image.Image, int]] = {
        "favicon-16.png": (favicon_artwork, 16),
        "favicon-32.png": (favicon_artwork, 32),
        "icon-32.png": (icon_src, 32),
        "apple-touch-icon.png": (icon_src, 180),
        "icon-192.png": (icon_src, 192),
        "icon-512.png": (icon_src, 512),
    }

    for filename, (src, size) in sizes.items():
        out = BRANDING / filename
        _resize_square(src, size).save(out, format="PNG")
        print(f"  wrote {out}")

    ico_images = [_resize_square(favicon_artwork, s) for s in (16, 32, 48)]
    ico_path = BRANDING / "favicon.ico"
    ico_path.write_bytes(_build_ico(ico_images))
    print(f"  wrote {ico_path}")

    manifest = {
        "name": "Audiohoard",
        "short_name": "Audiohoard",
        "description": "Private self-hosted music acquisition and library management",
        "start_url": "/",
        "display": "standalone",
        "background_color": bg_color,
        "theme_color": theme_color,
        "icons": [
            {"src": "/static/branding/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/static/branding/icon-512.png", "sizes": "512x512", "type": "image/png"},
        ],
    }
    manifest_path = BRANDING / "site.webmanifest"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"  wrote {manifest_path}")

    print("Branding assets generated successfully.")


if __name__ == "__main__":
    main()
