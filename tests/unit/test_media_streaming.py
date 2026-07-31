from __future__ import annotations

import os
from pathlib import Path

import pytest

from app.services import media_streaming
from app.services.media_streaming import (
    MediaAssetError,
    RangeNotSatisfiable,
    open_path_beneath_root,
    parse_single_byte_range,
)


def test_single_byte_ranges_cover_closed_open_and_suffix_forms() -> None:
    assert parse_single_byte_range(None, 10) is None
    assert parse_single_byte_range("bytes=2-5", 10) == (2, 5)
    assert parse_single_byte_range("bytes=2-", 10) == (2, 9)
    assert parse_single_byte_range("bytes=-4", 10) == (6, 9)
    assert parse_single_byte_range("bytes=0-99", 10) == (0, 9)


@pytest.mark.parametrize(
    "value",
    ["items=0-1", "bytes=", "bytes=1", "bytes=1-2,4-5", "bytes=-0", "bytes=9-2", "bytes=10-"],
)
def test_invalid_or_unsatisfiable_ranges_are_rejected(value: str) -> None:
    with pytest.raises(RangeNotSatisfiable):
        parse_single_byte_range(value, 10)


def test_empty_assets_reject_ranges() -> None:
    with pytest.raises(RangeNotSatisfiable):
        parse_single_byte_range("bytes=0-", 0)


def test_descriptor_walk_rejects_escape_symlink_nonregular_and_empty(tmp_path: Path) -> None:
    root = tmp_path / "library"
    album = root / "Artist" / "Album"
    album.mkdir(parents=True)
    valid = album / "song.mp3"
    valid.write_bytes(b"secure-bytes")

    asset = open_path_beneath_root(valid, root)
    try:
        assert os.pread(asset.fd, asset.size, 0) == b"secure-bytes"
    finally:
        asset.close()

    outside = tmp_path / "outside.mp3"
    outside.write_bytes(b"outside")
    (album / "linked.mp3").symlink_to(outside)
    (album / "empty.mp3").write_bytes(b"")

    for unsafe in (
        outside,
        album / "linked.mp3",
        album,
        album / "empty.mp3",
        Path("relative.mp3"),
    ):
        with pytest.raises(MediaAssetError):
            open_path_beneath_root(unsafe, root)


def test_open_descriptor_keeps_original_bytes_after_path_replacement(tmp_path: Path) -> None:
    root = tmp_path / "library"
    root.mkdir()
    path = root / "song.mp3"
    path.write_bytes(b"original")
    asset = open_path_beneath_root(path, root)
    replacement = root / "replacement.mp3"
    replacement.write_bytes(b"replacement")
    replacement.replace(path)
    try:
        assert os.pread(asset.fd, asset.size, 0) == b"original"
    finally:
        asset.close()


async def test_asset_async_iterator_closes_descriptor_when_cancelled(tmp_path: Path) -> None:
    root = tmp_path / "library"
    root.mkdir()
    path = root / "song.mp3"
    path.write_bytes(b"x" * 200_000)
    asset = open_path_beneath_root(path, root)
    fd = asset.fd
    iterator = asset.iter_bytes(0, asset.size - 1, chunk_size=1)
    assert await anext(iterator) == b"x"
    await iterator.aclose()
    with pytest.raises(OSError):
        os.fstat(fd)


def test_cache_cleanup_enforces_item_and_byte_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(media_streaming, "_CACHE_MAX_ITEMS", 2)
    monkeypatch.setattr(media_streaming, "_CACHE_MAX_BYTES", 7)
    for index, content in enumerate((b"1111", b"2222", b"3333")):
        path = tmp_path / f"{index}.mp3"
        path.write_bytes(content)
        os.utime(path, ns=(index + 1, index + 1))

    media_streaming._cleanup_cache(tmp_path)

    retained = list(tmp_path.glob("*.mp3"))
    assert len(retained) == 1
    assert retained[0].name == "2.mp3"
