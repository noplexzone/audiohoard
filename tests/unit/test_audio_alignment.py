import asyncio

import pytest

from app.services.audio_alignment import estimate_centered_offset, find_fingerprint_alignment


def test_fingerprint_alignment_finds_exact_embedded_window() -> None:
    reference = [0x12345678, 0xABCDEF01, 0x0F0F0F0F, 0x55AA55AA]
    full = [0, 1, 2, *reference, 3, 4]

    result = find_fingerprint_alignment(full, reference, frame_rate=2.0)

    assert result is not None
    assert result.offset_seconds == 1.5
    assert result.confidence == "high"


def test_fingerprint_alignment_tolerates_small_bit_noise() -> None:
    reference = [0x12345678, 0xABCDEF01, 0x0F0F0F0F, 0x55AA55AA]
    noisy = [value ^ 1 for value in reference]
    full = [0, 0, *noisy, 0xFFFFFFFF, 0xFFFFFFFF]

    result = find_fingerprint_alignment(full, reference, frame_rate=4.0)

    assert result is not None
    assert result.offset_seconds == 0.5
    assert result.confidence in {"high", "medium"}


def test_fingerprint_alignment_rejects_ambiguous_repeated_window() -> None:
    reference = [0x12345678, 0xABCDEF01, 0x0F0F0F0F, 0x55AA55AA]
    full = [*reference, 0, 0, *reference]

    assert find_fingerprint_alignment(full, reference, frame_rate=2.0) is None


def test_fingerprint_alignment_rejects_overlapping_periodic_match() -> None:
    motif = [0x12345678, 0xABCDEF01, 0x0F0F0F0F, 0x55AA55AA]
    reference = [*motif, *motif]
    full = [*reference, *motif]

    assert find_fingerprint_alignment(full, reference, frame_rate=8.0) is None


def test_fingerprint_alignment_rejects_unrelated_or_short_inputs() -> None:
    reference = [0, 0, 0, 0]

    assert find_fingerprint_alignment([0xFFFFFFFF] * 8, reference, frame_rate=2.0) is None
    assert find_fingerprint_alignment([0, 0], reference, frame_rate=2.0) is None
    assert find_fingerprint_alignment([0] * 8, [], frame_rate=2.0) is None


def test_centered_offset_uses_preview_start_not_track_midpoint() -> None:
    assert estimate_centered_offset(240.0, 30.0) == 105.0
    assert estimate_centered_offset(20.0, 30.0) == 0.0
    assert estimate_centered_offset(None, 30.0) is None


async def test_deezer_download_validates_every_redirect(httpx_mock, tmp_path) -> None:
    from app.services.audio_alignment import _download_reference

    source = "https://cdnt-preview.dzcdn.net/start.mp3"
    httpx_mock.add_response(
        status_code=302, url=source, headers={"Location": "https://evil.test/audio"}
    )

    with pytest.raises(ValueError, match="unapproved reference host"):
        await _download_reference(source, tmp_path / "preview.mp3")


def test_reference_url_rejects_non_deezer_network_targets() -> None:
    from app.services.audio_alignment import _validate_reference_url

    for url in (
        "http://cdnt-preview.dzcdn.net/audio.mp3",
        "https://127.0.0.1/audio.mp3",
        "https://deezer.com.evil.test/audio.mp3",
        "https://user:pass@deezer.com/audio.mp3",
    ):
        with pytest.raises(ValueError):
            _validate_reference_url(url)


async def test_deezer_alignment_removes_transient_preview(monkeypatch, tmp_path) -> None:
    from app.services import audio_alignment
    from app.services.audio_alignment import FingerprintData

    source = tmp_path / "full.mp3"
    source.write_bytes(b"full")
    transient_parent = None

    async def download(url, destination):
        nonlocal transient_parent
        assert url == "https://cdnt-preview.dzcdn.net/reference.mp3"
        destination.write_bytes(b"preview")
        transient_parent = destination.parent

    reference_values = (0x12345678, 0xABCDEF01, 0x0F0F0F0F, 0x55AA55AA)

    async def fingerprint(path, *, max_length_seconds):
        if path == source:
            assert max_length_seconds == 3600
            return FingerprintData((0, 1, *reference_values, 2, 3), 4.0)
        assert max_length_seconds == 90
        assert path.is_file()
        return FingerprintData(reference_values, 2.0)

    monkeypatch.setattr(audio_alignment, "_download_reference", download)
    monkeypatch.setattr(audio_alignment, "_fingerprint", fingerprint)

    result = await audio_alignment.align_deezer_preview(
        source, "https://cdnt-preview.dzcdn.net/reference.mp3"
    )

    assert result is not None
    assert transient_parent is not None
    assert not transient_parent.exists()


async def test_fingerprint_kills_child_when_request_is_cancelled(monkeypatch, tmp_path) -> None:
    from app.services import audio_alignment

    class FakeProcess:
        def __init__(self) -> None:
            self.returncode = None
            self.killed = False

        async def communicate(self):
            if self.killed:
                return b"", b""
            await asyncio.Future()

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9

    process = FakeProcess()

    async def create_process(*args, **kwargs):
        assert args[-3:] == ("-length", "3600", str(tmp_path / "source.flac"))
        return process

    monkeypatch.setattr(audio_alignment.shutil, "which", lambda name: "/usr/bin/fpcalc")
    monkeypatch.setattr(audio_alignment.asyncio, "create_subprocess_exec", create_process)
    task = asyncio.create_task(
        audio_alignment._fingerprint(tmp_path / "source.flac", max_length_seconds=3600)
    )
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.killed is True


async def test_alignment_cancels_sibling_fingerprint_after_failure(monkeypatch, tmp_path) -> None:
    from app.services import audio_alignment

    source = tmp_path / "full.flac"
    source.write_bytes(b"full")
    sibling_cancelled = False
    transient_parent = None

    async def download(url, destination):
        nonlocal transient_parent
        destination.write_bytes(b"preview")
        transient_parent = destination.parent

    async def fingerprint(path, *, max_length_seconds):
        nonlocal sibling_cancelled
        if path == source:
            assert max_length_seconds == 3600
            await asyncio.sleep(0)
            raise RuntimeError("full fingerprint failed")
        assert max_length_seconds == 90
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            sibling_cancelled = True
            raise

    monkeypatch.setattr(audio_alignment, "_download_reference", download)
    monkeypatch.setattr(audio_alignment, "_fingerprint", fingerprint)

    with pytest.raises(RuntimeError, match="full fingerprint failed"):
        await audio_alignment.align_deezer_preview(
            source, "https://cdnt-preview.dzcdn.net/reference.mp3"
        )

    assert sibling_cancelled is True
    assert transient_parent is not None
    assert not transient_parent.exists()
