from __future__ import annotations

import os
import secrets
import stat
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


class PinnedDestinationError(RuntimeError):
    pass


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


@dataclass
class PinnedDestination:
    """A destination parent held by file descriptor beneath a pinned library root."""

    library_root: Path
    destination: Path
    root_fd: int
    parent_fd: int
    parent_parts: tuple[str, ...]
    closed: bool = False

    @classmethod
    def open(cls, library_root: Path, destination: Path) -> PinnedDestination:
        library_root.mkdir(parents=True, exist_ok=True)
        root = library_root.resolve(strict=True)
        try:
            relative = destination.relative_to(root)
        except ValueError as exc:
            raise PinnedDestinationError("destination escapes library root") from exc
        if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
            raise PinnedDestinationError("invalid destination path")

        directory_flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        root_fd = -1
        parent_fd = -1
        try:
            root_fd = os.open(root, directory_flags)
            parent_fd = os.dup(root_fd)
            for part in relative.parts[:-1]:
                try:
                    next_fd = os.open(part, directory_flags, dir_fd=parent_fd)
                except FileNotFoundError:
                    with suppress(FileExistsError):
                        os.mkdir(part, mode=0o755, dir_fd=parent_fd)
                    next_fd = os.open(part, directory_flags, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = next_fd
            pinned = cls(
                library_root=root,
                destination=destination,
                root_fd=root_fd,
                parent_fd=parent_fd,
                parent_parts=tuple(relative.parts[:-1]),
            )
            pinned.verify_attached()
            return pinned
        except OSError as exc:
            if parent_fd >= 0:
                os.close(parent_fd)
            if root_fd >= 0:
                os.close(root_fd)
            raise PinnedDestinationError("destination directory is not safely pinned") from exc
        except Exception:
            if parent_fd >= 0:
                os.close(parent_fd)
            if root_fd >= 0:
                os.close(root_fd)
            raise

    @property
    def name(self) -> str:
        return self.destination.name

    def verify_attached(self) -> None:
        """Prove the pinned parent is still the live path below the pinned root."""
        try:
            root_path_stat = os.stat(self.library_root, follow_symlinks=False)
            if not _same_inode(root_path_stat, os.fstat(self.root_fd)):
                raise PinnedDestinationError("destination directory changed after it was pinned")
            directory_flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            current_fd = os.dup(self.root_fd)
            try:
                for part in self.parent_parts:
                    next_fd = os.open(part, directory_flags, dir_fd=current_fd)
                    os.close(current_fd)
                    current_fd = next_fd
                if not _same_inode(os.fstat(current_fd), os.fstat(self.parent_fd)):
                    raise PinnedDestinationError(
                        "destination directory changed after it was pinned"
                    )
            finally:
                os.close(current_fd)
        except PinnedDestinationError:
            raise
        except OSError as exc:
            raise PinnedDestinationError(
                "destination directory changed after it was pinned"
            ) from exc

    def exists(self, name: str | None = None) -> bool:
        try:
            os.stat(name or self.name, dir_fd=self.parent_fd, follow_symlinks=False)
            return True
        except FileNotFoundError:
            return False

    def is_regular_non_symlink(self, name: str | None = None) -> bool:
        try:
            result = os.stat(name or self.name, dir_fd=self.parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        return stat.S_ISREG(result.st_mode)

    def create_temp(self, *, suffix: str) -> tuple[int, str]:
        for _attempt in range(100):
            name = f".{self.name}.{secrets.token_hex(8)}{suffix}"
            try:
                fd = os.open(
                    name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                    dir_fd=self.parent_fd,
                )
                return fd, name
            except FileExistsError:
                continue
        raise PinnedDestinationError("could not allocate destination temporary file")

    def backup_existing(self, *, suffix: str) -> str:
        """Atomically claim a random backup name before removing the live name."""
        for _attempt in range(100):
            name = f".{self.name}.{secrets.token_hex(8)}{suffix}"
            try:
                os.link(
                    self.name,
                    name,
                    src_dir_fd=self.parent_fd,
                    dst_dir_fd=self.parent_fd,
                    follow_symlinks=False,
                )
            except FileExistsError:
                continue
            try:
                self.unlink(self.name)
            except OSError:
                self.unlink(name)
                raise
            return name
        raise PinnedDestinationError("could not allocate destination backup file")

    def open_read(self, name: str) -> BinaryIO:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(name, flags, dir_fd=self.parent_fd)
        return os.fdopen(fd, "rb")

    def proc_path(self, name: str) -> Path:
        return Path(f"/proc/self/fd/{self.parent_fd}") / name

    def display_path(self, name: str) -> Path:
        return self.destination.parent / name

    def replace(self, source_name: str, destination_name: str) -> None:
        os.replace(
            source_name,
            destination_name,
            src_dir_fd=self.parent_fd,
            dst_dir_fd=self.parent_fd,
        )

    def unlink(self, name: str) -> None:
        with suppress(FileNotFoundError):
            os.unlink(name, dir_fd=self.parent_fd)

    def fsync(self) -> None:
        os.fsync(self.parent_fd)

    def notify_changed(self, name: str | None = None) -> None:
        """Emit one final audio-file change after a batch rewrite completes."""
        os.utime(name or self.name, None, dir_fd=self.parent_fd, follow_symlinks=False)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        os.close(self.parent_fd)
        os.close(self.root_fd)
