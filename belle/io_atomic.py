from __future__ import annotations

import os
from pathlib import Path
from secrets import token_hex


def _tmp_path(dst_path: Path) -> Path:
    return dst_path.parent / f"{dst_path.name}.tmp.{os.getpid()}.{token_hex(8)}"


def _fsync_parent_dir(path: Path) -> None:
    # Best effort. Directory fsync is not available on every platform/filesystem.
    try:
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        pass
    finally:
        os.close(dir_fd)


def atomic_write_bytes(dst_path: Path, data: bytes) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _tmp_path(dst_path)
    try:
        with tmp_path.open("wb") as f:
            f.write(data)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_path, dst_path)
        _fsync_parent_dir(dst_path)
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


def atomic_write_text(
    dst_path: Path,
    text: str,
    encoding: str = "utf-8",
    newline: str = "\n",
) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _tmp_path(dst_path)
    try:
        with tmp_path.open("w", encoding=encoding, newline=newline) as f:
            f.write(text)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_path, dst_path)
        _fsync_parent_dir(dst_path)
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise
