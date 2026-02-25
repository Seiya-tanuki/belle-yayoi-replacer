from __future__ import annotations

import hashlib
from pathlib import Path
import typing


def sha256_file_chunked(path: Path, chunk_size: int = 1024 * 1024) -> str:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    digest = hashlib.sha256()
    with path.open("rb") as f:
        reader: typing.BinaryIO = f
        while True:
            data = reader.read(chunk_size)
            if not data:
                break
            digest.update(data)
    return digest.hexdigest()
