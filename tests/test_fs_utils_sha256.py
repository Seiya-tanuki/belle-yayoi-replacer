from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from belle.fs_utils import sha256_file_chunked


class FSUtilsSha256Tests(unittest.TestCase):
    def test_sha256_small_file_matches_hashlib(self) -> None:
        data = b"hello"
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "small.bin"
            path.write_bytes(data)

            actual = sha256_file_chunked(path)

        expected = hashlib.sha256(data).hexdigest()
        self.assertEqual(actual, expected)

    def test_sha256_empty_file_matches_hashlib(self) -> None:
        data = b""
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "empty.bin"
            path.write_bytes(data)

            actual = sha256_file_chunked(path)

        expected = hashlib.sha256(data).hexdigest()
        self.assertEqual(actual, expected)

    def test_sha256_large_file_spanning_multiple_chunks(self) -> None:
        chunk_size = 64 * 1024
        pattern = b"0123456789abcdef"
        target_size = (3 * 1024 * 1024) + 123
        repeats = (target_size // len(pattern)) + 1
        data = (pattern * repeats)[:target_size]

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "large.bin"
            path.write_bytes(data)

            actual = sha256_file_chunked(path, chunk_size=chunk_size)

        expected = hashlib.sha256(data).hexdigest()
        self.assertEqual(actual, expected)

    def test_sha256_invalid_chunk_size_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "small.bin"
            path.write_bytes(b"x")

            with self.assertRaises(ValueError):
                sha256_file_chunked(path, chunk_size=0)


if __name__ == "__main__":
    unittest.main()
