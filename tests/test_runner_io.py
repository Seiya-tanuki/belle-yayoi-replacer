from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from belle.runner_io import update_latest_run_id, write_json_atomic, write_text_atomic


class RunnerIOTests(unittest.TestCase):
    def test_write_text_atomic_writes_exact_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "note.txt"
            expected = "first line\nsecond line\n"
            write_text_atomic(path, expected)
            self.assertEqual(expected, path.read_text(encoding="utf-8"))

    def test_write_json_atomic_format_and_encoding(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "manifest.json"
            expected = {"key": "テスト", "value": 1}
            write_json_atomic(path, expected)

            raw = path.read_text(encoding="utf-8")
            self.assertIn("テスト", raw)
            self.assertIn('\n  "key":', raw)
            self.assertEqual(expected, json.loads(raw))

    def test_update_latest_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "LATEST.txt"
            update_latest_run_id(path, "RUN123")
            self.assertEqual("RUN123\n", path.read_text(encoding="utf-8"))

    def test_write_text_atomic_overwrites_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state.txt"
            write_text_atomic(path, "A")
            write_text_atomic(path, "B")
            self.assertEqual("B", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
