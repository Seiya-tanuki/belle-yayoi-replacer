from __future__ import annotations

import unittest
from pathlib import Path


class LocalUiDonePageTests(unittest.TestCase):
    def test_collect_zip_path_returns_none_when_missing(self) -> None:
        from belle.local_ui.pages.done import collect_zip_path

        self.assertIsNone(collect_zip_path({}))
        self.assertIsNone(collect_zip_path({"zip_path": ""}))
        self.assertIsNone(collect_zip_path(None))

    def test_collect_zip_path_returns_path_when_present(self) -> None:
        from belle.local_ui.pages.done import collect_zip_path

        result = collect_zip_path({"zip_path": "C:/tmp/result.zip"})
        self.assertEqual(Path("C:/tmp/result.zip"), result)


if __name__ == "__main__":
    unittest.main()
