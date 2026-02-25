from __future__ import annotations

import unittest

from belle.yayoi_csv import text_to_token, token_to_text
from belle.yayoi_text import safe_cell_text, set_cell_text


class YayoiTextHelpersTests(unittest.TestCase):
    def test_safe_cell_text_roundtrip_basic(self) -> None:
        tokens = [text_to_token("  ABC  ", "cp932", template_token=None)]
        self.assertEqual(safe_cell_text(tokens, 0, "cp932"), "  ABC  ")

    def test_safe_cell_text_out_of_range_returns_empty(self) -> None:
        tokens = [text_to_token("ABC", "cp932", template_token=None)]
        self.assertEqual(safe_cell_text(tokens, -1, "cp932"), "")
        self.assertEqual(safe_cell_text(tokens, 1, "cp932"), "")

    def test_set_cell_text_updates_token(self) -> None:
        tokens = [text_to_token("OLD", "cp932", template_token=None)]
        set_cell_text(tokens, 0, "cp932", "NEW")
        self.assertEqual(token_to_text(tokens[0], "cp932"), "NEW")

    def test_set_cell_text_raises_on_invalid_index(self) -> None:
        tokens = [text_to_token("OLD", "cp932", template_token=None)]
        with self.assertRaises(IndexError):
            set_cell_text(tokens, 1, "cp932", "NEW")


if __name__ == "__main__":
    unittest.main()
