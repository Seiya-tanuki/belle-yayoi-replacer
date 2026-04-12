from __future__ import annotations

import unittest
from pathlib import Path

from belle.defaults import load_category_defaults
from belle.lexicon import load_lexicon


class CategoryDefaultsAlignWithLexiconTests(unittest.TestCase):
    def test_defaults_files_match_lexicon_category_keyset(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        lexicon_path = repo_root / "lexicon" / "lexicon.json"
        lexicon = load_lexicon(lexicon_path)
        lexicon_keys = set(lexicon.categories_by_key.keys())

        self.assertEqual(69, len(lexicon_keys))

        for path in (
            repo_root / "defaults" / "receipt" / "category_defaults_tax_excluded.json",
            repo_root / "defaults" / "receipt" / "category_defaults_tax_included.json",
            repo_root / "defaults" / "credit_card_statement" / "category_defaults_tax_excluded.json",
            repo_root / "defaults" / "credit_card_statement" / "category_defaults_tax_included.json",
        ):
            loaded = load_category_defaults(path)
            loaded_keys = set(loaded.defaults.keys())

            self.assertEqual(lexicon_keys, loaded_keys, msg=str(path))
            self.assertEqual(69, len(loaded_keys), msg=str(path))


if __name__ == "__main__":
    unittest.main()
