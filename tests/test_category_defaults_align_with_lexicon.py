from __future__ import annotations

import json
import unittest
from pathlib import Path

from belle.defaults import load_category_defaults
from belle.lexicon import load_lexicon


class CategoryDefaultsAlignWithLexiconTests(unittest.TestCase):
    def test_defaults_files_match_lexicon_category_keyset_and_rules(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        lexicon_path = repo_root / "lexicon" / "lexicon.json"
        lexicon = load_lexicon(lexicon_path)
        lexicon_obj = json.loads(lexicon_path.read_text(encoding="utf-8"))

        lexicon_rules = {
            str(category["key"]): category["default_rule"]
            for category in lexicon_obj.get("categories", [])
        }
        lexicon_keys = set(lexicon.categories_by_key.keys())

        self.assertEqual(69, len(lexicon_keys))
        self.assertEqual(lexicon_keys, set(lexicon_rules.keys()))

        expected_accounts = {
            "restaurant_izakaya": "交際費",
            "apps_subscriptions_software": "通信費",
            "utilities": "水道光熱費",
            "banks_credit_unions": "支払手数料",
            "membership_fees": "諸会費",
        }

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
            self.assertEqual("仮払金", loaded.global_fallback.target_account, msg=str(path))
            self.assertEqual("", loaded.global_fallback.target_tax_division, msg=str(path))
            self.assertEqual(0.35, loaded.global_fallback.confidence, msg=str(path))
            self.assertEqual("HIGH", loaded.global_fallback.priority, msg=str(path))
            self.assertEqual("global_fallback", loaded.global_fallback.reason_code, msg=str(path))

            for key, expected_account in expected_accounts.items():
                with self.subTest(path=str(path), category=key):
                    lexicon_rule = lexicon_rules[key]
                    loaded_rule = loaded.defaults[key]
                    self.assertEqual(expected_account, lexicon_rule["target_account"])
                    self.assertEqual("", lexicon_rule["target_tax_division"])
                    self.assertEqual(lexicon_rule["target_account"], loaded_rule.target_account)
                    self.assertEqual(lexicon_rule["target_tax_division"], loaded_rule.target_tax_division)
                    self.assertEqual(float(lexicon_rule["confidence"]), loaded_rule.confidence)
                    self.assertEqual(str(lexicon_rule["priority"]), loaded_rule.priority)
                    self.assertEqual(str(lexicon_rule["reason_code"]), loaded_rule.reason_code)


if __name__ == "__main__":
    unittest.main()
