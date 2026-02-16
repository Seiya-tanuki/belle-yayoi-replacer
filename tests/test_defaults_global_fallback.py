from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from belle.defaults import load_category_defaults


class DefaultsGlobalFallbackTests(unittest.TestCase):
    def test_missing_global_fallback_uses_karibarai_kin(self) -> None:
        payload = {
            "schema": "belle.category_defaults.v1",
            "version": "test",
            "defaults": {},
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_defaults.json"
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            loaded = load_category_defaults(path)

        self.assertEqual(loaded.global_fallback.debit_account, "仮払金")
        self.assertEqual(loaded.global_fallback.confidence, 0.35)
        self.assertEqual(loaded.global_fallback.priority, "HIGH")
        self.assertEqual(loaded.global_fallback.reason_code, "global_fallback")


if __name__ == "__main__":
    unittest.main()
