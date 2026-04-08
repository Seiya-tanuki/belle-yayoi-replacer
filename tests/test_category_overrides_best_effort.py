from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from belle.defaults import (
    CATEGORY_OVERRIDES_SCHEMA_V2,
    CategoryOverride,
    UTF8_BOM,
    CategoryDefaults,
    DefaultRule,
    generate_full_category_overrides,
    try_load_category_overrides,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


class CategoryOverridesBestEffortTests(unittest.TestCase):
    def test_missing_keys_allowed_returns_partial_and_warning(self) -> None:
        payload = {
            "schema": CATEGORY_OVERRIDES_SCHEMA_V2,
            "overrides": {
                "known_a": {"target_account": "科目A", "target_tax_division": ""},
            },
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_overrides.json"
            _write_json(path, payload)

            resolved, warnings = try_load_category_overrides(path, ["known_a", "known_b"])

        self.assertEqual(
            {"known_a": CategoryOverride(target_account="科目A", target_tax_division="")},
            resolved,
        )
        self.assertTrue(any(w.startswith("category_overrides_missing_keys:") for w in warnings))
        self.assertTrue(
            any("count=1" in w and "sample=[" in w for w in warnings if w.startswith("category_overrides_missing_keys:"))
        )

    def test_extra_keys_are_ignored_and_warned(self) -> None:
        payload = {
            "schema": CATEGORY_OVERRIDES_SCHEMA_V2,
            "overrides": {
                "known_a": {"target_account": "科目A", "target_tax_division": ""},
                "extra_key": {"target_account": "科目X", "target_tax_division": ""},
            },
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_overrides.json"
            _write_json(path, payload)

            resolved, warnings = try_load_category_overrides(path, ["known_a"])

        self.assertEqual(
            {"known_a": CategoryOverride(target_account="科目A", target_tax_division="")},
            resolved,
        )
        self.assertTrue(any(w.startswith("category_overrides_extra_keys:") for w in warnings))

    def test_invalid_target_account_is_ignored_and_warned(self) -> None:
        payload = {
            "schema": CATEGORY_OVERRIDES_SCHEMA_V2,
            "overrides": {
                "known_a": {"target_account": "", "target_tax_division": ""},
            },
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_overrides.json"
            _write_json(path, payload)

            resolved, warnings = try_load_category_overrides(path, ["known_a"])

        self.assertEqual({}, resolved)
        self.assertTrue(any(w.startswith("category_overrides_value_invalid:") for w in warnings))

    def test_missing_target_tax_division_is_warned_and_ignored(self) -> None:
        payload = {
            "schema": CATEGORY_OVERRIDES_SCHEMA_V2,
            "overrides": {
                "known_a": {"target_account": "科目A"},
            },
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_overrides.json"
            _write_json(path, payload)

            resolved, warnings = try_load_category_overrides(path, ["known_a"])

        self.assertEqual({}, resolved)
        self.assertTrue(any(w.startswith("category_overrides_row_missing_keys:") for w in warnings))
        self.assertTrue(any(w.startswith("category_overrides_value_invalid:") for w in warnings))

    def test_extra_row_keys_are_warned_and_ignored(self) -> None:
        payload = {
            "schema": CATEGORY_OVERRIDES_SCHEMA_V2,
            "overrides": {
                "known_a": {
                    "target_account": "科目A",
                    "target_tax_division": "",
                    "ignored": "x",
                },
            },
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_overrides.json"
            _write_json(path, payload)

            resolved, warnings = try_load_category_overrides(path, ["known_a"])

        self.assertEqual(
            {"known_a": CategoryOverride(target_account="科目A", target_tax_division="")},
            resolved,
        )
        self.assertTrue(any(w.startswith("category_overrides_row_extra_keys:") for w in warnings))

    def test_schema_mismatch_returns_warning_without_raise(self) -> None:
        payload = {
            "schema": "wrong.schema",
            "overrides": {},
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_overrides.json"
            _write_json(path, payload)

            resolved, warnings = try_load_category_overrides(path, [])

        self.assertEqual({}, resolved)
        self.assertTrue(any(w.startswith("category_overrides_schema_invalid:") for w in warnings))

    def test_invalid_json_returns_warning_without_raise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_overrides.json"
            path.write_text("{", encoding="utf-8")

            resolved, warnings = try_load_category_overrides(path, [])

        self.assertEqual({}, resolved)
        self.assertTrue(any(w.startswith("category_overrides_invalid_json:") for w in warnings))

    def test_bom_is_removed_and_warning_is_emitted(self) -> None:
        payload = {
            "schema": CATEGORY_OVERRIDES_SCHEMA_V2,
            "overrides": {"known_a": {"target_account": "科目A", "target_tax_division": ""}},
        }
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_overrides.json"
            raw = UTF8_BOM + json.dumps(payload, ensure_ascii=False).encode("utf-8")
            path.write_bytes(raw)

            resolved, warnings = try_load_category_overrides(path, ["known_a"])
            rewritten = path.read_bytes()

        self.assertEqual(
            {"known_a": CategoryOverride(target_account="科目A", target_tax_division="")},
            resolved,
        )
        self.assertFalse(rewritten.startswith(UTF8_BOM))
        self.assertTrue(any(w.startswith("category_overrides_bom_removed:") for w in warnings))

    def test_generate_full_uses_global_fallback_for_missing_defaults(self) -> None:
        defaults = CategoryDefaults(
            schema="belle.category_defaults.v2",
            version="test",
            defaults={
                "known_a": DefaultRule(
                    target_account="科目A",
                    target_tax_division="",
                    confidence=0.7,
                    priority="MED",
                    reason_code="category_default",
                )
            },
            global_fallback=DefaultRule(
                target_account="仮払金",
                target_tax_division="",
                confidence=0.35,
                priority="HIGH",
                reason_code="global_fallback",
            ),
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "category_overrides.json"
            generate_full_category_overrides(
                path=path,
                client_id="C1",
                global_defaults=defaults,
                lexicon_category_keys=["known_a", "missing_b"],
            )
            payload = json.loads(path.read_text(encoding="utf-8"))

        overrides = payload.get("overrides") or {}
        self.assertEqual("科目A", (overrides.get("known_a") or {}).get("target_account"))
        self.assertEqual("", (overrides.get("known_a") or {}).get("target_tax_division"))
        self.assertEqual("仮払金", (overrides.get("missing_b") or {}).get("target_account"))
        self.assertEqual("", (overrides.get("missing_b") or {}).get("target_tax_division"))


if __name__ == "__main__":
    unittest.main()
