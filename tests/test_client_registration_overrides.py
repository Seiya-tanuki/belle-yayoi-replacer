from __future__ import annotations

import csv
import json
import shutil
import unittest
from pathlib import Path
from uuid import uuid4

from belle.category_override_bootstrap import analyze_category_override_teacher
from belle.client_registration_overrides import prepare_registration_category_overrides


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _write_mode_aware_defaults(repo_root: Path, line_id: str, *, excluded: dict, included: dict) -> None:
    base_dir = repo_root / "defaults" / line_id
    _write_json(base_dir / "category_defaults_tax_excluded.json", excluded)
    _write_json(base_dir / "category_defaults_tax_included.json", included)


def _prepare_shared_assets(repo_root: Path) -> None:
    _write_mode_aware_defaults(
        repo_root,
        "receipt",
        excluded={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "food": {
                    "target_account": "旅費交通費",
                    "target_tax_division": "課対仕入内10%適格",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "software": {
                    "target_account": "通信費",
                    "target_tax_division": "課対仕入内10%適格",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
            },
            "global_fallback": {
                "target_account": "仮払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
        included={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "food": {
                    "target_account": "租税公課",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "software": {
                    "target_account": "支払手数料",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
            },
            "global_fallback": {
                "target_account": "仮払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    _write_mode_aware_defaults(
        repo_root,
        "credit_card_statement",
        excluded={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "food": {
                    "target_account": "通信費",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "software": {
                    "target_account": "諸会費",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
            },
            "global_fallback": {
                "target_account": "未払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
        included={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "food": {
                    "target_account": "交際費",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "software": {
                    "target_account": "支払手数料",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
            },
            "global_fallback": {
                "target_account": "未払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    _write_json(
        repo_root / "lexicon" / "lexicon.json",
        {
            "schema": "belle.lexicon.v1",
            "version": "test",
            "categories": [
                {
                    "id": 1,
                    "key": "food",
                    "label": "飲食",
                    "kind": "expense",
                    "precision_hint": 0.9,
                    "deprecated": False,
                    "negative_terms": {"n0": [], "n1": []},
                },
                {
                    "id": 2,
                    "key": "software",
                    "label": "ソフトウェア",
                    "kind": "expense",
                    "precision_hint": 0.8,
                    "deprecated": False,
                    "negative_terms": {"n0": [], "n1": []},
                },
            ],
            "term_rows": [
                ["n0", "LUNCH", 1, 1.0, "S"],
                ["n0", "MEAL", 1, 1.0, "S"],
                ["n0", "SAAS", 2, 1.0, "S"],
                ["n0", "SOFT", 2, 1.0, "S"],
            ],
            "learned": {"policy": {"core_weight": 1.0}},
        },
    )


def _teacher_row(summary: str, debit_account: str) -> list[str]:
    row = [""] * 25
    row[4] = debit_account
    row[16] = summary
    return row


class ClientRegistrationOverridesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def test_prepare_registration_category_overrides_respects_line_scope_and_bookkeeping_mode(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_overrides_scope_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_shared_assets(repo_root)

            prepared = prepare_registration_category_overrides(
                repo_root=repo_root,
                client_id="C_SCOPE",
                line_ids=("receipt",),
                bookkeeping_mode="tax_included",
            )

            self.assertEqual({"receipt"}, set(prepared.keys()))
            payload = prepared["receipt"].payload
            self.assertEqual("belle.category_overrides.v2", payload.get("schema"))
            self.assertEqual("C_SCOPE", payload.get("client_id"))
            self.assertIn("generated_at", payload)
            self.assertEqual(
                {
                    "food": {"target_account": "租税公課", "target_tax_division": "課対仕入込10%"},
                    "software": {"target_account": "支払手数料", "target_tax_division": "課対仕入込10%"},
                },
                payload.get("overrides"),
            )
            self.assertEqual((), prepared["receipt"].changes)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_prepare_registration_category_overrides_applies_teacher_in_memory_without_touching_tax_division(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_overrides_bootstrap_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_shared_assets(repo_root)
            teacher_path = repo_root / "teacher" / "teacher.csv"
            _write_yayoi_rows(
                teacher_path,
                [
                    _teacher_row("LUNCH TOKYO", "交際費"),
                    _teacher_row("MEAL OSAKA", "交際費"),
                    _teacher_row("SAAS MONTHLY", "消耗品費"),
                    _teacher_row("SOFT LICENSE", "消耗品費"),
                ],
            )
            analysis = analyze_category_override_teacher(
                teacher_path=teacher_path,
                lexicon_path=repo_root / "lexicon" / "lexicon.json",
            )

            prepared = prepare_registration_category_overrides(
                repo_root=repo_root,
                client_id="C_BOOTSTRAP",
                line_ids=("receipt", "credit_card_statement"),
                bookkeeping_mode="tax_excluded",
                teacher_analysis=analysis,
            )

            self.assertEqual(
                {"target_account": "交際費", "target_tax_division": "課対仕入内10%適格"},
                (prepared["receipt"].payload.get("overrides") or {}).get("food"),
            )
            self.assertEqual(
                {"target_account": "消耗品費", "target_tax_division": "課対仕入内10%適格"},
                (prepared["receipt"].payload.get("overrides") or {}).get("software"),
            )
            self.assertEqual(
                {"target_account": "交際費", "target_tax_division": "課対仕入込10%"},
                (prepared["credit_card_statement"].payload.get("overrides") or {}).get("food"),
            )
            self.assertEqual(
                {"target_account": "消耗品費", "target_tax_division": "課対仕入込10%"},
                (prepared["credit_card_statement"].payload.get("overrides") or {}).get("software"),
            )
            self.assertEqual(
                [
                    ("food", "旅費交通費", "交際費"),
                    ("software", "通信費", "消耗品費"),
                ],
                [
                    (change.category_key, change.from_target_account, change.to_target_account)
                    for change in prepared["receipt"].changes
                ],
            )
            self.assertEqual(
                [
                    ("food", "通信費", "交際費"),
                    ("software", "諸会費", "消耗品費"),
                ],
                [
                    (change.category_key, change.from_target_account, change.to_target_account)
                    for change in prepared["credit_card_statement"].changes
                ],
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
