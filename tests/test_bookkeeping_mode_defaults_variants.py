from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from belle.defaults import load_category_defaults
from belle.line_runners import credit_card_statement as card_runner
from belle.line_runners import receipt as receipt_runner
from belle.lines import line_asset_paths, resolve_tracked_category_defaults_path


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _blank_row(summary: str) -> list[str]:
    row = [""] * 25
    row[16] = summary
    return row


def _write_mode_aware_defaults(repo_root: Path, line_id: str, *, excluded_account: str, included_account: str) -> None:
    for bookkeeping_mode, account_name in (
        ("tax_excluded", excluded_account),
        ("tax_included", included_account),
    ):
        _write_json(
            repo_root / "defaults" / line_id / f"category_defaults_{bookkeeping_mode}.json",
            {
                "schema": "belle.category_defaults.v2",
                "version": "test",
                "defaults": {
                    "misc": {
                        "target_account": account_name,
                        "target_tax_division": "",
                        "confidence": 0.6,
                        "priority": "MED",
                        "reason_code": "category_default",
                    }
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


def _write_shared_tax_config(repo_root: Path, client_id: str, *, bookkeeping_mode: str) -> None:
    _write_json(
        repo_root / "clients" / client_id / "config" / "yayoi_tax_config.json",
        {
            "schema": "belle.yayoi_tax_config.v1",
            "version": "1.0",
            "enabled": True,
            "bookkeeping_mode": bookkeeping_mode,
            "rounding_mode": "floor",
        },
    )


def _write_minimal_lexicon(repo_root: Path) -> None:
    _write_json(
        repo_root / "lexicon" / "lexicon.json",
        {
            "schema": "belle.lexicon.v1",
            "version": "test",
            "categories": [
                {
                    "id": 1,
                    "key": "misc",
                    "label": "Misc",
                    "kind": "expense",
                    "precision_hint": 0.9,
                    "deprecated": False,
                    "negative_terms": {"n0": [], "n1": []},
                }
            ],
            "term_rows": [["n0", "MISC", 1, 1.0, "S"]],
            "learned": {"policy": {"core_weight": 1.0}},
        },
    )


class BookkeepingModeDefaultsResolutionTests(unittest.TestCase):
    def test_resolve_tracked_category_defaults_path_covers_all_supported_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self.assertEqual(
                repo_root / "defaults" / "receipt" / "category_defaults_tax_excluded.json",
                resolve_tracked_category_defaults_path(repo_root, "receipt", bookkeeping_mode="tax_excluded"),
            )
            self.assertEqual(
                repo_root / "defaults" / "receipt" / "category_defaults_tax_included.json",
                resolve_tracked_category_defaults_path(repo_root, "receipt", bookkeeping_mode="tax_included"),
            )
            self.assertEqual(
                repo_root / "defaults" / "credit_card_statement" / "category_defaults_tax_excluded.json",
                resolve_tracked_category_defaults_path(
                    repo_root,
                    "credit_card_statement",
                    bookkeeping_mode="tax_excluded",
                ),
            )
            self.assertEqual(
                repo_root / "defaults" / "credit_card_statement" / "category_defaults_tax_included.json",
                resolve_tracked_category_defaults_path(
                    repo_root,
                    "credit_card_statement",
                    bookkeeping_mode="tax_included",
                ),
            )

    def test_line_asset_paths_fail_closed_for_missing_or_unsupported_bookkeeping_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            with self.assertRaisesRegex(ValueError, "bookkeeping_mode is required"):
                line_asset_paths(repo_root, "receipt")
            with self.assertRaisesRegex(ValueError, "invalid bookkeeping_mode"):
                line_asset_paths(repo_root, "credit_card_statement", bookkeeping_mode="broken_mode")
            with self.assertRaisesRegex(ValueError, "unsupported for line_id"):
                resolve_tracked_category_defaults_path(repo_root, "bank_statement", bookkeeping_mode="tax_excluded")


class BookkeepingModeBootstrapTests(unittest.TestCase):
    def test_receipt_runner_bootstrap_uses_shared_tax_config_selected_defaults_variant(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_RECEIPT_BOOTSTRAP"
            client_dir = repo_root / "clients" / client_id / "lines" / "receipt"
            config_path = repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json"
            _write_shared_tax_config(repo_root, client_id, bookkeeping_mode="tax_included")
            _write_minimal_lexicon(repo_root)
            _write_mode_aware_defaults(
                repo_root,
                "receipt",
                excluded_account="雑費",
                included_account="租税公課",
            )
            _write_json(
                config_path,
                {
                    "version": "1.16",
                    "csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"},
                },
            )
            _write_yayoi_rows(client_dir / "inputs" / "kari_shiwake" / "target.csv", [_blank_row("MISC")])

            tm = SimpleNamespace(t_numbers={})
            tm_summary = SimpleNamespace(applied_new_files=[], rows_used_added=0, warnings=[])
            autogrow_summary = SimpleNamespace(
                processed_files=0,
                processed_rows=0,
                unclassified_rows_seen=0,
                new_keys=0,
                updated_keys=0,
                skipped_by_reason={},
                warnings=[],
            )
            captured_defaults_paths: list[Path] = []
            captured_accounts: list[str] = []

            def _load_defaults(path: Path):
                captured_defaults_paths.append(path)
                return load_category_defaults(path)

            def _merge_defaults(global_defaults, overrides_by_category):
                captured_accounts.append(global_defaults.defaults["misc"].target_account)
                return global_defaults

            with mock.patch.object(receipt_runner, "load_category_defaults", side_effect=_load_defaults):
                with mock.patch.object(receipt_runner, "merge_effective_defaults", side_effect=_merge_defaults):
                    with mock.patch.object(
                        receipt_runner,
                        "ensure_client_cache_updated",
                        return_value=(tm, tm_summary),
                    ):
                        with mock.patch.object(
                            receipt_runner,
                            "ensure_lexicon_candidates_updated_from_ledger_ref",
                            return_value=autogrow_summary,
                        ):
                            with mock.patch.object(
                                receipt_runner,
                                "replace_yayoi_csv",
                                return_value={"changed_ratio": 0.0, "output_file": "stub.csv", "analysis": {}},
                            ):
                                receipt_runner.run_receipt(
                                    repo_root,
                                    client_id,
                                    client_layout_line_id="receipt",
                                    client_dir=client_dir,
                                    config_path=config_path,
                                )

            overrides_path = client_dir / "config" / "category_overrides.json"
            overrides_obj = json.loads(overrides_path.read_text(encoding="utf-8"))
            self.assertEqual(
                repo_root / "defaults" / "receipt" / "category_defaults_tax_included.json",
                captured_defaults_paths[0],
            )
            self.assertEqual(["租税公課"], captured_accounts)
            self.assertEqual(
                {"target_account": "租税公課", "target_tax_division": ""},
                (overrides_obj.get("overrides") or {}).get("misc"),
            )

    def test_credit_card_runner_bootstrap_uses_shared_tax_config_selected_defaults_variant(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_BOOTSTRAP"
            line_root = repo_root / "clients" / client_id / "lines" / "credit_card_statement"
            _write_shared_tax_config(repo_root, client_id, bookkeeping_mode="tax_included")
            _write_minimal_lexicon(repo_root)
            _write_mode_aware_defaults(
                repo_root,
                "credit_card_statement",
                excluded_account="雑費",
                included_account="諸会費",
            )
            _write_json(
                line_root / "config" / "credit_card_line_config.json",
                {
                    "schema": "belle.credit_card_line_config.v1",
                    "version": "0.2",
                    "placeholder_account_name": "仮払金",
                    "payable_account_name": "未払金",
                    "training": {"exclude_counter_accounts": []},
                    "thresholds": {
                        "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
                        "merchant_key_payable_subaccount": {"min_count": 1, "min_p_majority": 0.5},
                        "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.5},
                    },
                    "tax_division_thresholds": {
                        "merchant_key_target_account_exact": {"min_count": 1, "min_p_majority": 0.5},
                        "merchant_key_target_account_partial": {"min_count": 1, "min_p_majority": 0.5},
                    },
                    "candidate_extraction": {
                        "min_total_count": 1,
                        "min_unique_merchants": 1,
                        "min_unique_counter_accounts": 1,
                        "manual_allow": [],
                    },
                },
            )
            _write_json(
                repo_root / "rulesets" / "credit_card_statement" / "teacher_extraction_rules_v1.json",
                {
                    "schema": "belle.cc_teacher_extraction_rules.v1",
                    "version": "1",
                    "teacher_payable_candidate_accounts": ["未払費用", "未払金"],
                    "hard_include_terms": ["CARD", "カード"],
                    "soft_include_terms": ["VISA"],
                    "exclude_terms": ["デビット", "プリペイド", "ローン"],
                },
            )
            _write_yayoi_rows(line_root / "inputs" / "kari_shiwake" / "target.csv", [_blank_row("MISC")])

            cache_update_summary = {"cache_path": str(line_root / "artifacts" / "cache" / "client_cache.json")}
            captured_defaults_paths: list[Path] = []
            captured_accounts: list[str] = []

            def _load_defaults(path: Path):
                captured_defaults_paths.append(path)
                return load_category_defaults(path)

            def _merge_defaults(global_defaults, overrides_by_category):
                captured_accounts.append(global_defaults.defaults["misc"].target_account)
                return global_defaults

            with mock.patch.object(
                card_runner,
                "ensure_cc_client_cache_updated",
                return_value=(object(), cache_update_summary),
            ):
                with mock.patch.object(card_runner, "load_category_defaults", side_effect=_load_defaults):
                    with mock.patch.object(card_runner, "merge_effective_defaults", side_effect=_merge_defaults):
                        with mock.patch.object(
                            card_runner,
                            "replace_credit_card_yayoi_csv",
                            return_value={
                                "changed_ratio": 0.0,
                                "output_file": "stub.csv",
                                "reports": {},
                                "payable_sub_fill_required_failed": False,
                            },
                        ):
                            card_runner.run_card(repo_root, client_id)

            overrides_path = line_root / "config" / "category_overrides.json"
            overrides_obj = json.loads(overrides_path.read_text(encoding="utf-8"))
            self.assertEqual(
                repo_root / "defaults" / "credit_card_statement" / "category_defaults_tax_included.json",
                captured_defaults_paths[0],
            )
            self.assertEqual(["諸会費"], captured_accounts)
            self.assertEqual(
                {"target_account": "諸会費", "target_tax_division": ""},
                (overrides_obj.get("overrides") or {}).get("misc"),
            )


if __name__ == "__main__":
    unittest.main()
