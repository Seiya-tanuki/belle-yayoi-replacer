from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.build_cc_cache import ensure_cc_client_cache_updated
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_SUMMARY,
)
from belle.yayoi_csv import read_yayoi_csv


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "credit_card_statement"


def _write_cc_config(
    line_root: Path,
    *,
    canonical_min_count: int = 3,
    canonical_min_p_majority: float = 0.9,
) -> None:
    cfg = {
        "schema": "belle.credit_card_line_config.v1",
        "version": "0.3",
        "placeholder_account_name": "仮払金",
        "target_payable_placeholder_names": ["未払金"],
        "training": {"exclude_counter_accounts": ["普通預金", "当座預金"]},
        "thresholds": {
            "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
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
        "partial_match": {
            "enabled": False,
            "direction": "cache_key_in_input",
            "require_unique_longest": True,
            "min_match_len": 4,
            "min_stats_sample_total": 10,
            "min_stats_p_majority": 0.95,
        },
        "teacher_extraction": {
            "enabled": True,
            "ruleset_relpath": "rulesets/credit_card_statement/teacher_extraction_rules_v1.json",
            "payable_candidate_accounts": ["未払費用", "未払金"],
            "manual_include_subaccounts": [],
            "manual_exclude_subaccounts": [],
            "soft_match_thresholds": {
                "min_total_count": 1,
                "min_unique_counter_accounts": 1,
                "min_unique_summaries": 1,
            },
            "canonical_payable_thresholds": {
                "min_count": int(canonical_min_count),
                "min_p_majority": float(canonical_min_p_majority),
            },
        },
    }
    cfg_path = line_root / "config" / "credit_card_line_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    ruleset_path = line_root.parents[3] / "rulesets" / "credit_card_statement" / "teacher_extraction_rules_v1.json"
    ruleset_path.parent.mkdir(parents=True, exist_ok=True)
    ruleset_path.write_text(
        json.dumps(
            {
                "schema": "belle.cc_teacher_extraction_rules.v1",
                "version": "1",
                "teacher_payable_candidate_accounts": ["未払費用", "未払金"],
                "hard_include_terms": ["CARD", "カード"],
                "soft_include_terms": ["VISA"],
                "exclude_terms": ["デビット", "プリペイド", "ローン"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _build_row(
    *,
    summary: str,
    counter_account: str,
    payable_subaccount: str,
    payable_account: str = "未払金",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/04/11"
    cols[COL_DEBIT_ACCOUNT] = counter_account
    cols[COL_DEBIT_AMOUNT] = "1000"
    cols[COL_CREDIT_ACCOUNT] = payable_account
    cols[COL_CREDIT_SUBACCOUNT] = payable_subaccount
    cols[COL_SUMMARY] = summary
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


class CCDerivedTeacherPipelineTests(unittest.TestCase):
    def test_derived_csv_manifest_and_learning_counts_follow_selected_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_DERIVED_SELECTED"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="SHOPA / one", counter_account="旅費交通費", payable_subaccount="CARD_A"),
                    _build_row(summary="SHOPB / two", counter_account="消耗品費", payable_subaccount="CARD_A"),
                    _build_row(summary="SHOPC / skip", counter_account="雑費", payable_subaccount="CARD_A", payable_account="買掛金"),
                ],
            )

            cache, summary = ensure_cc_client_cache_updated(repo_root, client_id)

            self.assertEqual(3, int(summary.get("raw_rows_observed_added") or 0))
            self.assertEqual(2, int(summary.get("derived_rows_selected_added") or 0))
            self.assertEqual(2, int(summary.get("rows_total_added") or 0))
            self.assertEqual(2, int(summary.get("rows_used_added") or 0))
            self.assertEqual(2, int(cache.payable_sub_global_stats.sample_total))

            derived_manifest_path = line_root / "artifacts" / "derived" / "cc_teacher_manifest.json"
            derived_manifest = json.loads(derived_manifest_path.read_text(encoding="utf-8"))
            sources = derived_manifest.get("sources") or {}
            self.assertEqual(1, len(sources))
            raw_sha, source_entry = next(iter(sources.items()))
            self.assertEqual(2, int(((source_entry.get("row_counts") or {}).get("selected_rows")) or 0))
            self.assertTrue(bool(source_entry.get("applied_to_cache_learning")))
            self.assertEqual(
                f"artifacts/derived/cc_teacher/{raw_sha}__cc_teacher.csv",
                source_entry.get("derived_csv_relpath"),
            )
            derived_csv = line_root / Path(str(source_entry.get("derived_csv_relpath") or ""))
            self.assertTrue(derived_csv.exists())
            self.assertEqual(2, len(read_yayoi_csv(derived_csv).rows))

    def test_zero_selected_source_is_manifested_and_learns_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_DERIVED_ZERO"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="SHOPA / one", counter_account="旅費交通費", payable_subaccount="法人ローン", payable_account="未払費用"),
                ],
            )

            cache, summary = ensure_cc_client_cache_updated(repo_root, client_id)

            self.assertEqual(1, int(summary.get("raw_rows_observed_added") or 0))
            self.assertEqual(0, int(summary.get("derived_rows_selected_added") or 0))
            self.assertEqual(0, int(summary.get("rows_total_added") or 0))
            self.assertEqual(0, int(summary.get("rows_used_added") or 0))
            self.assertEqual({}, cache.merchant_key_account_stats)
            self.assertEqual("EMPTY", (cache.canonical_payable or {}).get("status"))

            derived_manifest = json.loads(
                (line_root / "artifacts" / "derived" / "cc_teacher_manifest.json").read_text(encoding="utf-8")
            )
            source_entry = next(iter((derived_manifest.get("sources") or {}).values()))
            self.assertEqual(0, int(((source_entry.get("row_counts") or {}).get("selected_rows")) or 0))
            self.assertTrue(bool(source_entry.get("applied_to_cache_learning")))
            derived_csv = line_root / Path(str(source_entry.get("derived_csv_relpath") or ""))
            self.assertEqual(0, len(read_yayoi_csv(derived_csv).rows))

    def test_canonical_payable_ok_and_rerun_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_DERIVED_IDEMPOTENT"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root, canonical_min_count=3, canonical_min_p_majority=0.75)

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="SHOPA / one", counter_account="旅費交通費", payable_subaccount="CARD_A", payable_account="未払費用"),
                    _build_row(summary="SHOPB / two", counter_account="消耗品費", payable_subaccount="CARD_A", payable_account="未払費用"),
                    _build_row(summary="SHOPC / three", counter_account="雑費", payable_subaccount="CARD_A", payable_account="未払費用"),
                ],
            )

            cache1, summary1 = ensure_cc_client_cache_updated(repo_root, client_id)
            manifest_before = (line_root / "artifacts" / "derived" / "cc_teacher_manifest.json").read_text(encoding="utf-8")

            self.assertEqual("OK", (cache1.canonical_payable or {}).get("status"))
            self.assertEqual("未払費用", (cache1.canonical_payable or {}).get("account_name"))
            self.assertEqual(1, int(summary1.get("applied_new_files") or 0))

            cache2, summary2 = ensure_cc_client_cache_updated(repo_root, client_id)
            manifest_after = (line_root / "artifacts" / "derived" / "cc_teacher_manifest.json").read_text(encoding="utf-8")

            self.assertEqual(0, int(summary2.get("applied_new_files") or 0))
            self.assertEqual(3, int(cache2.payable_sub_global_stats.sample_total))
            self.assertEqual(cache1.canonical_payable, cache2.canonical_payable)
            self.assertEqual(manifest_before, manifest_after)

    def test_canonical_payable_review_required_for_tie_low_majority_and_low_sample(self) -> None:
        cases = [
            (
                "tie",
                3,
                0.75,
                [
                    _build_row(summary="SHOPA / one", counter_account="旅費交通費", payable_subaccount="CARD_A", payable_account="未払費用"),
                    _build_row(summary="SHOPB / two", counter_account="消耗品費", payable_subaccount="CARD_A", payable_account="未払費用"),
                    _build_row(summary="SHOPC / three", counter_account="雑費", payable_subaccount="CARD_A", payable_account="未払金"),
                    _build_row(summary="SHOPD / four", counter_account="会議費", payable_subaccount="CARD_A", payable_account="未払金"),
                ],
                "top_count_tie",
            ),
            (
                "low_majority",
                3,
                0.8,
                [
                    _build_row(summary="SHOPA / one", counter_account="旅費交通費", payable_subaccount="CARD_A", payable_account="未払費用"),
                    _build_row(summary="SHOPB / two", counter_account="消耗品費", payable_subaccount="CARD_A", payable_account="未払費用"),
                    _build_row(summary="SHOPC / three", counter_account="雑費", payable_subaccount="CARD_A", payable_account="未払費用"),
                    _build_row(summary="SHOPD / four", counter_account="会議費", payable_subaccount="CARD_A", payable_account="未払金"),
                    _build_row(summary="SHOPE / five", counter_account="通信費", payable_subaccount="CARD_A", payable_account="未払金"),
                ],
                "p_majority_below_min_p_majority",
            ),
            (
                "low_sample",
                4,
                0.75,
                [
                    _build_row(summary="SHOPA / one", counter_account="旅費交通費", payable_subaccount="CARD_A", payable_account="未払費用"),
                    _build_row(summary="SHOPB / two", counter_account="消耗品費", payable_subaccount="CARD_A", payable_account="未払費用"),
                    _build_row(summary="SHOPC / three", counter_account="雑費", payable_subaccount="CARD_A", payable_account="未払費用"),
                ],
                "sample_total_below_min_count",
            ),
        ]

        for case_name, min_count, min_p_majority, rows, expected_reason in cases:
            with self.subTest(case=case_name):
                with tempfile.TemporaryDirectory() as td:
                    repo_root = Path(td)
                    client_id = f"C_CC_{case_name.upper()}"
                    line_root = _line_root(repo_root, client_id)
                    _write_cc_config(
                        line_root,
                        canonical_min_count=min_count,
                        canonical_min_p_majority=min_p_majority,
                    )
                    _write_yayoi_rows(line_root / "inputs" / "ledger_ref" / "ledger_ref.csv", rows)

                    cache, _summary = ensure_cc_client_cache_updated(repo_root, client_id)

                    canonical = cache.canonical_payable or {}
                    self.assertEqual("REVIEW_REQUIRED", canonical.get("status"))
                    self.assertIn(expected_reason, canonical.get("reasons") or [])


if __name__ == "__main__":
    unittest.main()
