from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.build_cc_cache import ensure_cc_client_cache_updated, load_credit_card_line_config
from belle.cc_replacer import replace_credit_card_yayoi_csv
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_SUMMARY,
)
from belle.yayoi_csv import read_yayoi_csv, token_to_text

PLACEHOLDER_ACCOUNT = "\u4eee\u6255\u91d1"
PAYABLE_ACCOUNT = "\u672a\u6255\u91d1"
CANONICAL_PAYABLE_ACCOUNT = "\u672a\u6255\u8cbb\u7528"
ACCOUNT_TRAVEL = "\u65c5\u8cbb\u4ea4\u901a\u8cbb"
ACCOUNT_SUPPLIES = "\u6d88\u8017\u54c1\u8cbb"


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "credit_card_statement"


def _write_cc_config(
    line_root: Path,
    *,
    canonical_min_count: int = 1,
    canonical_min_p_majority: float = 0.5,
) -> None:
    cfg = {
        "schema": "belle.credit_card_line_config.v0",
        "version": "0.1",
        "placeholder_account_name": PLACEHOLDER_ACCOUNT,
        "payable_account_name": PAYABLE_ACCOUNT,
        "target_payable_placeholder_names": [PAYABLE_ACCOUNT],
        "training": {"exclude_counter_accounts": []},
        "thresholds": {
            "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
            "merchant_key_payable_subaccount": {"min_count": 1, "min_p_majority": 0.5},
            "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.5},
        },
        "teacher_extraction": {
            "canonical_payable_thresholds": {
                "min_count": canonical_min_count,
                "min_p_majority": canonical_min_p_majority,
            }
        },
        "candidate_extraction": {
            "min_total_count": 1,
            "min_unique_merchants": 1,
            "min_unique_counter_accounts": 1,
            "manual_allow": [],
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
                "teacher_payable_candidate_accounts": [PAYABLE_ACCOUNT, CANONICAL_PAYABLE_ACCOUNT],
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
    debit_account: str,
    credit_account: str,
    debit_subaccount: str = "",
    credit_subaccount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/02/01"
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    cols[COL_DEBIT_AMOUNT] = "1000"
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_CREDIT_AMOUNT] = "1000"
    cols[COL_SUMMARY] = summary
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _read_rows(path: Path) -> list[list[str]]:
    csv_obj = read_yayoi_csv(path)
    return [[token_to_text(tok, csv_obj.encoding) for tok in row.tokens] for row in csv_obj.rows]


def _read_review_report(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


class CCReplacerBasicReplaceTests(unittest.TestCase):
    def test_basic_success_path_rewrites_payable_to_canonical_and_fills_subaccount(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            ledger_ref_path = line_root / "inputs" / "ledger_ref" / "ledger_ref.csv"
            _write_yayoi_rows(
                ledger_ref_path,
                [
                    _build_row(
                        summary="SHOPA /x",
                        debit_account=ACCOUNT_TRAVEL,
                        credit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    ),
                    _build_row(
                        summary="SHOPB /y",
                        debit_account=ACCOUNT_SUPPLIES,
                        credit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    ),
                ],
            )

            ensure_cc_client_cache_updated(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            self.assertTrue(cache_path.exists())

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        summary="SHOPA /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    ),
                    _build_row(
                        summary="SHOPB /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    ),
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_CC_BASIC"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            config = load_credit_card_line_config(repo_root, client_id)

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_CC_BASIC",
            )

            rows = _read_rows(out_path)
            self.assertEqual(ACCOUNT_TRAVEL, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(ACCOUNT_SUPPLIES, rows[1][COL_DEBIT_ACCOUNT])
            self.assertEqual(CANONICAL_PAYABLE_ACCOUNT, rows[0][COL_CREDIT_ACCOUNT])
            self.assertEqual(CANONICAL_PAYABLE_ACCOUNT, rows[1][COL_CREDIT_ACCOUNT])
            self.assertEqual("CARD_A", rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("CARD_A", rows[1][COL_CREDIT_SUBACCOUNT])

            file_inference = manifest.get("file_card_inference") or {}
            self.assertEqual("OK", file_inference.get("status"))
            self.assertEqual("CARD_A", file_inference.get("inferred_payable_subaccount"))
            self.assertFalse(bool(manifest.get("canonical_payable_required_failed")))
            canonical_block = manifest.get("canonical_payable") or {}
            self.assertEqual(2, canonical_block.get("rewrite_count"))
            self.assertEqual(0, canonical_block.get("noop_count"))
            self.assertEqual("OK", (canonical_block.get("cache_snapshot") or {}).get("status"))

            report_rows = _read_review_report(run_dir / "target_01_R_TEST_CC_BASIC_review_report.csv")
            self.assertEqual("credit", report_rows[0]["payable_side_detected"])
            self.assertEqual(PAYABLE_ACCOUNT, report_rows[0]["payable_account_before_raw"])
            self.assertEqual(CANONICAL_PAYABLE_ACCOUNT, report_rows[0]["payable_account_after_canonical"])
            self.assertEqual("1", report_rows[0]["payable_account_rewritten"])
            self.assertEqual("raw_placeholder_to_canonical", report_rows[0]["payable_account_rewrite_reason"])

    def test_already_canonical_payable_is_reported_as_noop(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C2"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            ledger_ref_path = line_root / "inputs" / "ledger_ref" / "ledger_ref.csv"
            _write_yayoi_rows(
                ledger_ref_path,
                [
                    _build_row(
                        summary="SHOPA /x",
                        debit_account=ACCOUNT_TRAVEL,
                        credit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    )
                ],
            )
            ensure_cc_client_cache_updated(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        summary="SHOPA /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    )
                ],
            )
            run_dir = line_root / "outputs" / "runs" / "R_TEST_CC_CANONICAL_NOOP"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            config = load_credit_card_line_config(repo_root, client_id)

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_CC_CANONICAL_NOOP",
            )

            rows = _read_rows(out_path)
            self.assertEqual(CANONICAL_PAYABLE_ACCOUNT, rows[0][COL_CREDIT_ACCOUNT])
            report_rows = _read_review_report(run_dir / "target_01_R_TEST_CC_CANONICAL_NOOP_review_report.csv")
            self.assertEqual("already_canonical", report_rows[0]["payable_account_rewrite_reason"])
            self.assertEqual("0", report_rows[0]["payable_account_rewritten"])
            canonical_block = manifest.get("canonical_payable") or {}
            self.assertEqual(0, canonical_block.get("rewrite_count"))
            self.assertEqual(1, canonical_block.get("noop_count"))

    def test_non_ok_canonical_payable_fails_closed_when_payable_side_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C3"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root, canonical_min_count=2, canonical_min_p_majority=0.9)

            ledger_ref_path = line_root / "inputs" / "ledger_ref" / "ledger_ref.csv"
            _write_yayoi_rows(
                ledger_ref_path,
                [
                    _build_row(
                        summary="SHOPA /x",
                        debit_account=ACCOUNT_TRAVEL,
                        credit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    )
                ],
            )
            ensure_cc_client_cache_updated(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        summary="SHOPA /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    )
                ],
            )
            run_dir = line_root / "outputs" / "runs" / "R_TEST_CC_CANONICAL_FAIL"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            config = load_credit_card_line_config(repo_root, client_id)

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_CC_CANONICAL_FAIL",
            )

            rows = _read_rows(out_path)
            self.assertEqual(PAYABLE_ACCOUNT, rows[0][COL_CREDIT_ACCOUNT])
            self.assertEqual("", rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertTrue(bool(manifest.get("canonical_payable_required_failed")))
            self.assertFalse(bool(manifest.get("payable_sub_fill_required_failed")))
            report_rows = _read_review_report(run_dir / "target_01_R_TEST_CC_CANONICAL_FAIL_review_report.csv")
            self.assertEqual("canonical_payable_not_ok", report_rows[0]["payable_account_rewrite_reason"])
            self.assertEqual("REVIEW_REQUIRED", report_rows[0]["canonical_payable_status"])

    def test_ambiguous_payable_side_does_not_silently_pick_one(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C4"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            ledger_ref_path = line_root / "inputs" / "ledger_ref" / "ledger_ref.csv"
            _write_yayoi_rows(
                ledger_ref_path,
                [
                    _build_row(
                        summary="SHOPA /x",
                        debit_account=ACCOUNT_TRAVEL,
                        credit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    )
                ],
            )
            ensure_cc_client_cache_updated(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        summary="SHOPA /target",
                        debit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        debit_subaccount="",
                        credit_subaccount="",
                    )
                ],
            )
            run_dir = line_root / "outputs" / "runs" / "R_TEST_CC_CANONICAL_AMBIG"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            config = load_credit_card_line_config(repo_root, client_id)

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_CC_CANONICAL_AMBIG",
            )

            rows = _read_rows(out_path)
            self.assertEqual(CANONICAL_PAYABLE_ACCOUNT, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(PAYABLE_ACCOUNT, rows[0][COL_CREDIT_ACCOUNT])
            self.assertFalse(bool(manifest.get("canonical_payable_required_failed")))
            report_rows = _read_review_report(run_dir / "target_01_R_TEST_CC_CANONICAL_AMBIG_review_report.csv")
            self.assertEqual("ambiguous", report_rows[0]["payable_side_detected"])
            self.assertEqual("payable_side_ambiguous", report_rows[0]["payable_account_rewrite_reason"])


if __name__ == "__main__":
    unittest.main()
