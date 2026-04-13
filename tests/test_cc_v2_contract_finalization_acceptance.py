from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.build_cc_cache import ensure_cc_client_cache_updated, load_credit_card_line_config
from belle.line_runners.credit_card_statement import run_card
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

PLACEHOLDER_ACCOUNT = "仮払金"
PAYABLE_PLACEHOLDER = "未払金"
CANONICAL_PAYABLE = "未払費用"
ACCOUNT_TRAVEL = "旅費交通費"
ACCOUNT_SUPPLIES = "消耗品費"


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "credit_card_statement"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_min_shared_assets(repo_root: Path, client_id: str) -> None:
    _write_json(
        repo_root / "lexicon" / "lexicon.json",
        {
            "schema": "belle.lexicon.v1",
            "version": "test",
            "categories": [],
            "term_rows": [],
        },
    )
    defaults_payload = {
        "schema": "belle.category_defaults.v2",
        "version": "test",
        "defaults": {},
        "global_fallback": {
            "target_account": PLACEHOLDER_ACCOUNT,
            "target_tax_division": "",
            "confidence": 0.35,
            "priority": "HIGH",
            "reason_code": "global_fallback",
        },
    }
    for filename in ("category_defaults_tax_excluded.json", "category_defaults_tax_included.json"):
        _write_json(repo_root / "defaults" / "credit_card_statement" / filename, defaults_payload)
    _write_json(
        repo_root / "clients" / client_id / "config" / "yayoi_tax_config.json",
        {
            "schema": "belle.yayoi_tax_config.v1",
            "version": "1.0",
            "enabled": False,
            "bookkeeping_mode": "tax_excluded",
            "rounding_mode": "floor",
        },
    )


def _base_cc_config(
    *,
    canonical_min_count: int = 1,
    canonical_min_p_majority: float = 0.5,
    target_payable_placeholder_names: list[str] | None = None,
) -> dict:
    return {
        "schema": "belle.credit_card_line_config.v1",
        "version": "0.3",
        "placeholder_account_name": PLACEHOLDER_ACCOUNT,
        "target_payable_placeholder_names": list(
            target_payable_placeholder_names if target_payable_placeholder_names is not None else [PAYABLE_PLACEHOLDER]
        ),
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
        "teacher_extraction": {
            "enabled": True,
            "ruleset_relpath": "rulesets/credit_card_statement/teacher_extraction_rules_v1.json",
            "payable_candidate_accounts": [CANONICAL_PAYABLE, PAYABLE_PLACEHOLDER],
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


def _write_cc_config(repo_root: Path, client_id: str, config: dict) -> Path:
    line_root = _line_root(repo_root, client_id)
    _write_json(line_root / "config" / "credit_card_line_config.json", config)
    _write_json(
        repo_root / "rulesets" / "credit_card_statement" / "teacher_extraction_rules_v1.json",
        {
            "schema": "belle.cc_teacher_extraction_rules.v1",
            "version": "1",
            "teacher_payable_candidate_accounts": [CANONICAL_PAYABLE, PAYABLE_PLACEHOLDER],
            "hard_include_terms": ["CARD", "カード"],
            "soft_include_terms": ["VISA"],
            "exclude_terms": ["デビット", "プリペイド", "ローン"],
        },
    )
    return line_root


def _build_row(
    *,
    summary: str,
    debit_account: str,
    credit_account: str,
    debit_subaccount: str = "",
    credit_subaccount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/04/11"
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
    return [[token_to_text(token, csv_obj.encoding) for token in row.tokens] for row in csv_obj.rows]


def _read_review_report(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _single_run_file(run_dir: Path, pattern: str) -> Path:
    matches = sorted(run_dir.glob(pattern))
    if len(matches) != 1:
        raise AssertionError(f"expected exactly one file for pattern={pattern}, got={matches}")
    return matches[0]


class CCCanonicalPayableV2AcceptanceTests(unittest.TestCase):
    def test_dominant_canonical_payable_rewrites_placeholder_but_preserves_raw_review_value(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_V2_CANONICAL"
            _write_min_shared_assets(repo_root, client_id)
            line_root = _write_cc_config(repo_root, client_id, _base_cc_config())

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="SHOPA / learn", debit_account=ACCOUNT_TRAVEL, credit_account=CANONICAL_PAYABLE, credit_subaccount="CARD_A"),
                    _build_row(summary="SHOPB / learn", debit_account=ACCOUNT_SUPPLIES, credit_account=CANONICAL_PAYABLE, credit_subaccount="CARD_A"),
                    _build_row(summary="SHOPC / learn", debit_account=ACCOUNT_TRAVEL, credit_account=CANONICAL_PAYABLE, credit_subaccount="CARD_A"),
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_row(summary="SHOPA / target", debit_account=PLACEHOLDER_ACCOUNT, credit_account=PAYABLE_PLACEHOLDER),
                ],
            )

            result = run_card(repo_root, client_id)
            run_dir = Path(str(result["run_dir"]))

            rows = _read_rows(_single_run_file(run_dir, "*_replaced_*.csv"))
            self.assertEqual(CANONICAL_PAYABLE, rows[0][COL_CREDIT_ACCOUNT])

            review_rows = _read_review_report(_single_run_file(run_dir, "*_review_report.csv"))
            self.assertEqual(PAYABLE_PLACEHOLDER, review_rows[0]["payable_account_before_raw"])
            self.assertEqual(CANONICAL_PAYABLE, review_rows[0]["payable_account_after_canonical"])
            self.assertEqual("1", review_rows[0]["payable_account_rewritten"])
            self.assertEqual("raw_placeholder_to_canonical", review_rows[0]["payable_account_rewrite_reason"])

    def test_placeholder_remains_noop_when_canonical_payable_matches_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_V2_NOOP"
            _write_min_shared_assets(repo_root, client_id)
            line_root = _write_cc_config(repo_root, client_id, _base_cc_config())

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="SHOPA / learn", debit_account=ACCOUNT_TRAVEL, credit_account=PAYABLE_PLACEHOLDER, credit_subaccount="CARD_A"),
                    _build_row(summary="SHOPB / learn", debit_account=ACCOUNT_SUPPLIES, credit_account=PAYABLE_PLACEHOLDER, credit_subaccount="CARD_A"),
                    _build_row(summary="SHOPC / learn", debit_account=ACCOUNT_TRAVEL, credit_account=PAYABLE_PLACEHOLDER, credit_subaccount="CARD_A"),
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_row(summary="SHOPA / target", debit_account=PLACEHOLDER_ACCOUNT, credit_account=PAYABLE_PLACEHOLDER),
                ],
            )

            result = run_card(repo_root, client_id)
            run_dir = Path(str(result["run_dir"]))

            rows = _read_rows(_single_run_file(run_dir, "*_replaced_*.csv"))
            self.assertEqual(PAYABLE_PLACEHOLDER, rows[0][COL_CREDIT_ACCOUNT])

            review_rows = _read_review_report(_single_run_file(run_dir, "*_review_report.csv"))
            self.assertEqual("0", review_rows[0]["payable_account_rewritten"])
            self.assertEqual("already_canonical", review_rows[0]["payable_account_rewrite_reason"])
            self.assertEqual(PAYABLE_PLACEHOLDER, review_rows[0]["payable_account_after_canonical"])

    def test_review_required_canonical_payable_returns_needs_review_result(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_V2_REVIEW_REQUIRED"
            _write_min_shared_assets(repo_root, client_id)
            line_root = _write_cc_config(
                repo_root,
                client_id,
                _base_cc_config(canonical_min_count=2, canonical_min_p_majority=0.9),
            )

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="SHOPA / learn", debit_account=ACCOUNT_TRAVEL, credit_account=CANONICAL_PAYABLE, credit_subaccount="CARD_A"),
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_row(summary="SHOPA / target", debit_account=PLACEHOLDER_ACCOUNT, credit_account=PAYABLE_PLACEHOLDER),
                ],
            )

            result = run_card(repo_root, client_id)
            self.assertEqual("needs_review", result.outcome)
            self.assertTrue(result.needs_review)
            self.assertTrue(result.strict_stop_applied)
            self.assertEqual("FAIL", result.exit_status)
            self.assertEqual(
                "RUN_NEEDS_REVIEW_CARD_CANONICAL_PAYABLE_FAILED",
                result.ui_reason_code,
            )

            latest_run_id = (line_root / "outputs" / "LATEST.txt").read_text(encoding="utf-8").strip()
            run_dir = line_root / "outputs" / "runs" / latest_run_id
            rows = _read_rows(_single_run_file(run_dir, "*_replaced_*.csv"))
            self.assertEqual(PAYABLE_PLACEHOLDER, rows[0][COL_CREDIT_ACCOUNT])

            review_rows = _read_review_report(_single_run_file(run_dir, "*_review_report.csv"))
            self.assertEqual("REVIEW_REQUIRED", review_rows[0]["canonical_payable_status"])
            self.assertEqual("canonical_payable_not_ok", review_rows[0]["payable_account_rewrite_reason"])

    def test_missing_target_payable_placeholder_names_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_V2_CFG_MISSING_PLACEHOLDER"
            _write_min_shared_assets(repo_root, client_id)
            config = _base_cc_config()
            del config["target_payable_placeholder_names"]
            _write_cc_config(repo_root, client_id, config)

            with self.assertRaises(ValueError) as ctx:
                load_credit_card_line_config(repo_root, client_id)

            self.assertIn("target_payable_placeholder_names is required", str(ctx.exception))

    def test_blank_target_payable_placeholder_names_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_V2_CFG_BLANK_PLACEHOLDER"
            _write_min_shared_assets(repo_root, client_id)
            _write_cc_config(
                repo_root,
                client_id,
                _base_cc_config(target_payable_placeholder_names=["", "  "]),
            )

            with self.assertRaises(ValueError) as ctx:
                load_credit_card_line_config(repo_root, client_id)

            self.assertIn("target_payable_placeholder_names must contain at least one non-blank value", str(ctx.exception))

    def test_missing_canonical_payable_thresholds_fail_closed_for_cache_update(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_V2_CFG_MISSING_CANONICAL"
            _write_min_shared_assets(repo_root, client_id)
            line_root = _write_cc_config(repo_root, client_id, _base_cc_config())
            config_path = line_root / "config" / "credit_card_line_config.json"
            config = json.loads(config_path.read_text(encoding="utf-8"))
            del config["teacher_extraction"]["canonical_payable_thresholds"]
            _write_json(config_path, config)

            with self.assertRaises(ValueError) as ctx:
                ensure_cc_client_cache_updated(repo_root, client_id)

            self.assertIn("teacher_extraction.canonical_payable_thresholds is required", str(ctx.exception))

    def test_invalid_canonical_payable_thresholds_fail_closed_for_cache_update(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_V2_CFG_INVALID_CANONICAL"
            _write_min_shared_assets(repo_root, client_id)
            line_root = _write_cc_config(repo_root, client_id, _base_cc_config())
            config_path = line_root / "config" / "credit_card_line_config.json"
            config = json.loads(config_path.read_text(encoding="utf-8"))
            config["teacher_extraction"]["canonical_payable_thresholds"]["min_p_majority"] = 1.2
            _write_json(config_path, config)

            with self.assertRaises(ValueError) as ctx:
                ensure_cc_client_cache_updated(repo_root, client_id)

            self.assertIn(
                "teacher_extraction.canonical_payable_thresholds.min_p_majority must be > 0 and <= 1",
                str(ctx.exception),
            )


if __name__ == "__main__":
    unittest.main()
