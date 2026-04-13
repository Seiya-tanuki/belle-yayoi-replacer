from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.line_runners import bank_statement as bank_runner
from belle.line_runners import credit_card_statement as card_runner
from belle.line_runners import receipt as receipt_runner
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_CREDIT_TAX_AMOUNT,
    COL_CREDIT_TAX_DIVISION,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_DEBIT_TAX_AMOUNT,
    COL_DEBIT_TAX_DIVISION,
    COL_MEMO,
    COL_SUMMARY,
)
from belle.yayoi_csv import read_yayoi_csv, token_to_text

RECEIPT_TAX_REVIEW_COLUMNS = [
    "debit_tax_division_before",
    "debit_tax_division_after",
    "debit_tax_division_changed",
    "tax_evidence_type",
    "tax_confidence",
    "tax_sample_total",
    "tax_p_majority",
    "tax_reasons",
]
CC_TAX_REVIEW_COLUMNS = [
    "target_tax_side",
    "target_tax_division_before",
    "target_tax_division_after",
    "target_tax_division_changed",
    "tax_evidence_type",
    "tax_lookup_key",
    "tax_confidence",
    "tax_sample_total",
    "tax_p_majority",
    "tax_reasons",
]
POSTPROCESS_COLUMNS = [
    "debit_tax_amount_before",
    "debit_tax_amount_after",
    "debit_tax_fill_status",
    "debit_tax_rate",
    "debit_tax_calc_mode",
    "credit_tax_amount_before",
    "credit_tax_amount_after",
    "credit_tax_fill_status",
    "credit_tax_rate",
    "credit_tax_calc_mode",
]
PLACEHOLDER_ACCOUNT = "仮払金"
PAYABLE_ACCOUNT = "未払金"
ACCOUNT_TRAVEL = "旅費交通費"
ACCOUNT_SUPPLIES = "消耗品費"


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _tracked_template_tax_config() -> dict:
    cfg_path = _repo_root() / "clients" / "TEMPLATE" / "config" / "yayoi_tax_config.json"
    return json.loads(cfg_path.read_text(encoding="utf-8"))


def _write_shared_tax_config_from_tracked_template(repo_root: Path, client_id: str) -> dict:
    payload = _tracked_template_tax_config()
    _write_text(
        repo_root / "clients" / client_id / "config" / "yayoi_tax_config.json",
        json.dumps(payload, ensure_ascii=False, indent=2),
    )
    return payload


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _write_mode_aware_defaults(repo_root: Path, line_id: str, payload: dict) -> None:
    defaults_dir = repo_root / "defaults" / line_id
    _write_text(
        defaults_dir / "category_defaults_tax_excluded.json",
        json.dumps(payload, ensure_ascii=False, indent=2),
    )
    _write_text(
        defaults_dir / "category_defaults_tax_included.json",
        json.dumps(payload, ensure_ascii=False, indent=2),
    )


def _read_csv_rows(path: Path) -> list[list[str]]:
    csv_obj = read_yayoi_csv(path)
    return [[token_to_text(token, csv_obj.encoding) for token in row.tokens] for row in csv_obj.rows]


def _read_review_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), list(reader)


def _receipt_row(
    *,
    summary: str,
    debit_account: str,
    amount: str = "605",
    debit_tax_division: str = "",
    debit_tax_amount: str = "",
) -> list[str]:
    row = [""] * 25
    row[COL_DATE] = "2026/04/09"
    row[COL_DEBIT_ACCOUNT] = debit_account
    row[COL_DEBIT_AMOUNT] = amount
    row[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    row[COL_DEBIT_TAX_AMOUNT] = debit_tax_amount
    row[COL_SUMMARY] = summary
    return row


def _bank_row(
    *,
    summary: str,
    debit_account: str,
    credit_account: str,
    amount: int,
    memo: str = "",
    debit_subaccount: str = "",
    credit_subaccount: str = "",
    debit_tax_division: str = "",
    debit_tax_amount: str = "",
) -> list[str]:
    row = [""] * 25
    row[COL_DATE] = "2026/04/09"
    row[COL_DEBIT_ACCOUNT] = debit_account
    row[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    row[COL_DEBIT_AMOUNT] = str(int(amount))
    row[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    row[COL_DEBIT_TAX_AMOUNT] = debit_tax_amount
    row[COL_CREDIT_ACCOUNT] = credit_account
    row[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    row[COL_CREDIT_AMOUNT] = str(int(amount))
    row[COL_SUMMARY] = summary
    row[COL_MEMO] = memo
    return row


def _cc_row(
    *,
    summary: str,
    debit_account: str,
    credit_account: str,
    amount: int,
    debit_subaccount: str = "",
    credit_subaccount: str = "",
    debit_tax_division: str = "",
    credit_tax_division: str = "",
    debit_tax_amount: str = "",
    credit_tax_amount: str = "",
) -> list[str]:
    row = [""] * 25
    row[COL_DATE] = "2026/04/09"
    row[COL_DEBIT_ACCOUNT] = debit_account
    row[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    row[COL_DEBIT_AMOUNT] = str(int(amount))
    row[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    row[COL_DEBIT_TAX_AMOUNT] = debit_tax_amount
    row[COL_CREDIT_ACCOUNT] = credit_account
    row[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    row[COL_CREDIT_AMOUNT] = str(int(amount))
    row[COL_CREDIT_TAX_DIVISION] = credit_tax_division
    row[COL_CREDIT_TAX_AMOUNT] = credit_tax_amount
    row[COL_SUMMARY] = summary
    return row


def _write_receipt_assets(repo_root: Path, client_id: str) -> Path:
    _write_text(
        repo_root / "lexicon" / "lexicon.json",
        json.dumps(
            {
                "schema": "belle.lexicon.v1",
                "version": "test",
                "categories": [
                    {
                        "id": 1,
                        "key": "known_category",
                        "label": "Known Category",
                        "kind": "expense",
                        "precision_hint": 0.9,
                        "deprecated": False,
                        "negative_terms": {"n0": [], "n1": []},
                    }
                ],
                "term_rows": [["n0", "KNOWNSTORE", 1, 1.0, "S"]],
                "term_buckets_prefix2": {"KN": [0]},
                "learned": {"policy": {"core_weight": 1.0}, "provenance_registry": []},
            },
            ensure_ascii=False,
            indent=2,
        ),
    )
    _write_mode_aware_defaults(
        repo_root,
        "receipt",
        {
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "known_category": {
                    "target_account": ACCOUNT_SUPPLIES,
                    "target_tax_division": "",
                    "confidence": 0.55,
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
    receipt_config_payload = json.dumps(
        {
            "schema": "belle.replacer_config.v1",
            "version": "1.16",
            "csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"},
            "thresholds": {
                "t_number_min_count": 1,
                "t_number_p_majority_min": 0.5,
                "vendor_key_min_count": 1,
                "vendor_key_p_majority_min": 0.5,
                "category_min_count": 1,
                "category_p_majority_min": 0.5,
                "t_number_x_category_min_count": 1,
                "t_number_x_category_p_majority_min": 0.5,
            },
            "tax_division_thresholds": {
                "t_number_x_category_target_account": {"min_count": 1, "min_p_majority": 0.5},
                "t_number_target_account": {"min_count": 1, "min_p_majority": 0.5},
                "vendor_key_target_account": {"min_count": 1, "min_p_majority": 0.5},
                "category_target_account": {"min_count": 1, "min_p_majority": 0.5},
                "global_target_account": {"min_count": 1, "min_p_majority": 0.5},
            },
            "tax_division_confidence": {
                "t_number_x_category_target_account_strength": 0.97,
                "t_number_target_account_strength": 0.95,
                "vendor_key_target_account_strength": 0.85,
                "category_target_account_strength": 0.65,
                "global_target_account_strength": 0.55,
                "category_default_strength": 0.55,
                "global_fallback_strength": 0.35,
                "learned_weight_multiplier": 0.85,
            },
        },
        ensure_ascii=False,
        indent=2,
    )
    _write_text(
        repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json",
        receipt_config_payload,
    )
    config_path = repo_root / "clients" / client_id / "lines" / "receipt" / "config" / "receipt_line_config.json"
    _write_text(
        config_path,
        json.dumps(
            json.loads(receipt_config_payload),
            ensure_ascii=False,
            indent=2,
        ),
    )
    return config_path


def _write_credit_card_assets(repo_root: Path, client_id: str) -> None:
    _write_text(
        repo_root / "lexicon" / "lexicon.json",
        json.dumps(
            {
                "schema": "belle.lexicon.v1",
                "version": "test",
                "categories": [
                    {
                        "id": 1,
                        "key": "shop_category",
                        "label": "SHOPA",
                        "kind": "merchant",
                        "precision_hint": 0.99,
                        "deprecated": False,
                        "negative_terms": {"n0": [], "n1": []},
                    }
                ],
                "term_rows": [["n0", "SHOPA", 1, 1.0, "S"]],
            },
            ensure_ascii=False,
            indent=2,
        ),
    )
    _write_mode_aware_defaults(
        repo_root,
        "credit_card_statement",
        {
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "shop_category": {
                    "target_account": ACCOUNT_SUPPLIES,
                    "target_tax_division": "",
                    "confidence": 0.55,
                    "priority": "MED",
                    "reason_code": "category_default",
                }
            },
            "global_fallback": {
                "target_account": PLACEHOLDER_ACCOUNT,
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    _write_text(
        repo_root / "clients" / client_id / "lines" / "credit_card_statement" / "config" / "credit_card_line_config.json",
        json.dumps(
            {
                "schema": "belle.credit_card_line_config.v1",
                "version": "0.2",
                "placeholder_account_name": PLACEHOLDER_ACCOUNT,
                "target_payable_placeholder_names": [PAYABLE_ACCOUNT],
                "training": {"exclude_counter_accounts": []},
                "thresholds": {
                    "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
                    "merchant_key_payable_subaccount": {"min_count": 1, "min_p_majority": 0.5},
                    "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.5},
                },
                "teacher_extraction": {
                    "canonical_payable_thresholds": {"min_count": 1, "min_p_majority": 0.5}
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
            ensure_ascii=False,
            indent=2,
        ),
    )
    _write_text(
        repo_root / "rulesets" / "credit_card_statement" / "teacher_extraction_rules_v1.json",
        json.dumps(
            {
                "schema": "belle.cc_teacher_extraction_rules.v1",
                "version": "1",
                "teacher_payable_candidate_accounts": [PAYABLE_ACCOUNT, "未払費用"],
                "hard_include_terms": ["CARD", "カード"],
                "soft_include_terms": ["VISA"],
                "exclude_terms": ["デビット", "プリペイド", "ローン"],
            },
            ensure_ascii=False,
            indent=2,
        ),
    )


class TaxDivisionAcceptanceE2ETests(unittest.TestCase):
    def test_receipt_acceptance_uses_tracked_shared_tax_default_and_fills_tax_amount(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_ACCEPT_RECEIPT"
            template_tax_cfg = _write_shared_tax_config_from_tracked_template(repo_root, client_id)
            self.assertEqual(True, bool(template_tax_cfg.get("enabled")))

            line_root = repo_root / "clients" / client_id / "lines" / "receipt"
            _write_receipt_assets(repo_root, client_id)
            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _receipt_row(
                        summary="KNOWNSTORE / meal T1234567890123",
                        debit_account=ACCOUNT_TRAVEL,
                        debit_tax_division="課対仕入内10%適格",
                    )
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _receipt_row(
                        summary="KNOWNSTORE / meal T1234567890123",
                        debit_account="BEFORE_ACCOUNT",
                        debit_tax_division="対象外",
                    )
                ],
            )

            result = receipt_runner.run_receipt(
                repo_root,
                client_id,
                client_layout_line_id="receipt",
                client_dir=line_root,
            )

            run_dir = Path(str(result["run_dir"]))
            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            replacer_manifest = run_manifest["outputs"][0]
            output_rows = _read_csv_rows(Path(str(replacer_manifest["output_file"])))
            fieldnames, review_rows = _read_review_rows(Path(str(replacer_manifest["reports"]["review_report_csv"])))

            self.assertEqual(ACCOUNT_TRAVEL, output_rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual("課対仕入内10%適格", output_rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("55", output_rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(
                RECEIPT_TAX_REVIEW_COLUMNS,
                fieldnames[-len(POSTPROCESS_COLUMNS) - len(RECEIPT_TAX_REVIEW_COLUMNS) : -len(POSTPROCESS_COLUMNS)],
            )
            self.assertEqual("対象外", review_rows[0]["debit_tax_division_before"])
            self.assertEqual("課対仕入内10%適格", review_rows[0]["debit_tax_division_after"])
            self.assertEqual("55", review_rows[0]["debit_tax_amount_after"])
            self.assertIn("tax_division_replacement", replacer_manifest)
            self.assertEqual(
                1,
                int(((replacer_manifest["tax_division_replacement"]["route_counts"]).get("t_number_x_category_target_account")) or 0),
            )
            self.assertEqual(0, int((replacer_manifest["tax_division_replacement"].get("gated_by_original_tax_count")) or 0))
            self.assertEqual(True, bool((run_manifest.get("yayoi_tax_config") or {}).get("enabled")))

    def test_bank_acceptance_keeps_tax_division_replacement_and_tax_postprocess_in_one_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_ACCEPT_BANK"
            template_tax_cfg = _write_shared_tax_config_from_tracked_template(repo_root, client_id)
            self.assertEqual(True, bool(template_tax_cfg.get("enabled")))

            line_root = repo_root / "clients" / client_id / "lines" / "bank_statement"
            _write_text(
                line_root / "config" / "bank_line_config.json",
                json.dumps(
                    {
                        "schema": "belle.bank_line_config.v0",
                        "version": "0.1",
                        "placeholder_account_name": "仮受金",
                        "bank_account_name": "普通預金",
                        "bank_account_subaccount": "MAIN",
                        "thresholds": {
                            "kana_sign_amount": {"min_count": 1, "min_p_majority": 0.5},
                            "kana_sign": {"min_count": 1, "min_p_majority": 0.5},
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ocr.csv",
                [
                    _bank_row(
                        summary="OCR_BANK_TARGET",
                        debit_account="仮受金",
                        credit_account="普通預金",
                        amount=605,
                        memo="SIGN=debit",
                        debit_subaccount="OCR_SUB",
                        credit_subaccount="MAIN",
                    )
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "reference_yayoi" / "teacher.csv",
                [
                    _bank_row(
                        summary="TEACHER_BANK_TARGET",
                        debit_account="租税公課",
                        credit_account="普通預金",
                        amount=605,
                        debit_subaccount="COUNTER_SUB",
                        credit_subaccount="MAIN",
                        debit_tax_division="課対仕入内10%区分80%",
                    )
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _bank_row(
                        summary="OCR_BANK_TARGET",
                        debit_account="仮受金",
                        credit_account="普通預金",
                        amount=605,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB",
                        credit_subaccount="MAIN",
                    )
                ],
            )

            result = bank_runner.run_bank(repo_root, client_id, client_dir=line_root)
            run_dir = Path(str(result["run_dir"]))
            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            replacer_manifest = json.loads(Path(str(run_manifest["replacer_manifest_path"])).read_text(encoding="utf-8"))
            output_rows = _read_csv_rows(Path(str(replacer_manifest["output_file"])))
            fieldnames, review_rows = _read_review_rows(Path(str(replacer_manifest["reports"]["review_report_csv"])))

            self.assertEqual("租税公課", output_rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual("課対仕入内10%区分80%", output_rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("55", output_rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(POSTPROCESS_COLUMNS, fieldnames[-len(POSTPROCESS_COLUMNS) :])
            self.assertEqual("55", review_rows[0]["debit_tax_amount_after"])
            self.assertEqual(True, bool((replacer_manifest.get("tax_postprocess") or {}).get("enabled")))
            self.assertEqual(1, int(((replacer_manifest.get("tax_postprocess") or {}).get("debit_filled_count")) or 0))
            self.assertEqual(False, bool(run_manifest.get("strict_stop_applied")))

    def test_credit_card_acceptance_uses_tracked_shared_tax_default_and_fills_target_side_tax(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_ACCEPT_CC"
            template_tax_cfg = _write_shared_tax_config_from_tracked_template(repo_root, client_id)
            self.assertEqual(True, bool(template_tax_cfg.get("enabled")))

            line_root = repo_root / "clients" / client_id / "lines" / "credit_card_statement"
            _write_credit_card_assets(repo_root, client_id)
            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _cc_row(
                        summary="SHOPA / learn",
                        debit_account=ACCOUNT_TRAVEL,
                        credit_account=PAYABLE_ACCOUNT,
                        amount=605,
                        credit_subaccount="CARD_A",
                        debit_tax_division="課対仕入内10%適格",
                    )
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _cc_row(
                        summary="SHOPA / target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        amount=605,
                    )
                ],
            )

            result = card_runner.run_card(repo_root, client_id)
            run_dir = Path(str(result["run_dir"]))
            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            replacer_manifest = json.loads(Path(str(run_manifest["replacer_manifest_path"])).read_text(encoding="utf-8"))
            output_rows = _read_csv_rows(Path(str(replacer_manifest["output_file"])))
            fieldnames, review_rows = _read_review_rows(Path(str(replacer_manifest["reports"]["review_report_csv"])))

            self.assertEqual(ACCOUNT_TRAVEL, output_rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual("課対仕入内10%適格", output_rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("55", output_rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual("CARD_A", output_rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual(
                CC_TAX_REVIEW_COLUMNS,
                fieldnames[-len(POSTPROCESS_COLUMNS) - len(CC_TAX_REVIEW_COLUMNS) : -len(POSTPROCESS_COLUMNS)],
            )
            self.assertEqual("debit", review_rows[0]["target_tax_side"])
            self.assertEqual("", review_rows[0]["target_tax_division_before"])
            self.assertEqual("課対仕入内10%適格", review_rows[0]["target_tax_division_after"])
            self.assertEqual("55", review_rows[0]["debit_tax_amount_after"])
            self.assertIn("tax_division_replacement", replacer_manifest)
            self.assertEqual(
                1,
                int((((replacer_manifest["tax_division_replacement"]).get("target_side_counts") or {}).get("debit")) or 0),
            )
            self.assertEqual(True, bool((run_manifest.get("yayoi_tax_config") or {}).get("enabled")))


if __name__ == "__main__":
    unittest.main()
