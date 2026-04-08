from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from belle.defaults import CategoryDefaults, DefaultRule
from belle.lexicon import Lexicon
from belle.line_runners import bank_statement as bank_runner
from belle.line_runners import credit_card_statement as card_runner
from belle.line_runners import receipt as receipt_runner
from belle.replacer import replace_yayoi_csv
from belle.tax_postprocess import (
    BOOKKEEPING_MODE_TAX_EXCLUDED,
    ROUNDING_MODE_FLOOR,
    STATUS_APPLIED_INNER_FLOOR,
    STATUS_DISABLED,
    STATUS_TAX_AMOUNT_ALREADY_PRESENT,
    STATUS_UNSUPPORTED_CALC_MODE,
    YayoiTaxPostprocessConfig,
)
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_CREDIT_TAX_AMOUNT,
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

PLACEHOLDER_ACCOUNT = "TEMP_PLACEHOLDER"
BANK_ACCOUNT = "BANK_ACCOUNT"
BANK_SUBACCOUNT = "BANK_SUB"
PAYABLE_ACCOUNT = "未払金"
RECEIPT_ACCOUNT = "BEFORE"
TRAVEL_ACCOUNT = "旅費交通費"
SUPPLIES_ACCOUNT = "消耗品費"

TAX_REVIEW_COLUMNS = [
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


def _enabled_tax_config() -> YayoiTaxPostprocessConfig:
    return YayoiTaxPostprocessConfig(
        enabled=True,
        bookkeeping_mode=BOOKKEEPING_MODE_TAX_EXCLUDED,
        rounding_mode=ROUNDING_MODE_FLOOR,
    )


def _empty_lexicon() -> Lexicon:
    return Lexicon(
        schema="belle.lexicon.v1",
        version="test",
        categories_by_id={},
        categories_by_key={},
        terms_by_field={"n0": [], "n1": []},
    )


def _receipt_defaults() -> CategoryDefaults:
    return CategoryDefaults(
        schema="belle.category_defaults.v1",
        version="test",
        defaults={},
        global_fallback=DefaultRule(
            debit_account=RECEIPT_ACCOUNT,
            confidence=0.5,
            priority="HIGH",
            reason_code="global_fallback",
        ),
    )


def _write_tax_config(repo_root: Path, client_id: str, *, enabled: bool) -> Path:
    cfg_path = repo_root / "clients" / client_id / "config" / "yayoi_tax_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        json.dumps(
            {
                "schema": "belle.yayoi_tax_config.v1",
                "version": "1.0",
                "enabled": bool(enabled),
                "bookkeeping_mode": "tax_excluded",
                "rounding_mode": "floor",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return cfg_path


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _read_csv_rows(path: Path) -> list[list[str]]:
    csv_obj = read_yayoi_csv(path)
    return [[token_to_text(token, csv_obj.encoding) for token in row.tokens] for row in csv_obj.rows]


def _read_review_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), list(reader)


def _basic_receipt_row(
    *,
    summary: str,
    debit_tax_division: str = "",
    debit_tax_amount: str = "",
    amount: str = "605",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/04/08"
    cols[COL_DEBIT_ACCOUNT] = RECEIPT_ACCOUNT
    cols[COL_DEBIT_AMOUNT] = amount
    cols[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    cols[COL_DEBIT_TAX_AMOUNT] = debit_tax_amount
    cols[COL_SUMMARY] = summary
    return cols


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
    cols = [""] * 25
    cols[COL_DATE] = "2026/04/08"
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    cols[COL_DEBIT_AMOUNT] = str(int(amount))
    cols[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    cols[COL_DEBIT_TAX_AMOUNT] = debit_tax_amount
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_CREDIT_AMOUNT] = str(int(amount))
    cols[COL_SUMMARY] = summary
    cols[COL_MEMO] = memo
    return cols


def _cc_row(
    *,
    summary: str,
    debit_account: str,
    credit_account: str,
    amount: int,
    debit_subaccount: str = "",
    credit_subaccount: str = "",
    debit_tax_division: str = "",
    debit_tax_amount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/04/08"
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    cols[COL_DEBIT_AMOUNT] = str(int(amount))
    cols[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    cols[COL_DEBIT_TAX_AMOUNT] = debit_tax_amount
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_CREDIT_AMOUNT] = str(int(amount))
    cols[COL_SUMMARY] = summary
    return cols


class TaxPostprocessRuntimeWiringTests(unittest.TestCase):
    def test_receipt_runner_wires_tax_postprocess_and_counts_tax_only_change(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_RECEIPT_TAX_RUNTIME"
            client_line_dir = repo_root / "clients" / client_id / "lines" / "receipt"
            (client_line_dir / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
            (client_line_dir / "config").mkdir(parents=True, exist_ok=True)
            (client_line_dir / "config" / "category_overrides.json").write_text("{}", encoding="utf-8")
            ruleset_path = repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json"
            ruleset_path.parent.mkdir(parents=True, exist_ok=True)
            ruleset_path.write_text(
                json.dumps(
                    {
                        "version": "1.15",
                        "csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"},
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            _write_tax_config(repo_root, client_id, enabled=True)
            _write_yayoi_rows(
                client_line_dir / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _basic_receipt_row(
                        summary="LOCAL SHOP",
                        debit_tax_division="課対仕入内10%適格",
                        amount="605",
                    )
                ],
            )

            tm = SimpleNamespace(
                t_numbers={},
                vendor_keys={},
                categories={},
                t_numbers_by_category={},
            )
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
            defaults = _receipt_defaults()

            with mock.patch.object(receipt_runner, "load_lexicon", return_value=_empty_lexicon()):
                with mock.patch.object(receipt_runner, "load_category_defaults", return_value=defaults):
                    with mock.patch.object(receipt_runner, "try_load_category_overrides", return_value=({}, [])):
                        with mock.patch.object(receipt_runner, "merge_effective_defaults", return_value=defaults):
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
                                    result = receipt_runner.run_receipt(
                                        repo_root,
                                        client_id,
                                        client_layout_line_id="receipt",
                                        client_dir=client_line_dir,
                                        config_path=ruleset_path,
                                    )

            run_dir = Path(result["run_dir"])
            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            replacer_manifest = run_manifest["outputs"][0]
            review_path = Path(str((replacer_manifest.get("reports") or {}).get("review_report_csv") or ""))
            output_path = Path(str(replacer_manifest.get("output_file") or ""))
            rows = _read_csv_rows(output_path)
            fieldnames, review_rows = _read_review_rows(review_path)

            self.assertEqual("55", rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(1, int(replacer_manifest["changed_count"]))
            self.assertEqual(TAX_REVIEW_COLUMNS, fieldnames[-len(TAX_REVIEW_COLUMNS) :])
            self.assertEqual("1", review_rows[0]["changed"])
            self.assertEqual("55", review_rows[0]["debit_tax_amount_after"])
            self.assertEqual(STATUS_APPLIED_INNER_FLOOR, review_rows[0]["debit_tax_fill_status"])
            self.assertEqual("10", review_rows[0]["debit_tax_rate"])
            self.assertEqual("inner", review_rows[0]["debit_tax_calc_mode"])
            self.assertEqual(True, bool((replacer_manifest.get("tax_postprocess") or {}).get("enabled")))
            self.assertEqual(1, int((replacer_manifest.get("tax_postprocess") or {}).get("rows_changed") or 0))

            runner_tax_cfg = run_manifest.get("yayoi_tax_config") or {}
            self.assertEqual(True, bool(runner_tax_cfg.get("enabled")))
            self.assertEqual("tax_excluded", runner_tax_cfg.get("bookkeeping_mode"))
            self.assertEqual("floor", runner_tax_cfg.get("rounding_mode"))
            self.assertTrue(str(runner_tax_cfg.get("path") or "").endswith("yayoi_tax_config.json"))

    def test_missing_shared_tax_config_keeps_receipt_runtime_behavior_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            run_dir = root / "run"
            run_dir.mkdir(parents=True, exist_ok=True)
            in_path = root / "input.csv"
            out_path = run_dir / "out.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _basic_receipt_row(
                        summary="NO TAX CONFIG",
                        debit_tax_division="課対仕入内10%適格",
                        amount="605",
                    )
                ],
            )

            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=_empty_lexicon(),
                client_cache=None,
                defaults=_receipt_defaults(),
                config={"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}},
                run_dir=run_dir,
                artifact_prefix="receipt_disabled",
            )

            rows = _read_csv_rows(out_path)
            review_path = Path(str((manifest.get("reports") or {}).get("review_report_csv") or ""))
            _, review_rows = _read_review_rows(review_path)

            self.assertEqual("", rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(0, int(manifest["changed_count"]))
            self.assertEqual(False, bool((manifest.get("tax_postprocess") or {}).get("enabled")))
            self.assertEqual(STATUS_DISABLED, review_rows[0]["debit_tax_fill_status"])

    def test_unsupported_calc_mode_remains_noop_and_observable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            run_dir = root / "run"
            run_dir.mkdir(parents=True, exist_ok=True)
            in_path = root / "input.csv"
            out_path = run_dir / "out.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _basic_receipt_row(
                        summary="OUTER TAX",
                        debit_tax_division="課対仕入外10%",
                        amount="605",
                    )
                ],
            )

            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=_empty_lexicon(),
                client_cache=None,
                defaults=_receipt_defaults(),
                config={"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}},
                run_dir=run_dir,
                artifact_prefix="receipt_outer",
                yayoi_tax_config=_enabled_tax_config(),
            )

            rows = _read_csv_rows(out_path)
            review_path = Path(str((manifest.get("reports") or {}).get("review_report_csv") or ""))
            _, review_rows = _read_review_rows(review_path)
            status_counts = (((manifest.get("tax_postprocess") or {}).get("status_counts") or {}).get("debit")) or {}

            self.assertEqual("", rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(0, int(manifest["changed_count"]))
            self.assertEqual(STATUS_UNSUPPORTED_CALC_MODE, review_rows[0]["debit_tax_fill_status"])
            self.assertEqual(1, int(status_counts.get(STATUS_UNSUPPORTED_CALC_MODE) or 0))

    def test_bank_runner_wires_tax_postprocess_and_emits_observability(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_TAX_RUNTIME"
            line_root = repo_root / "clients" / client_id / "lines" / "bank_statement"
            (line_root / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
            (line_root / "inputs" / "training" / "ocr_kari_shiwake").mkdir(parents=True, exist_ok=True)
            (line_root / "inputs" / "training" / "reference_yayoi").mkdir(parents=True, exist_ok=True)
            (line_root / "config").mkdir(parents=True, exist_ok=True)
            (line_root / "config" / "bank_line_config.json").write_text(
                json.dumps(
                    {
                        "schema": "belle.bank_line_config.v0",
                        "version": "0.1",
                        "placeholder_account_name": PLACEHOLDER_ACCOUNT,
                        "bank_account_name": BANK_ACCOUNT,
                        "bank_account_subaccount": BANK_SUBACCOUNT,
                        "thresholds": {
                            "kana_sign_amount": {"min_count": 1, "min_p_majority": 0.5},
                            "kana_sign": {"min_count": 1, "min_p_majority": 0.5},
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            _write_tax_config(repo_root, client_id, enabled=True)

            _write_yayoi_rows(
                line_root / "inputs" / "training" / "ocr_kari_shiwake" / "training_ocr.csv",
                [
                    _bank_row(
                        summary="OCR_BANK_TARGET",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=605,
                        memo="SIGN=debit",
                        debit_subaccount="OCR_SUB",
                        credit_subaccount=BANK_SUBACCOUNT,
                    )
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "reference_yayoi" / "teacher.csv",
                [
                    _bank_row(
                        summary="TEACHER_BANK_TARGET",
                        debit_account="租税公課",
                        credit_account=BANK_ACCOUNT,
                        amount=605,
                        debit_subaccount="COUNTER_SUB",
                        debit_tax_division="課対仕入内10%区分80%",
                        credit_subaccount=BANK_SUBACCOUNT,
                    )
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _bank_row(
                        summary="OCR_BANK_TARGET",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=605,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB",
                        credit_subaccount=BANK_SUBACCOUNT,
                    )
                ],
            )

            result = bank_runner.run_bank(repo_root, client_id, client_dir=line_root)
            run_dir = Path(result["run_dir"])
            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            replacer_manifest_path = Path(str(run_manifest.get("replacer_manifest_path") or ""))
            replacer_manifest = json.loads(replacer_manifest_path.read_text(encoding="utf-8"))
            review_path = Path(str((replacer_manifest.get("reports") or {}).get("review_report_csv") or ""))
            output_path = Path(str(replacer_manifest.get("output_file") or ""))
            rows = _read_csv_rows(output_path)
            fieldnames, review_rows = _read_review_rows(review_path)

            self.assertEqual("55", rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(False, bool(run_manifest.get("strict_stop_applied")))
            self.assertEqual("55", review_rows[0]["debit_tax_amount_after"])
            self.assertEqual(STATUS_APPLIED_INNER_FLOOR, review_rows[0]["debit_tax_fill_status"])
            self.assertEqual("10", review_rows[0]["debit_tax_rate"])
            self.assertEqual("inner", review_rows[0]["debit_tax_calc_mode"])
            self.assertEqual(TAX_REVIEW_COLUMNS, fieldnames[-len(TAX_REVIEW_COLUMNS) :])
            tax_manifest = replacer_manifest.get("tax_postprocess") or {}
            self.assertEqual(True, bool(tax_manifest.get("enabled")))
            self.assertEqual(1, int(tax_manifest.get("rows_changed") or 0))
            self.assertEqual(1, int(tax_manifest.get("debit_filled_count") or 0))
            self.assertEqual(
                1,
                int((((tax_manifest.get("status_counts") or {}).get("debit") or {}).get(STATUS_APPLIED_INNER_FLOOR)) or 0),
            )
            runner_tax_cfg = run_manifest.get("yayoi_tax_config") or {}
            self.assertEqual(True, bool(runner_tax_cfg.get("enabled")))
            self.assertEqual("tax_excluded", runner_tax_cfg.get("bookkeeping_mode"))
            self.assertEqual("floor", runner_tax_cfg.get("rounding_mode"))

    def test_credit_card_runner_wires_tax_postprocess_and_preserves_existing_tax_amount(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_TAX_RUNTIME"
            line_root = repo_root / "clients" / client_id / "lines" / "credit_card_statement"
            (line_root / "inputs" / "ledger_ref").mkdir(parents=True, exist_ok=True)
            (line_root / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
            (line_root / "config").mkdir(parents=True, exist_ok=True)
            (repo_root / "lexicon").mkdir(parents=True, exist_ok=True)
            (repo_root / "defaults" / "credit_card_statement").mkdir(parents=True, exist_ok=True)
            (repo_root / "lexicon" / "lexicon.json").write_text(
                json.dumps(
                    {
                        "schema": "belle.lexicon.v1",
                        "version": "test",
                        "categories": [],
                        "term_rows": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (repo_root / "defaults" / "credit_card_statement" / "category_defaults.json").write_text(
                json.dumps(
                    {
                        "schema": "belle.category_defaults.v1",
                        "version": "test",
                        "defaults": {},
                        "global_fallback": {
                            "debit_account": RECEIPT_ACCOUNT,
                            "confidence": 0.35,
                            "priority": "HIGH",
                            "reason_code": "global_fallback",
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (line_root / "config" / "category_overrides.json").write_text("{}", encoding="utf-8")
            (line_root / "config" / "credit_card_line_config.json").write_text(
                json.dumps(
                    {
                        "schema": "belle.credit_card_line_config.v0",
                        "version": "0.1",
                        "placeholder_account_name": PLACEHOLDER_ACCOUNT,
                        "payable_account_name": PAYABLE_ACCOUNT,
                        "training": {"exclude_counter_accounts": []},
                        "thresholds": {
                            "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
                            "merchant_key_payable_subaccount": {"min_count": 1, "min_p_majority": 0.5},
                            "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.5},
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
                encoding="utf-8",
            )
            _write_tax_config(repo_root, client_id, enabled=True)

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _cc_row(
                        summary="SHOPA /learn",
                        debit_account=TRAVEL_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        amount=605,
                        credit_subaccount="CARD_A",
                    ),
                    _cc_row(
                        summary="SHOPB /learn",
                        debit_account=SUPPLIES_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        amount=605,
                        credit_subaccount="CARD_A",
                    ),
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _cc_row(
                        summary="SHOPA /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        amount=605,
                        credit_subaccount="",
                        debit_tax_division="課対仕入内10%適格",
                    ),
                    _cc_row(
                        summary="SHOPB /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        amount=605,
                        credit_subaccount="",
                        debit_tax_division="課対仕入内10%適格",
                        debit_tax_amount="99",
                    ),
                ],
            )

            result = card_runner.run_card(repo_root, client_id)
            run_dir = Path(result["run_dir"])
            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            replacer_manifest_path = Path(str(run_manifest.get("replacer_manifest_path") or ""))
            replacer_manifest = json.loads(replacer_manifest_path.read_text(encoding="utf-8"))
            review_path = Path(str((replacer_manifest.get("reports") or {}).get("review_report_csv") or ""))
            output_path = Path(str(replacer_manifest.get("output_file") or ""))
            rows = _read_csv_rows(output_path)
            fieldnames, review_rows = _read_review_rows(review_path)

            self.assertEqual("55", rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual("99", rows[1][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual("CARD_A", rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("CARD_A", rows[1][COL_CREDIT_SUBACCOUNT])
            self.assertEqual(
                [STATUS_APPLIED_INNER_FLOOR, STATUS_TAX_AMOUNT_ALREADY_PRESENT],
                [row["debit_tax_fill_status"] for row in review_rows],
            )
            self.assertEqual(TAX_REVIEW_COLUMNS, fieldnames[-len(TAX_REVIEW_COLUMNS) :])
            tax_manifest = replacer_manifest.get("tax_postprocess") or {}
            self.assertEqual(True, bool(tax_manifest.get("enabled")))
            self.assertEqual(1, int(tax_manifest.get("debit_filled_count") or 0))
            runner_tax_cfg = run_manifest.get("yayoi_tax_config") or {}
            self.assertEqual(True, bool(runner_tax_cfg.get("enabled")))
            self.assertEqual("tax_excluded", runner_tax_cfg.get("bookkeeping_mode"))
            self.assertEqual("floor", runner_tax_cfg.get("rounding_mode"))


if __name__ == "__main__":
    unittest.main()
