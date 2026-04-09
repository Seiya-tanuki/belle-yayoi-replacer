from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from belle.line_runners import bank_statement as bank_runner
from belle.line_runners import receipt as receipt_runner
from belle.tax_postprocess import STATUS_DISABLED
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
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

ACCOUNT_BANK_TARGET = "租税公課"
ACCOUNT_CC_EXCLUDED = "通信費"
ACCOUNT_CC_INCLUDED = "諸会費"
ACCOUNT_RECEIPT_EXCLUDED = "旅費交通費"
ACCOUNT_RECEIPT_INCLUDED = "租税公課"
PLACEHOLDER_ACCOUNT = "仮払金"
PAYABLE_ACCOUNT = "未払金"


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _write_json(path: Path, payload: dict) -> None:
    _write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


def _load_register_module(real_repo_root: Path):
    script_path = real_repo_root / ".agents" / "skills" / "client-register" / "register_client.py"
    spec = importlib.util.spec_from_file_location(f"register_client_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _prepare_template(real_repo_root: Path, repo_root: Path) -> None:
    shutil.copytree(real_repo_root / "clients" / "TEMPLATE", repo_root / "clients" / "TEMPLATE")


def _write_mode_aware_defaults(repo_root: Path, line_id: str, *, excluded: dict, included: dict) -> None:
    base_dir = repo_root / "defaults" / line_id
    _write_json(base_dir / "category_defaults_tax_excluded.json", excluded)
    _write_json(base_dir / "category_defaults_tax_included.json", included)


def _prepare_shared_assets(repo_root: Path) -> Path:
    _write_json(
        repo_root / "lexicon" / "lexicon.json",
        {
            "schema": "belle.lexicon.v1",
            "version": "test",
            "categories": [
                {
                    "id": 1,
                    "key": "shared_misc",
                    "label": "Shared Misc",
                    "kind": "expense",
                    "precision_hint": 0.9,
                    "deprecated": False,
                    "negative_terms": {"n0": [], "n1": []},
                }
            ],
            "term_rows": [
                ["n0", "KNOWNSTORE", 1, 1.0, "S"],
                ["n0", "SHOPA", 1, 1.0, "S"],
            ],
            "term_buckets_prefix2": {"KN": [0], "SH": [1]},
            "learned": {"policy": {"core_weight": 1.0}, "provenance_registry": []},
        },
    )
    _write_mode_aware_defaults(
        repo_root,
        "receipt",
        excluded={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "shared_misc": {
                    "target_account": ACCOUNT_RECEIPT_EXCLUDED,
                    "target_tax_division": "課対仕入内10%適格",
                    "confidence": 0.7,
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
        included={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "shared_misc": {
                    "target_account": ACCOUNT_RECEIPT_INCLUDED,
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
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
    _write_mode_aware_defaults(
        repo_root,
        "credit_card_statement",
        excluded={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "shared_misc": {
                    "target_account": ACCOUNT_CC_EXCLUDED,
                    "target_tax_division": "課対仕入内10%適格",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                }
            },
            "global_fallback": {
                "target_account": PAYABLE_ACCOUNT,
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
                "shared_misc": {
                    "target_account": ACCOUNT_CC_INCLUDED,
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                }
            },
            "global_fallback": {
                "target_account": PAYABLE_ACCOUNT,
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    ruleset_path = repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json"
    _write_json(
        ruleset_path,
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
    )
    return ruleset_path


def _run_register(module, repo_root: Path, *, client_id: str, bookkeeping_mode: str) -> tuple[int, str]:
    output_buffer = io.StringIO()
    with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(output_buffer):
        rc = module.main(
            ["--client-id", client_id, "--bookkeeping-mode", bookkeeping_mode],
            repo_root=repo_root,
        )
    return rc, output_buffer.getvalue()


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


def _receipt_override(repo_root: Path, client_id: str) -> dict:
    payload = json.loads(
        (
            repo_root / "clients" / client_id / "lines" / "receipt" / "config" / "category_overrides.json"
        ).read_text(encoding="utf-8")
    )
    return (payload.get("overrides") or {}).get("shared_misc") or {}


def _credit_card_override(repo_root: Path, client_id: str) -> dict:
    payload = json.loads(
        (
            repo_root
            / "clients"
            / client_id
            / "lines"
            / "credit_card_statement"
            / "config"
            / "category_overrides.json"
        ).read_text(encoding="utf-8")
    )
    return (payload.get("overrides") or {}).get("shared_misc") or {}


def _write_receipt_inputs(repo_root: Path, client_id: str) -> Path:
    line_root = repo_root / "clients" / client_id / "lines" / "receipt"
    _write_yayoi_rows(
        line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
        [
            _receipt_row(
                summary="KNOWNSTORE / meal T1234567890123",
                debit_account=ACCOUNT_RECEIPT_EXCLUDED,
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
            )
        ],
    )
    return line_root


def _write_bank_inputs(repo_root: Path, client_id: str) -> Path:
    line_root = repo_root / "clients" / client_id / "lines" / "bank_statement"
    _write_json(
        line_root / "config" / "bank_line_config.json",
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
                debit_account=ACCOUNT_BANK_TARGET,
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
    return line_root


class BookkeepingModeRolloutAcceptanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.register_module = _load_register_module(self.real_repo_root)

    def test_tax_excluded_creation_uses_excluded_defaults_and_enables_tax_fill(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _prepare_template(self.real_repo_root, repo_root)
            ruleset_path = _prepare_shared_assets(repo_root)

            rc, output = _run_register(
                self.register_module,
                repo_root,
                client_id="C_PHASE3_EXCLUDED",
                bookkeeping_mode="tax_excluded",
            )
            self.assertEqual(0, rc, msg=output)

            shared_tax_config = json.loads(
                (repo_root / "clients" / "C_PHASE3_EXCLUDED" / "config" / "yayoi_tax_config.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(
                {
                    "schema": "belle.yayoi_tax_config.v1",
                    "version": "1.0",
                    "enabled": True,
                    "bookkeeping_mode": "tax_excluded",
                    "rounding_mode": "floor",
                },
                shared_tax_config,
            )
            self.assertEqual(
                {
                    "target_account": ACCOUNT_RECEIPT_EXCLUDED,
                    "target_tax_division": "課対仕入内10%適格",
                },
                _receipt_override(repo_root, "C_PHASE3_EXCLUDED"),
            )
            self.assertEqual(
                {
                    "target_account": ACCOUNT_CC_EXCLUDED,
                    "target_tax_division": "課対仕入内10%適格",
                },
                _credit_card_override(repo_root, "C_PHASE3_EXCLUDED"),
            )

            receipt_line_root = _write_receipt_inputs(repo_root, "C_PHASE3_EXCLUDED")
            result = receipt_runner.run_receipt(
                repo_root,
                "C_PHASE3_EXCLUDED",
                client_layout_line_id="receipt",
                client_dir=receipt_line_root,
                config_path=ruleset_path,
            )
            run_dir = Path(str(result["run_dir"]))
            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            replacer_manifest = run_manifest["outputs"][0]
            output_rows = _read_csv_rows(Path(str(replacer_manifest["output_file"])))
            _, review_rows = _read_review_rows(Path(str(replacer_manifest["reports"]["review_report_csv"])))

            self.assertEqual(ACCOUNT_RECEIPT_EXCLUDED, output_rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual("課対仕入内10%適格", output_rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("55", output_rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual("55", review_rows[0]["debit_tax_amount_after"])
            self.assertEqual(True, bool((run_manifest.get("yayoi_tax_config") or {}).get("enabled")))
            self.assertEqual(True, bool((replacer_manifest.get("tax_postprocess") or {}).get("enabled")))
            self.assertEqual(1, int(((replacer_manifest.get("tax_postprocess") or {}).get("debit_filled_count")) or 0))

    def test_tax_included_creation_uses_included_defaults_and_disables_tax_fill_client_wide(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _prepare_template(self.real_repo_root, repo_root)
            ruleset_path = _prepare_shared_assets(repo_root)

            rc, output = _run_register(
                self.register_module,
                repo_root,
                client_id="C_PHASE3_INCLUDED",
                bookkeeping_mode="tax_included",
            )
            self.assertEqual(0, rc, msg=output)

            shared_tax_config = json.loads(
                (repo_root / "clients" / "C_PHASE3_INCLUDED" / "config" / "yayoi_tax_config.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(
                {
                    "schema": "belle.yayoi_tax_config.v1",
                    "version": "1.0",
                    "enabled": False,
                    "bookkeeping_mode": "tax_included",
                    "rounding_mode": "floor",
                },
                shared_tax_config,
            )
            self.assertEqual(
                {
                    "target_account": ACCOUNT_RECEIPT_INCLUDED,
                    "target_tax_division": "課対仕入込10%",
                },
                _receipt_override(repo_root, "C_PHASE3_INCLUDED"),
            )
            self.assertEqual(
                {
                    "target_account": ACCOUNT_CC_INCLUDED,
                    "target_tax_division": "課対仕入込10%",
                },
                _credit_card_override(repo_root, "C_PHASE3_INCLUDED"),
            )

            receipt_line_root = _write_receipt_inputs(repo_root, "C_PHASE3_INCLUDED")
            receipt_result = receipt_runner.run_receipt(
                repo_root,
                "C_PHASE3_INCLUDED",
                client_layout_line_id="receipt",
                client_dir=receipt_line_root,
                config_path=ruleset_path,
            )
            receipt_run_dir = Path(str(receipt_result["run_dir"]))
            receipt_run_manifest = json.loads((receipt_run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            receipt_replacer_manifest = receipt_run_manifest["outputs"][0]
            receipt_output_rows = _read_csv_rows(Path(str(receipt_replacer_manifest["output_file"])))
            _, receipt_review_rows = _read_review_rows(
                Path(str(receipt_replacer_manifest["reports"]["review_report_csv"]))
            )

            self.assertEqual(ACCOUNT_RECEIPT_EXCLUDED, receipt_output_rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual("課対仕入内10%適格", receipt_output_rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("", receipt_output_rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(STATUS_DISABLED, receipt_review_rows[0]["debit_tax_fill_status"])
            self.assertEqual(False, bool((receipt_run_manifest.get("yayoi_tax_config") or {}).get("enabled")))
            self.assertEqual(False, bool((receipt_replacer_manifest.get("tax_postprocess") or {}).get("enabled")))

            bank_line_root = _write_bank_inputs(repo_root, "C_PHASE3_INCLUDED")
            bank_result = bank_runner.run_bank(repo_root, "C_PHASE3_INCLUDED", client_dir=bank_line_root)
            bank_run_dir = Path(str(bank_result["run_dir"]))
            bank_run_manifest = json.loads((bank_run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            bank_replacer_manifest = json.loads(
                Path(str(bank_run_manifest["replacer_manifest_path"])).read_text(encoding="utf-8")
            )
            bank_output_rows = _read_csv_rows(Path(str(bank_replacer_manifest["output_file"])))
            _, bank_review_rows = _read_review_rows(Path(str(bank_replacer_manifest["reports"]["review_report_csv"])))

            self.assertEqual(ACCOUNT_BANK_TARGET, bank_output_rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual("課対仕入内10%区分80%", bank_output_rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("", bank_output_rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(STATUS_DISABLED, bank_review_rows[0]["debit_tax_fill_status"])
            self.assertEqual(False, bool((bank_replacer_manifest.get("tax_postprocess") or {}).get("enabled")))
            self.assertEqual(False, bool((bank_run_manifest.get("yayoi_tax_config") or {}).get("enabled")))


if __name__ == "__main__":
    unittest.main()
