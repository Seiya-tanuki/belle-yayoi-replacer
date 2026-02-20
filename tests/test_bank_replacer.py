from __future__ import annotations

import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from belle.bank_cache import ROUTE_KANA_SIGN, ROUTE_KANA_SIGN_AMOUNT
from belle.bank_replacer import replace_bank_yayoi_csv
from belle.build_bank_cache import ensure_bank_client_cache_updated
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_CREDIT_TAX_DIVISION,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_DEBIT_TAX_DIVISION,
    COL_MEMO,
    COL_SUMMARY,
)
from belle.yayoi_csv import read_yayoi_csv, token_to_text

PLACEHOLDER_ACCOUNT = "TEMP_PLACEHOLDER"
BANK_ACCOUNT = "BANK_ACCOUNT"
BANK_SUBACCOUNT = "BANK_SUB"

OCR_SUMMARY_WITHDRAW = "OCR_WITHDRAW"
OCR_SUMMARY_DEPOSIT = "OCR_DEPOSIT"
WITHDRAW_AMOUNT = 1200
DEPOSIT_AMOUNT = 2500

LABEL_WITHDRAW = {
    "summary": "TEACHER_WITHDRAW",
    "counter_account": "COUNTER_WITHDRAW",
    "counter_subaccount": "COUNTER_WITHDRAW_SUB",
    "counter_tax_division": "COUNTER_WITHDRAW_TAX",
}
LABEL_DEPOSIT = {
    "summary": "TEACHER_DEPOSIT",
    "counter_account": "COUNTER_DEPOSIT",
    "counter_subaccount": "COUNTER_DEPOSIT_SUB",
    "counter_tax_division": "COUNTER_DEPOSIT_TAX",
}


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "bank_statement"


def _default_thresholds() -> dict[str, dict[str, float | int]]:
    return {
        ROUTE_KANA_SIGN_AMOUNT: {"min_count": 2, "min_p_majority": 0.85},
        ROUTE_KANA_SIGN: {"min_count": 3, "min_p_majority": 0.80},
    }


def _runtime_config(thresholds: dict[str, dict[str, float | int]] | None = None) -> dict[str, object]:
    return {
        "schema": "belle.bank_line_config.v0",
        "version": "0.1",
        "placeholder_account_name": PLACEHOLDER_ACCOUNT,
        "bank_account_name": BANK_ACCOUNT,
        "bank_account_subaccount": BANK_SUBACCOUNT,
        "thresholds": thresholds or _default_thresholds(),
    }


def _write_bank_config(line_root: Path, config: dict[str, object]) -> None:
    cfg_path = line_root / "config" / "bank_line_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_row(
    *,
    date_text: str,
    summary: str,
    debit_account: str,
    credit_account: str,
    amount: int,
    memo: str = "",
    debit_subaccount: str = "",
    credit_subaccount: str = "",
    debit_tax_division: str = "",
    credit_tax_division: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = date_text
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    cols[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    cols[COL_DEBIT_AMOUNT] = str(int(amount))
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_CREDIT_TAX_DIVISION] = credit_tax_division
    cols[COL_CREDIT_AMOUNT] = str(int(amount))
    cols[COL_SUMMARY] = summary
    cols[COL_MEMO] = memo
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _read_csv_rows(path: Path) -> list[list[str]]:
    csv_obj = read_yayoi_csv(path)
    return [[token_to_text(tok, csv_obj.encoding) for tok in row.tokens] for row in csv_obj.rows]


def _prepare_learning_cache(repo_root: Path, client_id: str) -> tuple[Path, Path]:
    line_root = _line_root(repo_root, client_id)
    ocr_dir = line_root / "inputs" / "training" / "ocr_kari_shiwake"
    ref_dir = line_root / "inputs" / "training" / "reference_yayoi"
    ocr_dir.mkdir(parents=True, exist_ok=True)
    ref_dir.mkdir(parents=True, exist_ok=True)

    _write_bank_config(line_root, _runtime_config())

    ocr_training_rows = [
        _build_row(
            date_text="2026/01/05",
            summary=OCR_SUMMARY_WITHDRAW,
            debit_account=PLACEHOLDER_ACCOUNT,
            credit_account=BANK_ACCOUNT,
            amount=WITHDRAW_AMOUNT,
            memo="SIGN=debit",
            debit_subaccount="OCR_D_SUB",
            debit_tax_division="OCR_D_TAX",
            credit_subaccount=BANK_SUBACCOUNT,
            credit_tax_division="OCR_C_TAX",
        ),
        _build_row(
            date_text="2026/01/06",
            summary=OCR_SUMMARY_WITHDRAW,
            debit_account=PLACEHOLDER_ACCOUNT,
            credit_account=BANK_ACCOUNT,
            amount=WITHDRAW_AMOUNT,
            memo="SIGN=debit",
            debit_subaccount="OCR_D_SUB_2",
            debit_tax_division="OCR_D_TAX_2",
            credit_subaccount=BANK_SUBACCOUNT,
            credit_tax_division="OCR_C_TAX_2",
        ),
        _build_row(
            date_text="2026/01/07",
            summary=OCR_SUMMARY_DEPOSIT,
            debit_account=BANK_ACCOUNT,
            credit_account=PLACEHOLDER_ACCOUNT,
            amount=DEPOSIT_AMOUNT,
            memo="SIGN=credit",
            debit_subaccount=BANK_SUBACCOUNT,
            debit_tax_division="OCR_D_TAX_3",
            credit_subaccount="OCR_C_SUB",
            credit_tax_division="OCR_C_TAX_3",
        ),
        _build_row(
            date_text="2026/01/08",
            summary=OCR_SUMMARY_DEPOSIT,
            debit_account=BANK_ACCOUNT,
            credit_account=PLACEHOLDER_ACCOUNT,
            amount=DEPOSIT_AMOUNT,
            memo="SIGN=credit",
            debit_subaccount=BANK_SUBACCOUNT,
            debit_tax_division="OCR_D_TAX_4",
            credit_subaccount="OCR_C_SUB_2",
            credit_tax_division="OCR_C_TAX_4",
        ),
    ]
    _write_yayoi_rows(ocr_dir / "training_ocr.csv", ocr_training_rows)

    teacher_rows = [
        _build_row(
            date_text="2026/01/05",
            summary=LABEL_WITHDRAW["summary"],
            debit_account=LABEL_WITHDRAW["counter_account"],
            credit_account=BANK_ACCOUNT,
            amount=WITHDRAW_AMOUNT,
            debit_subaccount=LABEL_WITHDRAW["counter_subaccount"],
            debit_tax_division=LABEL_WITHDRAW["counter_tax_division"],
            credit_subaccount=BANK_SUBACCOUNT,
            credit_tax_division="TEACHER_BANK_TAX_1",
        ),
        _build_row(
            date_text="2026/01/06",
            summary=LABEL_WITHDRAW["summary"],
            debit_account=LABEL_WITHDRAW["counter_account"],
            credit_account=BANK_ACCOUNT,
            amount=WITHDRAW_AMOUNT,
            debit_subaccount=LABEL_WITHDRAW["counter_subaccount"],
            debit_tax_division=LABEL_WITHDRAW["counter_tax_division"],
            credit_subaccount=BANK_SUBACCOUNT,
            credit_tax_division="TEACHER_BANK_TAX_2",
        ),
        _build_row(
            date_text="2026/01/07",
            summary=LABEL_DEPOSIT["summary"],
            debit_account=BANK_ACCOUNT,
            credit_account=LABEL_DEPOSIT["counter_account"],
            amount=DEPOSIT_AMOUNT,
            debit_subaccount=BANK_SUBACCOUNT,
            debit_tax_division="TEACHER_BANK_TAX_3",
            credit_subaccount=LABEL_DEPOSIT["counter_subaccount"],
            credit_tax_division=LABEL_DEPOSIT["counter_tax_division"],
        ),
        _build_row(
            date_text="2026/01/08",
            summary=LABEL_DEPOSIT["summary"],
            debit_account=BANK_ACCOUNT,
            credit_account=LABEL_DEPOSIT["counter_account"],
            amount=DEPOSIT_AMOUNT,
            debit_subaccount=BANK_SUBACCOUNT,
            debit_tax_division="TEACHER_BANK_TAX_4",
            credit_subaccount=LABEL_DEPOSIT["counter_subaccount"],
            credit_tax_division=LABEL_DEPOSIT["counter_tax_division"],
        ),
    ]
    _write_yayoi_rows(ref_dir / "teacher.csv", teacher_rows)

    summary = ensure_bank_client_cache_updated(repo_root, client_id)
    if int(summary.get("pairs_unique_used_total") or 0) != 4:
        raise AssertionError(f"unexpected training pair count: {summary}")

    cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
    if not cache_path.exists():
        raise AssertionError(f"cache not generated: {cache_path}")
    return line_root, cache_path


class BankReplacerTests(unittest.TestCase):
    def test_replaces_placeholder_side_for_withdrawal_and_deposit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_1"
            line_root, cache_path = _prepare_learning_cache(repo_root, client_id)

            target_rows = [
                _build_row(
                    date_text="2026/02/01",
                    summary=OCR_SUMMARY_WITHDRAW,
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=WITHDRAW_AMOUNT,
                    memo="SIGN=debit",
                    debit_subaccount="OLD_DEBIT_SUB",
                    debit_tax_division="OLD_DEBIT_TAX",
                    credit_subaccount=BANK_SUBACCOUNT,
                    credit_tax_division="KEEP_CREDIT_TAX",
                ),
                _build_row(
                    date_text="2026/02/02",
                    summary=OCR_SUMMARY_DEPOSIT,
                    debit_account=BANK_ACCOUNT,
                    credit_account=PLACEHOLDER_ACCOUNT,
                    amount=DEPOSIT_AMOUNT,
                    memo="SIGN=credit",
                    debit_subaccount=BANK_SUBACCOUNT,
                    debit_tax_division="KEEP_DEBIT_TAX",
                    credit_subaccount="OLD_CREDIT_SUB",
                    credit_tax_division="OLD_CREDIT_TAX",
                ),
            ]
            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(in_path, target_rows)

            run_dir = line_root / "outputs" / "runs" / "R_TEST_01"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            manifest = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=_runtime_config(),
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_01",
            )

            self.assertEqual(2, int(manifest["row_count"]))
            self.assertEqual(2, int(manifest["changed_count"]))
            self.assertIn(ROUTE_KANA_SIGN_AMOUNT, manifest["evidence_counts"])

            rows = _read_csv_rows(out_path)

            # Withdrawal row: debit side (placeholder side) must be replaced.
            self.assertEqual(LABEL_WITHDRAW["summary"], rows[0][COL_SUMMARY])
            self.assertEqual(LABEL_WITHDRAW["counter_account"], rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(LABEL_WITHDRAW["counter_subaccount"], rows[0][COL_DEBIT_SUBACCOUNT])
            self.assertEqual(LABEL_WITHDRAW["counter_tax_division"], rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual(BANK_ACCOUNT, rows[0][COL_CREDIT_ACCOUNT])
            self.assertEqual(BANK_SUBACCOUNT, rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("KEEP_CREDIT_TAX", rows[0][COL_CREDIT_TAX_DIVISION])

            # Deposit row: credit side (placeholder side) must be replaced.
            self.assertEqual(LABEL_DEPOSIT["summary"], rows[1][COL_SUMMARY])
            self.assertEqual(LABEL_DEPOSIT["counter_account"], rows[1][COL_CREDIT_ACCOUNT])
            self.assertEqual(LABEL_DEPOSIT["counter_subaccount"], rows[1][COL_CREDIT_SUBACCOUNT])
            self.assertEqual(LABEL_DEPOSIT["counter_tax_division"], rows[1][COL_CREDIT_TAX_DIVISION])
            self.assertEqual(BANK_ACCOUNT, rows[1][COL_DEBIT_ACCOUNT])
            self.assertEqual(BANK_SUBACCOUNT, rows[1][COL_DEBIT_SUBACCOUNT])
            self.assertEqual("KEEP_DEBIT_TAX", rows[1][COL_DEBIT_TAX_DIVISION])

            review_path = Path(manifest["reports"]["review_report_csv"])
            manifest_path = Path(manifest["reports"]["manifest_json"])
            self.assertTrue(review_path.exists())
            self.assertTrue(manifest_path.exists())
            self.assertTrue(review_path.read_bytes().startswith(b"\xEF\xBB\xBF"))

            with review_path.open("r", encoding="utf-8-sig", newline="") as f:
                report_rows = list(csv.DictReader(f))
            self.assertEqual(2, len(report_rows))
            self.assertEqual(
                {ROUTE_KANA_SIGN_AMOUNT},
                {row["evidence_type"] for row in report_rows},
            )

    def test_threshold_gating_min_count_and_p_majority(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_2"
            line_root, cache_path = _prepare_learning_cache(repo_root, client_id)

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        date_text="2026/02/10",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=WITHDRAW_AMOUNT,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_SUB",
                        debit_tax_division="ORIG_TAX",
                        credit_subaccount=BANK_SUBACCOUNT,
                        credit_tax_division="ORIG_BANK_TAX",
                    )
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_02"
            run_dir.mkdir(parents=True, exist_ok=True)

            # min_count gate: strong(2 samples) / weak(2 samples) both rejected.
            out_min_count = run_dir / "target_min_count.csv"
            manifest_min_count = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_min_count,
                cache_path=cache_path,
                config=_runtime_config(
                    {
                        ROUTE_KANA_SIGN_AMOUNT: {"min_count": 3, "min_p_majority": 0.10},
                        ROUTE_KANA_SIGN: {"min_count": 3, "min_p_majority": 0.10},
                    }
                ),
                run_dir=run_dir,
                artifact_prefix="target_min_count",
            )
            self.assertEqual(0, int(manifest_min_count["changed_count"]))

            # p_majority gate: p=1.0 from cache, but threshold 1.01 forces fail-closed.
            out_p_majority = run_dir / "target_p_majority.csv"
            manifest_p_majority = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_p_majority,
                cache_path=cache_path,
                config=_runtime_config(
                    {
                        ROUTE_KANA_SIGN_AMOUNT: {"min_count": 1, "min_p_majority": 1.01},
                        ROUTE_KANA_SIGN: {"min_count": 1, "min_p_majority": 1.01},
                    }
                ),
                run_dir=run_dir,
                artifact_prefix="target_p_majority",
            )
            self.assertEqual(0, int(manifest_p_majority["changed_count"]))

            rows = _read_csv_rows(out_p_majority)
            self.assertEqual(OCR_SUMMARY_WITHDRAW, rows[0][COL_SUMMARY])
            self.assertEqual(PLACEHOLDER_ACCOUNT, rows[0][COL_DEBIT_ACCOUNT])

            review_path = Path(manifest_p_majority["reports"]["review_report_csv"])
            with review_path.open("r", encoding="utf-8-sig", newline="") as f:
                row = next(csv.DictReader(f))
            self.assertEqual("none", row["evidence_type"])
            self.assertIn("p_majority_not_met", row["reasons"])

    def test_fail_closed_on_sign_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_3"
            line_root, cache_path = _prepare_learning_cache(repo_root, client_id)

            in_path = line_root / "inputs" / "kari_shiwake" / "target_mismatch.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        date_text="2026/02/20",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=WITHDRAW_AMOUNT,
                        memo="SIGN=credit",
                        debit_subaccount="ORIG_SUB",
                        debit_tax_division="ORIG_TAX",
                        credit_subaccount=BANK_SUBACCOUNT,
                        credit_tax_division="ORIG_BANK_TAX",
                    )
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_03"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_mismatch_replaced.csv"
            manifest = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=_runtime_config(),
                run_dir=run_dir,
                artifact_prefix="target_sign_mismatch",
            )

            self.assertEqual(1, int(manifest["row_count"]))
            self.assertEqual(0, int(manifest["changed_count"]))

            rows = _read_csv_rows(out_path)
            self.assertEqual(OCR_SUMMARY_WITHDRAW, rows[0][COL_SUMMARY])
            self.assertEqual(PLACEHOLDER_ACCOUNT, rows[0][COL_DEBIT_ACCOUNT])

            review_path = Path(manifest["reports"]["review_report_csv"])
            with review_path.open("r", encoding="utf-8-sig", newline="") as f:
                row = next(csv.DictReader(f))
            self.assertEqual("none", row["evidence_type"])
            self.assertIn("sign_mismatch", row["reasons"])


if __name__ == "__main__":
    unittest.main()

