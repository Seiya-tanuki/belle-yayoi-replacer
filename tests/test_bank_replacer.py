from __future__ import annotations

import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from belle.bank_cache import ROUTE_KANA_SIGN, ROUTE_KANA_SIGN_AMOUNT
from belle.bank_pairing import normalize_kana_key
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


def _default_bank_side_subaccount_config() -> dict[str, object]:
    return {
        "enabled": True,
        "weak_enabled": True,
        "weak_min_count": 3,
    }


def _runtime_config(
    thresholds: dict[str, dict[str, float | int]] | None = None,
    *,
    bank_account_subaccount: str = BANK_SUBACCOUNT,
    bank_side_subaccount: dict[str, object] | None = None,
) -> dict[str, object]:
    bank_sub_cfg = _default_bank_side_subaccount_config()
    if isinstance(bank_side_subaccount, dict):
        bank_sub_cfg.update(bank_side_subaccount)
    return {
        "schema": "belle.bank_line_config.v0",
        "version": "0.1",
        "placeholder_account_name": PLACEHOLDER_ACCOUNT,
        "bank_account_name": BANK_ACCOUNT,
        "bank_account_subaccount": bank_account_subaccount,
        "bank_side_subaccount": bank_sub_cfg,
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


def _prepare_learning_cache(
    repo_root: Path,
    client_id: str,
    *,
    teacher_bank_subaccounts: tuple[str, str, str, str] | None = None,
    config_bank_subaccount: str = BANK_SUBACCOUNT,
) -> tuple[Path, Path]:
    line_root = _line_root(repo_root, client_id)
    ocr_dir = line_root / "inputs" / "training" / "ocr_kari_shiwake"
    ref_dir = line_root / "inputs" / "training" / "reference_yayoi"
    ocr_dir.mkdir(parents=True, exist_ok=True)
    ref_dir.mkdir(parents=True, exist_ok=True)

    _write_bank_config(line_root, _runtime_config(bank_account_subaccount=config_bank_subaccount))

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

    teacher_bank_subs = teacher_bank_subaccounts or (
        BANK_SUBACCOUNT,
        BANK_SUBACCOUNT,
        BANK_SUBACCOUNT,
        BANK_SUBACCOUNT,
    )
    teacher_rows = [
        _build_row(
            date_text="2026/01/05",
            summary=LABEL_WITHDRAW["summary"],
            debit_account=LABEL_WITHDRAW["counter_account"],
            credit_account=BANK_ACCOUNT,
            amount=WITHDRAW_AMOUNT,
            debit_subaccount=LABEL_WITHDRAW["counter_subaccount"],
            debit_tax_division=LABEL_WITHDRAW["counter_tax_division"],
            credit_subaccount=teacher_bank_subs[0],
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
            credit_subaccount=teacher_bank_subs[1],
            credit_tax_division="TEACHER_BANK_TAX_2",
        ),
        _build_row(
            date_text="2026/01/07",
            summary=LABEL_DEPOSIT["summary"],
            debit_account=BANK_ACCOUNT,
            credit_account=LABEL_DEPOSIT["counter_account"],
            amount=DEPOSIT_AMOUNT,
            debit_subaccount=teacher_bank_subs[2],
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
            debit_subaccount=teacher_bank_subs[3],
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


def _prepare_withdraw_learning_cache(
    repo_root: Path,
    client_id: str,
    *,
    amount_to_bank_subaccount: list[tuple[int, str]],
    config_bank_subaccount: str = "",
) -> tuple[Path, Path]:
    line_root = _line_root(repo_root, client_id)
    ocr_dir = line_root / "inputs" / "training" / "ocr_kari_shiwake"
    ref_dir = line_root / "inputs" / "training" / "reference_yayoi"
    ocr_dir.mkdir(parents=True, exist_ok=True)
    ref_dir.mkdir(parents=True, exist_ok=True)

    _write_bank_config(line_root, _runtime_config(bank_account_subaccount=config_bank_subaccount))

    ocr_rows: list[list[str]] = []
    teacher_rows: list[list[str]] = []
    for idx, (amount, bank_sub) in enumerate(amount_to_bank_subaccount, start=1):
        day = 10 + idx
        date_text = f"2026/01/{day:02d}"
        ocr_rows.append(
            _build_row(
                date_text=date_text,
                summary=OCR_SUMMARY_WITHDRAW,
                debit_account=PLACEHOLDER_ACCOUNT,
                credit_account=BANK_ACCOUNT,
                amount=int(amount),
                memo="SIGN=debit",
                debit_subaccount=f"OCR_D_SUB_{idx}",
                debit_tax_division=f"OCR_D_TAX_{idx}",
                credit_subaccount=BANK_SUBACCOUNT,
                credit_tax_division=f"OCR_C_TAX_{idx}",
            )
        )
        teacher_rows.append(
            _build_row(
                date_text=date_text,
                summary=LABEL_WITHDRAW["summary"],
                debit_account=LABEL_WITHDRAW["counter_account"],
                credit_account=BANK_ACCOUNT,
                amount=int(amount),
                debit_subaccount=LABEL_WITHDRAW["counter_subaccount"],
                debit_tax_division=LABEL_WITHDRAW["counter_tax_division"],
                credit_subaccount=bank_sub,
                credit_tax_division=f"TEACHER_BANK_TAX_{idx}",
            )
        )

    _write_yayoi_rows(ocr_dir / "training_ocr.csv", ocr_rows)
    _write_yayoi_rows(ref_dir / "teacher.csv", teacher_rows)

    summary = ensure_bank_client_cache_updated(repo_root, client_id)
    if int(summary.get("pairs_unique_used_total") or 0) != len(amount_to_bank_subaccount):
        raise AssertionError(f"unexpected training pair count: {summary}")

    cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
    if not cache_path.exists():
        raise AssertionError(f"cache not generated: {cache_path}")
    return line_root, cache_path


class BankReplacerTests(unittest.TestCase):
    def test_replaces_counter_and_bank_side_subaccount_when_strong_is_deterministic(self) -> None:
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
                    credit_subaccount="OLD_BANK_SUB_WITHDRAW",
                    credit_tax_division="KEEP_CREDIT_TAX",
                ),
                _build_row(
                    date_text="2026/02/02",
                    summary=OCR_SUMMARY_DEPOSIT,
                    debit_account=BANK_ACCOUNT,
                    credit_account=PLACEHOLDER_ACCOUNT,
                    amount=DEPOSIT_AMOUNT,
                    memo="SIGN=credit",
                    debit_subaccount="OLD_BANK_SUB_DEPOSIT",
                    debit_tax_division="KEEP_DEBIT_TAX",
                    credit_subaccount="OLD_CREDIT_SUB",
                    credit_tax_division="OLD_CREDIT_TAX",
                ),
                _build_row(
                    date_text="2026/02/03",
                    summary=OCR_SUMMARY_WITHDRAW,
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=WITHDRAW_AMOUNT,
                    memo="SIGN=debit",
                    debit_subaccount="OLD_DEBIT_SUB_2",
                    debit_tax_division="OLD_DEBIT_TAX_2",
                    credit_subaccount="OLD_BANK_SUB_WITHDRAW_2",
                    credit_tax_division="KEEP_CREDIT_TAX_2",
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

            self.assertEqual(3, int(manifest["row_count"]))
            self.assertEqual(3, int(manifest["changed_count"]))
            self.assertIn(ROUTE_KANA_SIGN_AMOUNT, manifest["evidence_counts"])
            self.assertEqual(3, int(manifest["bank_side_subaccount_changed_count"]))
            self.assertEqual(
                3,
                int((manifest["bank_side_subaccount_evidence_counts"] or {}).get("strong") or 0),
            )

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

            self.assertEqual(LABEL_WITHDRAW["summary"], rows[2][COL_SUMMARY])
            self.assertEqual(LABEL_WITHDRAW["counter_account"], rows[2][COL_DEBIT_ACCOUNT])
            self.assertEqual(LABEL_WITHDRAW["counter_subaccount"], rows[2][COL_DEBIT_SUBACCOUNT])
            self.assertEqual(BANK_ACCOUNT, rows[2][COL_CREDIT_ACCOUNT])
            self.assertEqual(BANK_SUBACCOUNT, rows[2][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("KEEP_CREDIT_TAX_2", rows[2][COL_CREDIT_TAX_DIVISION])

            review_path = Path(manifest["reports"]["review_report_csv"])
            manifest_path = Path(manifest["reports"]["manifest_json"])
            self.assertTrue(review_path.exists())
            self.assertTrue(manifest_path.exists())
            self.assertTrue(review_path.read_bytes().startswith(b"\xEF\xBB\xBF"))

            with review_path.open("r", encoding="utf-8-sig", newline="") as f:
                report_rows = list(csv.DictReader(f))
            self.assertEqual(3, len(report_rows))
            self.assertEqual(
                {ROUTE_KANA_SIGN_AMOUNT},
                {row["evidence_type"] for row in report_rows},
            )
            self.assertEqual({"1"}, {row["bank_sub_changed"] for row in report_rows})
            self.assertEqual({"bank_sub_kana_sign_amount"}, {row["bank_sub_evidence"] for row in report_rows})

    def test_bank_side_subaccount_is_not_replaced_when_strong_key_is_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_1_AMBIG"
            line_root, cache_path = _prepare_learning_cache(
                repo_root,
                client_id,
                teacher_bank_subaccounts=(
                    "BANK_SUB_A",
                    "BANK_SUB_B",
                    BANK_SUBACCOUNT,
                    BANK_SUBACCOUNT,
                ),
                config_bank_subaccount="",
            )

            cache_obj = json.loads(cache_path.read_text(encoding="utf-8"))
            strong_stats = (((cache_obj.get("bank_account_subaccount_stats") or {}).get(ROUTE_KANA_SIGN_AMOUNT)) or {})
            strong_key = f"{normalize_kana_key(OCR_SUMMARY_WITHDRAW)}|debit|{WITHDRAW_AMOUNT}"
            entry = strong_stats.get(strong_key) or {}
            self.assertEqual(2, int(entry.get("sample_total") or 0))
            self.assertEqual(1, int(entry.get("top_count") or 0))

            target_rows = [
                _build_row(
                    date_text="2026/02/03",
                    summary=OCR_SUMMARY_WITHDRAW,
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=WITHDRAW_AMOUNT,
                    memo="SIGN=debit",
                    debit_subaccount="OLD_DEBIT_SUB",
                    debit_tax_division="OLD_DEBIT_TAX",
                    credit_subaccount="ORIG_BANK_SUB",
                    credit_tax_division="KEEP_BANK_TAX",
                )
            ]
            in_path = line_root / "inputs" / "kari_shiwake" / "target_ambiguous.csv"
            _write_yayoi_rows(in_path, target_rows)

            run_dir = line_root / "outputs" / "runs" / "R_TEST_01_AMBIG"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_ambiguous_replaced.csv"
            manifest = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=_runtime_config(bank_account_subaccount=""),
                run_dir=run_dir,
                artifact_prefix="target_ambiguous",
            )

            self.assertEqual(1, int(manifest["row_count"]))
            self.assertEqual(1, int(manifest["changed_count"]))
            self.assertEqual(0, int(manifest["bank_side_subaccount_changed_count"]))
            self.assertEqual({}, manifest["bank_side_subaccount_evidence_counts"])

            rows = _read_csv_rows(out_path)
            self.assertEqual(LABEL_WITHDRAW["summary"], rows[0][COL_SUMMARY])
            self.assertEqual(LABEL_WITHDRAW["counter_account"], rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(LABEL_WITHDRAW["counter_subaccount"], rows[0][COL_DEBIT_SUBACCOUNT])
            self.assertEqual(LABEL_WITHDRAW["counter_tax_division"], rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual(BANK_ACCOUNT, rows[0][COL_CREDIT_ACCOUNT])
            self.assertEqual("ORIG_BANK_SUB", rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("KEEP_BANK_TAX", rows[0][COL_CREDIT_TAX_DIVISION])

            review_path = Path(manifest["reports"]["review_report_csv"])
            with review_path.open("r", encoding="utf-8-sig", newline="") as f:
                row = next(csv.DictReader(f))
            self.assertEqual(ROUTE_KANA_SIGN_AMOUNT, row["evidence_type"])
            self.assertEqual("credit", row["bank_side"])
            self.assertEqual("ORIG_BANK_SUB", row["bank_sub_before"])
            self.assertEqual("ORIG_BANK_SUB", row["bank_sub_after"])
            self.assertEqual("0", row["bank_sub_changed"])
            self.assertEqual("none", row["bank_sub_evidence"])
            self.assertEqual("2", row["bank_sub_sample_total"])
            self.assertEqual("1", row["bank_sub_top_count"])
            self.assertIn("bank_sub:kana_sign_amount_not_deterministic", row["reasons"])

    def test_bank_side_subaccount_weak_applies_when_strong_missing_and_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_WEAK_APPLY"
            line_root, cache_path = _prepare_withdraw_learning_cache(
                repo_root,
                client_id,
                amount_to_bank_subaccount=[
                    (1100, "BANK_SUB_WEAK"),
                    (1200, "BANK_SUB_WEAK"),
                    (1300, "BANK_SUB_WEAK"),
                ],
                config_bank_subaccount="",
            )

            cache_obj = json.loads(cache_path.read_text(encoding="utf-8"))
            strong_stats = (((cache_obj.get("bank_account_subaccount_stats") or {}).get(ROUTE_KANA_SIGN_AMOUNT)) or {})
            strong_key = f"{normalize_kana_key(OCR_SUMMARY_WITHDRAW)}|debit|9999"
            self.assertNotIn(strong_key, strong_stats)

            in_path = line_root / "inputs" / "kari_shiwake" / "target_weak_apply.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        date_text="2026/02/05",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB",
                        debit_tax_division="ORIG_COUNTER_TAX",
                        credit_subaccount="ORIG_BANK_SUB_1",
                        credit_tax_division="KEEP_BANK_TAX_1",
                    ),
                    _build_row(
                        date_text="2026/02/06",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB_2",
                        debit_tax_division="ORIG_COUNTER_TAX_2",
                        credit_subaccount="ORIG_BANK_SUB_2",
                        credit_tax_division="KEEP_BANK_TAX_2",
                    ),
                    _build_row(
                        date_text="2026/02/07",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB_3",
                        debit_tax_division="ORIG_COUNTER_TAX_3",
                        credit_subaccount="ORIG_BANK_SUB_3",
                        credit_tax_division="KEEP_BANK_TAX_3",
                    ),
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_WEAK_APPLY"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_weak_apply_replaced.csv"
            manifest = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=_runtime_config(bank_account_subaccount=""),
                run_dir=run_dir,
                artifact_prefix="target_weak_apply",
            )

            self.assertEqual(3, int(manifest["changed_count"]))
            self.assertEqual(3, int(manifest["bank_side_subaccount_changed_count"]))
            self.assertEqual(3, int((manifest["evidence_counts"] or {}).get(ROUTE_KANA_SIGN) or 0))
            self.assertEqual(3, int((manifest["bank_side_subaccount_evidence_counts"] or {}).get("weak") or 0))

            rows = _read_csv_rows(out_path)
            self.assertEqual(3, len(rows))
            for row in rows:
                self.assertEqual(LABEL_WITHDRAW["summary"], row[COL_SUMMARY])
                self.assertEqual(LABEL_WITHDRAW["counter_account"], row[COL_DEBIT_ACCOUNT])
                self.assertEqual(LABEL_WITHDRAW["counter_subaccount"], row[COL_DEBIT_SUBACCOUNT])
                self.assertEqual("BANK_SUB_WEAK", row[COL_CREDIT_SUBACCOUNT])

            review_path = Path(manifest["reports"]["review_report_csv"])
            with review_path.open("r", encoding="utf-8-sig", newline="") as f:
                report_rows = list(csv.DictReader(f))
            self.assertEqual(3, len(report_rows))
            self.assertEqual({ROUTE_KANA_SIGN}, {row["evidence_type"] for row in report_rows})
            self.assertEqual({"bank_sub_kana_sign"}, {row["bank_sub_evidence"] for row in report_rows})
            self.assertEqual({"3"}, {row["bank_sub_sample_total"] for row in report_rows})
            self.assertEqual({"3"}, {row["bank_sub_top_count"] for row in report_rows})
            self.assertTrue(
                all("bank_sub:kana_sign_amount_stats_not_found" in row["reasons"] for row in report_rows)
            )

    def test_file_level_bank_sub_inference_ok_fills_all_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_FILE_LEVEL_OK"
            line_root, cache_path = _prepare_withdraw_learning_cache(
                repo_root,
                client_id,
                amount_to_bank_subaccount=[
                    (4100, "BANK_SUB_FILE_OK"),
                    (4200, "BANK_SUB_FILE_OK"),
                    (4300, "BANK_SUB_FILE_OK"),
                ],
                config_bank_subaccount="",
            )

            in_path = line_root / "inputs" / "kari_shiwake" / "target_file_level_ok.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        date_text="2026/03/01",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB_1",
                        debit_tax_division="ORIG_COUNTER_TAX_1",
                        credit_subaccount="",
                        credit_tax_division="KEEP_BANK_TAX_1",
                    ),
                    _build_row(
                        date_text="2026/03/02",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB_2",
                        debit_tax_division="ORIG_COUNTER_TAX_2",
                        credit_subaccount="",
                        credit_tax_division="KEEP_BANK_TAX_2",
                    ),
                    _build_row(
                        date_text="2026/03/03",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB_3",
                        debit_tax_division="ORIG_COUNTER_TAX_3",
                        credit_subaccount="",
                        credit_tax_division="KEEP_BANK_TAX_3",
                    ),
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_FILE_LEVEL_OK"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_file_level_ok_replaced.csv"
            manifest = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=_runtime_config(bank_account_subaccount=""),
                run_dir=run_dir,
                artifact_prefix="target_file_level_ok",
            )

            rows = _read_csv_rows(out_path)
            self.assertEqual(3, len(rows))
            self.assertTrue(all(row[COL_CREDIT_SUBACCOUNT] == "BANK_SUB_FILE_OK" for row in rows))

            file_inf = manifest.get("file_bank_sub_inference") or {}
            self.assertEqual("OK", file_inf.get("status"))
            self.assertEqual("BANK_SUB_FILE_OK", file_inf.get("value"))
            self.assertEqual(3, int(file_inf.get("votes_total") or 0))
            self.assertEqual(False, bool(manifest.get("bank_sub_fill_required_failed")))
            self.assertEqual(3, int(manifest.get("bank_side_rows_total") or 0))
            self.assertEqual(3, int(manifest.get("required_fill_rows_total") or 0))
            self.assertEqual(3, int(manifest.get("filled_rows_total") or 0))

    def test_file_level_bank_sub_inference_below_min_votes_no_fill_and_flagged(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_FILE_LEVEL_FAIL_MIN_VOTES"
            line_root, cache_path = _prepare_withdraw_learning_cache(
                repo_root,
                client_id,
                amount_to_bank_subaccount=[
                    (5100, "BANK_SUB_FILE_FAIL"),
                    (5200, "BANK_SUB_FILE_FAIL"),
                    (5300, "BANK_SUB_FILE_FAIL"),
                ],
                config_bank_subaccount="",
            )

            in_path = line_root / "inputs" / "kari_shiwake" / "target_file_level_fail.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        date_text="2026/03/11",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB_1",
                        debit_tax_division="ORIG_COUNTER_TAX_1",
                        credit_subaccount="",
                        credit_tax_division="KEEP_BANK_TAX_1",
                    ),
                    _build_row(
                        date_text="2026/03/12",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB_2",
                        debit_tax_division="ORIG_COUNTER_TAX_2",
                        credit_subaccount="",
                        credit_tax_division="KEEP_BANK_TAX_2",
                    ),
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_FILE_LEVEL_FAIL"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_file_level_fail_replaced.csv"
            manifest = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=_runtime_config(bank_account_subaccount=""),
                run_dir=run_dir,
                artifact_prefix="target_file_level_fail",
            )

            rows = _read_csv_rows(out_path)
            self.assertEqual(2, len(rows))
            self.assertEqual("", rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("", rows[1][COL_CREDIT_SUBACCOUNT])

            file_inf = manifest.get("file_bank_sub_inference") or {}
            self.assertEqual("FAIL", file_inf.get("status"))
            self.assertEqual(2, int(file_inf.get("votes_total") or 0))
            self.assertTrue(
                any(str(reason).startswith("below_min_votes") for reason in (file_inf.get("reasons") or []))
            )
            self.assertEqual(True, bool(manifest.get("bank_sub_fill_required_failed")))
            self.assertEqual(2, int(manifest.get("required_fill_rows_total") or 0))
            self.assertEqual(0, int(manifest.get("filled_rows_total") or 0))

    def test_bank_side_subaccount_weak_min_count_gate_blocks_subaccount_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_WEAK_MIN"
            line_root, cache_path = _prepare_withdraw_learning_cache(
                repo_root,
                client_id,
                amount_to_bank_subaccount=[
                    (2100, "BANK_SUB_WEAK"),
                    (2200, "BANK_SUB_WEAK"),
                ],
                config_bank_subaccount="",
            )

            in_path = line_root / "inputs" / "kari_shiwake" / "target_weak_min.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        date_text="2026/02/06",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB",
                        debit_tax_division="ORIG_COUNTER_TAX",
                        credit_subaccount="ORIG_BANK_SUB",
                        credit_tax_division="KEEP_BANK_TAX",
                    )
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_WEAK_MIN"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_weak_min_replaced.csv"
            manifest = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=_runtime_config(
                    {
                        ROUTE_KANA_SIGN_AMOUNT: {"min_count": 2, "min_p_majority": 0.85},
                        ROUTE_KANA_SIGN: {"min_count": 2, "min_p_majority": 0.80},
                    },
                    bank_account_subaccount="",
                ),
                run_dir=run_dir,
                artifact_prefix="target_weak_min",
            )

            self.assertEqual(1, int(manifest["changed_count"]))
            self.assertEqual(0, int(manifest["bank_side_subaccount_changed_count"]))
            self.assertEqual({}, manifest["bank_side_subaccount_evidence_counts"])

            rows = _read_csv_rows(out_path)
            self.assertEqual(LABEL_WITHDRAW["summary"], rows[0][COL_SUMMARY])
            self.assertEqual(LABEL_WITHDRAW["counter_account"], rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(LABEL_WITHDRAW["counter_subaccount"], rows[0][COL_DEBIT_SUBACCOUNT])
            self.assertEqual("ORIG_BANK_SUB", rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("KEEP_BANK_TAX", rows[0][COL_CREDIT_TAX_DIVISION])

            review_path = Path(manifest["reports"]["review_report_csv"])
            with review_path.open("r", encoding="utf-8-sig", newline="") as f:
                row = next(csv.DictReader(f))
            self.assertEqual(ROUTE_KANA_SIGN, row["evidence_type"])
            self.assertEqual("none", row["bank_sub_evidence"])
            self.assertEqual("2", row["bank_sub_sample_total"])
            self.assertEqual("2", row["bank_sub_top_count"])
            self.assertIn("bank_sub:kana_sign_min_count_not_met", row["reasons"])

    def test_bank_side_subaccount_weak_is_not_applied_when_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_WEAK_AMBIG"
            line_root, cache_path = _prepare_withdraw_learning_cache(
                repo_root,
                client_id,
                amount_to_bank_subaccount=[
                    (3100, "BANK_SUB_A"),
                    (3200, "BANK_SUB_A"),
                    (3300, "BANK_SUB_B"),
                ],
                config_bank_subaccount="",
            )

            in_path = line_root / "inputs" / "kari_shiwake" / "target_weak_ambiguous.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        date_text="2026/02/07",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB",
                        debit_tax_division="ORIG_COUNTER_TAX",
                        credit_subaccount="ORIG_BANK_SUB",
                        credit_tax_division="KEEP_BANK_TAX",
                    )
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_WEAK_AMBIG"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_weak_ambiguous_replaced.csv"
            manifest = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=_runtime_config(bank_account_subaccount=""),
                run_dir=run_dir,
                artifact_prefix="target_weak_ambiguous",
            )

            self.assertEqual(1, int(manifest["changed_count"]))
            self.assertEqual(0, int(manifest["bank_side_subaccount_changed_count"]))
            self.assertEqual({}, manifest["bank_side_subaccount_evidence_counts"])

            rows = _read_csv_rows(out_path)
            self.assertEqual(LABEL_WITHDRAW["summary"], rows[0][COL_SUMMARY])
            self.assertEqual(LABEL_WITHDRAW["counter_account"], rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(LABEL_WITHDRAW["counter_subaccount"], rows[0][COL_DEBIT_SUBACCOUNT])
            self.assertEqual("ORIG_BANK_SUB", rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("KEEP_BANK_TAX", rows[0][COL_CREDIT_TAX_DIVISION])

            review_path = Path(manifest["reports"]["review_report_csv"])
            with review_path.open("r", encoding="utf-8-sig", newline="") as f:
                row = next(csv.DictReader(f))
            self.assertEqual(ROUTE_KANA_SIGN, row["evidence_type"])
            self.assertEqual("none", row["bank_sub_evidence"])
            self.assertEqual("3", row["bank_sub_sample_total"])
            self.assertEqual("2", row["bank_sub_top_count"])
            self.assertIn("bank_sub:kana_sign_not_deterministic", row["reasons"])

    def test_counter_replacement_is_unaffected_when_bank_side_subaccount_is_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_COUNTER_ONLY"
            line_root, cache_path = _prepare_learning_cache(repo_root, client_id)

            in_path = line_root / "inputs" / "kari_shiwake" / "target_counter_only.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        date_text="2026/02/08",
                        summary=OCR_SUMMARY_WITHDRAW,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=WITHDRAW_AMOUNT,
                        memo="SIGN=debit",
                        debit_subaccount="ORIG_COUNTER_SUB",
                        debit_tax_division="ORIG_COUNTER_TAX",
                        credit_subaccount="ORIG_BANK_SUB",
                        credit_tax_division="KEEP_BANK_TAX",
                    )
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_COUNTER_ONLY"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_counter_only_replaced.csv"
            manifest = replace_bank_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=_runtime_config(
                    bank_side_subaccount={
                        "enabled": False,
                        "weak_enabled": True,
                        "weak_min_count": 3,
                    }
                ),
                run_dir=run_dir,
                artifact_prefix="target_counter_only",
            )

            self.assertEqual(1, int(manifest["changed_count"]))
            self.assertEqual(0, int(manifest["bank_side_subaccount_changed_count"]))
            self.assertEqual({}, manifest["bank_side_subaccount_evidence_counts"])

            rows = _read_csv_rows(out_path)
            self.assertEqual(LABEL_WITHDRAW["summary"], rows[0][COL_SUMMARY])
            self.assertEqual(LABEL_WITHDRAW["counter_account"], rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(LABEL_WITHDRAW["counter_subaccount"], rows[0][COL_DEBIT_SUBACCOUNT])
            self.assertEqual("ORIG_BANK_SUB", rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("KEEP_BANK_TAX", rows[0][COL_CREDIT_TAX_DIVISION])

            review_path = Path(manifest["reports"]["review_report_csv"])
            with review_path.open("r", encoding="utf-8-sig", newline="") as f:
                row = next(csv.DictReader(f))
            self.assertEqual(ROUTE_KANA_SIGN_AMOUNT, row["evidence_type"])
            self.assertEqual("none", row["bank_sub_evidence"])
            self.assertIn("bank_sub:disabled", row["reasons"])

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
