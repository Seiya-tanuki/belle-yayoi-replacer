from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.bank_cache import make_bank_label_id
from belle.bank_pairing import normalize_kana_key
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


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "bank_statement"


def _prepare_bank_layout(repo_root: Path, client_id: str) -> Path:
    line_root = _line_root(repo_root, client_id)
    (line_root / "inputs" / "training" / "ocr_kari_shiwake").mkdir(parents=True, exist_ok=True)
    (line_root / "inputs" / "training" / "reference_yayoi").mkdir(parents=True, exist_ok=True)
    (line_root / "config").mkdir(parents=True, exist_ok=True)
    cfg_path = line_root / "config" / "bank_line_config.json"
    cfg_path.write_text(
        json.dumps(
            {
                "schema": "belle.bank_line_config.v0",
                "version": "0.1",
                "placeholder_account_name": "仮払金",
                "bank_account_name": "普通預金",
                "bank_account_subaccount": "",
                "thresholds": {
                    "kana_sign_amount": {"min_count": 2, "min_p_majority": 0.85},
                    "kana_sign": {"min_count": 3, "min_p_majority": 0.80},
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return line_root


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


class BankCachePairLearningTests(unittest.TestCase):
    def test_unique_pair_updates_cache(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_rows = [
                _build_row(
                    date_text="2026/01/05",
                    summary="PBﾃｽﾄ",
                    debit_account="仮払金",
                    credit_account="普通預金",
                    amount=1200,
                    memo="SIGN=debit",
                ),
                _build_row(
                    date_text="2026/01/06",
                    summary="ﾆｺｳﾃｽﾄ",
                    debit_account="普通預金",
                    credit_account="仮払金",
                    amount=2500,
                    memo="SIGN=credit",
                ),
            ]
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ocr_train.csv",
                ocr_rows,
            )

            teacher_rows = [
                _build_row(
                    date_text="2026/01/05",
                    summary="教師摘要A",
                    debit_account="消耗品費",
                    credit_account="普通預金",
                    debit_tax_division="課税仕入10%",
                    amount=1200,
                ),
                _build_row(
                    date_text="2026/01/06",
                    summary="教師摘要B",
                    debit_account="普通預金",
                    credit_account="売上高",
                    credit_tax_division="課税売上10%",
                    amount=2500,
                ),
            ]
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "reference_yayoi" / "teacher.txt",
                teacher_rows,
            )

            summary = ensure_bank_client_cache_updated(repo_root, client_id)
            self.assertEqual(summary["pairs_unique_used_total"], 2)

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            self.assertTrue(cache_path.exists())
            cache_obj = json.loads(cache_path.read_text(encoding="utf-8"))

            self.assertEqual(cache_obj.get("schema"), "belle.bank_client_cache.v0")

            label_withdraw = make_bank_label_id("教師摘要A", "消耗品費", "", "課税仕入10%")
            label_deposit = make_bank_label_id("教師摘要B", "売上高", "", "課税売上10%")
            labels = cache_obj.get("labels") or {}
            self.assertIn(label_withdraw, labels)
            self.assertIn(label_deposit, labels)

            key_withdraw = f"{normalize_kana_key('PBﾃｽﾄ')}|debit|1200"
            key_deposit = f"{normalize_kana_key('ﾆｺｳﾃｽﾄ')}|credit|2500"
            stats = ((cache_obj.get("stats") or {}).get("kana_sign_amount") or {})
            self.assertEqual(int((stats.get(key_withdraw) or {}).get("sample_total") or -1), 1)
            self.assertEqual(int((stats.get(key_deposit) or {}).get("sample_total") or -1), 1)

    def test_ambiguous_join_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C2"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_rows = [
                _build_row(
                    date_text="2026/01/10",
                    summary="ｱｲﾏｲ1",
                    debit_account="仮払金",
                    credit_account="普通預金",
                    amount=1000,
                    memo="SIGN=debit",
                ),
                _build_row(
                    date_text="2026/01/10",
                    summary="ｱｲﾏｲ2",
                    debit_account="仮払金",
                    credit_account="普通預金",
                    amount=1000,
                    memo="SIGN=debit",
                ),
            ]
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ocr_dupe.csv",
                ocr_rows,
            )

            teacher_rows = [
                _build_row(
                    date_text="2026/01/10",
                    summary="教師摘要C",
                    debit_account="消耗品費",
                    credit_account="普通預金",
                    debit_tax_division="課税仕入10%",
                    amount=1000,
                ),
            ]
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "reference_yayoi" / "teacher.csv",
                teacher_rows,
            )

            summary = ensure_bank_client_cache_updated(repo_root, client_id)
            self.assertEqual(summary["pairs_unique_used_total"], 0)

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            cache_obj = json.loads(cache_path.read_text(encoding="utf-8"))
            stats = cache_obj.get("stats") or {}
            self.assertEqual(stats.get("kana_sign_amount") or {}, {})
            self.assertEqual(stats.get("kana_sign") or {}, {})

            applied = cache_obj.get("applied_training_sets") or {}
            self.assertEqual(len(applied), 1)
            entry = next(iter(applied.values()))
            self.assertEqual(int(entry.get("pairs_unique_used", -1)), 0)
            self.assertEqual(int(entry.get("ocr_dup_keys") or 0), 1)

    def test_idempotent_second_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C3"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_rows = [
                _build_row(
                    date_text="2026/01/20",
                    summary="ﾘﾋﾟｰﾄ",
                    debit_account="仮払金",
                    credit_account="普通預金",
                    amount=3300,
                    memo="SIGN=debit",
                ),
            ]
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ocr_once.csv",
                ocr_rows,
            )

            teacher_rows = [
                _build_row(
                    date_text="2026/01/20",
                    summary="教師摘要D",
                    debit_account="旅費交通費",
                    credit_account="普通預金",
                    debit_tax_division="対象外",
                    amount=3300,
                ),
            ]
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "reference_yayoi" / "teacher.csv",
                teacher_rows,
            )

            ensure_bank_client_cache_updated(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            first_obj = json.loads(cache_path.read_text(encoding="utf-8"))
            key = f"{normalize_kana_key('ﾘﾋﾟｰﾄ')}|debit|3300"
            first_total = int(
                (((first_obj.get("stats") or {}).get("kana_sign_amount") or {}).get(key) or {}).get("sample_total")
                or 0
            )
            first_applied_size = len(first_obj.get("applied_training_sets") or {})

            ensure_bank_client_cache_updated(repo_root, client_id)
            second_obj = json.loads(cache_path.read_text(encoding="utf-8"))
            second_total = int(
                (((second_obj.get("stats") or {}).get("kana_sign_amount") or {}).get(key) or {}).get("sample_total")
                or 0
            )
            second_applied_size = len(second_obj.get("applied_training_sets") or {})

            self.assertEqual(first_total, 1)
            self.assertEqual(second_total, 1)
            self.assertEqual(first_applied_size, 1)
            self.assertEqual(second_applied_size, 1)


if __name__ == "__main__":
    unittest.main()
