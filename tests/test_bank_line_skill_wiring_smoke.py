from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_MEMO,
    COL_SUMMARY,
)

PLACEHOLDER_ACCOUNT = "TEMP_PLACEHOLDER"
BANK_ACCOUNT = "BANK_ACCOUNT"
BANK_SUBACCOUNT = "BANK_SUB"
OCR_SUMMARY = "OCR_WITHDRAW"
TEACHER_SUMMARY = "TEACHER_WITHDRAW"
COUNTER_ACCOUNT = "COUNTER_EXPENSE"
TARGET_AMOUNT = 1200


def _load_replacer_script_module(repo_root: Path):
    script_path = repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
    spec = importlib.util.spec_from_file_location(f"run_yayoi_replacer_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "bank_statement"


def _build_row(
    *,
    date_text: str,
    summary: str,
    debit_account: str,
    credit_account: str,
    amount: int,
    memo: str,
    debit_subaccount: str = "",
    credit_subaccount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = date_text
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_AMOUNT] = str(int(amount))
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_AMOUNT] = str(int(amount))
    cols[COL_SUMMARY] = summary
    cols[COL_MEMO] = memo
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _prepare_bank_client_layout(repo_root: Path, client_id: str) -> Path:
    line_root = _line_root(repo_root, client_id)
    (line_root / "inputs" / "training" / "ocr_kari_shiwake").mkdir(parents=True, exist_ok=True)
    (line_root / "inputs" / "training" / "reference_yayoi").mkdir(parents=True, exist_ok=True)
    (line_root / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
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


class BankLineSkillWiringSmokeTests(unittest.TestCase):
    def test_bank_line_is_runnable_end_to_end_via_yayoi_replacer_skill(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_BANK_SMOKE"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = _prepare_bank_client_layout(temp_repo_root, client_id)

            training_ocr_rows = [
                _build_row(
                    date_text="2026/01/05",
                    summary=OCR_SUMMARY,
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=TARGET_AMOUNT,
                    memo="SIGN=debit",
                    credit_subaccount=BANK_SUBACCOUNT,
                ),
                _build_row(
                    date_text="2026/01/06",
                    summary=OCR_SUMMARY,
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=TARGET_AMOUNT,
                    memo="SIGN=debit",
                    credit_subaccount=BANK_SUBACCOUNT,
                ),
            ]
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "ocr_kari_shiwake" / "training_ocr.csv",
                training_ocr_rows,
            )

            teacher_rows = [
                _build_row(
                    date_text="2026/01/05",
                    summary=TEACHER_SUMMARY,
                    debit_account=COUNTER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=TARGET_AMOUNT,
                    memo="",
                    credit_subaccount=BANK_SUBACCOUNT,
                ),
                _build_row(
                    date_text="2026/01/06",
                    summary=TEACHER_SUMMARY,
                    debit_account=COUNTER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=TARGET_AMOUNT,
                    memo="",
                    credit_subaccount=BANK_SUBACCOUNT,
                ),
            ]
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "reference_yayoi" / "teacher.csv",
                teacher_rows,
            )

            target_rows = [
                _build_row(
                    date_text="2026/02/01",
                    summary=OCR_SUMMARY,
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=TARGET_AMOUNT,
                    memo="SIGN=debit",
                    credit_subaccount=BANK_SUBACCOUNT,
                )
            ]
            _write_yayoi_rows(line_root / "inputs" / "kari_shiwake" / "target.csv", target_rows)

            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                with contextlib.redirect_stderr(buf):
                    with mock.patch.object(
                        sys,
                        "argv",
                        [
                            "run_yayoi_replacer.py",
                            "--client",
                            client_id,
                            "--line",
                            "bank_statement",
                        ],
                    ):
                        rc = module.main()

            self.assertEqual(0, rc, msg=buf.getvalue())

            latest_path = line_root / "outputs" / "LATEST.txt"
            self.assertTrue(latest_path.exists(), msg=buf.getvalue())
            run_id = latest_path.read_text(encoding="utf-8").strip()
            self.assertTrue(run_id)

            run_dir = line_root / "outputs" / "runs" / run_id
            self.assertTrue(run_dir.is_dir(), msg=buf.getvalue())

            replaced_csv_files = sorted(run_dir.glob("*_replaced_*.csv"))
            review_reports = sorted(run_dir.glob("*_review_report.csv"))
            input_manifests = sorted(
                p for p in run_dir.glob("*_manifest.json") if p.name != "run_manifest.json"
            )
            run_manifest_path = run_dir / "run_manifest.json"

            self.assertTrue(replaced_csv_files, msg=buf.getvalue())
            self.assertTrue(review_reports, msg=buf.getvalue())
            self.assertTrue(input_manifests, msg=buf.getvalue())
            self.assertTrue(run_manifest_path.exists(), msg=buf.getvalue())


if __name__ == "__main__":
    unittest.main()
