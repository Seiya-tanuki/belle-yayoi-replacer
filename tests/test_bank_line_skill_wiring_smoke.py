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


def _load_cache_builder_script_module(repo_root: Path):
    script_path = (
        repo_root / ".agents" / "skills" / "client-cache-builder" / "scripts" / "build_client_cache.py"
    )
    spec = importlib.util.spec_from_file_location(f"build_client_cache_{uuid4().hex}", script_path)
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


def _read_yayoi_rows(path: Path) -> list[list[str]]:
    with path.open("r", encoding="cp932", newline="") as f:
        return list(csv.reader(f))


def _write_file_level_training_pair(
    line_root: Path,
    *,
    bank_subaccount: str,
    amounts: list[int],
) -> None:
    training_ocr_rows: list[list[str]] = []
    teacher_rows: list[list[str]] = []
    for idx, amount in enumerate(amounts, start=1):
        training_ocr_rows.append(
            _build_row(
                date_text=f"2026/01/{10 + idx:02d}",
                summary=OCR_SUMMARY,
                debit_account=PLACEHOLDER_ACCOUNT,
                credit_account=BANK_ACCOUNT,
                amount=int(amount),
                memo="SIGN=debit",
                credit_subaccount=BANK_SUBACCOUNT,
            )
        )
        teacher_rows.append(
            _build_row(
                date_text=f"2026/01/{10 + idx:02d}",
                summary=TEACHER_SUMMARY,
                debit_account=COUNTER_ACCOUNT,
                credit_account=BANK_ACCOUNT,
                amount=int(amount),
                memo="",
                credit_subaccount=bank_subaccount,
            )
        )

    _write_yayoi_rows(
        line_root / "inputs" / "training" / "ocr_kari_shiwake" / "training_ocr.csv",
        training_ocr_rows,
    )
    _write_yayoi_rows(
        line_root / "inputs" / "training" / "reference_yayoi" / "teacher.csv",
        teacher_rows,
    )


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
                            "--yes",
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
            self.assertFalse(
                (line_root / "artifacts" / "ingest" / "ledger_ref").exists(),
                msg=buf.getvalue(),
            )

            run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual("belle.bank_replacer_skill_run.v2", run_manifest.get("schema"))
            bank_cache_update = run_manifest.get("bank_cache_update") or {}
            self.assertEqual(1, int(bank_cache_update.get("applied_pair_set_count") or 0))
            self.assertEqual(0, int(bank_cache_update.get("skipped_pair_set_count") or 0))
            self.assertEqual(2, int(bank_cache_update.get("pairs_unique_used_total") or 0))
            self.assertNotIn("applied_pair_set_ids", bank_cache_update)
            self.assertNotIn("skipped_pair_set_ids", bank_cache_update)
            self.assertNotIn("applied_pair_ids", bank_cache_update)
            self.assertNotIn("skipped_pair_ids", bank_cache_update)

    def test_bank_side_subaccount_can_be_disabled_via_config_through_runner_path(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_BANK_SUBCFG_SMOKE"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = _prepare_bank_client_layout(temp_repo_root, client_id)

            cfg_path = line_root / "config" / "bank_line_config.json"
            cfg_obj = json.loads(cfg_path.read_text(encoding="utf-8"))
            cfg_obj["bank_side_subaccount"] = {
                "enabled": False,
                "weak_enabled": True,
                "weak_min_count": 3,
            }
            cfg_path.write_text(
                json.dumps(cfg_obj, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

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
                    credit_subaccount="KEEP_SUB",
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
                            "--yes",
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
            self.assertTrue(replaced_csv_files, msg=buf.getvalue())
            replaced_path = replaced_csv_files[0]

            with replaced_path.open("r", encoding="cp932", newline="") as f:
                replaced_rows = list(csv.reader(f))
            self.assertTrue(replaced_rows, msg=buf.getvalue())

            replaced_row = replaced_rows[0]
            self.assertEqual("KEEP_SUB", replaced_row[COL_CREDIT_SUBACCOUNT], msg=buf.getvalue())

    def test_bank_line_file_level_subaccount_success_fills_all_rows(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_BANK_FILE_LEVEL_OK_SMOKE"
        inferred_bank_subaccount = "BANK_SUB_FILE_OK"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = _prepare_bank_client_layout(temp_repo_root, client_id)
            cfg_path = line_root / "config" / "bank_line_config.json"
            cfg_obj = json.loads(cfg_path.read_text(encoding="utf-8"))
            cfg_obj["bank_account_subaccount"] = ""
            cfg_path.write_text(json.dumps(cfg_obj, ensure_ascii=False, indent=2), encoding="utf-8")
            _write_file_level_training_pair(
                line_root,
                bank_subaccount=inferred_bank_subaccount,
                amounts=[5100, 5200, 5300],
            )

            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_row(
                        date_text="2026/02/11",
                        summary=OCR_SUMMARY,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        credit_subaccount="",
                    ),
                    _build_row(
                        date_text="2026/02/12",
                        summary=OCR_SUMMARY,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        credit_subaccount="",
                    ),
                    _build_row(
                        date_text="2026/02/13",
                        summary=OCR_SUMMARY,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        credit_subaccount="",
                    ),
                ],
            )

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
                            "--yes",
                        ],
                    ):
                        rc = module.main()
            self.assertEqual(0, rc, msg=buf.getvalue())

            run_id = (line_root / "outputs" / "LATEST.txt").read_text(encoding="utf-8").strip()
            run_dir = line_root / "outputs" / "runs" / run_id
            self.assertTrue(run_dir.is_dir(), msg=buf.getvalue())

            replaced_csv_files = sorted(run_dir.glob("*_replaced_*.csv"))
            self.assertTrue(replaced_csv_files, msg=buf.getvalue())
            rows = _read_yayoi_rows(replaced_csv_files[0])
            self.assertTrue(rows, msg=buf.getvalue())
            self.assertTrue(
                all(row[COL_CREDIT_SUBACCOUNT] == inferred_bank_subaccount for row in rows),
                msg=buf.getvalue(),
            )

            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual("OK", run_manifest.get("exit_status"))
            self.assertFalse(bool(run_manifest.get("strict_stop_applied")))
            replacer_manifest_path = Path(str(run_manifest.get("replacer_manifest_path") or ""))
            self.assertTrue(replacer_manifest_path.exists(), msg=buf.getvalue())

            replacer_manifest = json.loads(replacer_manifest_path.read_text(encoding="utf-8"))
            file_inf = replacer_manifest.get("file_bank_sub_inference") or {}
            self.assertEqual("OK", file_inf.get("status"))
            self.assertFalse(bool(replacer_manifest.get("bank_sub_fill_required_failed")))

    def test_bank_line_file_level_subaccount_min_votes_override_succeeds_with_two_rows(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_BANK_FILE_LEVEL_MIN2_SMOKE"
        inferred_bank_subaccount = "BANK_SUB_FILE_MIN2"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = _prepare_bank_client_layout(temp_repo_root, client_id)
            cfg_path = line_root / "config" / "bank_line_config.json"
            cfg_obj = json.loads(cfg_path.read_text(encoding="utf-8"))
            cfg_obj["bank_account_subaccount"] = ""
            thresholds = cfg_obj.get("thresholds")
            if not isinstance(thresholds, dict):
                thresholds = {}
            thresholds["file_level_bank_sub_inference"] = {
                "min_votes": 2,
                "min_p_majority": 0.9,
            }
            cfg_obj["thresholds"] = thresholds
            cfg_path.write_text(json.dumps(cfg_obj, ensure_ascii=False, indent=2), encoding="utf-8")
            _write_file_level_training_pair(
                line_root,
                bank_subaccount=inferred_bank_subaccount,
                amounts=[6100, 6200, 6300],
            )

            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_row(
                        date_text="2026/02/16",
                        summary=OCR_SUMMARY,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        credit_subaccount="",
                    ),
                    _build_row(
                        date_text="2026/02/17",
                        summary=OCR_SUMMARY,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        credit_subaccount="",
                    ),
                ],
            )

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
                            "--yes",
                        ],
                    ):
                        rc = module.main()
            self.assertEqual(0, rc, msg=buf.getvalue())

            run_id = (line_root / "outputs" / "LATEST.txt").read_text(encoding="utf-8").strip()
            run_dir = line_root / "outputs" / "runs" / run_id
            self.assertTrue(run_dir.is_dir(), msg=buf.getvalue())

            replaced_csv_files = sorted(run_dir.glob("*_replaced_*.csv"))
            self.assertTrue(replaced_csv_files, msg=buf.getvalue())
            rows = _read_yayoi_rows(replaced_csv_files[0])
            self.assertEqual(2, len(rows))
            self.assertTrue(
                all(row[COL_CREDIT_SUBACCOUNT] == inferred_bank_subaccount for row in rows),
                msg=buf.getvalue(),
            )

            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual("OK", run_manifest.get("exit_status"))
            self.assertFalse(bool(run_manifest.get("strict_stop_applied")))

            replacer_manifest_path = Path(str(run_manifest.get("replacer_manifest_path") or ""))
            self.assertTrue(replacer_manifest_path.exists(), msg=buf.getvalue())
            replacer_manifest = json.loads(replacer_manifest_path.read_text(encoding="utf-8"))
            file_inf = replacer_manifest.get("file_bank_sub_inference") or {}
            self.assertEqual("OK", file_inf.get("status"))
            self.assertEqual(2, int(file_inf.get("votes_total") or 0))
            self.assertFalse(bool(replacer_manifest.get("bank_sub_fill_required_failed")))

    def test_bank_line_file_level_subaccount_failure_triggers_runner_strict_stop(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_BANK_FILE_LEVEL_FAIL_SMOKE"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = _prepare_bank_client_layout(temp_repo_root, client_id)
            cfg_path = line_root / "config" / "bank_line_config.json"
            cfg_obj = json.loads(cfg_path.read_text(encoding="utf-8"))
            cfg_obj["bank_account_subaccount"] = ""
            thresholds = cfg_obj.get("thresholds")
            if isinstance(thresholds, dict):
                thresholds.pop("file_level_bank_sub_inference", None)
            cfg_path.write_text(json.dumps(cfg_obj, ensure_ascii=False, indent=2), encoding="utf-8")
            _write_file_level_training_pair(
                line_root,
                bank_subaccount="BANK_SUB_FILE_FAIL",
                amounts=[5100, 5200, 5300],
            )

            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_row(
                        date_text="2026/02/21",
                        summary=OCR_SUMMARY,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        credit_subaccount="",
                    ),
                    _build_row(
                        date_text="2026/02/22",
                        summary=OCR_SUMMARY,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=9999,
                        memo="SIGN=debit",
                        credit_subaccount="",
                    ),
                ],
            )

            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            buf = io.StringIO()
            with self.assertRaises(SystemExit) as ctx:
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
                                "--yes",
                            ],
                        ):
                            module.main()
            self.assertEqual(2, int(ctx.exception.code), msg=buf.getvalue())

            latest_path = line_root / "outputs" / "LATEST.txt"
            self.assertTrue(latest_path.exists(), msg=buf.getvalue())
            run_id = latest_path.read_text(encoding="utf-8").strip()
            self.assertTrue(run_id)

            run_dir = line_root / "outputs" / "runs" / run_id
            self.assertTrue(run_dir.is_dir(), msg=buf.getvalue())

            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual("FAIL", run_manifest.get("exit_status"))
            self.assertTrue(bool(run_manifest.get("strict_stop_applied")))
            self.assertIn("bank_sub_fill_required_failed", run_manifest.get("reasons") or [])

            replacer_manifest_path = Path(str(run_manifest.get("replacer_manifest_path") or ""))
            self.assertTrue(replacer_manifest_path.exists(), msg=buf.getvalue())
            replacer_manifest = json.loads(replacer_manifest_path.read_text(encoding="utf-8"))
            file_inf = replacer_manifest.get("file_bank_sub_inference") or {}
            self.assertEqual("FAIL", file_inf.get("status"))
            self.assertTrue(
                any(
                    "below_min_votes" in str(reason) and "min_votes=3" in str(reason)
                    for reason in (file_inf.get("reasons") or [])
                ),
                msg=buf.getvalue(),
            )
            self.assertTrue(bool(replacer_manifest.get("bank_sub_fill_required_failed")))

            replaced_csv_files = sorted(run_dir.glob("*_replaced_*.csv"))
            self.assertTrue(replaced_csv_files, msg=buf.getvalue())
            rows = _read_yayoi_rows(replaced_csv_files[0])
            self.assertTrue(rows, msg=buf.getvalue())
            self.assertTrue(all(row[COL_CREDIT_SUBACCOUNT] == "" for row in rows), msg=buf.getvalue())

    def test_bank_line_client_cache_builder_does_not_create_ledger_ref_ingest_dir(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_BANK_CACHE_SMOKE"
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

            module = _load_cache_builder_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root
                / ".agents"
                / "skills"
                / "client-cache-builder"
                / "scripts"
                / "build_client_cache.py"
            )

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                with contextlib.redirect_stderr(buf):
                    with mock.patch.object(
                        sys,
                        "argv",
                        [
                            "build_client_cache.py",
                            "--client",
                            client_id,
                            "--line",
                            "bank_statement",
                        ],
                    ):
                        module.main()

            self.assertTrue((line_root / "artifacts" / "cache" / "client_cache.json").exists(), msg=buf.getvalue())
            self.assertFalse(
                (line_root / "artifacts" / "ingest" / "ledger_ref").exists(),
                msg=buf.getvalue(),
            )

            telemetry_files = sorted((line_root / "artifacts" / "telemetry").glob("client_cache_update_run_*.json"))
            self.assertEqual(1, len(telemetry_files), msg=buf.getvalue())
            telemetry_obj = json.loads(telemetry_files[0].read_text(encoding="utf-8"))
            self.assertEqual("belle.bank_client_cache_update_run.v2", telemetry_obj.get("schema"))
            self.assertEqual("0.2", telemetry_obj.get("version"))
            summary = telemetry_obj.get("summary") or {}
            self.assertEqual(1, int(summary.get("applied_pair_set_count") or 0))
            self.assertEqual(0, int(summary.get("skipped_pair_set_count") or 0))
            self.assertEqual(2, int(summary.get("pairs_unique_used_total") or 0))
            self.assertNotIn("applied_pair_set_ids", summary)
            self.assertNotIn("skipped_pair_set_ids", summary)
            self.assertNotIn("applied_pair_ids", summary)
            self.assertNotIn("skipped_pair_ids", summary)


if __name__ == "__main__":
    unittest.main()
