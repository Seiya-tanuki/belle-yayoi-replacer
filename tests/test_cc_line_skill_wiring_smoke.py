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
    COL_SUMMARY,
)

PLACEHOLDER_ACCOUNT = "仮払金"
PAYABLE_ACCOUNT = "未払金"
CANONICAL_PAYABLE_ACCOUNT = "未払費用"
ACCOUNT_TRAVEL = "旅費交通費"
ACCOUNT_SUPPLIES = "消耗品費"


def _load_replacer_script_module(repo_root: Path):
    script_path = repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
    spec = importlib.util.spec_from_file_location(f"run_yayoi_replacer_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "credit_card_statement"


def _write_min_shared_assets(repo_root: Path) -> None:
    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    lexicon_path.parent.mkdir(parents=True, exist_ok=True)
    lexicon_path.write_text(
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
    defaults_dir = repo_root / "defaults" / "credit_card_statement"
    defaults_dir.mkdir(parents=True, exist_ok=True)
    for filename in ("category_defaults_tax_excluded.json", "category_defaults_tax_included.json"):
        (defaults_dir / filename).write_text(
            json.dumps(defaults_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _prepare_cc_client_layout(
    repo_root: Path,
    client_id: str,
    *,
    file_min_p_majority: float,
    canonical_min_count: int = 1,
    canonical_min_p_majority: float = 0.5,
) -> Path:
    _write_min_shared_assets(repo_root)
    line_root = _line_root(repo_root, client_id)
    (line_root / "inputs" / "ledger_ref").mkdir(parents=True, exist_ok=True)
    (line_root / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
    (line_root / "config").mkdir(parents=True, exist_ok=True)
    (line_root / "config" / "credit_card_line_config.json").write_text(
        json.dumps(
            {
                "schema": "belle.credit_card_line_config.v0",
                "version": "0.1",
                "placeholder_account_name": PLACEHOLDER_ACCOUNT,
                "target_payable_placeholder_names": [PAYABLE_ACCOUNT],
                "training": {"exclude_counter_accounts": []},
                "thresholds": {
                    "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
                    "merchant_key_payable_subaccount": {"min_count": 1, "min_p_majority": 0.5},
                    "file_level_card_inference": {"min_votes": 1, "min_p_majority": float(file_min_p_majority)},
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
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    ruleset_path = repo_root / "rulesets" / "credit_card_statement" / "teacher_extraction_rules_v1.json"
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
    return line_root


def _build_row(
    *,
    date_text: str,
    summary: str,
    debit_account: str,
    credit_account: str,
    debit_subaccount: str = "",
    credit_subaccount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = date_text
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
    with path.open("r", encoding="cp932", newline="") as fh:
        return list(csv.reader(fh))


class CCLineSkillWiringSmokeTests(unittest.TestCase):
    def test_credit_card_line_success_end_to_end_via_yayoi_replacer_skill(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_CC_SMOKE_OK"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = _prepare_cc_client_layout(temp_repo_root, client_id, file_min_p_majority=0.5)

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(
                        date_text="2026/01/05",
                        summary="SHOPA /x",
                        debit_account=ACCOUNT_TRAVEL,
                        credit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    ),
                    _build_row(
                        date_text="2026/01/06",
                        summary="SHOPB /y",
                        debit_account=ACCOUNT_SUPPLIES,
                        credit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    ),
                ],
            )

            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_row(
                        date_text="2026/02/01",
                        summary="SHOPA /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    ),
                    _build_row(
                        date_text="2026/02/02",
                        summary="SHOPB /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
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
                            "credit_card_statement",
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
            rows = _read_rows(replaced_csv_files[0])
            self.assertEqual(ACCOUNT_TRAVEL, rows[0][COL_DEBIT_ACCOUNT], msg=buf.getvalue())
            self.assertEqual(ACCOUNT_SUPPLIES, rows[1][COL_DEBIT_ACCOUNT], msg=buf.getvalue())
            self.assertEqual(CANONICAL_PAYABLE_ACCOUNT, rows[0][COL_CREDIT_ACCOUNT], msg=buf.getvalue())
            self.assertEqual(CANONICAL_PAYABLE_ACCOUNT, rows[1][COL_CREDIT_ACCOUNT], msg=buf.getvalue())
            self.assertEqual("CARD_A", rows[0][COL_CREDIT_SUBACCOUNT], msg=buf.getvalue())
            self.assertEqual("CARD_A", rows[1][COL_CREDIT_SUBACCOUNT], msg=buf.getvalue())

            run_manifest_path = run_dir / "run_manifest.json"
            self.assertTrue(run_manifest_path.exists(), msg=buf.getvalue())
            run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
            replacer_manifest_path = Path(str(run_manifest.get("replacer_manifest_path") or ""))
            self.assertTrue(replacer_manifest_path.exists(), msg=buf.getvalue())
            replacer_manifest = json.loads(replacer_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual("OK", (replacer_manifest.get("file_card_inference") or {}).get("status"))
            self.assertFalse(bool(replacer_manifest.get("canonical_payable_required_failed")))
            self.assertFalse(bool(replacer_manifest.get("payable_sub_fill_required_failed")))

    def test_credit_card_line_non_ok_canonical_payable_triggers_distinct_strict_stop(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_CC_SMOKE_CANONICAL_FAIL"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = _prepare_cc_client_layout(
                temp_repo_root,
                client_id,
                file_min_p_majority=0.5,
                canonical_min_count=2,
                canonical_min_p_majority=0.9,
            )

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(
                        date_text="2026/01/05",
                        summary="SHOPA /x",
                        debit_account=ACCOUNT_TRAVEL,
                        credit_account=CANONICAL_PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    )
                ],
            )

            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_row(
                        date_text="2026/02/01",
                        summary="SHOPA /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    )
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
                            "credit_card_statement",
                            "--yes",
                        ],
                    ):
                        rc = module.main()
            self.assertEqual(2, rc, msg=buf.getvalue())

            latest_path = line_root / "outputs" / "LATEST.txt"
            self.assertTrue(latest_path.exists(), msg=buf.getvalue())
            run_id = latest_path.read_text(encoding="utf-8").strip()
            run_dir = line_root / "outputs" / "runs" / run_id
            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual("FAIL", run_manifest.get("exit_status"))
            self.assertEqual(
                "RUN_NEEDS_REVIEW_CARD_CANONICAL_PAYABLE_FAILED",
                run_manifest.get("ui_reason_code"),
            )

            replacer_manifest_path = Path(str(run_manifest.get("replacer_manifest_path") or ""))
            replacer_manifest = json.loads(replacer_manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(bool(replacer_manifest.get("canonical_payable_required_failed")))
            self.assertFalse(bool(replacer_manifest.get("payable_sub_fill_required_failed")))

            replaced_csv_files = sorted(run_dir.glob("*_replaced_*.csv"))
            rows = _read_rows(replaced_csv_files[0])
            self.assertEqual(PAYABLE_ACCOUNT, rows[0][COL_CREDIT_ACCOUNT], msg=buf.getvalue())
            self.assertEqual("", rows[0][COL_CREDIT_SUBACCOUNT], msg=buf.getvalue())

    def test_credit_card_line_ambiguous_file_card_inference_triggers_strict_stop(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_CC_SMOKE_FAIL"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = _prepare_cc_client_layout(temp_repo_root, client_id, file_min_p_majority=0.6)

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(
                        date_text="2026/01/05",
                        summary="SHOPA /x",
                        debit_account=ACCOUNT_TRAVEL,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    ),
                    _build_row(
                        date_text="2026/01/06",
                        summary="SHOPB /y",
                        debit_account=ACCOUNT_SUPPLIES,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_B",
                    ),
                ],
            )

            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_row(
                        date_text="2026/02/01",
                        summary="SHOPA /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    ),
                    _build_row(
                        date_text="2026/02/02",
                        summary="SHOPB /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
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
                            "credit_card_statement",
                            "--yes",
                        ],
                    ):
                        rc = module.main()
            self.assertEqual(2, rc, msg=buf.getvalue())

            latest_path = line_root / "outputs" / "LATEST.txt"
            self.assertTrue(latest_path.exists(), msg=buf.getvalue())
            run_id = latest_path.read_text(encoding="utf-8").strip()
            self.assertTrue(run_id)

            run_dir = line_root / "outputs" / "runs" / run_id
            self.assertTrue(run_dir.is_dir(), msg=buf.getvalue())

            run_manifest_path = run_dir / "run_manifest.json"
            self.assertTrue(run_manifest_path.exists(), msg=buf.getvalue())
            run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual("FAIL", run_manifest.get("exit_status"))
            self.assertTrue(bool(run_manifest.get("strict_stop_applied")))

            replacer_manifest_path = Path(str(run_manifest.get("replacer_manifest_path") or ""))
            self.assertTrue(replacer_manifest_path.exists(), msg=buf.getvalue())
            replacer_manifest = json.loads(replacer_manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(bool(replacer_manifest.get("payable_sub_fill_required_failed")))

            replaced_csv_files = sorted(run_dir.glob("*_replaced_*.csv"))
            self.assertTrue(replaced_csv_files, msg=buf.getvalue())
            rows = _read_rows(replaced_csv_files[0])
            self.assertEqual("", rows[0][COL_CREDIT_SUBACCOUNT], msg=buf.getvalue())
            self.assertEqual("", rows[1][COL_CREDIT_SUBACCOUNT], msg=buf.getvalue())


if __name__ == "__main__":
    unittest.main()
