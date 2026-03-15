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
    COL_CREDIT_SUBACCOUNT,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_SUMMARY,
)

PAYABLE_ACCOUNT = "PAYABLE_ACCOUNT"
ACCOUNT_TRAVEL = "EXP_TRAVEL"
ACCOUNT_SUPPLIES = "EXP_SUPPLIES"


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
    return repo_root / "clients" / client_id / "lines" / "credit_card_statement"


def _prepare_cc_client_layout(repo_root: Path, client_id: str) -> Path:
    line_root = _line_root(repo_root, client_id)
    (line_root / "inputs" / "ledger_ref").mkdir(parents=True, exist_ok=True)
    (line_root / "config").mkdir(parents=True, exist_ok=True)
    (line_root / "config" / "credit_card_line_config.json").write_text(
        json.dumps(
            {
                "schema": "belle.credit_card_line_config.v0",
                "version": "0.1",
                "placeholder_account_name": "TEMP_PLACEHOLDER",
                "payable_account_name": PAYABLE_ACCOUNT,
                "training": {"exclude_counter_accounts": []},
                "thresholds": {
                    "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
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
    return line_root


def _build_row(*, date_text: str, summary: str, debit_account: str, payable_subaccount: str) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = date_text
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_SUBACCOUNT] = ""
    cols[COL_DEBIT_AMOUNT] = "1000"
    cols[COL_CREDIT_ACCOUNT] = PAYABLE_ACCOUNT
    cols[COL_CREDIT_SUBACCOUNT] = payable_subaccount
    cols[COL_SUMMARY] = summary
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


class CCCacheBuilderSkillWiringSmokeTests(unittest.TestCase):
    def test_credit_card_line_uses_cc_cache_builder_without_receipt_config(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_CC_CACHE_WIRING_SMOKE"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = _prepare_cc_client_layout(temp_repo_root, client_id)
            receipt_config_path = temp_repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json"
            self.assertFalse(receipt_config_path.exists())

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(
                        date_text="2026/01/05",
                        summary="SHOPA /x",
                        debit_account=ACCOUNT_TRAVEL,
                        payable_subaccount="CARD_A",
                    ),
                    _build_row(
                        date_text="2026/01/06",
                        summary="SHOPB /y",
                        debit_account=ACCOUNT_SUPPLIES,
                        payable_subaccount="CARD_A",
                    ),
                ],
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
                            "credit_card_statement",
                        ],
                    ):
                        module.main()

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            self.assertTrue(cache_path.exists(), msg=buf.getvalue())

            cache_obj = json.loads(cache_path.read_text(encoding="utf-8"))
            self.assertEqual("belle.cc_client_cache.v0", cache_obj.get("schema"))
            self.assertEqual("credit_card_statement", cache_obj.get("line_id"))

            merchant_account_stats = cache_obj.get("merchant_key_account_stats") or {}
            self.assertEqual(2, len(merchant_account_stats), msg=buf.getvalue())
            self.assertEqual(
                1,
                ((merchant_account_stats.get("SHOPA") or {}).get("debit_account_counts") or {}).get(ACCOUNT_TRAVEL),
                msg=buf.getvalue(),
            )
            self.assertEqual(
                1,
                ((merchant_account_stats.get("SHOPB") or {}).get("debit_account_counts") or {}).get(ACCOUNT_SUPPLIES),
                msg=buf.getvalue(),
            )

            payable_sub_stats = cache_obj.get("merchant_key_payable_sub_stats") or {}
            self.assertEqual(2, len(payable_sub_stats), msg=buf.getvalue())
            self.assertEqual(
                "CARD_A",
                (payable_sub_stats.get("SHOPA") or {}).get("top_value"),
                msg=buf.getvalue(),
            )

            applied = cache_obj.get("applied_ledger_ref_sha256") or {}
            self.assertEqual(1, len(applied), msg=buf.getvalue())
            applied_entry = next(iter(applied.values()))
            self.assertTrue(str(applied_entry.get("applied_at") or ""), msg=buf.getvalue())
            self.assertEqual(2, int(applied_entry.get("rows_total") or 0), msg=buf.getvalue())
            self.assertEqual(2, int(applied_entry.get("rows_used") or 0), msg=buf.getvalue())

            telemetry_files = sorted((line_root / "artifacts" / "telemetry").glob("client_cache_update_run_*.json"))
            self.assertEqual(1, len(telemetry_files), msg=buf.getvalue())
            telemetry_obj = json.loads(telemetry_files[0].read_text(encoding="utf-8"))
            self.assertEqual("belle.cc_client_cache_update_run.v1", telemetry_obj.get("schema"))
            self.assertEqual("credit_card_statement", telemetry_obj.get("line_id"))
            self.assertEqual(1, int((telemetry_obj.get("summary") or {}).get("applied_new_files") or 0))
            self.assertEqual(
                2,
                int((telemetry_obj.get("cache_stats") or {}).get("merchant_key_account_stats") or 0),
            )
            self.assertEqual(str(cache_path), ((telemetry_obj.get("paths") or {}).get("client_cache") or ""))


if __name__ == "__main__":
    unittest.main()
