from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.build_cc_cache import ensure_cc_client_cache_updated, load_credit_card_line_config
from belle.cc_replacer import replace_credit_card_yayoi_csv
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
from belle.yayoi_csv import read_yayoi_csv, token_to_text

PLACEHOLDER_ACCOUNT = "仮払金"
PAYABLE_ACCOUNT = "未払金"
ACCOUNT_A = "消耗品費"
ACCOUNT_B = "旅費交通費"


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "credit_card_statement"


def _write_cc_config(line_root: Path) -> None:
    cfg = {
        "schema": "belle.credit_card_line_config.v0",
        "version": "0.1",
        "placeholder_account_name": PLACEHOLDER_ACCOUNT,
        "payable_account_name": PAYABLE_ACCOUNT,
        "training": {"exclude_counter_accounts": []},
        "thresholds": {
            "merchant_key_account": {"min_count": 3, "min_p_majority": 0.9},
            "merchant_key_payable_subaccount": {"min_count": 3, "min_p_majority": 0.9},
            "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.9},
        },
        "candidate_extraction": {
            "min_total_count": 1,
            "min_unique_merchants": 1,
            "min_unique_counter_accounts": 1,
            "manual_allow": [],
        },
        "partial_match": {
            "enabled": True,
            "direction": "cache_key_in_input",
            "require_unique_longest": True,
            "min_match_len": 4,
            "min_stats_sample_total": 10,
            "min_stats_p_majority": 0.95,
        },
    }
    cfg_path = line_root / "config" / "credit_card_line_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_row(
    *,
    summary: str,
    debit_account: str,
    credit_account: str,
    debit_subaccount: str = "",
    credit_subaccount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/02/24"
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
    csv_obj = read_yayoi_csv(path)
    return [[token_to_text(tok, csv_obj.encoding) for tok in row.tokens] for row in csv_obj.rows]


class CCPartialMatchTieFailClosedTests(unittest.TestCase):
    def test_equal_longest_partial_candidates_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_TIE"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            ledger_ref_path = line_root / "inputs" / "ledger_ref" / "ledger_ref.csv"
            _write_yayoi_rows(
                ledger_ref_path,
                [
                    _build_row(
                        summary="ABCD",
                        debit_account=ACCOUNT_A,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
                    )
                    for _ in range(10)
                ]
                + [
                    _build_row(
                        summary="WXYZ",
                        debit_account=ACCOUNT_B,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_B",
                    )
                    for _ in range(10)
                ],
            )

            ensure_cc_client_cache_updated(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            self.assertTrue(cache_path.exists())

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        summary="ABCDWXYZ",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    )
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_CC_PARTIAL_TIE"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            config = load_credit_card_line_config(repo_root, client_id)

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_CC_PARTIAL_TIE",
            )

            rows = _read_rows(out_path)
            self.assertEqual(PLACEHOLDER_ACCOUNT, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual("", rows[0][COL_CREDIT_SUBACCOUNT])

            partial = manifest.get("partial_match") or {}
            self.assertEqual(0, int(partial.get("account_partial_rows_used") or 0))
            self.assertEqual(0, int(partial.get("votes_partial_used") or 0))

            inference = manifest.get("file_card_inference") or {}
            self.assertIn(str(inference.get("status") or ""), {"SKIP", "FAIL"})


if __name__ == "__main__":
    unittest.main()
