from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.build_cc_cache import ensure_cc_client_cache_updated
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_SUMMARY,
)


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "credit_card_statement"


def _write_cc_config(line_root: Path) -> None:
    cfg = {
        "schema": "belle.credit_card_line_config.v0",
        "version": "0.1",
        "placeholder_account_name": "仮払金",
        "payable_account_name": "未払金",
        "training": {"exclude_counter_accounts": ["普通預金", "当座預金"]},
        "thresholds": {
            "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
            "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.5},
        },
        "candidate_extraction": {
            "min_total_count": 2,
            "min_unique_merchants": 2,
            "min_unique_counter_accounts": 2,
            "manual_allow": [],
        },
    }
    cfg_path = line_root / "config" / "credit_card_line_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    ruleset_path = line_root.parents[3] / "rulesets" / "credit_card_statement" / "teacher_extraction_rules_v1.json"
    ruleset_path.parent.mkdir(parents=True, exist_ok=True)
    ruleset_path.write_text(
        json.dumps(
            {
                "schema": "belle.cc_teacher_extraction_rules.v1",
                "version": "1",
                "teacher_payable_candidate_accounts": ["未払費用", "未払金"],
                "hard_include_terms": ["CARD", "カード"],
                "soft_include_terms": ["VISA"],
                "exclude_terms": ["デビット", "プリペイド", "ローン"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _build_row(*, summary: str, counter_account: str, payable_subaccount: str) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/02/02"
    cols[COL_DEBIT_ACCOUNT] = counter_account
    cols[COL_DEBIT_AMOUNT] = "2400"
    cols[COL_CREDIT_ACCOUNT] = "未払金"
    cols[COL_CREDIT_SUBACCOUNT] = payable_subaccount
    cols[COL_SUMMARY] = summary
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


class CCCacheMultiCardCandidateTests(unittest.TestCase):
    def test_multi_card_subaccounts_become_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_MULTI"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            rows = [
                _build_row(summary="ALPHA MART / purchase", counter_account="消耗品費", payable_subaccount="CARD_A"),
                _build_row(summary="BETA SHOP / purchase", counter_account="旅費交通費", payable_subaccount="CARD_A"),
                _build_row(summary="GAMMA STORE / purchase", counter_account="交際費", payable_subaccount="CARD_B"),
                _build_row(summary="DELTA CAFE / purchase", counter_account="通信費", payable_subaccount="CARD_B"),
            ]
            _write_yayoi_rows(line_root / "inputs" / "ledger_ref" / "batch.csv", rows)

            cache, summary = ensure_cc_client_cache_updated(repo_root, client_id)
            self.assertEqual(1, int(summary.get("applied_new_files") or 0))
            self.assertEqual(4, int(summary.get("rows_used_added") or 0))

            candidates = cache.card_subaccount_candidates
            self.assertIn("CARD_A", candidates)
            self.assertIn("CARD_B", candidates)
            self.assertTrue(bool(candidates["CARD_A"].get("is_candidate")))
            self.assertTrue(bool(candidates["CARD_B"].get("is_candidate")))
            self.assertGreaterEqual(int(candidates["CARD_A"].get("unique_merchants") or 0), 2)
            self.assertGreaterEqual(int(candidates["CARD_B"].get("unique_merchants") or 0), 2)
            self.assertGreaterEqual(int(candidates["CARD_A"].get("unique_counter_accounts") or 0), 2)
            self.assertGreaterEqual(int(candidates["CARD_B"].get("unique_counter_accounts") or 0), 2)


if __name__ == "__main__":
    unittest.main()
