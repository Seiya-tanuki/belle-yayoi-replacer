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

PLACEHOLDER_ACCOUNT = "\u4eee\u6255\u91d1"
PAYABLE_ACCOUNT = "\u672a\u6255\u91d1"
ACCOUNT_TRAVEL = "\u65c5\u8cbb\u4ea4\u901a\u8cbb"


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
            "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
            "merchant_key_payable_subaccount": {"min_count": 1, "min_p_majority": 0.5},
            "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.5},
        },
        "candidate_extraction": {
            "min_total_count": 1,
            "min_unique_merchants": 1,
            "min_unique_counter_accounts": 1,
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
                "teacher_payable_candidate_accounts": [PAYABLE_ACCOUNT, "未払費用"],
                "hard_include_terms": ["CARD", "カード"],
                "soft_include_terms": ["VISA"],
                "exclude_terms": ["デビット", "プリペイド", "ローン"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _build_row(
    *,
    summary: str,
    debit_account: str,
    credit_account: str,
    debit_subaccount: str = "",
    credit_subaccount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/02/03"
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    cols[COL_DEBIT_AMOUNT] = "3000"
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_CREDIT_AMOUNT] = "3000"
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


class CCReplacerCreditSidePlaceholderTests(unittest.TestCase):
    def test_refund_like_credit_placeholder_replaces_credit_and_fills_debit_payable_sub(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CREDIT_SIDE"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            ledger_ref_path = line_root / "inputs" / "ledger_ref" / "ledger_ref.csv"
            _write_yayoi_rows(
                ledger_ref_path,
                [
                    _build_row(
                        summary="SHOPC /learn",
                        debit_account=PAYABLE_ACCOUNT,
                        credit_account=ACCOUNT_TRAVEL,
                        debit_subaccount="CARD_A",
                        credit_subaccount="",
                    )
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
                        summary="SHOPC /z",
                        debit_account=PAYABLE_ACCOUNT,
                        credit_account=PLACEHOLDER_ACCOUNT,
                        debit_subaccount="",
                        credit_subaccount="",
                    )
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_CC_CREDIT_SIDE"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            config = load_credit_card_line_config(repo_root, client_id)

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_CC_CREDIT_SIDE",
            )

            rows = _read_rows(out_path)
            self.assertEqual(ACCOUNT_TRAVEL, rows[0][COL_CREDIT_ACCOUNT])
            self.assertEqual("CARD_A", rows[0][COL_DEBIT_SUBACCOUNT])
            self.assertEqual("OK", (manifest.get("file_card_inference") or {}).get("status"))


if __name__ == "__main__":
    unittest.main()
