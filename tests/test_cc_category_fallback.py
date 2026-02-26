from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.build_cc_cache import ensure_cc_client_cache_updated, load_credit_card_line_config
from belle.cc_replacer import replace_credit_card_yayoi_csv
from belle.defaults import load_category_defaults
from belle.lexicon import load_lexicon
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
ACCOUNT_SUPPLIES = "\u6d88\u8017\u54c1\u8cbb"
CATEGORY_KEY_SHOPC = "shopc_category"


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


def _write_shared_assets(repo_root: Path) -> tuple[Path, Path]:
    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    lexicon_path.parent.mkdir(parents=True, exist_ok=True)
    lexicon_path.write_text(
        json.dumps(
            {
                "schema": "belle.lexicon.v1",
                "version": "test",
                "categories": [
                    {
                        "id": 1,
                        "key": CATEGORY_KEY_SHOPC,
                        "label": "SHOPC_CATEGORY",
                        "kind": "merchant",
                        "precision_hint": 0.99,
                        "deprecated": False,
                        "negative_terms": {"n0": [], "n1": []},
                    }
                ],
                "term_rows": [
                    ["n0", "SHOPC", 1, 1.0, "S"],
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    defaults_path = repo_root / "defaults" / "credit_card_statement" / "category_defaults.json"
    defaults_path.parent.mkdir(parents=True, exist_ok=True)
    defaults_path.write_text(
        json.dumps(
            {
                "schema": "belle.category_defaults.v1",
                "version": "test",
                "defaults": {
                    CATEGORY_KEY_SHOPC: {
                        "debit_account": ACCOUNT_TRAVEL,
                        "confidence": 0.7,
                        "priority": "MED",
                        "reason_code": "category_default",
                    }
                },
                "global_fallback": {
                    "debit_account": PLACEHOLDER_ACCOUNT,
                    "confidence": 0.35,
                    "priority": "HIGH",
                    "reason_code": "global_fallback",
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return lexicon_path, defaults_path


def _build_row(
    *,
    summary: str,
    debit_account: str,
    credit_account: str,
    debit_subaccount: str = "",
    credit_subaccount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/02/26"
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


class CCCategoryFallbackTests(unittest.TestCase):
    def test_category_fallback_applies_only_when_merchant_key_route_unresolved(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_FALLBACK"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)
            lexicon_path, defaults_path = _write_shared_assets(repo_root)

            ledger_ref_path = line_root / "inputs" / "ledger_ref" / "ledger_ref.csv"
            _write_yayoi_rows(
                ledger_ref_path,
                [
                    _build_row(
                        summary="SHOPA /learn",
                        debit_account=ACCOUNT_SUPPLIES,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="CARD_A",
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
                        summary="SHOPC /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    ),
                    _build_row(
                        summary="SHOPA /target",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        credit_subaccount="",
                    ),
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_CC_CATEGORY_FALLBACK"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            config = load_credit_card_line_config(repo_root, client_id)
            lex = load_lexicon(lexicon_path)
            defaults = load_category_defaults(defaults_path)

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_CC_CATEGORY_FALLBACK",
                lex=lex,
                defaults=defaults,
            )

            rows = _read_rows(out_path)
            self.assertEqual(ACCOUNT_TRAVEL, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(ACCOUNT_SUPPLIES, rows[1][COL_DEBIT_ACCOUNT])
            self.assertEqual("CARD_A", rows[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("CARD_A", rows[1][COL_CREDIT_SUBACCOUNT])

            file_inference = manifest.get("file_card_inference") or {}
            self.assertEqual("OK", file_inference.get("status"))
            self.assertFalse(bool(manifest.get("payable_sub_fill_required_failed")))

            evidence_counts = manifest.get("evidence_counts") or {}
            self.assertGreaterEqual(int(evidence_counts.get("category_default") or 0), 1)
            self.assertGreaterEqual(int(evidence_counts.get("merchant_key") or 0), 1)

            review_path = Path(str((manifest.get("reports") or {}).get("review_report_csv") or ""))
            self.assertTrue(review_path.exists())
            with review_path.open("r", encoding="utf-8-sig", newline="") as fh:
                report_rows = list(csv.DictReader(fh))

            self.assertEqual("category_default", report_rows[0]["evidence_type"])
            self.assertEqual(CATEGORY_KEY_SHOPC, report_rows[0]["category_key"])
            self.assertEqual("SHOPC", report_rows[0]["matched_needle"])
            self.assertIn("category_default_applied", report_rows[0]["reasons"])


if __name__ == "__main__":
    unittest.main()
