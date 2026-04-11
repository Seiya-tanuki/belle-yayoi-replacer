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
    COL_DEBIT_SUBACCOUNT,
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
    cols[COL_DATE] = "2026/02/01"
    cols[COL_DEBIT_ACCOUNT] = counter_account
    cols[COL_DEBIT_SUBACCOUNT] = ""
    cols[COL_DEBIT_AMOUNT] = "1200"
    cols[COL_CREDIT_ACCOUNT] = "未払金"
    cols[COL_CREDIT_SUBACCOUNT] = payable_subaccount
    cols[COL_SUMMARY] = summary
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


class CCCacheLedgerRefIngestTests(unittest.TestCase):
    def test_ingest_moves_inbox_and_manifest_stores_line_relative_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            inbox = line_root / "inputs" / "ledger_ref"
            _write_yayoi_rows(
                inbox / "batch1.csv",
                [_build_row(summary="STORE-A / item", counter_account="消耗品費", payable_subaccount="CARD_A")],
            )

            cache, summary = ensure_cc_client_cache_updated(repo_root, client_id)

            remaining = [p for p in inbox.iterdir() if p.is_file() and p.name != ".gitkeep"]
            self.assertEqual([], remaining)
            self.assertEqual(1, int(summary.get("ingested_new_files") or 0))
            self.assertEqual(1, int(summary.get("applied_new_files") or 0))
            self.assertEqual(1, int(summary.get("raw_rows_observed_added") or 0))
            self.assertEqual(1, int(summary.get("derived_rows_selected_added") or 0))
            self.assertEqual(1, int(summary.get("rows_total_added") or 0))
            self.assertEqual(1, int(summary.get("rows_used_added") or 0))

            manifest_path = line_root / "artifacts" / "ingest" / "ledger_ref_ingested.json"
            self.assertTrue(manifest_path.exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            ingested = manifest.get("ingested") or {}
            self.assertEqual(1, len(ingested))

            entry = next(iter(ingested.values()))
            stored_relpath = str(entry.get("stored_relpath") or "")
            self.assertTrue(stored_relpath.startswith("artifacts/ingest/ledger_ref/"))
            self.assertTrue((line_root / Path(stored_relpath)).exists())

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            self.assertTrue(cache_path.exists())
            applied_entry = next(iter((cache.applied_ledger_ref_sha256 or {}).values()))
            self.assertEqual(stored_relpath, applied_entry.get("stored_relpath"))
            self.assertEqual(1, int(applied_entry.get("derived_rows_total") or 0))

            derived_manifest_path = line_root / "artifacts" / "derived" / "cc_teacher_manifest.json"
            self.assertTrue(derived_manifest_path.exists())
            derived_manifest = json.loads(derived_manifest_path.read_text(encoding="utf-8"))
            source_entry = next(iter((derived_manifest.get("sources") or {}).values()))
            self.assertTrue(bool(source_entry.get("applied_to_cache_learning")))
            derived_relpath = str(source_entry.get("derived_csv_relpath") or "")
            self.assertTrue((line_root / Path(derived_relpath)).exists())

    def test_duplicate_sha_is_ignored_and_cache_counts_do_not_double(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C2"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            inbox = line_root / "inputs" / "ledger_ref"
            rows = [_build_row(summary="STORE-B / item", counter_account="雑費", payable_subaccount="CARD_B")]
            _write_yayoi_rows(inbox / "first.csv", rows)
            cache1, summary1 = ensure_cc_client_cache_updated(repo_root, client_id)

            total1 = int(cache1.payable_sub_global_stats.sample_total)
            applied1 = len(cache1.applied_cc_teacher_by_raw_sha256)
            self.assertEqual(1, total1)
            self.assertEqual(1, applied1)
            self.assertEqual(1, int(summary1.get("applied_new_files") or 0))

            _write_yayoi_rows(inbox / "second.csv", rows)
            cache2, summary2 = ensure_cc_client_cache_updated(repo_root, client_id)

            total2 = int(cache2.payable_sub_global_stats.sample_total)
            applied2 = len(cache2.applied_cc_teacher_by_raw_sha256)
            self.assertEqual(total1, total2)
            self.assertEqual(applied1, applied2)
            self.assertEqual(1, int(summary2.get("ingested_duplicate_files") or 0))
            self.assertEqual(0, int(summary2.get("applied_new_files") or 0))

            manifest_path = line_root / "artifacts" / "ingest" / "ledger_ref_ingested.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            ignored = manifest.get("ignored_duplicates") or {}
            self.assertEqual(1, len(ignored))
            dup_entries = next(iter(ignored.values()))
            self.assertGreaterEqual(len(dup_entries), 1)
            self.assertTrue(str(dup_entries[0].get("stored_name") or "").startswith("IGNORED_DUPLICATE_"))


if __name__ == "__main__":
    unittest.main()
