from __future__ import annotations

import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from belle.build_bank_cache import ensure_bank_client_cache_updated
from belle.build_cc_cache import ensure_cc_client_cache_updated
from belle.build_client_cache import ensure_client_cache_updated
from belle.lexicon import load_lexicon
from belle.line_runners import receipt as receipt_runner
from belle.line_runners.common import compute_target_file_status
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_MEMO,
    COL_SUMMARY,
)


def _load_script_module(repo_root: Path, rel_path: str):
    script_path = repo_root / rel_path
    spec = importlib.util.spec_from_file_location(f"input_discovery_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _receipt_row(*, summary: str, debit_account: str = "旅費交通費") -> list[str]:
    row = [""] * 25
    row[COL_DEBIT_ACCOUNT] = debit_account
    row[COL_SUMMARY] = summary
    return row


def _credit_card_row(*, summary: str, counter_account: str, payable_subaccount: str) -> list[str]:
    row = [""] * 25
    row[COL_DATE] = "2026/02/01"
    row[COL_DEBIT_ACCOUNT] = counter_account
    row[COL_DEBIT_AMOUNT] = "1200"
    row[COL_CREDIT_ACCOUNT] = "未払金"
    row[COL_CREDIT_SUBACCOUNT] = payable_subaccount
    row[COL_CREDIT_AMOUNT] = "1200"
    row[COL_SUMMARY] = summary
    return row


def _bank_ocr_row(*, date_text: str, summary: str, amount: int) -> list[str]:
    row = [""] * 25
    row[COL_DATE] = date_text
    row[COL_DEBIT_ACCOUNT] = "TEMP_PLACEHOLDER"
    row[COL_DEBIT_AMOUNT] = str(int(amount))
    row[COL_CREDIT_ACCOUNT] = "BANK_ACCOUNT"
    row[COL_CREDIT_SUBACCOUNT] = "BANK_SUB"
    row[COL_CREDIT_AMOUNT] = str(int(amount))
    row[COL_SUMMARY] = summary
    row[COL_MEMO] = "SIGN=debit"
    return row


def _bank_reference_row(*, date_text: str, summary: str, amount: int, bank_subaccount: str) -> list[str]:
    row = [""] * 25
    row[COL_DATE] = date_text
    row[COL_DEBIT_ACCOUNT] = "COUNTER_EXPENSE"
    row[COL_DEBIT_AMOUNT] = str(int(amount))
    row[COL_CREDIT_ACCOUNT] = "BANK_ACCOUNT"
    row[COL_CREDIT_SUBACCOUNT] = bank_subaccount
    row[COL_CREDIT_AMOUNT] = str(int(amount))
    row[COL_SUMMARY] = summary
    return row


def _write_minimal_lexicon(repo_root: Path) -> Path:
    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    lexicon_path.parent.mkdir(parents=True, exist_ok=True)
    lexicon_path.write_text(
        json.dumps(
            {
                "schema": "belle.lexicon.v1",
                "version": "1.0",
                "categories": [
                    {
                        "id": 1,
                        "key": "known",
                        "label": "Known",
                        "kind": "expense",
                        "precision_hint": 0.9,
                        "deprecated": False,
                        "negative_terms": {"n0": [], "n1": []},
                    }
                ],
                "term_rows": [["n0", "KNOWNSTORE", 1, 1.0, "S"]],
                "term_buckets_prefix2": {"KN": [0]},
                "learned": {"policy": {"core_weight": 1.0}, "provenance_registry": []},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return lexicon_path


def _write_cc_config(line_root: Path) -> None:
    cfg_path = line_root / "config" / "credit_card_line_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        json.dumps(
            {
                "schema": "belle.credit_card_line_config.v0",
                "version": "0.1",
                "placeholder_account_name": "仮払金",
                "target_payable_placeholder_names": ["未払金"],
                "training": {"exclude_counter_accounts": []},
                "thresholds": {
                    "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
                    "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.5},
                },
                "teacher_extraction": {
                    "canonical_payable_thresholds": {"min_count": 1, "min_p_majority": 0.5}
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


def _write_bank_config(line_root: Path) -> None:
    cfg_path = line_root / "config" / "bank_line_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        json.dumps(
            {
                "schema": "belle.bank_line_config.v0",
                "version": "0.1",
                "placeholder_account_name": "TEMP_PLACEHOLDER",
                "bank_account_name": "BANK_ACCOUNT",
                "bank_account_subaccount": "BANK_SUB",
                "thresholds": {
                    "kana_sign_amount": {"min_count": 1, "min_p_majority": 0.5},
                    "kana_sign": {"min_count": 1, "min_p_majority": 0.5},
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


class InputDiscoveryHardeningTests(unittest.TestCase):
    def test_kari_shiwake_counting_ignores_unsupported_files_and_accepts_uppercase_csv(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client_dir = Path(td) / "clients" / "C1" / "lines" / "receipt"
            inbox = client_dir / "inputs" / "kari_shiwake"
            _write_yayoi_rows(inbox / "TARGET.CSV", [_receipt_row(summary="UPPER TARGET")])
            _write_text(inbox / ".DS_Store", "ignored")
            _write_text(inbox / "notes.md", "ignored")
            _write_text(inbox / "draft.csv.tmp", "ignored")

            status, reason_key, reason, target_files = compute_target_file_status(client_dir)

            self.assertEqual("OK", status)
            self.assertEqual("single_target_input", reason_key)
            self.assertEqual("single target input", reason)
            self.assertEqual(["TARGET.CSV"], target_files)

    def test_kari_shiwake_counting_skips_when_only_unsupported_files_exist(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client_dir = Path(td) / "clients" / "C1" / "lines" / "receipt"
            inbox = client_dir / "inputs" / "kari_shiwake"
            _write_text(inbox / ".DS_Store", "ignored")
            _write_text(inbox / "notes.md", "ignored")
            _write_text(inbox / "draft.csv.tmp", "ignored")

            status, reason_key, reason, target_files = compute_target_file_status(client_dir)

            self.assertEqual("SKIP", status)
            self.assertEqual("no_target_input", reason_key)
            self.assertEqual("no target input", reason)
            self.assertEqual([], target_files)

    def test_receipt_target_ingest_ignores_non_targets_and_accepts_uppercase_csv(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            client_dir = repo_root / "clients" / client_id / "lines" / "receipt"
            inbox = client_dir / "inputs" / "kari_shiwake"
            _write_yayoi_rows(inbox / "TARGET.CSV", [_receipt_row(summary="UPPER TARGET")])
            _write_text(inbox / "readme.md", "ignored")

            ingest_result = receipt_runner._ingest_single_kari_input(
                repo_root=repo_root,
                client_id=client_id,
                client_layout_line_id="receipt",
                client_dir=client_dir,
            )

            self.assertEqual("TARGET.CSV", ingest_result.original_name)
            self.assertFalse((inbox / "TARGET.CSV").exists())
            self.assertTrue((inbox / "readme.md").exists())
            self.assertTrue(ingest_result.stored_path.exists())

    def test_receipt_ledger_ref_ingest_accepts_uppercase_csv_and_txt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            line_root = repo_root / "clients" / client_id / "lines" / "receipt"
            inbox = line_root / "inputs" / "ledger_ref"
            _write_yayoi_rows(inbox / "BATCH_A.CSV", [_receipt_row(summary="KNOWNSTORE / A")])
            _write_yayoi_rows(inbox / "BATCH_B.TXT", [_receipt_row(summary="OTHERSTORE / B")])
            _write_text(inbox / "ignore.json", "ignored")
            lex = load_lexicon(_write_minimal_lexicon(repo_root))

            _cache, summary = ensure_client_cache_updated(
                repo_root=repo_root,
                client_id=client_id,
                lex=lex,
                config={},
                line_id="receipt",
            )

            self.assertEqual(2, len(summary.ingested_new_files))
            self.assertEqual(2, len(summary.applied_new_files))
            self.assertFalse((inbox / "BATCH_A.CSV").exists())
            self.assertFalse((inbox / "BATCH_B.TXT").exists())
            self.assertTrue((inbox / "ignore.json").exists())

    def test_credit_card_ledger_ref_ingest_accepts_uppercase_csv_and_txt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            line_root = repo_root / "clients" / client_id / "lines" / "credit_card_statement"
            _write_cc_config(line_root)
            inbox = line_root / "inputs" / "ledger_ref"
            _write_yayoi_rows(
                inbox / "LEDGER_A.CSV",
                [_credit_card_row(summary="SHOPA / x", counter_account="旅費交通費", payable_subaccount="CARD_A")],
            )
            _write_yayoi_rows(
                inbox / "LEDGER_B.TXT",
                [_credit_card_row(summary="SHOPB / y", counter_account="消耗品費", payable_subaccount="CARD_B")],
            )
            _write_text(inbox / "ignore.bak", "ignored")

            cache, summary = ensure_cc_client_cache_updated(repo_root, client_id)

            self.assertEqual(2, int(summary.get("ingested_new_files") or 0))
            self.assertEqual(2, int(summary.get("applied_new_files") or 0))
            self.assertEqual(2, len(cache.applied_ledger_ref_sha256 or {}))
            self.assertFalse((inbox / "LEDGER_A.CSV").exists())
            self.assertFalse((inbox / "LEDGER_B.TXT").exists())
            self.assertTrue((inbox / "ignore.bak").exists())

    def test_client_cache_builder_auto_detect_bank_ignores_unsupported_files_and_accepts_uppercase(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        module = _load_script_module(
            repo_root,
            ".agents/skills/client-cache-builder/scripts/build_client_cache.py",
        )

        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = temp_repo_root / "clients" / "AUTO_OK" / "lines" / "bank_statement"
            _write_text(line_root / "inputs" / "training" / "ocr_kari_shiwake" / "OCR.CSV", "ok")
            _write_text(line_root / "inputs" / "training" / "reference_yayoi" / "REF.TXT", "ok")
            _write_text(line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ignore.md", "ignored")
            _write_text(line_root / "inputs" / "training" / "reference_yayoi" / "ignore.json", "ignored")
            other_line_root = temp_repo_root / "clients" / "AUTO_SKIP" / "lines" / "bank_statement"
            _write_text(other_line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ignore.md", "ignored")
            _write_text(other_line_root / "inputs" / "training" / "reference_yayoi" / "ignore.json", "ignored")

            client_id, client_layout_line_id = module.find_client_id_auto(temp_repo_root, "bank_statement")

            self.assertEqual("AUTO_OK", client_id)
            self.assertEqual("bank_statement", client_layout_line_id)

    def test_client_cache_builder_receipt_auto_detect_requires_line_layout(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        module = _load_script_module(
            repo_root,
            ".agents/skills/client-cache-builder/scripts/build_client_cache.py",
        )

        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            legacy_root = temp_repo_root / "clients" / "LEGACY_ONLY"
            _write_text(legacy_root / "inputs" / "ledger_ref" / "LEDGER.CSV", "ok")

            with self.assertRaises(SystemExit) as ctx:
                module.find_client_id_auto(temp_repo_root, "receipt")

            self.assertIn("no ledger_ref inbox files or ingest manifest entries found", str(ctx.exception))

    def test_lexicon_extract_auto_detect_ignores_unsupported_files_and_accepts_uppercase(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        module = _load_script_module(
            repo_root,
            ".agents/skills/lexicon-extract/scripts/run_lexicon_extract.py",
        )

        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            line_root = temp_repo_root / "clients" / "AUTO_OK" / "lines" / "receipt"
            _write_text(line_root / "inputs" / "ledger_ref" / "LEDGER.CSV", "ok")
            _write_text(line_root / "inputs" / "ledger_ref" / "ignore.json", "ignored")
            other_line_root = temp_repo_root / "clients" / "AUTO_SKIP" / "lines" / "receipt"
            _write_text(other_line_root / "inputs" / "ledger_ref" / "ignore.md", "ignored")

            client_id, client_layout_line_id, client_dir = module.find_client_id_auto(temp_repo_root, "receipt")

            self.assertEqual("AUTO_OK", client_id)
            self.assertEqual("receipt", client_layout_line_id)
            self.assertEqual(line_root, client_dir)

    def test_lexicon_extract_receipt_auto_detect_requires_line_layout(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        module = _load_script_module(
            repo_root,
            ".agents/skills/lexicon-extract/scripts/run_lexicon_extract.py",
        )

        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            legacy_root = temp_repo_root / "clients" / "LEGACY_ONLY"
            _write_text(legacy_root / "inputs" / "ledger_ref" / "LEDGER.CSV", "ok")

            with self.assertRaises(SystemExit) as ctx:
                module.find_client_id_auto(temp_repo_root, "receipt")

            self.assertIn("no ledger_ref inbox files or ingest manifest entries found", str(ctx.exception))

    def test_bank_training_detection_ignores_unsupported_files_and_respects_extension_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK"
            line_root = repo_root / "clients" / client_id / "lines" / "bank_statement"
            _write_bank_config(line_root)
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "ocr_kari_shiwake" / "OCR.CSV",
                [_bank_ocr_row(date_text="2026/01/05", summary="OCR_WITHDRAW", amount=1200)],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "reference_yayoi" / "REF.TXT",
                [
                    _bank_reference_row(
                        date_text="2026/01/05",
                        summary="TEACHER_WITHDRAW",
                        amount=1200,
                        bank_subaccount="BANK_SUB",
                    )
                ],
            )
            _write_text(line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ignore.md", "ignored")
            _write_text(line_root / "inputs" / "training" / "reference_yayoi" / "ignore.json", "ignored")

            summary = ensure_bank_client_cache_updated(repo_root, client_id)

            self.assertEqual("pair", summary.get("training_input_state"))
            self.assertEqual(1, int(summary.get("training_ocr_input_count") or 0))
            self.assertEqual(1, int(summary.get("training_reference_input_count") or 0))
            self.assertEqual(1, len(summary.get("applied_pair_set_ids") or []))
            self.assertFalse((line_root / "inputs" / "training" / "ocr_kari_shiwake" / "OCR.CSV").exists())
            self.assertFalse((line_root / "inputs" / "training" / "reference_yayoi" / "REF.TXT").exists())
            self.assertTrue((line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ignore.md").exists())
            self.assertTrue((line_root / "inputs" / "training" / "reference_yayoi" / "ignore.json").exists())


if __name__ == "__main__":
    unittest.main()
