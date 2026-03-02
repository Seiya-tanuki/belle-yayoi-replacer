from __future__ import annotations

import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from belle.bank_cache import make_bank_label_id
from belle.bank_pairing import normalize_kana_key
from belle.build_bank_cache import ensure_bank_client_cache_updated
from belle.ingest import sha256_file
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_CREDIT_TAX_DIVISION,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_DEBIT_TAX_DIVISION,
    COL_MEMO,
    COL_SUMMARY,
)

PLACEHOLDER_ACCOUNT = "TEMP_PLACEHOLDER"
BANK_ACCOUNT = "BANK_ACCOUNT"
LEARNED_BANK_SUBACCOUNT = "BANK_SUBACCOUNT_LEARNED"


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "bank_statement"


def _prepare_bank_layout(repo_root: Path, client_id: str) -> Path:
    line_root = _line_root(repo_root, client_id)
    (line_root / "inputs" / "training" / "ocr_kari_shiwake").mkdir(parents=True, exist_ok=True)
    (line_root / "inputs" / "training" / "reference_yayoi").mkdir(parents=True, exist_ok=True)
    (line_root / "config").mkdir(parents=True, exist_ok=True)
    cfg_path = line_root / "config" / "bank_line_config.json"
    cfg_path.write_text(
        json.dumps(
            {
                "schema": "belle.bank_line_config.v0",
                "version": "0.1",
                "placeholder_account_name": PLACEHOLDER_ACCOUNT,
                "bank_account_name": BANK_ACCOUNT,
                "bank_account_subaccount": "",
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


def _build_row(
    *,
    date_text: str,
    summary: str,
    debit_account: str,
    credit_account: str,
    amount: int,
    memo: str = "",
    debit_subaccount: str = "",
    credit_subaccount: str = "",
    debit_tax_division: str = "",
    credit_tax_division: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = date_text
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    cols[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    cols[COL_DEBIT_AMOUNT] = str(int(amount))
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_CREDIT_TAX_DIVISION] = credit_tax_division
    cols[COL_CREDIT_AMOUNT] = str(int(amount))
    cols[COL_SUMMARY] = summary
    cols[COL_MEMO] = memo
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _write_training_pair(
    line_root: Path,
    *,
    ocr_rows: list[list[str]],
    ref_rows: list[list[str]],
    ocr_name: str = "ocr.csv",
    ref_name: str = "teacher.csv",
) -> None:
    _write_yayoi_rows(line_root / "inputs" / "training" / "ocr_kari_shiwake" / ocr_name, ocr_rows)
    _write_yayoi_rows(line_root / "inputs" / "training" / "reference_yayoi" / ref_name, ref_rows)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _manifest_ingested_count(path: Path) -> int:
    if not path.exists():
        return 0
    obj = _load_json(path)
    ingested = obj.get("ingested") or {}
    if not isinstance(ingested, dict):
        return 0
    return len(ingested)


class BankCachePairLearningTests(unittest.TestCase):
    def test_unique_pair_updates_cache(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_summary_withdraw = "OCR_WITHDRAW"
            ocr_summary_deposit = "OCR_DEPOSIT"
            _write_training_pair(
                line_root,
                ocr_rows=[
                    _build_row(
                        date_text="2026/01/05",
                        summary=ocr_summary_withdraw,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=1200,
                        memo="SIGN=debit",
                    ),
                    _build_row(
                        date_text="2026/01/06",
                        summary=ocr_summary_deposit,
                        debit_account=BANK_ACCOUNT,
                        credit_account=PLACEHOLDER_ACCOUNT,
                        amount=2500,
                        memo="SIGN=credit",
                    ),
                ],
                ref_rows=[
                    _build_row(
                        date_text="2026/01/05",
                        summary="TEACHER_WITHDRAW",
                        debit_account="COUNTER_WITHDRAW",
                        credit_account=BANK_ACCOUNT,
                        credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                        debit_tax_division="TAX_D10",
                        amount=1200,
                    ),
                    _build_row(
                        date_text="2026/01/06",
                        summary="TEACHER_DEPOSIT",
                        debit_account=BANK_ACCOUNT,
                        debit_subaccount=LEARNED_BANK_SUBACCOUNT,
                        credit_account="COUNTER_DEPOSIT",
                        credit_tax_division="TAX_C_EX",
                        amount=2500,
                    ),
                ],
                ref_name="teacher.txt",
            )

            summary = ensure_bank_client_cache_updated(repo_root, client_id)
            self.assertEqual(summary["pairs_unique_used_total"], 2)
            self.assertEqual(len(summary.get("applied_pair_set_ids") or []), 1)

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            self.assertTrue(cache_path.exists())
            cache_obj = _load_json(cache_path)
            self.assertEqual(cache_obj.get("schema"), "belle.bank_client_cache.v0")

            label_withdraw = make_bank_label_id("TEACHER_WITHDRAW", "COUNTER_WITHDRAW", "", "TAX_D10")
            label_deposit = make_bank_label_id("TEACHER_DEPOSIT", "COUNTER_DEPOSIT", "", "TAX_C_EX")
            labels = cache_obj.get("labels") or {}
            self.assertIn(label_withdraw, labels)
            self.assertIn(label_deposit, labels)

            key_withdraw = f"{normalize_kana_key(ocr_summary_withdraw)}|debit|1200"
            key_deposit = f"{normalize_kana_key(ocr_summary_deposit)}|credit|2500"
            stats = ((cache_obj.get("stats") or {}).get("kana_sign_amount") or {})
            self.assertEqual(int((stats.get(key_withdraw) or {}).get("sample_total") or -1), 1)
            self.assertEqual(int((stats.get(key_deposit) or {}).get("sample_total") or -1), 1)

            bank_sub_stats = cache_obj.get("bank_account_subaccount_stats") or {}
            self.assertIn("kana_sign_amount", bank_sub_stats)
            self.assertIn("kana_sign", bank_sub_stats)

    def test_unique_pair_updates_cache_with_wareki_teacher_dates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1W"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_summary_withdraw = "OCR_WITHDRAW"
            ocr_summary_deposit = "OCR_DEPOSIT"
            _write_training_pair(
                line_root,
                ocr_rows=[
                    _build_row(
                        date_text="2026/01/05",
                        summary=ocr_summary_withdraw,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=1200,
                        memo="SIGN=debit",
                    ),
                    _build_row(
                        date_text="2026/01/06",
                        summary=ocr_summary_deposit,
                        debit_account=BANK_ACCOUNT,
                        credit_account=PLACEHOLDER_ACCOUNT,
                        amount=2500,
                        memo="SIGN=credit",
                    ),
                ],
                ref_rows=[
                    _build_row(
                        date_text="R.08/01/05",
                        summary="TEACHER_WITHDRAW",
                        debit_account="COUNTER_WITHDRAW",
                        credit_account=BANK_ACCOUNT,
                        credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                        debit_tax_division="TAX_D10",
                        amount=1200,
                    ),
                    _build_row(
                        date_text="R.08/01/06",
                        summary="TEACHER_DEPOSIT",
                        debit_account=BANK_ACCOUNT,
                        debit_subaccount=LEARNED_BANK_SUBACCOUNT,
                        credit_account="COUNTER_DEPOSIT",
                        credit_tax_division="TAX_C_EX",
                        amount=2500,
                    ),
                ],
                ref_name="teacher_wareki.txt",
            )

            summary = ensure_bank_client_cache_updated(repo_root, client_id)
            self.assertEqual(summary["pairs_unique_used_total"], 2)
            self.assertEqual(len(summary.get("applied_pair_set_ids") or []), 1)

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            self.assertTrue(cache_path.exists())
            cache_obj = _load_json(cache_path)
            self.assertEqual(cache_obj.get("schema"), "belle.bank_client_cache.v0")

            label_withdraw = make_bank_label_id("TEACHER_WITHDRAW", "COUNTER_WITHDRAW", "", "TAX_D10")
            label_deposit = make_bank_label_id("TEACHER_DEPOSIT", "COUNTER_DEPOSIT", "", "TAX_C_EX")
            labels = cache_obj.get("labels") or {}
            self.assertIn(label_withdraw, labels)
            self.assertIn(label_deposit, labels)

            key_withdraw = f"{normalize_kana_key(ocr_summary_withdraw)}|debit|1200"
            key_deposit = f"{normalize_kana_key(ocr_summary_deposit)}|credit|2500"
            stats = ((cache_obj.get("stats") or {}).get("kana_sign_amount") or {})
            self.assertEqual(int((stats.get(key_withdraw) or {}).get("sample_total") or -1), 1)
            self.assertEqual(int((stats.get(key_deposit) or {}).get("sample_total") or -1), 1)

    def test_ambiguous_join_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C2"
            line_root = _prepare_bank_layout(repo_root, client_id)

            _write_training_pair(
                line_root,
                ocr_rows=[
                    _build_row(
                        date_text="2026/01/10",
                        summary="OCR_DUP_1",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=1000,
                        memo="SIGN=debit",
                    ),
                    _build_row(
                        date_text="2026/01/10",
                        summary="OCR_DUP_2",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=1000,
                        memo="SIGN=debit",
                    ),
                ],
                ref_rows=[
                    _build_row(
                        date_text="2026/01/10",
                        summary="TEACHER_OK",
                        debit_account="COUNTER",
                        credit_account=BANK_ACCOUNT,
                        credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                        debit_tax_division="TAX_D10",
                        amount=1000,
                    ),
                ],
            )

            with self.assertRaises(SystemExit):
                ensure_bank_client_cache_updated(repo_root, client_id)

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            self.assertFalse(cache_path.exists())

            ocr_inbox = line_root / "inputs" / "training" / "ocr_kari_shiwake"
            ref_inbox = line_root / "inputs" / "training" / "reference_yayoi"
            self.assertEqual(1, len(list(ocr_inbox.glob("*.csv"))))
            self.assertEqual(1, len(list(ref_inbox.glob("*.csv"))))

            ocr_manifest_path = line_root / "artifacts" / "ingest" / "training_ocr_ingested.json"
            ref_manifest_path = line_root / "artifacts" / "ingest" / "training_reference_ingested.json"
            self.assertEqual(0, _manifest_ingested_count(ocr_manifest_path))
            self.assertEqual(0, _manifest_ingested_count(ref_manifest_path))

    def test_skip_duplicate_pair_set_ingests_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C3"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_summary = "OCR_ONCE"
            ocr_rows = [
                _build_row(
                    date_text="2026/01/20",
                    summary=ocr_summary,
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=3300,
                    memo="SIGN=debit",
                ),
            ]
            ref_rows = [
                _build_row(
                    date_text="2026/01/20",
                    summary="TEACHER_ONCE",
                    debit_account="COUNTER_ONCE",
                    credit_account=BANK_ACCOUNT,
                    credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                    debit_tax_division="TAX_D10",
                    amount=3300,
                ),
            ]

            ocr_manifest_path = line_root / "artifacts" / "ingest" / "training_ocr_ingested.json"
            ref_manifest_path = line_root / "artifacts" / "ingest" / "training_reference_ingested.json"

            _write_training_pair(line_root, ocr_rows=ocr_rows, ref_rows=ref_rows, ocr_name="ocr_once.csv")
            first_summary = ensure_bank_client_cache_updated(repo_root, client_id)
            self.assertEqual(1, int(first_summary.get("pairs_unique_used_total") or 0))
            self.assertEqual(1, len(first_summary.get("applied_pair_set_ids") or []))

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            first_obj = _load_json(cache_path)
            key = f"{normalize_kana_key(ocr_summary)}|debit|3300"
            first_total = int(
                (((first_obj.get("stats") or {}).get("kana_sign_amount") or {}).get(key) or {}).get("sample_total")
                or 0
            )
            first_applied_size = len(first_obj.get("applied_training_sets") or {})

            # Re-insert the same content with different filenames: should be detected as same pair_set and skipped.
            _write_training_pair(
                line_root,
                ocr_rows=ocr_rows,
                ref_rows=ref_rows,
                ocr_name="ocr_once_again.csv",
                ref_name="teacher_again.csv",
            )
            second_summary = ensure_bank_client_cache_updated(repo_root, client_id)
            self.assertEqual(0, int(second_summary.get("pairs_unique_used_total") or 0))
            self.assertEqual(1, len(second_summary.get("skipped_pair_set_ids") or []))

            second_obj = _load_json(cache_path)
            second_total = int(
                (((second_obj.get("stats") or {}).get("kana_sign_amount") or {}).get(key) or {}).get("sample_total")
                or 0
            )
            second_applied_size = len(second_obj.get("applied_training_sets") or {})

            self.assertEqual(first_total, 1)
            self.assertEqual(second_total, 1)
            self.assertEqual(first_applied_size, 1)
            self.assertEqual(second_applied_size, 1)
            self.assertEqual(0, len(list((line_root / "inputs" / "training" / "ocr_kari_shiwake").glob("*.csv"))))
            self.assertEqual(0, len(list((line_root / "inputs" / "training" / "reference_yayoi").glob("*.csv"))))

            ocr_manifest = _load_json(ocr_manifest_path)
            ref_manifest = _load_json(ref_manifest_path)
            ocr_ingested = ocr_manifest.get("ingested") or {}
            ref_ingested = ref_manifest.get("ingested") or {}
            self.assertEqual(1, len(ocr_ingested))
            self.assertEqual(1, len(ref_ingested))

            ocr_sha = next(iter(ocr_ingested.keys()))
            ref_sha = next(iter(ref_ingested.keys()))
            ocr_ignored = ocr_manifest.get("ignored_duplicates") or {}
            ref_ignored = ref_manifest.get("ignored_duplicates") or {}
            self.assertIn(ocr_sha, ocr_ignored)
            self.assertIn(ref_sha, ref_ignored)
            self.assertGreaterEqual(len(ocr_ignored.get(ocr_sha) or []), 1)
            self.assertGreaterEqual(len(ref_ignored.get(ref_sha) or []), 1)

            self.assertIn(ocr_sha, second_summary.get("ingested_duplicate_training_ocr_shas") or [])
            self.assertIn(ref_sha, second_summary.get("ingested_duplicate_training_reference_shas") or [])

    def test_skip_applied_pairset_recovers_from_manifest_asymmetry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C8"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_summary = "OCR_RECOVERY"
            ocr_rows = [
                _build_row(
                    date_text="2026/01/21",
                    summary=ocr_summary,
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=4400,
                    memo="SIGN=debit",
                ),
            ]
            ref_rows = [
                _build_row(
                    date_text="2026/01/21",
                    summary="TEACHER_RECOVERY",
                    debit_account="COUNTER_RECOVERY",
                    credit_account=BANK_ACCOUNT,
                    credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                    debit_tax_division="TAX_D10",
                    amount=4400,
                ),
            ]

            _write_training_pair(line_root, ocr_rows=ocr_rows, ref_rows=ref_rows, ocr_name="ocr_seed.csv", ref_name="ref_seed.csv")
            first_summary = ensure_bank_client_cache_updated(repo_root, client_id)
            pair_set_id = str(first_summary.get("applied_pair_set_ids")[0])

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            ocr_manifest_path = line_root / "artifacts" / "ingest" / "training_ocr_ingested.json"
            ref_manifest_path = line_root / "artifacts" / "ingest" / "training_reference_ingested.json"

            cache_before_obj = _load_json(cache_path)
            key = f"{normalize_kana_key(ocr_summary)}|debit|4400"
            stats_before = int(
                (
                    ((cache_before_obj.get("stats") or {}).get("kana_sign_amount") or {}).get(key) or {}
                ).get("sample_total")
                or 0
            )
            applied_size_before = len(cache_before_obj.get("applied_training_sets") or {})

            ocr_manifest_before = _load_json(ocr_manifest_path)
            ref_manifest_before = _load_json(ref_manifest_path)
            ocr_sha = next(iter((ocr_manifest_before.get("ingested") or {}).keys()))
            ref_sha = next(iter((ref_manifest_before.get("ingested") or {}).keys()))

            # Simulate asymmetric manifests: OCR side has sha, REF side lost sha.
            ref_ingested = dict(ref_manifest_before.get("ingested") or {})
            ref_order = list(ref_manifest_before.get("ingested_order") or [])
            ref_ingested.pop(ref_sha, None)
            ref_order = [v for v in ref_order if str(v) != ref_sha]
            ref_manifest_before["ingested"] = ref_ingested
            ref_manifest_before["ingested_order"] = ref_order
            ref_manifest_path.write_text(
                json.dumps(ref_manifest_before, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            _write_training_pair(
                line_root,
                ocr_rows=ocr_rows,
                ref_rows=ref_rows,
                ocr_name="ocr_reingest.csv",
                ref_name="ref_reingest.csv",
            )

            second_summary = ensure_bank_client_cache_updated(repo_root, client_id)

            self.assertEqual(0, int(second_summary.get("pairs_unique_used_total") or 0))
            self.assertIn(pair_set_id, second_summary.get("skipped_pair_set_ids") or [])

            self.assertEqual(0, len(list((line_root / "inputs" / "training" / "ocr_kari_shiwake").glob("*.csv"))))
            self.assertEqual(0, len(list((line_root / "inputs" / "training" / "reference_yayoi").glob("*.csv"))))

            cache_after_obj = _load_json(cache_path)
            stats_after = int(
                (
                    ((cache_after_obj.get("stats") or {}).get("kana_sign_amount") or {}).get(key) or {}
                ).get("sample_total")
                or 0
            )
            applied_size_after = len(cache_after_obj.get("applied_training_sets") or {})
            self.assertEqual(stats_before, stats_after)
            self.assertEqual(applied_size_before, applied_size_after)

            ocr_manifest_after = _load_json(ocr_manifest_path)
            ref_manifest_after = _load_json(ref_manifest_path)
            ocr_ignored = ocr_manifest_after.get("ignored_duplicates") or {}
            self.assertIn(ocr_sha, ocr_manifest_after.get("ingested") or {})
            self.assertIn(ocr_sha, ocr_ignored)
            self.assertGreaterEqual(len(ocr_ignored.get(ocr_sha) or []), 1)
            self.assertIn(ref_sha, ref_manifest_after.get("ingested") or {})

    def test_fail_when_manifests_known_but_pairset_not_applied(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C9"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_rows = [
                _build_row(
                    date_text="2026/01/22",
                    summary="OCR_INCONSISTENT",
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=5100,
                    memo="SIGN=debit",
                ),
            ]
            ref_rows = [
                _build_row(
                    date_text="2026/01/22",
                    summary="TEACHER_INCONSISTENT",
                    debit_account="COUNTER_INCONSISTENT",
                    credit_account=BANK_ACCOUNT,
                    credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                    debit_tax_division="TAX_D10",
                    amount=5100,
                ),
            ]

            _write_training_pair(
                line_root,
                ocr_rows=ocr_rows,
                ref_rows=ref_rows,
                ocr_name="ocr_inconsistent.csv",
                ref_name="ref_inconsistent.csv",
            )

            ocr_input_path = line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ocr_inconsistent.csv"
            ref_input_path = line_root / "inputs" / "training" / "reference_yayoi" / "ref_inconsistent.csv"
            ocr_sha = sha256_file(ocr_input_path)
            ref_sha = sha256_file(ref_input_path)
            pair_set_sha256 = hashlib.sha256(f"ocr={ocr_sha}|ref={ref_sha}".encode("utf-8")).hexdigest()
            self.assertEqual(64, len(pair_set_sha256))

            ocr_manifest_path = line_root / "artifacts" / "ingest" / "training_ocr_ingested.json"
            ref_manifest_path = line_root / "artifacts" / "ingest" / "training_reference_ingested.json"
            ocr_manifest_path.parent.mkdir(parents=True, exist_ok=True)
            ref_manifest_path.parent.mkdir(parents=True, exist_ok=True)

            ocr_manifest_obj = {
                "version": "1.0",
                "client_id": client_id,
                "kind": "training_ocr",
                "ingested_order": [ocr_sha],
                "ingested": {
                    ocr_sha: {
                        "sha256": ocr_sha,
                        "stored_name": "INGESTED_FAKE_OCR.csv",
                        "ingested_at": "2026-01-01T00:00:00+00:00",
                        "status": "ingested",
                    }
                },
                "ignored_duplicates": {},
            }
            ref_manifest_obj = {
                "version": "1.0",
                "client_id": client_id,
                "kind": "training_reference",
                "ingested_order": [ref_sha],
                "ingested": {
                    ref_sha: {
                        "sha256": ref_sha,
                        "stored_name": "INGESTED_FAKE_REF.csv",
                        "ingested_at": "2026-01-01T00:00:00+00:00",
                        "status": "ingested",
                    }
                },
                "ignored_duplicates": {},
            }
            ocr_manifest_path.write_text(json.dumps(ocr_manifest_obj, ensure_ascii=False, indent=2), encoding="utf-8")
            ref_manifest_path.write_text(json.dumps(ref_manifest_obj, ensure_ascii=False, indent=2), encoding="utf-8")

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            self.assertFalse(cache_path.exists())

            ocr_manifest_before = ocr_manifest_path.read_text(encoding="utf-8")
            ref_manifest_before = ref_manifest_path.read_text(encoding="utf-8")

            with self.assertRaises(SystemExit) as ctx:
                ensure_bank_client_cache_updated(repo_root, client_id)
            self.assertIn("inconsistent", str(ctx.exception))

            self.assertEqual(1, len(list((line_root / "inputs" / "training" / "ocr_kari_shiwake").glob("*.csv"))))
            self.assertEqual(1, len(list((line_root / "inputs" / "training" / "reference_yayoi").glob("*.csv"))))
            self.assertFalse(cache_path.exists())
            self.assertEqual(ocr_manifest_before, ocr_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(ref_manifest_before, ref_manifest_path.read_text(encoding="utf-8"))

    def test_noop_when_no_training_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C6"
            line_root = _prepare_bank_layout(repo_root, client_id)

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            ocr_manifest_path = line_root / "artifacts" / "ingest" / "training_ocr_ingested.json"
            ref_manifest_path = line_root / "artifacts" / "ingest" / "training_reference_ingested.json"
            self.assertFalse(cache_path.exists())
            self.assertFalse(ocr_manifest_path.exists())
            self.assertFalse(ref_manifest_path.exists())

            summary = ensure_bank_client_cache_updated(repo_root, client_id)
            self.assertEqual("none", summary.get("training_input_state"))
            self.assertEqual(0, int(summary.get("training_ocr_input_count", -1)))
            self.assertEqual(0, int(summary.get("training_reference_input_count", -1)))

            self.assertFalse(cache_path.exists())
            self.assertFalse(ocr_manifest_path.exists())
            self.assertFalse(ref_manifest_path.exists())

    def test_multiple_pair_sets_accumulate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C4"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_summary = "OCR_ACCUM"
            _write_training_pair(
                line_root,
                ocr_rows=[
                    _build_row(
                        date_text="2026/01/01",
                        summary=ocr_summary,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=1100,
                        memo="SIGN=debit",
                    ),
                ],
                ref_rows=[
                    _build_row(
                        date_text="2026/01/01",
                        summary="TEACHER_ACCUM",
                        debit_account="COUNTER_ACCUM",
                        credit_account=BANK_ACCOUNT,
                        credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                        debit_tax_division="TAX_D10",
                        amount=1100,
                    ),
                ],
                ocr_name="ocr_pair_1.csv",
                ref_name="teacher_pair_1.csv",
            )
            ensure_bank_client_cache_updated(repo_root, client_id)

            _write_training_pair(
                line_root,
                ocr_rows=[
                    _build_row(
                        date_text="2026/01/02",
                        summary=ocr_summary,
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=1100,
                        memo="SIGN=debit",
                    ),
                ],
                ref_rows=[
                    _build_row(
                        date_text="2026/01/02",
                        summary="TEACHER_ACCUM",
                        debit_account="COUNTER_ACCUM",
                        credit_account=BANK_ACCOUNT,
                        credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                        debit_tax_division="TAX_D10",
                        amount=1100,
                    ),
                ],
                ocr_name="ocr_pair_2.csv",
                ref_name="teacher_pair_2.csv",
            )
            ensure_bank_client_cache_updated(repo_root, client_id)

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            cache_obj = _load_json(cache_path)
            applied = cache_obj.get("applied_training_sets") or {}
            self.assertEqual(2, len(applied))

            key = f"{normalize_kana_key(ocr_summary)}|debit|1100"
            stats = ((cache_obj.get("stats") or {}).get("kana_sign_amount") or {})
            self.assertEqual(2, int((stats.get(key) or {}).get("sample_total") or 0))

    def test_fail_when_only_one_side_new(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C5"
            line_root = _prepare_bank_layout(repo_root, client_id)

            first_ocr_rows = [
                _build_row(
                    date_text="2026/01/11",
                    summary="OCR_FIRST",
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=2100,
                    memo="SIGN=debit",
                )
            ]
            first_ref_rows = [
                _build_row(
                    date_text="2026/01/11",
                    summary="TEACHER_FIRST",
                    debit_account="COUNTER_FIRST",
                    credit_account=BANK_ACCOUNT,
                    credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                    debit_tax_division="TAX_D10",
                    amount=2100,
                )
            ]

            _write_training_pair(line_root, ocr_rows=first_ocr_rows, ref_rows=first_ref_rows)
            ensure_bank_client_cache_updated(repo_root, client_id)

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            cache_before = cache_path.read_text(encoding="utf-8")

            ocr_manifest_path = line_root / "artifacts" / "ingest" / "training_ocr_ingested.json"
            ref_manifest_path = line_root / "artifacts" / "ingest" / "training_reference_ingested.json"
            self.assertEqual(1, _manifest_ingested_count(ocr_manifest_path))
            self.assertEqual(1, _manifest_ingested_count(ref_manifest_path))

            # OCR is new, reference is identical content as first run -> one-side-new must fail.
            _write_training_pair(
                line_root,
                ocr_rows=[
                    _build_row(
                        date_text="2026/01/12",
                        summary="OCR_SECOND_NEW",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=BANK_ACCOUNT,
                        amount=2200,
                        memo="SIGN=debit",
                    )
                ],
                ref_rows=first_ref_rows,
                ocr_name="ocr_second.csv",
                ref_name="teacher_same_content.csv",
            )

            with self.assertRaises(SystemExit):
                ensure_bank_client_cache_updated(repo_root, client_id)

            cache_after = cache_path.read_text(encoding="utf-8")
            self.assertEqual(cache_before, cache_after)
            self.assertEqual(1, _manifest_ingested_count(ocr_manifest_path))
            self.assertEqual(1, _manifest_ingested_count(ref_manifest_path))

            self.assertEqual(1, len(list((line_root / "inputs" / "training" / "ocr_kari_shiwake").glob("*.csv"))))
            self.assertEqual(1, len(list((line_root / "inputs" / "training" / "reference_yayoi").glob("*.csv"))))

    def test_fail_when_multiple_training_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C7"
            line_root = _prepare_bank_layout(repo_root, client_id)

            ocr_rows_1 = [
                _build_row(
                    date_text="2026/01/13",
                    summary="OCR_MULTI_1",
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=1500,
                    memo="SIGN=debit",
                )
            ]
            ocr_rows_2 = [
                _build_row(
                    date_text="2026/01/14",
                    summary="OCR_MULTI_2",
                    debit_account=PLACEHOLDER_ACCOUNT,
                    credit_account=BANK_ACCOUNT,
                    amount=1600,
                    memo="SIGN=debit",
                )
            ]
            ref_rows = [
                _build_row(
                    date_text="2026/01/13",
                    summary="TEACHER_MULTI",
                    debit_account="COUNTER_MULTI",
                    credit_account=BANK_ACCOUNT,
                    credit_subaccount=LEARNED_BANK_SUBACCOUNT,
                    debit_tax_division="TAX_D10",
                    amount=1500,
                )
            ]

            _write_training_pair(
                line_root,
                ocr_rows=ocr_rows_1,
                ref_rows=ref_rows,
                ocr_name="ocr_multi_1.csv",
                ref_name="teacher_multi.csv",
            )
            _write_yayoi_rows(
                line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ocr_multi_2.csv",
                ocr_rows_2,
            )

            with self.assertRaises(SystemExit):
                ensure_bank_client_cache_updated(repo_root, client_id)

            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            ocr_manifest_path = line_root / "artifacts" / "ingest" / "training_ocr_ingested.json"
            ref_manifest_path = line_root / "artifacts" / "ingest" / "training_reference_ingested.json"
            self.assertFalse(cache_path.exists())
            self.assertFalse(ocr_manifest_path.exists())
            self.assertFalse(ref_manifest_path.exists())

            ocr_inbox = line_root / "inputs" / "training" / "ocr_kari_shiwake"
            ref_inbox = line_root / "inputs" / "training" / "reference_yayoi"
            self.assertEqual(2, len(list(ocr_inbox.glob("*.csv"))))
            self.assertEqual(1, len(list(ref_inbox.glob("*.csv"))))


if __name__ == "__main__":
    unittest.main()
