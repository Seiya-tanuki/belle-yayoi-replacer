from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from belle.lexicon import load_lexicon
from belle.lexicon_manager import ensure_lexicon_candidates_updated_from_ledger_ref


def _write_minimal_lexicon(lexicon_path: Path) -> None:
    lexicon_path.parent.mkdir(parents=True, exist_ok=True)
    obj = {
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
    }
    lexicon_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_yayoi_row(path: Path, *, summary: str, debit: str = "旅費交通費") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [""] * 25
    cols[4] = debit
    cols[16] = summary
    cols[21] = "memo-not-used"
    path.write_text(",".join(cols) + "\n", encoding="utf-8")


def _read_queue_count(queue_csv: Path, norm_key: str) -> int:
    if not queue_csv.exists():
        return 0
    with queue_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("norm_key") or "") == norm_key:
                return int(row.get("count_total") or 0)
    return 0


class LexiconAutogrowIdempotencyTests(unittest.TestCase):
    def test_same_ingested_sha_is_not_double_counted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            ledger_ref_file = repo_root / "clients" / client_id / "inputs" / "ledger_ref" / "batch1.csv"
            _write_yayoi_row(ledger_ref_file, summary="ACME SHOP / test")
            _write_minimal_lexicon(repo_root / "lexicon" / "lexicon.json")

            lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")
            config = {"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}}

            first = ensure_lexicon_candidates_updated_from_ledger_ref(
                repo_root=repo_root,
                client_id=client_id,
                lex=lex,
                config=config,
                ingest_inputs=True,
                lock_timeout_sec=5,
                lock_stale_sec=5,
            )
            self.assertEqual(first.processed_files, 1)

            queue_csv = repo_root / "lexicon" / "pending" / "label_queue.csv"
            first_count = _read_queue_count(queue_csv, "ACMESHOP")
            self.assertEqual(first_count, 1)

            second = ensure_lexicon_candidates_updated_from_ledger_ref(
                repo_root=repo_root,
                client_id=client_id,
                lex=lex,
                config=config,
                ingest_inputs=True,
                lock_timeout_sec=5,
                lock_stale_sec=5,
            )
            self.assertEqual(second.processed_files, 0)
            second_count = _read_queue_count(queue_csv, "ACMESHOP")
            self.assertEqual(second_count, first_count)

            manifest_path = repo_root / "clients" / client_id / "artifacts" / "ingest" / "ledger_ref_ingested.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            ingested_entries = manifest.get("ingested") or {}
            self.assertEqual(len(ingested_entries), 1)
            entry = next(iter(ingested_entries.values()))
            self.assertTrue(entry.get("processed_to_label_queue_at"))


class YayoiReplacerFailClosedTests(unittest.TestCase):
    def test_lock_timeout_prevents_run_dir_creation(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        client_id = f"TEST_FAIL_CLOSED_{uuid4().hex[:8]}"
        client_dir = repo_root / "clients" / client_id
        lock_path = repo_root / "lexicon" / "pending" / "locks" / "label_queue.lock"
        lock_backup: Path | None = None

        try:
            (client_dir / "config").mkdir(parents=True, exist_ok=True)
            _write_yayoi_row(client_dir / "inputs" / "ledger_ref" / "batch1.csv", summary="LOCK TEST SHOP / row")
            _write_yayoi_row(client_dir / "inputs" / "kari_shiwake" / "target.csv", summary="dummy")

            lock_path.parent.mkdir(parents=True, exist_ok=True)
            if lock_path.exists():
                lock_backup = lock_path.with_name(f"{lock_path.name}.bak.{uuid4().hex[:8]}")
                lock_path.rename(lock_backup)
            lock_path.write_text(json.dumps({"owner_id": "unit-test"}), encoding="utf-8")

            env = os.environ.copy()
            env["BELLE_LABEL_QUEUE_LOCK_TIMEOUT_SEC"] = "1"
            env["BELLE_LABEL_QUEUE_LOCK_STALE_SEC"] = "120"

            proc = subprocess.run(
                [
                    sys.executable,
                    str(repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"),
                    "--client",
                    client_id,
                ],
                cwd=repo_root,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )

            self.assertNotEqual(proc.returncode, 0, msg=proc.stdout + "\n" + proc.stderr)
            self.assertIn("label_queue 自動更新に失敗しました", proc.stdout + proc.stderr)

            runs_dir = client_dir / "outputs" / "runs"
            run_dirs = [p for p in runs_dir.iterdir() if p.is_dir()] if runs_dir.exists() else []
            self.assertEqual(run_dirs, [], msg=proc.stdout + "\n" + proc.stderr)
            self.assertFalse((client_dir / "outputs" / "LATEST.txt").exists())
        finally:
            shutil.rmtree(client_dir, ignore_errors=True)
            if lock_path.exists():
                try:
                    lock_path.unlink()
                except OSError:
                    pass
            if lock_backup and lock_backup.exists():
                lock_backup.rename(lock_path)


if __name__ == "__main__":
    unittest.main()
