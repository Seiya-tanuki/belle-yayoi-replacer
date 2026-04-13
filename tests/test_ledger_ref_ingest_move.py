from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.build_client_cache import ensure_client_cache_updated
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


def _write_yayoi_row(path: Path, *, summary: str, debit: str = "DEBIT_ACCOUNT") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [""] * 25
    cols[4] = debit
    cols[16] = summary
    path.write_text(",".join(cols) + "\n", encoding="cp932")


def _read_queue_count(queue_csv: Path, norm_key: str) -> int:
    if not queue_csv.exists():
        return 0
    with queue_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("norm_key") or "") == norm_key:
                return int(row.get("count_total") or 0)
    return 0


class LedgerRefIngestMoveTests(unittest.TestCase):
    def test_ingest_moves_input_and_manifest_points_to_artifacts_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            client_dir = repo_root / "clients" / client_id / "lines" / "receipt"
            inbox = client_dir / "inputs" / "ledger_ref"
            _write_yayoi_row(inbox / "batch1.csv", summary="MOVE CHECK SHOP / one")
            _write_minimal_lexicon(repo_root / "lexicon" / "lexicon.json")

            lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")
            config = {"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}}
            tm, summary = ensure_client_cache_updated(
                repo_root=repo_root,
                client_id=client_id,
                lex=lex,
                config=config,
                line_id="receipt",
            )

            remaining = [p for p in inbox.iterdir() if p.is_file() and p.name != ".gitkeep"]
            self.assertEqual(remaining, [])
            self.assertEqual(summary.rows_total_added, 1)
            self.assertEqual(len(summary.applied_new_files), 1)

            manifest_path = client_dir / "artifacts" / "ingest" / "ledger_ref_ingested.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            ingested = manifest.get("ingested") or {}
            self.assertEqual(len(ingested), 1)
            entry = next(iter(ingested.values()))
            self.assertTrue(str(entry.get("stored_name") or "").startswith("INGESTED_"))
            stored_relpath = str(entry.get("stored_relpath") or "")
            self.assertTrue(stored_relpath.startswith("artifacts/ingest/ledger_ref/"))
            self.assertTrue((client_dir / Path(stored_relpath)).exists())

            applied_entry = next(iter((tm.applied_ledger_ref_sha256 or {}).values()))
            self.assertEqual(applied_entry.get("stored_relpath"), stored_relpath)

    def test_duplicate_sha_is_moved_to_ignored_duplicate_under_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            client_dir = repo_root / "clients" / client_id / "lines" / "receipt"
            inbox = client_dir / "inputs" / "ledger_ref"
            _write_minimal_lexicon(repo_root / "lexicon" / "lexicon.json")
            lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")
            config = {"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}}

            _write_yayoi_row(inbox / "first.csv", summary="DUP CHECK SHOP / same")
            ensure_client_cache_updated(repo_root=repo_root, client_id=client_id, lex=lex, config=config, line_id="receipt")

            _write_yayoi_row(inbox / "second.csv", summary="DUP CHECK SHOP / same")
            ensure_client_cache_updated(repo_root=repo_root, client_id=client_id, lex=lex, config=config, line_id="receipt")

            remaining = [p for p in inbox.iterdir() if p.is_file() and p.name != ".gitkeep"]
            self.assertEqual(remaining, [])

            manifest_path = client_dir / "artifacts" / "ingest" / "ledger_ref_ingested.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            ignored = manifest.get("ignored_duplicates") or {}
            self.assertEqual(len(ignored), 1)
            dup_entry = next(iter(ignored.values()))[0]
            self.assertTrue(str(dup_entry.get("stored_name") or "").startswith("IGNORED_DUPLICATE_"))
            dup_relpath = str(dup_entry.get("stored_relpath") or "")
            self.assertTrue(dup_relpath.startswith("artifacts/ingest/ledger_ref/IGNORED_DUPLICATE_"))
            self.assertTrue((client_dir / Path(dup_relpath)).exists())

    def test_autogrow_reads_ingested_file_via_manifest_relpath(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            client_dir = repo_root / "clients" / client_id / "lines" / "receipt"
            inbox = client_dir / "inputs" / "ledger_ref"
            _write_yayoi_row(inbox / "batch1.csv", summary="AUTOGROW SHOP / x")
            _write_minimal_lexicon(repo_root / "lexicon" / "lexicon.json")

            lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")
            config = {"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}}

            ensure_client_cache_updated(
                repo_root=repo_root,
                client_id=client_id,
                lex=lex,
                config=config,
                line_id="receipt",
            )
            autogrow = ensure_lexicon_candidates_updated_from_ledger_ref(
                repo_root=repo_root,
                client_id=client_id,
                lex=lex,
                config=config,
                ingest_inputs=False,
                lock_timeout_sec=5,
                lock_stale_sec=5,
                line_id="receipt",
                client_line_id="receipt",
            )
            self.assertEqual(autogrow.processed_files, 1)
            self.assertFalse(any("missing_ingested_file" in w for w in autogrow.warnings))

            queue_csv = repo_root / "lexicon" / "receipt" / "pending" / "label_queue.csv"
            self.assertEqual(_read_queue_count(queue_csv, "AUTOGROWSHOP"), 1)


if __name__ == "__main__":
    unittest.main()
