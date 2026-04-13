from __future__ import annotations

import csv
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

from belle.ingest import ingest_csv_dir
from belle.lexicon import load_lexicon
from belle.lexicon_manager import (
    LABEL_QUEUE_COLUMNS,
    _is_stale_lock,
    acquire_label_queue_lock,
    ensure_lexicon_candidates_updated_from_ledger_ref,
    release_label_queue_lock,
    save_label_queue_state,
    write_label_queue,
)


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


def _write_receipt_line_config(config_path: Path) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(
            {
                "schema": "belle.replacer_config.v1",
                "version": "1.16",
                "csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_yayoi_row(path: Path, *, summary: str, debit: str = "旅費交通費") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [""] * 25
    cols[4] = debit
    cols[16] = summary
    cols[21] = "memo-not-used"
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


def _ingest_one_ledger_ref(repo_root: Path, *, client_id: str, summary: str) -> Path:
    client_dir = repo_root / "clients" / client_id / "lines" / "receipt"
    ledger_ref_dir = client_dir / "inputs" / "ledger_ref"
    ledger_ref_store_dir = client_dir / "artifacts" / "ingest" / "ledger_ref"
    _write_yayoi_row(ledger_ref_dir / "batch1.csv", summary=summary)
    manifest_path = client_dir / "artifacts" / "ingest" / "ledger_ref_ingested.json"
    ingest_csv_dir(
        dir_path=ledger_ref_dir,
        store_dir=ledger_ref_store_dir,
        manifest_path=manifest_path,
        client_id=client_id,
        kind="ledger_ref",
        allow_rename=True,
        include_glob="*.csv",
        relpath_base_dir=client_dir,
    )
    return manifest_path


def _build_queue_row(norm_key: str, *, count_total: int) -> dict[str, str]:
    row = {k: "" for k in LABEL_QUEUE_COLUMNS}
    row["norm_key"] = norm_key
    row["raw_example"] = f"{norm_key}_RAW"
    row["example_summary"] = f"{norm_key}_SUMMARY"
    row["count_total"] = str(count_total)
    row["clients_seen"] = "1"
    row["first_seen_at"] = "2026-01-01T00:00:00+00:00"
    row["last_seen_at"] = "2026-01-01T00:00:00+00:00"
    return row


class LexiconAutogrowIdempotencyTests(unittest.TestCase):
    def test_same_ingested_sha_is_not_double_counted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            ledger_ref_file = repo_root / "clients" / client_id / "lines" / "receipt" / "inputs" / "ledger_ref" / "batch1.csv"
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
                line_id="receipt",
                client_line_id="receipt",
            )
            self.assertEqual(first.processed_files, 1)

            queue_csv = repo_root / "lexicon" / "receipt" / "pending" / "label_queue.csv"
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
                line_id="receipt",
                client_line_id="receipt",
            )
            self.assertEqual(second.processed_files, 0)
            second_count = _read_queue_count(queue_csv, "ACMESHOP")
            self.assertEqual(second_count, first_count)

            manifest_path = repo_root / "clients" / client_id / "lines" / "receipt" / "artifacts" / "ingest" / "ledger_ref_ingested.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            ingested_entries = manifest.get("ingested") or {}
            self.assertEqual(len(ingested_entries), 1)
            entry = next(iter(ingested_entries.values()))
            self.assertTrue(entry.get("processed_to_label_queue_at"))

    def test_atomic_replace_failure_keeps_queue_and_state_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            _write_minimal_lexicon(repo_root / "lexicon" / "lexicon.json")
            _ingest_one_ledger_ref(repo_root, client_id=client_id, summary="FAILURE SHOP / test")

            queue_csv = repo_root / "lexicon" / "receipt" / "pending" / "label_queue.csv"
            queue_state = repo_root / "lexicon" / "receipt" / "pending" / "label_queue_state.json"
            write_label_queue(queue_csv, {"BASEKEY": _build_queue_row("BASEKEY", count_total=7)})
            save_label_queue_state(
                queue_state,
                {
                    "version": "1.0",
                    "clients_by_norm_key": {
                        "BASEKEY": {
                            client_id: {"count_total": 7, "last_seen_at": "2026-01-01T00:00:00+00:00"}
                        }
                    },
                },
            )
            queue_before = queue_csv.read_bytes()
            state_before = queue_state.read_bytes()

            lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")
            config = {"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}}
            real_replace = os.replace

            def _replace_fail_on_queue(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
                if Path(dst).resolve() == queue_csv.resolve():
                    raise OSError("simulated queue replace failure")
                real_replace(src, dst)

            with mock.patch("belle.io_atomic.os.replace", side_effect=_replace_fail_on_queue):
                with self.assertRaises(OSError):
                    ensure_lexicon_candidates_updated_from_ledger_ref(
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

            self.assertEqual(queue_before, queue_csv.read_bytes())
            self.assertEqual(state_before, queue_state.read_bytes())
            self.assertEqual(list(queue_csv.parent.glob("label_queue.csv.tmp.*")), [])
            self.assertEqual(list(queue_state.parent.glob("label_queue_state.json.tmp.*")), [])

    def test_processed_marker_not_set_if_commit_fails_before_queue_state_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C1"
            _write_minimal_lexicon(repo_root / "lexicon" / "lexicon.json")
            manifest_path = _ingest_one_ledger_ref(repo_root, client_id=client_id, summary="MARKER SHOP / test")

            lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")
            config = {"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}}

            with mock.patch(
                "belle.lexicon_manager._merge_terms_into_queue",
                side_effect=RuntimeError("simulated merge failure"),
            ):
                with self.assertRaises(RuntimeError):
                    ensure_lexicon_candidates_updated_from_ledger_ref(
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

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            ingested_entries = manifest.get("ingested") or {}
            self.assertEqual(len(ingested_entries), 1)
            entry = next(iter(ingested_entries.values()))
            self.assertNotIn("processed_to_label_queue_at", entry)
            self.assertNotIn("processed_to_label_queue_run_id", entry)
            self.assertNotIn("processed_to_label_queue_version", entry)


class LabelQueueLockHeartbeatTests(unittest.TestCase):
    def test_heartbeat_updates_mtime_and_prevents_stale_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_path = Path(td) / "locks" / "label_queue.lock"
            token = acquire_label_queue_lock(
                lock_path=lock_path,
                client_id="heartbeat-test",
                timeout_sec=1,
                stale_after_sec=1,
            )
            try:
                stale_mtime = time.time() - 10
                os.utime(lock_path, (stale_mtime, stale_mtime))
                self.assertTrue(_is_stale_lock(lock_path, 1))

                before = lock_path.stat().st_mtime
                touched = token.heartbeat(now_mono=token.last_heartbeat_mono + 31)
                after = lock_path.stat().st_mtime

                self.assertTrue(touched)
                self.assertGreater(after, before)
                self.assertFalse(_is_stale_lock(lock_path, 1))
            finally:
                release_label_queue_lock(token)


class LexiconApplyExitCodeTests(unittest.TestCase):
    def test_exit_code_helper_returns_nonzero_when_errors_exist(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        script_path = repo_root / ".agents" / "skills" / "lexicon-apply" / "scripts" / "run_lexicon_apply.py"
        spec = importlib.util.spec_from_file_location("run_lexicon_apply_script", script_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        spec.loader.exec_module(module)  # type: ignore[union-attr]

        self.assertEqual(module.exit_code_from_summary_errors([]), 0)
        self.assertEqual(module.exit_code_from_summary_errors(["x"]), 1)


class YayoiReplacerFailClosedTests(unittest.TestCase):
    def test_lock_timeout_prevents_run_dir_creation(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        client_id = f"TEST_FAIL_CLOSED_{uuid4().hex[:8]}"
        client_dir = repo_root / "clients" / client_id / "lines" / "receipt"
        lock_path = repo_root / "lexicon" / "receipt" / "pending" / "locks" / "label_queue.lock"
        lock_backup: Path | None = None

        try:
            (client_dir / "config").mkdir(parents=True, exist_ok=True)
            _write_receipt_line_config(client_dir / "config" / "receipt_line_config.json")
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
                    "--line",
                    "receipt",
                    "--yes",
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
