from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from belle.lexicon_manager import LABEL_QUEUE_COLUMNS


def _copy_tree(src_root: Path, dst_root: Path, rel_path: str) -> None:
    src = src_root / rel_path
    dst = dst_root / rel_path
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    shutil.copytree(src, dst, ignore=ignore)


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
            },
            {
                "id": 2,
                "key": "other",
                "label": "Other",
                "kind": "expense",
                "precision_hint": 0.9,
                "deprecated": False,
                "negative_terms": {"n0": [], "n1": []},
            },
        ],
        "term_rows": [["n0", "KNOWNSTORE", 1, 1.0, "S"]],
        "term_buckets_prefix2": {"KN": [0]},
        "learned": {"policy": {"core_weight": 1.0}, "provenance_registry": []},
    }
    lexicon_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_queue_row(
    norm_key: str,
    *,
    user_category_key: str = "",
    action: str = "ADD",
    count_total: str = "1",
) -> dict[str, str]:
    row = {k: "" for k in LABEL_QUEUE_COLUMNS}
    row["norm_key"] = norm_key
    row["raw_example"] = f"{norm_key}_RAW"
    row["example_summary"] = f"{norm_key}_SUMMARY"
    row["count_total"] = count_total
    row["clients_seen"] = "1"
    row["first_seen_at"] = "2026-01-01T00:00:00+00:00"
    row["last_seen_at"] = "2026-01-01T00:00:00+00:00"
    row["user_category_key"] = user_category_key
    row["action"] = action
    return row


def _write_queue_csv(
    queue_csv: Path,
    rows: list[dict[str, str]],
    *,
    fieldnames: list[str] | None = None,
) -> None:
    queue_csv.parent.mkdir(parents=True, exist_ok=True)
    with queue_csv.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames or LABEL_QUEUE_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_queue_state(queue_state: Path, norm_keys: list[str]) -> None:
    queue_state.parent.mkdir(parents=True, exist_ok=True)
    obj = {
        "version": "1.0",
        "clients_by_norm_key": {
            norm_key: {"CLIENT1": {"count_total": 1, "last_seen_at": "2026-01-01T00:00:00+00:00"}}
            for norm_key in norm_keys
        },
    }
    queue_state.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_queue_rows(queue_csv: Path) -> list[dict[str, str]]:
    with queue_csv.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


class LexiconApplyFailClosedTests(unittest.TestCase):
    def _create_temp_repo(self) -> Path:
        source_root = Path(__file__).resolve().parents[1]
        temp_root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, temp_root, True)
        _copy_tree(source_root, temp_root, "belle")
        _copy_tree(source_root, temp_root, ".agents/skills/lexicon-apply")
        _write_minimal_lexicon(temp_root / "lexicon" / "lexicon.json")
        (temp_root / "lexicon" / "receipt" / "pending" / "locks").mkdir(parents=True, exist_ok=True)
        return temp_root

    def _run_apply(self, repo_root: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(repo_root / ".agents" / "skills" / "lexicon-apply" / "scripts" / "run_lexicon_apply.py"),
                "--line",
                "receipt",
            ],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def _assert_fail_closed(
        self,
        *,
        proc: subprocess.CompletedProcess[str],
        lexicon_path: Path,
        queue_csv: Path,
        queue_state: Path,
        lexicon_before: bytes,
        queue_before: bytes,
        state_before: bytes,
        expected_error: str,
        pending_dir: Path,
    ) -> None:
        combined = proc.stdout + proc.stderr
        self.assertNotEqual(0, proc.returncode, msg=combined)
        self.assertIn(expected_error, combined, msg=combined)
        self.assertEqual(lexicon_before, lexicon_path.read_bytes())
        self.assertEqual(queue_before, queue_csv.read_bytes())
        self.assertEqual(state_before, queue_state.read_bytes())
        self.assertFalse((pending_dir / "applied_log.jsonl").exists(), msg=combined)

    def test_mixed_batch_unknown_category_fails_closed(self) -> None:
        repo_root = self._create_temp_repo()
        pending_dir = repo_root / "lexicon" / "receipt" / "pending"
        lexicon_path = repo_root / "lexicon" / "lexicon.json"
        queue_csv = pending_dir / "label_queue.csv"
        queue_state = pending_dir / "label_queue_state.json"
        rows = [
            _build_queue_row("VALIDSHOP", user_category_key="known", action="ADD"),
            _build_queue_row("INVALIDSHOP", user_category_key="does_not_exist", action="ADD"),
        ]
        _write_queue_csv(queue_csv, rows)
        _write_queue_state(queue_state, ["VALIDSHOP", "INVALIDSHOP"])
        lexicon_before = lexicon_path.read_bytes()
        queue_before = queue_csv.read_bytes()
        state_before = queue_state.read_bytes()

        proc = self._run_apply(repo_root)

        self._assert_fail_closed(
            proc=proc,
            lexicon_path=lexicon_path,
            queue_csv=queue_csv,
            queue_state=queue_state,
            lexicon_before=lexicon_before,
            queue_before=queue_before,
            state_before=state_before,
            expected_error="unknown_category_key",
            pending_dir=pending_dir,
        )

    def test_mixed_batch_missing_user_category_fails_closed(self) -> None:
        repo_root = self._create_temp_repo()
        pending_dir = repo_root / "lexicon" / "receipt" / "pending"
        lexicon_path = repo_root / "lexicon" / "lexicon.json"
        queue_csv = pending_dir / "label_queue.csv"
        queue_state = pending_dir / "label_queue_state.json"
        rows = [
            _build_queue_row("VALIDSHOP", user_category_key="known", action="ADD"),
            _build_queue_row("MISSINGCATSHOP", user_category_key="", action="ADD"),
        ]
        _write_queue_csv(queue_csv, rows)
        _write_queue_state(queue_state, ["VALIDSHOP", "MISSINGCATSHOP"])
        lexicon_before = lexicon_path.read_bytes()
        queue_before = queue_csv.read_bytes()
        state_before = queue_state.read_bytes()

        proc = self._run_apply(repo_root)

        self._assert_fail_closed(
            proc=proc,
            lexicon_path=lexicon_path,
            queue_csv=queue_csv,
            queue_state=queue_state,
            lexicon_before=lexicon_before,
            queue_before=queue_before,
            state_before=state_before,
            expected_error="missing_user_category_key",
            pending_dir=pending_dir,
        )

    def test_mixed_batch_conflicting_existing_category_fails_closed(self) -> None:
        repo_root = self._create_temp_repo()
        pending_dir = repo_root / "lexicon" / "receipt" / "pending"
        lexicon_path = repo_root / "lexicon" / "lexicon.json"
        queue_csv = pending_dir / "label_queue.csv"
        queue_state = pending_dir / "label_queue_state.json"
        rows = [
            _build_queue_row("VALIDSHOP", user_category_key="known", action="ADD"),
            _build_queue_row("KNOWNSTORE", user_category_key="other", action="ADD"),
        ]
        _write_queue_csv(queue_csv, rows)
        _write_queue_state(queue_state, ["VALIDSHOP", "KNOWNSTORE"])
        lexicon_before = lexicon_path.read_bytes()
        queue_before = queue_csv.read_bytes()
        state_before = queue_state.read_bytes()

        proc = self._run_apply(repo_root)

        self._assert_fail_closed(
            proc=proc,
            lexicon_path=lexicon_path,
            queue_csv=queue_csv,
            queue_state=queue_state,
            lexicon_before=lexicon_before,
            queue_before=queue_before,
            state_before=state_before,
            expected_error="conflict_existing_category",
            pending_dir=pending_dir,
        )

    def test_missing_required_queue_columns_fails_closed(self) -> None:
        repo_root = self._create_temp_repo()
        pending_dir = repo_root / "lexicon" / "receipt" / "pending"
        lexicon_path = repo_root / "lexicon" / "lexicon.json"
        queue_csv = pending_dir / "label_queue.csv"
        queue_state = pending_dir / "label_queue_state.json"
        fieldnames = [col for col in LABEL_QUEUE_COLUMNS if col != "action"]
        rows = [
            _build_queue_row("VALIDSHOP", user_category_key="known", action="ADD"),
            _build_queue_row("OTHERSHOP", user_category_key="other", action="ADD"),
        ]
        _write_queue_csv(queue_csv, rows, fieldnames=fieldnames)
        _write_queue_state(queue_state, ["VALIDSHOP", "OTHERSHOP"])
        lexicon_before = lexicon_path.read_bytes()
        queue_before = queue_csv.read_bytes()
        state_before = queue_state.read_bytes()

        proc = self._run_apply(repo_root)

        self._assert_fail_closed(
            proc=proc,
            lexicon_path=lexicon_path,
            queue_csv=queue_csv,
            queue_state=queue_state,
            lexicon_before=lexicon_before,
            queue_before=queue_before,
            state_before=state_before,
            expected_error="missing_queue_columns",
            pending_dir=pending_dir,
        )

    def test_unknown_nonempty_action_fails_closed(self) -> None:
        repo_root = self._create_temp_repo()
        pending_dir = repo_root / "lexicon" / "receipt" / "pending"
        lexicon_path = repo_root / "lexicon" / "lexicon.json"
        queue_csv = pending_dir / "label_queue.csv"
        queue_state = pending_dir / "label_queue_state.json"
        rows = [
            _build_queue_row("VALIDSHOP", user_category_key="known", action="ADD"),
            _build_queue_row("TYPOACTIONSHOP", user_category_key="other", action="ADDD"),
        ]
        _write_queue_csv(queue_csv, rows)
        _write_queue_state(queue_state, ["VALIDSHOP", "TYPOACTIONSHOP"])
        lexicon_before = lexicon_path.read_bytes()
        queue_before = queue_csv.read_bytes()
        state_before = queue_state.read_bytes()

        proc = self._run_apply(repo_root)

        self._assert_fail_closed(
            proc=proc,
            lexicon_path=lexicon_path,
            queue_csv=queue_csv,
            queue_state=queue_state,
            lexicon_before=lexicon_before,
            queue_before=queue_before,
            state_before=state_before,
            expected_error="unsupported_action",
            pending_dir=pending_dir,
        )

    def test_fully_valid_batch_applies_adds_and_keeps_supported_nonapply_rows(self) -> None:
        repo_root = self._create_temp_repo()
        pending_dir = repo_root / "lexicon" / "receipt" / "pending"
        lexicon_path = repo_root / "lexicon" / "lexicon.json"
        queue_csv = pending_dir / "label_queue.csv"
        queue_state = pending_dir / "label_queue_state.json"
        rows = [
            _build_queue_row("NEWSHOP", user_category_key="other", action="ADD", count_total="3"),
            _build_queue_row("KNOWNSTORE", user_category_key="known", action="ADD", count_total="2"),
            _build_queue_row("KEEPHOLD", user_category_key="", action="HOLD", count_total="1"),
        ]
        _write_queue_csv(queue_csv, rows)
        _write_queue_state(queue_state, ["NEWSHOP", "KNOWNSTORE", "KEEPHOLD"])

        proc = self._run_apply(repo_root)
        combined = proc.stdout + proc.stderr
        self.assertEqual(0, proc.returncode, msg=combined)
        self.assertIn("[OK] added=1 skipped=1 removed_from_queue=2", combined, msg=combined)

        lexicon_obj = json.loads(lexicon_path.read_text(encoding="utf-8"))
        self.assertIn(["n0", "NEWSHOP", 2, 0.85, "S"], lexicon_obj.get("term_rows") or [])
        learned = (lexicon_obj.get("learned") or {}).get("provenance_registry") or []
        self.assertEqual(1, len(learned), msg=combined)
        self.assertEqual("NEWSHOP", learned[0].get("norm_key"))
        self.assertEqual("other", learned[0].get("category_key"))

        queue_rows = _read_queue_rows(queue_csv)
        self.assertEqual(1, len(queue_rows), msg=combined)
        self.assertEqual("KEEPHOLD", queue_rows[0].get("norm_key"))
        self.assertEqual("HOLD", queue_rows[0].get("action"))

        queue_state_obj = json.loads(queue_state.read_text(encoding="utf-8"))
        self.assertEqual({"KEEPHOLD"}, set((queue_state_obj.get("clients_by_norm_key") or {}).keys()))

        applied_log = pending_dir / "applied_log.jsonl"
        self.assertTrue(applied_log.exists(), msg=combined)
        applied_rows = [json.loads(line) for line in applied_log.read_text(encoding="utf-8").splitlines() if line]
        self.assertEqual(2, len(applied_rows), msg=combined)
        self.assertEqual({"added", "already_exists_same_category"}, {row.get("status") for row in applied_rows})


if __name__ == "__main__":
    unittest.main()
