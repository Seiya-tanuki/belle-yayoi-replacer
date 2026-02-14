from __future__ import annotations

import inspect
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from belle import lexicon_manager
from belle.lexicon_manager import LABEL_QUEUE_COLUMNS, apply_label_queue_adds, write_label_queue


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


def _build_add_row(norm_key: str) -> dict[str, str]:
    row = {k: "" for k in LABEL_QUEUE_COLUMNS}
    row["norm_key"] = norm_key
    row["raw_example"] = f"{norm_key}_RAW"
    row["example_summary"] = f"{norm_key}_SUMMARY"
    row["count_total"] = "1"
    row["clients_seen"] = "1"
    row["first_seen_at"] = "2026-01-01T00:00:00+00:00"
    row["last_seen_at"] = "2026-01-01T00:00:00+00:00"
    row["user_category_key"] = "known"
    row["action"] = "ADD"
    return row


class LexiconAtomicWriteTests(unittest.TestCase):
    def test_atomic_replace_failure_keeps_original_lexicon(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            lexicon_path = root / "lexicon" / "lexicon.json"
            _write_minimal_lexicon(lexicon_path)

            queue_csv = root / "lexicon" / "pending" / "label_queue.csv"
            write_label_queue(queue_csv, {"ACMESHOP": _build_add_row("ACMESHOP")})

            queue_state = root / "lexicon" / "pending" / "label_queue_state.json"
            applied_log = root / "lexicon" / "pending" / "applied_log.jsonl"
            lexicon_before = lexicon_path.read_bytes()
            real_replace = os.replace

            def _replace_fail_on_lexicon(
                src: str | os.PathLike[str], dst: str | os.PathLike[str]
            ) -> None:
                if Path(dst).resolve() == lexicon_path.resolve():
                    raise OSError("simulated lexicon replace failure")
                real_replace(src, dst)

            with mock.patch("belle.io_atomic.os.replace", side_effect=_replace_fail_on_lexicon):
                with self.assertRaises(OSError):
                    apply_label_queue_adds(
                        lexicon_path=lexicon_path,
                        queue_csv_path=queue_csv,
                        queue_state_path=queue_state,
                        applied_log_path=applied_log,
                    )

            self.assertEqual(lexicon_before, lexicon_path.read_bytes())
            self.assertEqual(list(lexicon_path.parent.glob("lexicon.json.tmp.*")), [])

    def test_apply_implementation_avoids_fixed_json_tmp_literal(self) -> None:
        src = inspect.getsource(lexicon_manager.apply_label_queue_adds)
        self.assertIn("atomic_write_bytes", src)
        self.assertNotIn(".json.tmp", src)


if __name__ == "__main__":
    unittest.main()
