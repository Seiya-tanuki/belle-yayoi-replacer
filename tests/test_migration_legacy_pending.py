from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from belle.migration import MigrationSafetyError, migrate_legacy_pending_to_receipt


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8", newline="\n")


class LegacyPendingMigrationTests(unittest.TestCase):
    def test_apply_copy_migrates_queue_state_and_markers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_text(repo_root / "lexicon" / "pending" / "label_queue.csv", "queue")
            _write_text(repo_root / "lexicon" / "pending" / "label_queue_state.json", "{\"n\":1}")
            _write_text(
                repo_root / "lexicon" / "pending" / "ledger_ref_processed_markers.json",
                "{\"m\":1}",
            )
            _write_text(repo_root / "lexicon" / "pending" / "label_queue.lock", "LOCK")
            _write_text(repo_root / "lexicon" / "pending" / "locks" / "label_queue.lock", "LOCK2")
            _write_text(repo_root / "lexicon" / "pending" / "ignore_me.txt", "ignore")
            _write_text(repo_root / "lexicon" / "receipt" / "pending" / ".gitkeep", "")
            _write_text(repo_root / "lexicon" / "receipt" / "pending" / "locks" / ".gitkeep", "")

            result = migrate_legacy_pending_to_receipt(
                repo_root=repo_root,
                mode="copy",
                apply=True,
                dry_run=False,
            )

            self.assertEqual("applied", result["status"])
            self.assertTrue((repo_root / "lexicon" / "receipt" / "pending" / "label_queue.csv").exists())
            self.assertTrue((repo_root / "lexicon" / "receipt" / "pending" / "label_queue_state.json").exists())
            self.assertTrue(
                (repo_root / "lexicon" / "receipt" / "pending" / "ledger_ref_processed_markers.json").exists()
            )
            self.assertFalse((repo_root / "lexicon" / "receipt" / "pending" / "label_queue.lock").exists())
            self.assertTrue((repo_root / "lexicon" / "pending" / "label_queue.csv").exists())
            self.assertGreaterEqual(len(result["skipped_locks"]), 2)

    def test_existing_destination_queue_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_text(repo_root / "lexicon" / "pending" / "label_queue.csv", "queue")
            _write_text(repo_root / "lexicon" / "receipt" / "pending" / "label_queue.csv", "already")

            with self.assertRaises(MigrationSafetyError):
                migrate_legacy_pending_to_receipt(
                    repo_root=repo_root,
                    mode="copy",
                    apply=False,
                    dry_run=True,
                )


if __name__ == "__main__":
    unittest.main()
