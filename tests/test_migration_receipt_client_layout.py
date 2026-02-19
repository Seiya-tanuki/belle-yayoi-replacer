from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from belle.migration import MigrationSafetyError, migrate_receipt_client_layout


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8", newline="\n")


class ReceiptClientLayoutMigrationTests(unittest.TestCase):
    def test_dry_run_plans_existing_legacy_directories(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_text(repo_root / "clients" / "ACME" / "config" / "category_overrides.json", "{}")
            _write_text(repo_root / "clients" / "ACME" / "inputs" / "kari_shiwake" / "a.csv", "x")
            _write_text(repo_root / "clients" / "ACME" / "outputs" / "LATEST.txt", "")
            _write_text(repo_root / "clients" / "ACME" / "artifacts" / "cache" / "client_cache.json", "{}")

            result = migrate_receipt_client_layout(
                repo_root=repo_root,
                client_id="ACME",
                mode="copy",
                apply=False,
                dry_run=True,
            )

            self.assertEqual("planned", result["status"])
            self.assertEqual("dry_run", result["reason"])
            self.assertEqual(4, len(result["operations"]))
            self.assertTrue((repo_root / "clients" / "ACME" / "config").exists())
            self.assertFalse((repo_root / "clients" / "ACME" / "lines" / "receipt" / "config").exists())

    def test_apply_copy_creates_line_scoped_dirs_and_keeps_legacy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_text(repo_root / "clients" / "ACME" / "config" / "category_overrides.json", "{\"a\":1}")
            _write_text(repo_root / "clients" / "ACME" / "inputs" / "ledger_ref" / "ref.csv", "REF")
            _write_text(repo_root / "clients" / "ACME" / "outputs" / "runs" / "R1" / "out.csv", "OUT")
            _write_text(repo_root / "clients" / "ACME" / "artifacts" / "ingest" / "state.json", "{\"ok\":true}")

            result = migrate_receipt_client_layout(
                repo_root=repo_root,
                client_id="ACME",
                mode="copy",
                apply=True,
                dry_run=False,
            )

            self.assertEqual("applied", result["status"])
            self.assertTrue(result["applied"])
            self.assertTrue((repo_root / "clients" / "ACME" / "config" / "category_overrides.json").exists())
            self.assertTrue(
                (
                    repo_root
                    / "clients"
                    / "ACME"
                    / "lines"
                    / "receipt"
                    / "config"
                    / "category_overrides.json"
                ).exists()
            )
            self.assertEqual(
                "REF",
                (
                    repo_root
                    / "clients"
                    / "ACME"
                    / "lines"
                    / "receipt"
                    / "inputs"
                    / "ledger_ref"
                    / "ref.csv"
                ).read_text(encoding="utf-8"),
            )

    def test_non_empty_destination_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_text(repo_root / "clients" / "ACME" / "config" / "category_overrides.json", "{}")
            _write_text(repo_root / "clients" / "ACME" / "lines" / "receipt" / "outputs" / "LATEST.txt", "exists")

            with self.assertRaises(MigrationSafetyError):
                migrate_receipt_client_layout(
                    repo_root=repo_root,
                    client_id="ACME",
                    mode="copy",
                    apply=False,
                    dry_run=True,
                )


if __name__ == "__main__":
    unittest.main()
