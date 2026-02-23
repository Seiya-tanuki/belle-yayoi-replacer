from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from belle.paths import ensure_client_system_dirs


class EnsureClientSystemDirsLineAwareTests(unittest.TestCase):
    def test_bank_statement_provisioning_skips_ledger_ref_ingest_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_DIRS"
            ensure_client_system_dirs(repo_root, client_id, line_id="bank_statement")

            line_root = repo_root / "clients" / client_id / "lines" / "bank_statement"
            self.assertTrue((line_root / "outputs" / "runs").is_dir())
            self.assertTrue((line_root / "artifacts" / "cache").is_dir())
            self.assertTrue((line_root / "artifacts" / "telemetry").is_dir())
            self.assertTrue((line_root / "artifacts" / "ingest" / "kari_shiwake").is_dir())
            self.assertTrue((line_root / "artifacts" / "ingest" / "training_ocr").is_dir())
            self.assertTrue((line_root / "artifacts" / "ingest" / "training_reference").is_dir())
            self.assertFalse((line_root / "artifacts" / "ingest" / "ledger_ref").exists())

    def test_receipt_provisioning_still_creates_ledger_ref_ingest_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_RECEIPT_DIRS"
            ensure_client_system_dirs(repo_root, client_id, line_id="receipt")

            line_root = repo_root / "clients" / client_id / "lines" / "receipt"
            self.assertTrue((line_root / "artifacts" / "ingest" / "ledger_ref").is_dir())
            self.assertTrue((line_root / "artifacts" / "ingest" / "kari_shiwake").is_dir())

    def test_legacy_receipt_provisioning_still_creates_ledger_ref_ingest_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_LEGACY_DIRS"
            ensure_client_system_dirs(repo_root, client_id, line_id=None)

            client_root = repo_root / "clients" / client_id
            self.assertTrue((client_root / "artifacts" / "ingest" / "ledger_ref").is_dir())
            self.assertTrue((client_root / "artifacts" / "ingest" / "kari_shiwake").is_dir())

    def test_credit_card_statement_provisioning_creates_ledger_ref_ingest_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_DIRS"
            ensure_client_system_dirs(repo_root, client_id, line_id="credit_card_statement")

            line_root = repo_root / "clients" / client_id / "lines" / "credit_card_statement"
            self.assertTrue((line_root / "artifacts" / "ingest" / "ledger_ref").is_dir())
            self.assertTrue((line_root / "artifacts" / "ingest" / "kari_shiwake").is_dir())


if __name__ == "__main__":
    unittest.main()
