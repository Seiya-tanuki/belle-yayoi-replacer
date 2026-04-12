from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from belle.paths import (
    ensure_client_system_dirs,
    get_cc_teacher_derived_dir,
    get_client_registration_artifacts_dir,
    get_client_registration_latest_path,
    get_client_registration_runs_dir,
    make_client_registration_run_dir,
)


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

    def test_shared_root_provisioning_still_creates_root_scoped_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_SHARED_ROOT_DIRS"
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
            self.assertTrue((line_root / "artifacts" / "derived").is_dir())
            self.assertEqual(
                line_root / "artifacts" / "derived" / "cc_teacher",
                get_cc_teacher_derived_dir(repo_root, client_id, line_id="credit_card_statement"),
            )
            self.assertTrue(get_cc_teacher_derived_dir(repo_root, client_id, line_id="credit_card_statement").is_dir())

    def test_client_registration_audit_helpers_resolve_shared_client_root_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_REG_AUDIT"
            run_id, run_dir = make_client_registration_run_dir(repo_root, client_id, run_id="RID_AUDIT")

            self.assertEqual("RID_AUDIT", run_id)
            self.assertEqual(
                repo_root / "clients" / client_id / "artifacts" / "client_registration",
                get_client_registration_artifacts_dir(repo_root, client_id),
            )
            self.assertEqual(
                repo_root / "clients" / client_id / "artifacts" / "client_registration" / "runs",
                get_client_registration_runs_dir(repo_root, client_id),
            )
            self.assertEqual(
                repo_root
                / "clients"
                / client_id
                / "artifacts"
                / "client_registration"
                / "LATEST.txt",
                get_client_registration_latest_path(repo_root, client_id),
            )
            self.assertEqual(
                repo_root
                / "clients"
                / client_id
                / "artifacts"
                / "client_registration"
                / "runs"
                / "RID_AUDIT",
                run_dir,
            )
            self.assertTrue(run_dir.is_dir())


if __name__ == "__main__":
    unittest.main()
