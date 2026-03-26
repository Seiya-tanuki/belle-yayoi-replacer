from __future__ import annotations

import shutil
import unittest
from pathlib import Path
from uuid import uuid4


class LocalUiUploadsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def test_resolve_slot_dir_matches_expected_inputs_path(self) -> None:
        from belle.local_ui.services.uploads import resolve_slot_dir

        cases = {
            "receipt.target": Path("clients/C1/lines/receipt/inputs/kari_shiwake"),
            "receipt.ledger_ref": Path("clients/C1/lines/receipt/inputs/ledger_ref"),
            "bank_statement.target": Path("clients/C1/lines/bank_statement/inputs/kari_shiwake"),
            "bank_statement.training_ocr": Path("clients/C1/lines/bank_statement/inputs/training/ocr_kari_shiwake"),
            "bank_statement.training_reference": Path("clients/C1/lines/bank_statement/inputs/training/reference_yayoi"),
            "credit_card_statement.target": Path("clients/C1/lines/credit_card_statement/inputs/kari_shiwake"),
            "credit_card_statement.ledger_ref": Path("clients/C1/lines/credit_card_statement/inputs/ledger_ref"),
        }
        for slot_key, expected in cases.items():
            self.assertEqual(expected, resolve_slot_dir("C1", slot_key, self.repo_root).relative_to(self.repo_root))

    def test_save_single_file_slot_replaces_existing_file(self) -> None:
        from belle.local_ui.services.uploads import list_slot_files, save_uploaded_file

        repo_root = self.test_tmp_root / f"local_ui_upload_single_{uuid4().hex}"
        try:
            save_uploaded_file("C1", "receipt.target", "first.csv", b"one", repo_root)
            save_uploaded_file("C1", "receipt.target", "second.csv", b"two", repo_root)
            self.assertEqual(["second.csv"], list_slot_files("C1", "receipt.target", repo_root))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_save_multi_file_slot_appends_files(self) -> None:
        from belle.local_ui.services.uploads import list_slot_files, save_uploaded_file

        repo_root = self.test_tmp_root / f"local_ui_upload_multi_{uuid4().hex}"
        try:
            save_uploaded_file("C1", "receipt.ledger_ref", "a.csv", b"a", repo_root)
            save_uploaded_file("C1", "receipt.ledger_ref", "b.csv", b"b", repo_root)
            self.assertEqual(["a.csv", "b.csv"], list_slot_files("C1", "receipt.ledger_ref", repo_root))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_clear_slot_removes_inputs_only(self) -> None:
        from belle.local_ui.services.uploads import clear_slot, resolve_slot_dir, save_uploaded_file

        repo_root = self.test_tmp_root / f"local_ui_upload_clear_{uuid4().hex}"
        try:
            save_uploaded_file("C1", "receipt.target", "a.csv", b"a", repo_root)
            artifacts_file = repo_root / "clients" / "C1" / "lines" / "receipt" / "artifacts" / "cache" / "keep.txt"
            outputs_file = repo_root / "clients" / "C1" / "lines" / "receipt" / "outputs" / "runs" / "keep.txt"
            artifacts_file.parent.mkdir(parents=True, exist_ok=True)
            outputs_file.parent.mkdir(parents=True, exist_ok=True)
            artifacts_file.write_text("keep", encoding="utf-8")
            outputs_file.write_text("keep", encoding="utf-8")

            clear_slot("C1", "receipt.target", repo_root)

            self.assertFalse(any(resolve_slot_dir("C1", "receipt.target", repo_root).iterdir()))
            self.assertTrue(artifacts_file.exists())
            self.assertTrue(outputs_file.exists())
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_validate_bank_training_allows_zero_zero(self) -> None:
        from belle.local_ui.services.uploads import validate_line_uploads

        repo_root = self.test_tmp_root / f"local_ui_upload_validate_00_{uuid4().hex}"
        try:
            self.assertTrue(validate_line_uploads("C1", "bank_statement", repo_root).ok is False)
            repo_root.joinpath("clients", "C1", "lines", "bank_statement", "inputs", "kari_shiwake").mkdir(
                parents=True, exist_ok=True
            )
            target = repo_root / "clients" / "C1" / "lines" / "bank_statement" / "inputs" / "kari_shiwake" / "a.csv"
            target.write_text("x", encoding="utf-8")
            result = validate_line_uploads("C1", "bank_statement", repo_root)
            self.assertTrue(result.ok)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_validate_bank_training_allows_one_one(self) -> None:
        from belle.local_ui.services.uploads import save_uploaded_file, validate_line_uploads

        repo_root = self.test_tmp_root / f"local_ui_upload_validate_11_{uuid4().hex}"
        try:
            save_uploaded_file("C1", "bank_statement.target", "target.csv", b"x", repo_root)
            save_uploaded_file("C1", "bank_statement.training_ocr", "ocr.csv", b"x", repo_root)
            save_uploaded_file("C1", "bank_statement.training_reference", "ref.txt", b"x", repo_root)
            self.assertTrue(validate_line_uploads("C1", "bank_statement", repo_root).ok)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_validate_bank_training_rejects_one_zero(self) -> None:
        from belle.local_ui.services.uploads import save_uploaded_file, validate_line_uploads

        repo_root = self.test_tmp_root / f"local_ui_upload_validate_10_{uuid4().hex}"
        try:
            save_uploaded_file("C1", "bank_statement.target", "target.csv", b"x", repo_root)
            save_uploaded_file("C1", "bank_statement.training_ocr", "ocr.csv", b"x", repo_root)
            result = validate_line_uploads("C1", "bank_statement", repo_root)
            self.assertFalse(result.ok)
            self.assertIn("学習用ファイルは2つそろえて入れてください。", result.errors)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_validate_bank_training_rejects_zero_one(self) -> None:
        from belle.local_ui.services.uploads import save_uploaded_file, validate_line_uploads

        repo_root = self.test_tmp_root / f"local_ui_upload_validate_01_{uuid4().hex}"
        try:
            save_uploaded_file("C1", "bank_statement.target", "target.csv", b"x", repo_root)
            save_uploaded_file("C1", "bank_statement.training_reference", "ref.txt", b"x", repo_root)
            result = validate_line_uploads("C1", "bank_statement", repo_root)
            self.assertFalse(result.ok)
            self.assertIn("学習用ファイルは2つそろえて入れてください。", result.errors)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_validate_bank_training_rejects_two_plus(self) -> None:
        from belle.local_ui.services.uploads import clear_slot, save_uploaded_file, validate_line_uploads

        repo_root = self.test_tmp_root / f"local_ui_upload_validate_2p_{uuid4().hex}"
        try:
            save_uploaded_file("C1", "bank_statement.target", "target.csv", b"x", repo_root)
            save_uploaded_file("C1", "bank_statement.training_ocr", "ocr.csv", b"x", repo_root)
            clear_slot("C1", "bank_statement.training_reference", repo_root)
            slot_dir = (
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "bank_statement"
                / "inputs"
                / "training"
                / "reference_yayoi"
            )
            slot_dir.mkdir(parents=True, exist_ok=True)
            (slot_dir / "a.csv").write_text("a", encoding="utf-8")
            (slot_dir / "b.txt").write_text("b", encoding="utf-8")
            result = validate_line_uploads("C1", "bank_statement", repo_root)
            self.assertFalse(result.ok)
            self.assertIn("学習用ファイルはそれぞれ1つだけ入れてください。", result.errors)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_extension_rules_match_contract(self) -> None:
        from belle.local_ui.services.uploads import is_allowed_extension

        self.assertTrue(is_allowed_extension("bank_statement.training_reference", "ref.csv"))
        self.assertTrue(is_allowed_extension("bank_statement.training_reference", "ref.txt"))
        self.assertFalse(is_allowed_extension("bank_statement.training_reference", "ref.pdf"))
        self.assertTrue(is_allowed_extension("receipt.target", "target.csv"))
        self.assertFalse(is_allowed_extension("receipt.target", "target.txt"))


if __name__ == "__main__":
    unittest.main()
