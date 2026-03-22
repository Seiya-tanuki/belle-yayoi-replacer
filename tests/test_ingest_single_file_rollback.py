from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from belle.ingest import ingest_single_file


class IngestSingleFileRollbackTests(unittest.TestCase):
    def test_restore_source_and_remove_stray_file_when_manifest_save_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source_path = root / "inputs" / "sample.csv"
            store_dir = root / "artifacts" / "ingest"
            manifest_path = root / "artifacts" / "manifest.json"

            payload = "a,b,c\r\n1,2,3\r\n".encode("cp932")
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_bytes(payload)

            with mock.patch("belle.ingest.save_manifest", side_effect=RuntimeError("manifest write failed")):
                with self.assertRaises(RuntimeError):
                    ingest_single_file(
                        source_path=source_path,
                        store_dir=store_dir,
                        manifest_path=manifest_path,
                        client_id="C1",
                        kind="training_ocr",
                    )

            self.assertTrue(source_path.exists())
            self.assertEqual(payload, source_path.read_bytes())
            self.assertFalse(manifest_path.exists())
            stored_files = [p for p in store_dir.rglob("*") if p.is_file()] if store_dir.exists() else []
            self.assertEqual([], stored_files)


if __name__ == "__main__":
    unittest.main()
