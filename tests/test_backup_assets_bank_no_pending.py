from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _load_backup_module():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / ".agents" / "skills" / "backup-assets" / "scripts" / "backup_assets.py"
    spec = importlib.util.spec_from_file_location(f"backup_assets_script_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class BackupAssetsBankNoPendingTests(unittest.TestCase):
    def test_bank_backup_is_clients_only_and_excludes_forbidden_ledger_ref_paths(self) -> None:
        module = _load_backup_module()
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            backup_dir = repo_root / "exports" / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)

            _write_text(
                repo_root / "clients" / "C1" / "lines" / "bank_statement" / "inputs" / "kari_shiwake" / "ok.csv",
                "ok",
            )
            _write_text(
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "bank_statement"
                / "inputs"
                / "ledger_ref"
                / "legacy.csv",
                "legacy",
            )
            _write_text(
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "bank_statement"
                / "artifacts"
                / "ingest"
                / "ledger_ref"
                / "legacy.json",
                "legacy",
            )
            _write_text(repo_root / "lexicon" / "bank_statement" / "pending" / "label_queue.csv", "must_not_pack")

            zip_path = backup_dir / "bank_backup_test.zip"
            manifest_json, counts = module._write_assets_zip(
                zip_path,
                repo_root,
                datetime(2026, 2, 20, tzinfo=timezone.utc),
                "bank_statement",
            )

            self.assertTrue(zip_path.exists())
            self.assertGreaterEqual(counts["files"], 1)
            self.assertIn("clients", manifest_json)

            with zipfile.ZipFile(zip_path, mode="r") as zf:
                names = sorted(info.filename.replace("\\", "/") for info in zf.infolist())
                self.assertIn("clients/", names)
                self.assertIn("clients/C1/lines/bank_statement/inputs/kari_shiwake/ok.csv", names)
                self.assertFalse(any(name.startswith("lexicon/") for name in names))
                self.assertFalse(
                    any(
                        name.startswith("clients/C1/lines/bank_statement/inputs/ledger_ref/")
                        for name in names
                    )
                )
                self.assertFalse(
                    any(
                        name.startswith("clients/C1/lines/bank_statement/artifacts/ingest/ledger_ref/")
                        for name in names
                    )
                )

                manifest = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
                manifest_paths = [str(item.get("path", "")) for item in manifest.get("files", [])]
                self.assertFalse(any(path.startswith("lexicon/") for path in manifest_paths))
                self.assertFalse(any("/pending/" in path for path in manifest_paths))
                self.assertFalse(
                    any(path.startswith("clients/C1/lines/bank_statement/inputs/ledger_ref/") for path in manifest_paths)
                )
                self.assertFalse(
                    any(
                        path.startswith("clients/C1/lines/bank_statement/artifacts/ingest/ledger_ref/")
                        for path in manifest_paths
                    )
                )


if __name__ == "__main__":
    unittest.main()
