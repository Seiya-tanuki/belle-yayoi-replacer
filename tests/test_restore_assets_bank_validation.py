from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from uuid import uuid4


def _load_restore_module():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / ".agents" / "skills" / "restore-assets" / "scripts" / "restore_assets.py"
    spec = importlib.util.spec_from_file_location(f"restore_assets_script_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _build_zip(zip_path: Path, *, files: dict[str, bytes], line_id: str = "bank_statement") -> None:
    manifest_files = []
    total_bytes = 0
    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("clients/", b"")
        for path, data in sorted(files.items()):
            zf.writestr(path, data)
            manifest_files.append(
                {
                    "path": path,
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "size_bytes": len(data),
                }
            )
            total_bytes += len(data)

        manifest = {
            "schema": "belle.assets_backup_manifest.v1",
            "exported_at_utc": "2026-02-20T00:00:00Z",
            "git_head": "unknown",
            "line_id": line_id,
            "files": manifest_files,
            "counts": {
                "files": len(manifest_files),
                "clients": 1,
                "total_bytes": total_bytes,
            },
            "notes_ja": "test backup",
        }
        zf.writestr("MANIFEST.json", (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"))


class RestoreAssetsBankValidationTests(unittest.TestCase):
    def test_receipt_validation_rejects_legacy_global_pending_paths(self) -> None:
        module = _load_restore_module()
        with tempfile.TemporaryDirectory() as td:
            zip_path = Path(td) / "receipt_legacy_pending.zip"
            _build_zip(
                zip_path,
                files={"/".join(("lexicon", "pending", "label_queue.csv")): b"legacy"},
                line_id="receipt",
            )
            with self.assertRaises(ValueError) as ctx:
                module._validate_backup_zip(zip_path, line_id="receipt")
            self.assertIn(
                "Legacy global pending backups are no longer supported.",
                str(ctx.exception),
            )

    def test_bank_validation_does_not_require_pending_root(self) -> None:
        module = _load_restore_module()
        with tempfile.TemporaryDirectory() as td:
            zip_path = Path(td) / "bank_ok.zip"
            _build_zip(
                zip_path,
                files={"clients/C1/lines/bank_statement/artifacts/cache/client_cache.json": b"{}"},
            )
            counts = module._validate_backup_zip(zip_path, line_id="bank_statement")
            self.assertEqual(1, counts["files"])
            self.assertEqual(1, counts["clients"])

    def test_bank_validation_rejects_forbidden_ledger_ref_paths(self) -> None:
        module = _load_restore_module()
        with tempfile.TemporaryDirectory() as td:
            zip_path = Path(td) / "bank_forbidden.zip"
            _build_zip(
                zip_path,
                files={"clients/C1/lines/bank_statement/inputs/ledger_ref/legacy.csv": b"legacy"},
            )
            with self.assertRaises(ValueError) as ctx:
                module._validate_backup_zip(zip_path, line_id="bank_statement")
            self.assertIn(
                "Zip contains receipt-only bank forbidden paths (ledger_ref).",
                str(ctx.exception),
            )


if __name__ == "__main__":
    unittest.main()
