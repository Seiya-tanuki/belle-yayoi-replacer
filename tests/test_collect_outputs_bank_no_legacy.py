from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import unittest
import zipfile
from pathlib import Path
from uuid import uuid4


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _load_collect_module():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / ".agents" / "skills" / "collect-outputs" / "scripts" / "collect_outputs.py"
    spec = importlib.util.spec_from_file_location("collect_outputs_script_bank_no_legacy", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class CollectOutputsBankNoLegacyTests(unittest.TestCase):
    def test_bank_statement_collect_ignores_legacy_run_root(self) -> None:
        module = _load_collect_module()
        test_tmp_root = Path(__file__).resolve().parents[1] / ".tmp"
        test_tmp_root.mkdir(parents=True, exist_ok=True)
        repo_root = test_tmp_root / f"collect_outputs_bank_no_legacy_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)
            line_run = (
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "bank_statement"
                / "outputs"
                / "runs"
                / "20260215T010203Z_LINE"
            )
            legacy_run = repo_root / "clients" / "C1" / "outputs" / "runs" / "20260215T010500Z_LEGACY"

            _write_bytes(line_run / "line_replaced_20260215T010203Z_LINE.csv", b"LINE-CSV")
            _write_bytes(line_run / "line_01_20260215T010203Z_LINE_review_report.csv", b"LINE-REPORT")
            _write_bytes(line_run / "run_manifest.json", b"{\"run\":\"LINE\"}\n")

            _write_bytes(legacy_run / "legacy_replaced_20260215T010500Z_LEGACY.csv", b"LEGACY-CSV")
            _write_bytes(legacy_run / "legacy_01_20260215T010500Z_LEGACY_review_report.csv", b"LEGACY-REPORT")
            _write_bytes(legacy_run / "run_manifest.json", b"{\"run\":\"LEGACY\"}\n")

            rc = module.main(
                [
                    "--line",
                    "bank_statement",
                    "--date",
                    "2026-02-15",
                    "--client",
                    "C1",
                    "--yes",
                ],
                repo_root=repo_root,
            )
            self.assertEqual(0, rc)

            collect_dir = repo_root / "exports" / "collect"
            zip_paths = sorted(collect_dir.glob("collect_2026-02-15_*.zip"))
            self.assertEqual(1, len(zip_paths))

            with zipfile.ZipFile(zip_paths[0], mode="r") as zf:
                names = sorted(zf.namelist())
                self.assertIn(
                    "csv/C1__20260215T010203Z_LINE__line_replaced_20260215T010203Z_LINE.csv",
                    names,
                )
                self.assertIn(
                    "reports/C1__20260215T010203Z_LINE__line_01_20260215T010203Z_LINE_review_report.csv",
                    names,
                )
                self.assertIn(
                    "manifests/C1__20260215T010203Z_LINE__run_manifest.json",
                    names,
                )

                self.assertNotIn(
                    "csv/C1__20260215T010500Z_LEGACY__legacy_replaced_20260215T010500Z_LEGACY.csv",
                    names,
                )
                self.assertNotIn(
                    "reports/C1__20260215T010500Z_LEGACY__legacy_01_20260215T010500Z_LEGACY_review_report.csv",
                    names,
                )
                self.assertNotIn(
                    "manifests/C1__20260215T010500Z_LEGACY__run_manifest.json",
                    names,
                )

                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
                source_relpaths = [item["source_relpath"] for item in manifest_obj["items"]]
                self.assertTrue(
                    any(
                        "clients/C1/lines/bank_statement/outputs/runs/20260215T010203Z_LINE" in rel
                        for rel in source_relpaths
                    )
                )
                self.assertFalse(
                    any("clients/C1/outputs/runs/20260215T010500Z_LEGACY" in rel for rel in source_relpaths)
                )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
