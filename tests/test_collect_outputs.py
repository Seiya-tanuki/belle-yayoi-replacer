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
    spec = importlib.util.spec_from_file_location("collect_outputs_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class CollectOutputsTests(unittest.TestCase):
    def test_collect_outputs_creates_zip_and_preserves_sources(self) -> None:
        module = _load_collect_module()
        test_tmp_root = Path(__file__).resolve().parents[1] / ".tmp"
        test_tmp_root.mkdir(parents=True, exist_ok=True)
        repo_root = test_tmp_root / f"collect_outputs_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            # Required base dirs.
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)

            run_a = repo_root / "clients" / "A" / "outputs" / "runs" / "20260215T010203Z_AAAA"
            run_b = repo_root / "clients" / "B" / "outputs" / "runs" / "20260214T233000Z_BBBB"
            run_skip = repo_root / "clients" / "B" / "outputs" / "runs" / "20260215T120000Z_SKIP"
            run_line = (
                repo_root
                / "clients"
                / "C"
                / "lines"
                / "receipt"
                / "outputs"
                / "runs"
                / "20260215T033000Z_LINE"
            )

            src_a_csv = run_a / "a_replaced_20260215T010203Z_AAAA.csv"
            src_a_report = run_a / "a_01_20260215T010203Z_AAAA_review_report.csv"
            src_a_run_manifest = run_a / "run_manifest.json"
            src_a_manifest = run_a / "a_01_20260215T010203Z_AAAA_manifest.json"

            src_b_csv = run_b / "b_replaced_20260214T233000Z_BBBB.csv"
            src_b_report = run_b / "b_01_20260214T233000Z_BBBB_review_report.csv"
            src_b_run_manifest = run_b / "run_manifest.json"

            src_skip_report = run_skip / "skip_01_20260215T120000Z_SKIP_review_report.csv"
            src_skip_run_manifest = run_skip / "run_manifest.json"
            src_line_csv = run_line / "c_replaced_20260215T033000Z_LINE.csv"
            src_line_report = run_line / "c_01_20260215T033000Z_LINE_review_report.csv"
            src_line_run_manifest = run_line / "run_manifest.json"

            _write_bytes(src_a_csv, b"A-CSV")
            _write_bytes(src_a_report, b"A-REPORT")
            _write_bytes(src_a_run_manifest, b"{\"run\":\"A\"}\n")
            _write_bytes(src_a_manifest, b"{\"file\":\"A\"}\n")

            _write_bytes(src_b_csv, b"B-CSV")
            _write_bytes(src_b_report, b"B-REPORT")
            _write_bytes(src_b_run_manifest, b"{\"run\":\"B\"}\n")

            _write_bytes(src_skip_report, b"SKIP-REPORT")
            _write_bytes(src_skip_run_manifest, b"{\"run\":\"SKIP\"}\n")
            _write_bytes(src_line_csv, b"C-CSV")
            _write_bytes(src_line_report, b"C-REPORT")
            _write_bytes(src_line_run_manifest, b"{\"run\":\"C\"}\n")

            source_paths = [
                src_a_csv,
                src_a_report,
                src_a_run_manifest,
                src_a_manifest,
                src_b_csv,
                src_b_report,
                src_b_run_manifest,
                src_skip_report,
                src_skip_run_manifest,
                src_line_csv,
                src_line_report,
                src_line_run_manifest,
            ]
            source_before = {str(path): path.read_bytes() for path in source_paths}

            rc = module.main(["--date", "2026-02-15", "--yes"], repo_root=repo_root)
            self.assertEqual(0, rc)

            collect_dir = repo_root / "exports" / "collect"
            zip_paths = sorted(collect_dir.glob("collect_2026-02-15_*.zip"))
            self.assertEqual(1, len(zip_paths))
            zip_path = zip_paths[0]

            latest_text = (collect_dir / "LATEST.txt").read_text(encoding="utf-8").strip()
            self.assertEqual(zip_path.name, latest_text)

            with zipfile.ZipFile(zip_path, mode="r") as zf:
                names = sorted(zf.namelist())
                self.assertIn("MANIFEST.json", names)
                self.assertIn(
                    "csv/A__20260215T010203Z_AAAA__a_replaced_20260215T010203Z_AAAA.csv",
                    names,
                )
                self.assertIn(
                    "csv/B__20260214T233000Z_BBBB__b_replaced_20260214T233000Z_BBBB.csv",
                    names,
                )
                self.assertIn(
                    "csv/C__20260215T033000Z_LINE__c_replaced_20260215T033000Z_LINE.csv",
                    names,
                )
                self.assertIn(
                    "reports/A__20260215T010203Z_AAAA__a_01_20260215T010203Z_AAAA_review_report.csv",
                    names,
                )
                self.assertIn(
                    "reports/B__20260214T233000Z_BBBB__b_01_20260214T233000Z_BBBB_review_report.csv",
                    names,
                )
                self.assertIn(
                    "reports/C__20260215T033000Z_LINE__c_01_20260215T033000Z_LINE_review_report.csv",
                    names,
                )
                self.assertIn(
                    "manifests/A__20260215T010203Z_AAAA__run_manifest.json",
                    names,
                )
                self.assertIn(
                    "manifests/A__20260215T010203Z_AAAA__a_01_20260215T010203Z_AAAA_manifest.json",
                    names,
                )
                self.assertIn(
                    "manifests/B__20260214T233000Z_BBBB__run_manifest.json",
                    names,
                )
                self.assertIn(
                    "manifests/C__20260215T033000Z_LINE__run_manifest.json",
                    names,
                )
                self.assertNotIn(
                    "reports/B__20260215T120000Z_SKIP__skip_01_20260215T120000Z_SKIP_review_report.csv",
                    names,
                )

                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
                self.assertEqual("belle.collect_outputs_manifest.v1", manifest_obj["schema"])
                self.assertEqual("2026-02-15", manifest_obj["jst_date"])
                item_zip_paths = {item["zip_relpath"] for item in manifest_obj["items"]}
                self.assertNotIn(
                    "reports/B__20260215T120000Z_SKIP__skip_01_20260215T120000Z_SKIP_review_report.csv",
                    item_zip_paths,
                )

            for path in source_paths:
                self.assertTrue(path.exists(), str(path))
                self.assertEqual(source_before[str(path)], path.read_bytes(), str(path))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
