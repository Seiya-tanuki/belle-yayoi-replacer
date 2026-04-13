from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from belle.application.collect import CollectRequest, prepare_collect_plan, run_collect


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


class ApplicationCollectTests(unittest.TestCase):
    def test_run_collect_returns_structured_success_result(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)
            run_dir = (
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "receipt"
                / "outputs"
                / "runs"
                / "20260215T010203Z_R1"
            )
            _write_bytes(run_dir / "r_replaced_20260215T010203Z_R1.csv", b"CSV")
            _write_bytes(run_dir / "r_01_20260215T010203Z_R1_review_report.csv", b"REPORT")
            _write_bytes(run_dir / "run_manifest.json", b"{\"run\":\"RID\"}\n")

            result = run_collect(
                repo_root,
                CollectRequest(
                    line_id="receipt",
                    requested_run_refs=("C1:20260215T010203Z_R1",),
                    expected_run_refs=("C1:20260215T010203Z_R1",),
                ),
            )

            self.assertTrue(result.ok)
            self.assertEqual("success", result.status)
            self.assertEqual("COLLECT_OK_EXACT", result.ui_reason_code)
            self.assertEqual(("C1:20260215T010203Z_R1",), result.included_run_refs)
            self.assertEqual(("C1:20260215T010203Z_R1",), result.requested_run_refs)
            self.assertTrue(Path(result.zip_path).exists())
            with zipfile.ZipFile(result.zip_path, mode="r") as zf:
                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
            self.assertEqual("receipt", manifest_obj["line_id"])

    def test_prepare_collect_plan_classifies_no_runs_without_text_parsing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)
            (repo_root / "clients" / "EMPTY").mkdir(parents=True, exist_ok=True)

            plan = prepare_collect_plan(
                repo_root,
                CollectRequest(
                    line_id="all",
                    target_jst_date="2026-02-15",
                    client_ids=("EMPTY",),
                ),
            )

            self.assertEqual("error", plan.status)
            self.assertEqual("COLLECT_FAIL_NO_RUNS_FOUND", plan.ui_reason_code)
            self.assertEqual("no runs found", plan.message)


if __name__ == "__main__":
    unittest.main()
