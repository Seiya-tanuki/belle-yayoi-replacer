from __future__ import annotations

import contextlib
import importlib.util
import io
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
    def test_collect_outputs_default_line_is_all(self) -> None:
        module = _load_collect_module()
        args = module._parse_args(["--yes"])
        self.assertEqual("all", args.line)

    def test_collect_outputs_accepts_run_ref_arg(self) -> None:
        module = _load_collect_module()
        args = module._parse_args(["--run-ref", "C1:RID1", "--yes"])
        self.assertEqual(["C1:RID1"], args.run_refs)

    def test_collect_outputs_default_all_includes_multiple_lines(self) -> None:
        module = _load_collect_module()
        test_tmp_root = Path(__file__).resolve().parents[1] / ".tmp"
        test_tmp_root.mkdir(parents=True, exist_ok=True)
        repo_root = test_tmp_root / f"collect_outputs_default_all_multi_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)

            receipt_run = (
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "receipt"
                / "outputs"
                / "runs"
                / "20260215T010203Z_RECEIPT"
            )
            bank_run = (
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "bank_statement"
                / "outputs"
                / "runs"
                / "20260215T020304Z_BANK"
            )

            _write_bytes(
                receipt_run / "r_replaced_20260215T010203Z_RECEIPT.csv",
                b"RECEIPT-CSV",
            )
            _write_bytes(
                receipt_run / "r_01_20260215T010203Z_RECEIPT_review_report.csv",
                b"RECEIPT-REPORT",
            )
            _write_bytes(receipt_run / "run_manifest.json", b"{\"run\":\"RECEIPT\"}\n")

            _write_bytes(
                bank_run / "b_replaced_20260215T020304Z_BANK.csv",
                b"BANK-CSV",
            )
            _write_bytes(
                bank_run / "b_01_20260215T020304Z_BANK_review_report.csv",
                b"BANK-REPORT",
            )
            _write_bytes(bank_run / "run_manifest.json", b"{\"run\":\"BANK\"}\n")

            rc = module.main(["--date", "2026-02-15", "--yes"], repo_root=repo_root)
            self.assertEqual(0, rc)

            collect_dir = repo_root / "exports" / "collect"
            zip_paths = sorted(collect_dir.glob("collect_2026-02-15_*.zip"))
            self.assertEqual(1, len(zip_paths))

            with zipfile.ZipFile(zip_paths[0], mode="r") as zf:
                names = sorted(zf.namelist())
                self.assertIn(
                    "receipt/csv/C1__20260215T010203Z_RECEIPT__r_replaced_20260215T010203Z_RECEIPT.csv",
                    names,
                )
                self.assertIn(
                    "bank_statement/csv/C1__20260215T020304Z_BANK__b_replaced_20260215T020304Z_BANK.csv",
                    names,
                )
                self.assertNotIn(
                    "credit_card_statement/csv/C1__20260215T020304Z_BANK__b_replaced_20260215T020304Z_BANK.csv",
                    names,
                )

                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
                self.assertEqual("all", manifest_obj["line_id"])
                self.assertEqual("2026-02-15", manifest_obj["jst_date"])
                summary = manifest_obj["summary"]
                self.assertEqual(2, summary["collected_runs"])
                self.assertIn("credit_card_statement", summary["skipped_lines"])
                self.assertEqual(
                    ["C1:20260215T010203Z_RECEIPT"],
                    summary["lines"]["receipt"]["included_run_ids"],
                )
                self.assertEqual(
                    ["C1:20260215T020304Z_BANK"],
                    summary["lines"]["bank_statement"]["included_run_ids"],
                )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_collect_outputs_all_skips_missing_line(self) -> None:
        module = _load_collect_module()
        test_tmp_root = Path(__file__).resolve().parents[1] / ".tmp"
        test_tmp_root.mkdir(parents=True, exist_ok=True)
        repo_root = test_tmp_root / f"collect_outputs_all_skip_missing_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)
            receipt_run = (
                repo_root
                / "clients"
                / "C2"
                / "lines"
                / "receipt"
                / "outputs"
                / "runs"
                / "20260215T030405Z_RECEIPT"
            )
            _write_bytes(
                receipt_run / "r_replaced_20260215T030405Z_RECEIPT.csv",
                b"RECEIPT-CSV",
            )
            _write_bytes(
                receipt_run / "r_01_20260215T030405Z_RECEIPT_review_report.csv",
                b"RECEIPT-REPORT",
            )
            _write_bytes(receipt_run / "run_manifest.json", b"{\"run\":\"RECEIPT\"}\n")

            rc = module.main(["--date", "2026-02-15", "--yes"], repo_root=repo_root)
            self.assertEqual(0, rc)

            collect_dir = repo_root / "exports" / "collect"
            zip_paths = sorted(collect_dir.glob("collect_2026-02-15_*.zip"))
            self.assertEqual(1, len(zip_paths))

            with zipfile.ZipFile(zip_paths[0], mode="r") as zf:
                names = sorted(zf.namelist())
                self.assertTrue(any(name.startswith("receipt/") for name in names))
                self.assertFalse(any(name.startswith("bank_statement/") for name in names))
                self.assertFalse(any(name.startswith("credit_card_statement/") for name in names))

                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
                summary = manifest_obj["summary"]
                self.assertEqual(1, summary["lines"]["receipt"]["collected_runs"])
                self.assertEqual(0, summary["lines"]["bank_statement"]["collected_runs"])
                self.assertEqual(0, summary["lines"]["credit_card_statement"]["collected_runs"])
                self.assertEqual(
                    ["bank_statement", "credit_card_statement"],
                    summary["skipped_lines"],
                )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_collect_outputs_receipt_mode_ignores_legacy_root(self) -> None:
        module = _load_collect_module()
        test_tmp_root = Path(__file__).resolve().parents[1] / ".tmp"
        test_tmp_root.mkdir(parents=True, exist_ok=True)
        repo_root = test_tmp_root / f"collect_outputs_receipt_legacy_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)

            line_run = (
                repo_root
                / "clients"
                / "R1"
                / "lines"
                / "receipt"
                / "outputs"
                / "runs"
                / "20260215T010203Z_LINE"
            )
            legacy_run = repo_root / "clients" / "R1" / "outputs" / "runs" / "20260215T010500Z_LEGACY"

            _write_bytes(line_run / "line_replaced_20260215T010203Z_LINE.csv", b"LINE-CSV")
            _write_bytes(line_run / "line_01_20260215T010203Z_LINE_review_report.csv", b"LINE-REPORT")
            _write_bytes(line_run / "run_manifest.json", b"{\"run\":\"LINE\"}\n")

            _write_bytes(legacy_run / "legacy_replaced_20260215T010500Z_LEGACY.csv", b"LEGACY-CSV")
            _write_bytes(legacy_run / "legacy_01_20260215T010500Z_LEGACY_review_report.csv", b"LEGACY-REPORT")
            _write_bytes(legacy_run / "run_manifest.json", b"{\"run\":\"LEGACY\"}\n")

            rc = module.main(
                [
                    "--line",
                    "receipt",
                    "--date",
                    "2026-02-15",
                    "--client",
                    "R1",
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
                    "csv/R1__20260215T010203Z_LINE__line_replaced_20260215T010203Z_LINE.csv",
                    names,
                )
                self.assertNotIn(
                    "csv/R1__20260215T010500Z_LEGACY__legacy_replaced_20260215T010500Z_LEGACY.csv",
                    names,
                )

                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
                source_relpaths = [item["source_relpath"] for item in manifest_obj["items"]]
                self.assertTrue(
                    any("clients/R1/lines/receipt/outputs/runs/20260215T010203Z_LINE" in rel for rel in source_relpaths)
                )
                self.assertFalse(any("clients/R1/outputs/runs/20260215T010500Z_LEGACY" in rel for rel in source_relpaths))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_collect_outputs_all_fails_when_no_runs_found(self) -> None:
        module = _load_collect_module()
        test_tmp_root = Path(__file__).resolve().parents[1] / ".tmp"
        test_tmp_root.mkdir(parents=True, exist_ok=True)
        repo_root = test_tmp_root / f"collect_outputs_all_no_runs_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)
            (repo_root / "clients" / "EMPTY").mkdir(parents=True, exist_ok=True)

            rc = module.main(["--date", "2026-02-15", "--yes"], repo_root=repo_root)
            self.assertEqual(1, rc)
            collect_dir = repo_root / "exports" / "collect"
            self.assertFalse(collect_dir.exists())
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_collect_outputs_run_ref_mode_collects_exact_run(self) -> None:
        module = _load_collect_module()
        test_tmp_root = Path(__file__).resolve().parents[1] / ".tmp"
        test_tmp_root.mkdir(parents=True, exist_ok=True)
        repo_root = test_tmp_root / f"collect_outputs_run_ref_exact_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)
            wanted_run = (
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "receipt"
                / "outputs"
                / "runs"
                / "20260215T010203Z_WANTED"
            )
            extra_run = (
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "receipt"
                / "outputs"
                / "runs"
                / "20260215T010500Z_EXTRA"
            )

            _write_bytes(wanted_run / "wanted_replaced_20260215T010203Z_WANTED.csv", b"WANTED-CSV")
            _write_bytes(wanted_run / "wanted_01_20260215T010203Z_WANTED_review_report.csv", b"WANTED-REPORT")
            _write_bytes(wanted_run / "run_manifest.json", b"{\"run\":\"WANTED\"}\n")

            _write_bytes(extra_run / "extra_replaced_20260215T010500Z_EXTRA.csv", b"EXTRA-CSV")
            _write_bytes(extra_run / "extra_01_20260215T010500Z_EXTRA_review_report.csv", b"EXTRA-REPORT")
            _write_bytes(extra_run / "run_manifest.json", b"{\"run\":\"EXTRA\"}\n")

            rc = module.main(
                [
                    "--run-ref",
                    "C1:20260215T010203Z_WANTED",
                    "--yes",
                ],
                repo_root=repo_root,
            )
            self.assertEqual(0, rc)

            collect_dir = repo_root / "exports" / "collect"
            zip_paths = sorted(collect_dir.glob("collect_*.zip"))
            self.assertEqual(1, len(zip_paths))

            with zipfile.ZipFile(zip_paths[0], mode="r") as zf:
                names = sorted(zf.namelist())
                self.assertIn(
                    "receipt/csv/C1__20260215T010203Z_WANTED__wanted_replaced_20260215T010203Z_WANTED.csv",
                    names,
                )
                self.assertFalse(any("20260215T010500Z_EXTRA" in name for name in names))
                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
                self.assertEqual("run_refs", manifest_obj["filters"]["mode"])
                self.assertEqual(
                    ["C1:20260215T010203Z_WANTED"],
                    manifest_obj["filters"]["run_refs"],
                )
                self.assertEqual(
                    ["C1:20260215T010203Z_WANTED"],
                    manifest_obj["summary"]["lines"]["receipt"]["included_run_ids"],
                )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_collect_outputs_cli_renders_success_markers_from_structured_result(self) -> None:
        module = _load_collect_module()
        test_tmp_root = Path(__file__).resolve().parents[1] / ".tmp"
        test_tmp_root.mkdir(parents=True, exist_ok=True)
        repo_root = test_tmp_root / f"collect_outputs_cli_markers_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
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

            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                rc = module.main(
                    [
                        "--line",
                        "receipt",
                        "--date",
                        "2026-02-15",
                        "--client",
                        "C1",
                        "--yes",
                    ],
                    repo_root=repo_root,
                )

            out = stdout.getvalue()
            self.assertEqual(0, rc, msg=stderr.getvalue())
            self.assertIn("[OK] ZIP: ", out)
            self.assertIn("[OK] LATEST: ", out)
            self.assertIn("[OK] 件数: runs=1, csv=1, reports=1, manifests=1, items=3", out)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
