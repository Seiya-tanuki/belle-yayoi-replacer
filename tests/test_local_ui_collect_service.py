from __future__ import annotations

import json
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from uuid import uuid4


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _copy_collect_script(source_root: Path, repo_root: Path) -> None:
    src = source_root / ".agents" / "skills" / "collect-outputs" / "scripts" / "collect_outputs.py"
    dst = repo_root / ".agents" / "skills" / "collect-outputs" / "scripts" / "collect_outputs.py"
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


class LocalUiCollectServiceTests(unittest.TestCase):
    def test_build_collect_command_for_single_line_session(self) -> None:
        from belle.local_ui.services.collect import build_collect_command

        command = build_collect_command(
            client_id="C1",
            run_results=[{"line_id": "receipt", "run_id": "20260326T010200Z_R1"}],
            session_started_at_utc="",
            session_finished_at_utc="",
            root=Path("C:/repo"),
        )
        self.assertIn("--client", command)
        self.assertIn("C1", command)
        self.assertIn("--line", command)
        self.assertIn("receipt", command)
        self.assertIn("--date", command)
        self.assertIn("2026-03-26", command)
        self.assertIn("--time", command)
        self.assertIn("10:02-10:02", command)
        self.assertIn("--yes", command)

    def test_build_collect_command_for_multi_line_session(self) -> None:
        from belle.local_ui.services.collect import build_collect_command

        command = build_collect_command(
            client_id="C1",
            run_results=[
                {"line_id": "bank_statement", "run_id": "20260326T010200Z_B1"},
                {"line_id": "receipt", "run_id": "20260326T010500Z_R1"},
            ],
            session_started_at_utc="",
            session_finished_at_utc="",
            root=Path("C:/repo"),
        )
        line_index = command.index("--line")
        self.assertEqual("all", command[line_index + 1])
        time_index = command.index("--time")
        self.assertEqual("10:02-10:05", command[time_index + 1])

    def test_build_collect_command_uses_session_timestamp_fallback(self) -> None:
        from belle.local_ui.services.collect import build_collect_command

        command = build_collect_command(
            client_id="C1",
            run_results=[],
            session_started_at_utc="2026-03-26T01:00:00Z",
            session_finished_at_utc="2026-03-26T01:07:00Z",
            root=Path("C:/repo"),
        )
        self.assertIn("2026-03-26", command)
        time_index = command.index("--time")
        self.assertEqual("10:00-10:07", command[time_index + 1])

    def test_manifest_compare_exact_match(self) -> None:
        from belle.local_ui.services.collect import _manifest_included_run_refs

        manifest = {
            "line_id": "receipt",
            "summary": {"included_run_ids": ["C1:RID1", "C1:RID2"]},
        }
        self.assertEqual(["C1:RID1", "C1:RID2"], _manifest_included_run_refs(manifest))

    def test_manifest_compare_extra_runs_in_all_mode(self) -> None:
        from belle.local_ui.services.collect import _manifest_included_run_refs

        manifest = {
            "line_id": "all",
            "summary": {
                "lines": {
                    "receipt": {"included_run_ids": ["C1:RID1"]},
                    "bank_statement": {"included_run_ids": ["C1:RID2", "C1:RIDX"]},
                }
            },
        }
        self.assertEqual(["C1:RID1", "C1:RID2", "C1:RIDX"], _manifest_included_run_refs(manifest))

    def test_run_collect_reads_generated_zip_manifest(self) -> None:
        from belle.local_ui.services.collect import run_collect, source_repo_root

        real_repo_root = source_repo_root()
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
                / "20260326T010200Z_R1"
            )
            _write_bytes(run_dir / "r_replaced_20260326T010200Z_R1.csv", b"CSV")
            _write_bytes(run_dir / "r_01_20260326T010200Z_R1_review_report.csv", b"REPORT")
            _write_bytes(run_dir / "run_manifest.json", b"{\"run\":\"RID\"}\n")
            _copy_collect_script(real_repo_root, repo_root)

            result = run_collect(
                client_id="C1",
                run_results=[{"line_id": "receipt", "run_id": "20260326T010200Z_R1", "status": "success"}],
                session_started_at_utc="2026-03-26T01:02:00Z",
                session_finished_at_utc="2026-03-26T01:02:30Z",
                root=repo_root,
            )

            self.assertTrue(result.ok, msg=result.stdout + result.stderr)
            self.assertTrue(result.exact_match)
            self.assertEqual("COLLECT_OK_EXACT", result.ui_reason_code)
            self.assertTrue(Path(result.zip_path).exists())
            self.assertIn(Path(result.zip_path).name, result.message)
            with zipfile.ZipFile(result.zip_path, mode="r") as zf:
                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
            self.assertEqual("receipt", manifest_obj["line_id"])

    def test_overall_result_title_maps_success_needs_review_failure(self) -> None:
        from belle.local_ui.services.collect import overall_result_title

        self.assertEqual("処理が完了しました", overall_result_title([{"status": "success"}]))
        self.assertEqual("処理は完了しましたが、確認が必要です", overall_result_title([{"status": "needs_review"}]))
        self.assertEqual("処理を完了できませんでした", overall_result_title([{"status": "failure"}]))


if __name__ == "__main__":
    unittest.main()
