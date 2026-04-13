from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


class LocalUiCollectServiceTests(unittest.TestCase):
    def test_build_collect_request_for_single_line_session(self) -> None:
        from belle.local_ui.services.collect import build_collect_request

        request = build_collect_request(
            client_id="C1",
            run_results=[{"line_id": "receipt", "run_id": "20260326T010200Z_R1"}],
            session_started_at_utc="",
            session_finished_at_utc="",
        )
        self.assertEqual("receipt", request.line_id)
        self.assertEqual(("C1",), request.client_ids)
        self.assertEqual("2026-03-26", request.target_jst_date)
        self.assertEqual("10:02-10:02", request.time_range)
        self.assertEqual(("C1:20260326T010200Z_R1",), request.expected_run_refs)

    def test_build_collect_request_for_multi_line_session(self) -> None:
        from belle.local_ui.services.collect import build_collect_request

        request = build_collect_request(
            client_id="C1",
            run_results=[
                {"line_id": "bank_statement", "run_id": "20260326T010200Z_B1"},
                {"line_id": "receipt", "run_id": "20260326T010500Z_R1"},
            ],
            session_started_at_utc="",
            session_finished_at_utc="",
        )
        self.assertEqual("all", request.line_id)
        self.assertEqual("10:02-10:05", request.time_range)

    def test_build_collect_request_uses_session_timestamp_fallback(self) -> None:
        from belle.local_ui.services.collect import build_collect_request

        request = build_collect_request(
            client_id="C1",
            run_results=[],
            session_started_at_utc="2026-03-26T01:00:00Z",
            session_finished_at_utc="2026-03-26T01:07:00Z",
        )
        self.assertEqual("2026-03-26", request.target_jst_date)
        self.assertEqual("10:00-10:07", request.time_range)

    def test_build_collect_request_uses_run_refs_when_present(self) -> None:
        from belle.local_ui.services.collect import build_collect_request

        request = build_collect_request(
            client_id="C1",
            run_results=[
                {"line_id": "receipt", "run_id": "20260326T010200Z_R1"},
                {"line_id": "bank_statement", "run_id": "20260326T010500Z_B1"},
            ],
            session_started_at_utc="2026-03-26T01:00:00Z",
            session_finished_at_utc="2026-03-26T01:07:00Z",
            requested_run_refs=["C1:20260326T010200Z_R1", "C1:20260326T010500Z_B1"],
        )
        self.assertEqual((), request.client_ids)
        self.assertEqual("", request.target_jst_date)
        self.assertEqual("", request.time_range)
        self.assertEqual(
            ("C1:20260326T010200Z_R1", "C1:20260326T010500Z_B1"),
            request.requested_run_refs,
        )

    def test_build_collect_request_uses_today_all_mode_without_time_filter(self) -> None:
        from belle.local_ui.services.collect import build_collect_request

        request = build_collect_request(
            client_id="C1",
            run_results=[{"line_id": "receipt", "run_id": "20260326T010200Z_R1"}],
            session_started_at_utc="2026-03-26T01:00:00Z",
            session_finished_at_utc="2026-03-26T01:07:00Z",
            collect_today_all=True,
        )
        self.assertEqual("all", request.line_id)
        self.assertEqual(("C1",), request.client_ids)
        self.assertEqual("", request.time_range)

    def test_build_collect_request_uses_today_all_clients_mode_without_client_or_time_filter(self) -> None:
        from belle.local_ui.services.collect import build_collect_request

        request = build_collect_request(
            client_id="C1",
            run_results=[{"line_id": "receipt", "run_id": "20260326T010200Z_R1"}],
            session_started_at_utc="2026-03-26T01:00:00Z",
            session_finished_at_utc="2026-03-26T01:07:00Z",
            collect_today_all_clients=True,
        )
        self.assertEqual("all", request.line_id)
        self.assertEqual((), request.client_ids)
        self.assertEqual("", request.time_range)

    def test_manifest_compare_exact_match(self) -> None:
        from belle.local_ui.services.collect import manifest_included_run_refs

        manifest = {
            "line_id": "receipt",
            "summary": {"included_run_ids": ["C1:RID1", "C1:RID2"]},
        }
        self.assertEqual(["C1:RID1", "C1:RID2"], manifest_included_run_refs(manifest))

    def test_manifest_compare_extra_runs_in_all_mode(self) -> None:
        from belle.local_ui.services.collect import manifest_included_run_refs

        manifest = {
            "line_id": "all",
            "summary": {
                "lines": {
                    "receipt": {"included_run_ids": ["C1:RID1"]},
                    "bank_statement": {"included_run_ids": ["C1:RID2", "C1:RIDX"]},
                }
            },
        }
        self.assertEqual(["C1:RID1", "C1:RID2", "C1:RIDX"], manifest_included_run_refs(manifest))

    def test_run_collect_reads_generated_zip_manifest_without_subprocess(self) -> None:
        from belle.local_ui.services.collect import run_collect

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

            with mock.patch("subprocess.run", side_effect=AssertionError("subprocess must not be used")):
                result = run_collect(
                    client_id="C1",
                    run_results=[{"line_id": "receipt", "run_id": "20260326T010200Z_R1", "status": "success"}],
                    session_started_at_utc="2026-03-26T01:02:00Z",
                    session_finished_at_utc="2026-03-26T01:06:00Z",
                    root=repo_root,
                )

            self.assertTrue(result.ok)
            self.assertTrue(result.exact_match)
            self.assertEqual("COLLECT_OK_EXACT", result.ui_reason_code)
            self.assertTrue(Path(result.zip_path).exists())
            self.assertIn(Path(result.zip_path).name, result.message)
            with zipfile.ZipFile(result.zip_path, mode="r") as zf:
                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))
            self.assertEqual("receipt", manifest_obj["line_id"])

    def test_run_collect_with_extra_runs_keeps_success_without_stdout_parsing(self) -> None:
        from belle.local_ui.services.collect import run_collect

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)
            wanted_run = (
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "receipt"
                / "outputs"
                / "runs"
                / "20260326T010200Z_R1"
            )
            extra_run = (
                repo_root
                / "clients"
                / "C1"
                / "lines"
                / "receipt"
                / "outputs"
                / "runs"
                / "20260326T010500Z_R2"
            )
            for run_dir, marker in [(wanted_run, b"R1"), (extra_run, b"R2")]:
                _write_bytes(run_dir / f"x_replaced_{run_dir.name}.csv", marker)
                _write_bytes(run_dir / f"x_01_{run_dir.name}_review_report.csv", marker)
                _write_bytes(run_dir / "run_manifest.json", b"{\"run\":\"RID\"}\n")

            result = run_collect(
                client_id="C1",
                run_results=[{"line_id": "receipt", "run_id": "20260326T010200Z_R1", "status": "success"}],
                session_started_at_utc="2026-03-26T01:02:00Z",
                session_finished_at_utc="2026-03-26T01:02:30Z",
                collect_today_all=True,
                root=repo_root,
            )

            self.assertTrue(result.ok)
            self.assertEqual("success", result.status)
            self.assertEqual("COLLECT_WARN_EXTRA_RUNS_INCLUDED", result.ui_reason_code)
            self.assertFalse(result.exact_match)
            self.assertEqual(("C1:20260326T010500Z_R2",), result.extra_run_refs)

    def test_run_collect_returns_no_runs_failure_without_stdout_parsing(self) -> None:
        from belle.local_ui.services.collect import run_collect

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=True)
            (repo_root / "clients" / "EMPTY").mkdir(parents=True, exist_ok=True)

            result = run_collect(
                client_id="EMPTY",
                run_results=[],
                session_started_at_utc="2026-03-26T01:02:00Z",
                session_finished_at_utc="2026-03-26T01:06:00Z",
                collect_today_all_clients=True,
                root=repo_root,
            )

            self.assertFalse(result.ok)
            self.assertEqual("error", result.status)
            self.assertEqual("COLLECT_FAIL_NO_RUNS_FOUND", result.ui_reason_code)

    def test_serialize_collect_result_preserves_legacy_session_run_refs_alias(self) -> None:
        from belle.local_ui.services.collect import run_collect, serialize_collect_result

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

            result = run_collect(
                client_id="C1",
                run_results=[{"line_id": "receipt", "run_id": "20260326T010200Z_R1", "status": "success"}],
                session_started_at_utc="2026-03-26T01:02:00Z",
                session_finished_at_utc="2026-03-26T01:06:00Z",
                root=repo_root,
            )
            payload = serialize_collect_result(result)

            self.assertEqual(["C1:20260326T010200Z_R1"], payload["session_run_refs"])

    def test_overall_result_title_maps_success_needs_review_failure(self) -> None:
        from belle.local_ui.services.collect import overall_result_title

        self.assertEqual("処理が完了しました", overall_result_title([{"status": "success"}]))
        self.assertEqual(
            "処理は完了しましたが確認が必要です",
            overall_result_title([{"status": "needs_review"}]),
        )
        self.assertEqual("処理に失敗しました", overall_result_title([{"status": "failure"}]))

    def test_overall_result_title_prefers_reason_codes_when_present(self) -> None:
        from belle.local_ui.services.collect import overall_result_title

        self.assertEqual(
            "処理は完了しましたが確認が必要です",
            overall_result_title(
                [
                    {
                        "status": "success",
                        "ui_reason_code": "RUN_NEEDS_REVIEW_CARD_CANONICAL_PAYABLE_FAILED",
                    }
                ]
            ),
        )
        self.assertEqual(
            "処理は完了しましたが確認が必要です",
            overall_result_title(
                [
                    {
                        "status": "success",
                        "ui_reason_code": "RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED",
                    }
                ]
            ),
        )
        self.assertEqual(
            "処理に失敗しました",
            overall_result_title(
                [
                    {
                        "status": "success",
                        "ui_reason_code": "RUN_FAIL_CARD_CONFIG_MISSING",
                    }
                ]
            ),
        )


if __name__ == "__main__":
    unittest.main()
