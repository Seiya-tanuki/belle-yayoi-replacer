from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from belle.application import LinePlan, ReplacerPlanResult, ReplacerRunResult, RunLineResult


class LocalUiReplacerServiceTests(unittest.TestCase):
    def test_normalized_line_order_uses_fixed_execution_order(self) -> None:
        from belle.local_ui.services.replacer import normalized_line_order

        self.assertEqual(
            ["receipt", "bank_statement", "credit_card_statement"],
            normalized_line_order(["credit_card_statement", "receipt", "bank_statement"]),
        )

    def test_run_precheck_for_lines_maps_structured_plan_results(self) -> None:
        from belle.local_ui.services import replacer as replacer_service

        plan_result = ReplacerPlanResult(
            client_id="C1",
            requested_line="receipt",
            plans=(
                LinePlan(
                    line_id="receipt",
                    status="RUN",
                    reason="ready",
                    target_files=("target.csv",),
                    ui_reason_code="PRECHECK_READY",
                    ui_reason_detail={"phase": "plan", "status": "RUN", "reason": "ready"},
                ),
            ),
        )

        with mock.patch.object(replacer_service, "plan_replacer", return_value=plan_result) as mocked_plan:
            results = replacer_service.run_precheck_for_lines("C1", ["receipt"], root=Path("C:/repo"))

        mocked_plan.assert_called_once_with(Path("C:/repo"), "C1", requested_line="receipt")
        self.assertEqual(1, len(results))
        self.assertEqual("receipt", results[0].line_id)
        self.assertEqual("RUN", results[0].status)
        self.assertEqual("準備OK", results[0].status_label)
        self.assertEqual(["target.csv"], results[0].target_files)
        self.assertEqual("PRECHECK_READY", results[0].ui_reason_code)
        self.assertEqual(0, results[0].returncode)

    def test_run_precheck_for_lines_raises_session_fatal_when_shared_layer_fails(self) -> None:
        from belle.local_ui.services import replacer as replacer_service

        with mock.patch.object(replacer_service, "plan_replacer", side_effect=RuntimeError("boom")):
            with self.assertRaises(replacer_service.SessionFatalError) as ctx:
                replacer_service.run_precheck_for_lines("C1", ["receipt"], root=Path("C:/repo"))

        self.assertEqual("SESSION_FATAL_APPLICATION_CALL_FAILED", ctx.exception.ui_reason_code)
        self.assertEqual("precheck", ctx.exception.detail["phase"])
        self.assertEqual("receipt", ctx.exception.detail["origin_line_id"])
        self.assertIn("shared-layer call failed", ctx.exception.detail["raw_error"])

    def test_run_selected_lines_maps_structured_success_results(self) -> None:
        from belle.local_ui.services import replacer as replacer_service

        plan_result = ReplacerPlanResult(
            client_id="C1",
            requested_line="receipt",
            plans=(
                LinePlan(
                    line_id="receipt",
                    status="RUN",
                    reason="ready",
                    target_files=("target.csv",),
                    ui_reason_code="PRECHECK_READY",
                    ui_reason_detail={"phase": "plan", "status": "RUN", "reason": "ready"},
                    details={
                        "client_layout_line_id": "receipt",
                        "client_dir": "C:/repo/clients/C1/lines/receipt",
                    },
                ),
            ),
        )
        run_result = ReplacerRunResult(
            client_id="C1",
            requested_line="receipt",
            plan_result=plan_result,
            line_results=(
                RunLineResult.success(
                    line_id="receipt",
                    ui_reason_code="RUN_OK",
                    ui_reason_detail={"phase": "run", "status": "success"},
                    run_id="RID001",
                    run_dir="C:/repo/run",
                    run_manifest_path="C:/repo/run/run_manifest.json",
                    changed_ratio=0.25,
                    output_file="C:/repo/run/out.csv",
                ),
            ),
        )

        with mock.patch.object(replacer_service, "plan_replacer", return_value=plan_result) as mocked_plan:
            with mock.patch.object(replacer_service, "run_replacer", return_value=run_result) as mocked_run:
                results = replacer_service.run_selected_lines("C1", ["receipt"], root=Path("C:/repo"))

        mocked_plan.assert_called_once_with(Path("C:/repo"), "C1", requested_line="receipt")
        mocked_run.assert_called_once_with(Path("C:/repo"), "C1", plan_result=plan_result)
        self.assertEqual(1, len(results))
        self.assertEqual("success", results[0].status)
        self.assertEqual("処理が完了しました", results[0].status_label)
        self.assertEqual("RUN_OK", results[0].ui_reason_code)
        self.assertEqual("RID001", results[0].run_id)
        self.assertEqual("C:/repo/run/run_manifest.json", results[0].run_manifest)
        self.assertEqual("0.250", results[0].changed_ratio)
        self.assertEqual(0, results[0].returncode)

    def test_run_selected_lines_handles_needs_review_without_exit_code_two(self) -> None:
        from belle.local_ui.services import replacer as replacer_service

        plan_result = ReplacerPlanResult(
            client_id="C1",
            requested_line="bank_statement",
            plans=(
                LinePlan(
                    line_id="bank_statement",
                    status="RUN",
                    reason="ready",
                    target_files=("target.csv",),
                    ui_reason_code="PRECHECK_READY",
                    ui_reason_detail={"phase": "plan", "status": "RUN", "reason": "ready"},
                    details={
                        "client_layout_line_id": "bank_statement",
                        "client_dir": "C:/repo/clients/C1/lines/bank_statement",
                    },
                ),
            ),
        )
        run_result = ReplacerRunResult(
            client_id="C1",
            requested_line="bank_statement",
            plan_result=plan_result,
            line_results=(
                RunLineResult.needs_review_result(
                    line_id="bank_statement",
                    reason="[ERROR] strict-stop: Contract A failed (bank_sub_fill_required_failed=True).",
                    ui_reason_code="RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED",
                    ui_reason_detail={"strict_stop_applied": True, "reasons": ["bank_sub_fill_required_failed"]},
                    run_id="RID002",
                    run_dir="C:/repo/run",
                    run_manifest_path="C:/repo/run/run_manifest.json",
                    changed_ratio=0.0,
                    output_file="C:/repo/run/out.csv",
                    reasons=("bank_sub_fill_required_failed",),
                ),
            ),
            stopped_early=True,
        )

        with mock.patch.object(replacer_service, "plan_replacer", return_value=plan_result):
            with mock.patch.object(replacer_service, "run_replacer", return_value=run_result):
                results = replacer_service.run_selected_lines("C1", ["bank_statement"], root=Path("C:/repo"))

        self.assertEqual(1, len(results))
        self.assertEqual("needs_review", results[0].status)
        self.assertEqual("処理は完了しましたが、確認が必要です", results[0].status_label)
        self.assertEqual("RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED", results[0].ui_reason_code)
        self.assertEqual(0, results[0].returncode)
        self.assertEqual("0.000", results[0].changed_ratio)

    def test_run_selected_lines_maps_plan_gate_failure_without_cli_stdout(self) -> None:
        from belle.local_ui.services import replacer as replacer_service

        plan_result = ReplacerPlanResult(
            client_id="C1",
            requested_line="credit_card_statement",
            plans=(
                LinePlan(
                    line_id="credit_card_statement",
                    status="FAIL",
                    reason="plan text is no longer the classifier",
                    reason_key="missing_cc_config",
                    target_files=("target.csv",),
                    ui_reason_code="PRECHECK_FAIL_CARD_CONFIG_MISSING",
                    ui_reason_detail={"phase": "plan", "status": "FAIL", "reason": "missing_cc_config"},
                    run_failure_ui_reason_code="RUN_FAIL_CARD_CONFIG_MISSING",
                ),
            ),
        )

        with mock.patch.object(replacer_service, "plan_replacer", return_value=plan_result):
            with mock.patch.object(replacer_service, "run_replacer") as mocked_run:
                results = replacer_service.run_selected_lines(
                    "C1",
                    ["credit_card_statement"],
                    root=Path("C:/repo"),
                )

        mocked_run.assert_not_called()
        self.assertEqual(1, len(results))
        self.assertEqual("failure", results[0].status)
        self.assertEqual("RUN_FAIL_CARD_CONFIG_MISSING", results[0].ui_reason_code)
        self.assertEqual("plan text is no longer the classifier", results[0].stdout)
        self.assertEqual(1, results[0].returncode)

    def test_run_selected_lines_maps_shared_layer_run_failure_from_structured_error_fields(self) -> None:
        from belle.local_ui.services import replacer as replacer_service

        plan_result = ReplacerPlanResult(
            client_id="C1",
            requested_line="receipt",
            plans=(
                LinePlan(
                    line_id="receipt",
                    status="RUN",
                    reason="ready",
                    target_files=("target.csv",),
                    ui_reason_code="PRECHECK_READY",
                    ui_reason_detail={"phase": "plan", "status": "RUN", "reason": "ready"},
                    details={
                        "client_layout_line_id": "receipt",
                        "client_dir": "C:/repo/clients/C1/lines/receipt",
                    },
                ),
            ),
        )

        with mock.patch.object(replacer_service, "plan_replacer", return_value=plan_result):
            with mock.patch.object(
                replacer_service,
                "run_replacer",
                side_effect=replacer_service.ReplacerRunFailedError(
                    line_id="receipt",
                    message="message text is no longer the classifier",
                    failure_key="target_ingest_failed",
                    ui_reason_code="RUN_FAIL_TARGET_INGEST",
                    ui_reason_detail={"phase": "run", "status": "failure", "failure_key": "target_ingest_failed"},
                ),
            ):
                results = replacer_service.run_selected_lines("C1", ["receipt"], root=Path("C:/repo"))

        self.assertEqual(1, len(results))
        self.assertEqual("failure", results[0].status)
        self.assertEqual("RUN_FAIL_TARGET_INGEST", results[0].ui_reason_code)
        self.assertEqual("message text is no longer the classifier", results[0].stdout)
        self.assertEqual(1, results[0].returncode)

    def test_run_selected_lines_raises_session_fatal_on_unexpected_shared_layer_exception(self) -> None:
        from belle.local_ui.services import replacer as replacer_service

        plan_result = ReplacerPlanResult(
            client_id="C1",
            requested_line="receipt",
            plans=(
                LinePlan(
                    line_id="receipt",
                    status="RUN",
                    reason="ready",
                    target_files=("target.csv",),
                    ui_reason_code="PRECHECK_READY",
                    ui_reason_detail={"phase": "plan", "status": "RUN", "reason": "ready"},
                    details={
                        "client_layout_line_id": "receipt",
                        "client_dir": "C:/repo/clients/C1/lines/receipt",
                    },
                ),
            ),
        )

        with mock.patch.object(replacer_service, "plan_replacer", return_value=plan_result):
            with mock.patch.object(replacer_service, "run_replacer", side_effect=ValueError("boom")):
                with self.assertRaises(replacer_service.SessionFatalError) as ctx:
                    replacer_service.run_selected_lines("C1", ["receipt"], root=Path("C:/repo"))

        self.assertEqual("SESSION_FATAL_APPLICATION_CALL_FAILED", ctx.exception.ui_reason_code)
        self.assertEqual("run", ctx.exception.detail["phase"])
        self.assertEqual("receipt", ctx.exception.detail["origin_line_id"])
        self.assertIn("shared-layer call failed", ctx.exception.detail["raw_error"])

    def test_build_session_fatal_results_expand_to_all_selected_lines(self) -> None:
        from belle.local_ui.services.replacer import (
            SessionFatalError,
            build_session_fatal_precheck_results,
            build_session_fatal_run_results,
            session_fatal_payload,
        )

        error = SessionFatalError(
            phase="run",
            line_id="bank_statement",
            raw_error="run shared-layer call failed: boom",
        )
        precheck_results = build_session_fatal_precheck_results(
            ["credit_card_statement", "receipt", "bank_statement"],
            error=error,
        )
        run_results = build_session_fatal_run_results(
            ["credit_card_statement", "receipt", "bank_statement"],
            error=error,
        )
        payload = session_fatal_payload(error)

        self.assertEqual(
            ["receipt", "bank_statement", "credit_card_statement"],
            [result.line_id for result in precheck_results],
        )
        self.assertTrue(all(result.status == "FAIL" for result in precheck_results))
        self.assertTrue(all(result.ui_reason_code == "SESSION_FATAL_APPLICATION_CALL_FAILED" for result in precheck_results))
        self.assertEqual(
            ["receipt", "bank_statement", "credit_card_statement"],
            [result.line_id for result in run_results],
        )
        self.assertTrue(all(result.status == "failure" for result in run_results))
        self.assertEqual("SESSION_FATAL_APPLICATION_CALL_FAILED", payload["ui_reason_code"])
        self.assertEqual("run", payload["detail"]["phase"])


if __name__ == "__main__":
    unittest.main()
