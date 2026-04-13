from __future__ import annotations

import contextlib
import importlib.util
import io
import sys
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

from belle.application import (
    LinePlan,
    ReplacerPlanResult,
    ReplacerRunResult,
    RunLineResult,
)
from belle.application import replacer as replacer_app
from belle.ui_reason_codes import (
    PRECHECK_FAIL_CARD_CONFIG_MISSING,
    RUN_FAIL_CARD_CONFIG_MISSING,
    RUN_FAIL_MULTIPLE_TARGET_INPUTS,
    RUN_FAIL_TARGET_INGEST,
    parse_ui_reason_from_text,
)


def _load_replacer_script_module(repo_root: Path):
    script_path = repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
    spec = importlib.util.spec_from_file_location(f"run_yayoi_replacer_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class ReplacerApplicationTests(unittest.TestCase):
    def test_plan_replacer_returns_structured_plans_in_fixed_order(self) -> None:
        repo_root = Path("C:/repo")
        with mock.patch.object(
            replacer_app,
            "plan_receipt",
            return_value=LinePlan(
                line_id="receipt",
                status="RUN",
                reason="ready",
                target_files=("target.csv",),
                ui_reason_code="PRECHECK_READY",
                ui_reason_detail={"phase": "plan", "status": "RUN", "reason": "ready"},
                details={"client_layout_line_id": "receipt", "client_dir": "C:/repo/clients/C1/lines/receipt"},
            ),
        ):
            with mock.patch.object(
                replacer_app,
                "plan_bank",
                return_value=LinePlan(
                    line_id="bank_statement",
                    status="SKIP",
                    reason="no target input",
                    target_files=(),
                    ui_reason_code="PRECHECK_SKIP_NO_TARGET",
                    ui_reason_detail={"phase": "plan", "status": "SKIP", "reason": "no target input"},
                ),
            ):
                with mock.patch.object(
                    replacer_app,
                    "plan_card",
                    return_value=LinePlan(
                        line_id="credit_card_statement",
                        status="FAIL",
                        reason="missing_cc_config",
                        target_files=("target.csv",),
                        ui_reason_code="PRECHECK_FAIL_CARD_CONFIG_MISSING",
                        ui_reason_detail={"phase": "plan", "status": "FAIL", "reason": "missing_cc_config"},
                    ),
                ):
                    result = replacer_app.plan_replacer(repo_root, "C1", requested_line="all")

        self.assertEqual("C1", result.client_id)
        self.assertEqual("all", result.requested_line)
        self.assertEqual(
            ("receipt", "bank_statement", "credit_card_statement"),
            tuple(plan.line_id for plan in result.plans),
        )
        self.assertTrue(result.has_failures)
        self.assertEqual(("receipt",), tuple(plan.line_id for plan in result.runnable_plans))

    def test_run_replacer_returns_structured_success_results(self) -> None:
        repo_root = Path("C:/repo")
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
                        "client_dir": str(repo_root / "clients" / "C1" / "lines" / "receipt"),
                    },
                ),
            ),
        )

        with mock.patch.object(
            replacer_app,
            "run_receipt",
            return_value=RunLineResult.success(
                line_id="receipt",
                ui_reason_code="RUN_OK",
                ui_reason_detail={"phase": "run", "status": "success"},
                run_id="RID001",
                run_dir="C:/repo/run",
                run_manifest_path="C:/repo/run/run_manifest.json",
                changed_ratio=0.25,
                output_file="C:/repo/run/out.csv",
            ),
        ) as mocked_run_receipt:
            result = replacer_app.run_replacer(repo_root, "C1", plan_result=plan_result)

        mocked_run_receipt.assert_called_once()
        self.assertIsInstance(result, ReplacerRunResult)
        self.assertFalse(result.has_needs_review)
        self.assertFalse(result.stopped_early)
        self.assertEqual("success", result.line_results[0].outcome)
        self.assertEqual("RID001", result.line_results[0].run_id)

    def test_run_replacer_represents_bank_needs_review_without_system_exit(self) -> None:
        repo_root = Path("C:/repo")
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
                        "client_dir": str(repo_root / "clients" / "C1" / "lines" / "bank_statement"),
                    },
                ),
            ),
        )

        with mock.patch.object(
            replacer_app,
            "run_bank",
            return_value=RunLineResult.needs_review_result(
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
        ):
            result = replacer_app.run_replacer(repo_root, "C1", plan_result=plan_result)

        self.assertTrue(result.has_needs_review)
        self.assertTrue(result.stopped_early)
        self.assertEqual("needs_review", result.line_results[0].outcome)
        self.assertTrue(result.line_results[0].strict_stop_applied)

    def test_enforce_cc_config_required_sets_structured_codes_without_reason_text_classification(self) -> None:
        repo_root = Path("C:/repo")
        plan = LinePlan(
            line_id="credit_card_statement",
            status="RUN",
            reason="arbitrary precheck text",
            reason_key="ready",
            target_files=("target.csv",),
            ui_reason_code="PRECHECK_READY",
        )

        result = replacer_app._enforce_cc_config_required(repo_root, "C1", plan)

        self.assertEqual("FAIL", result.status)
        self.assertEqual("missing_cc_config", result.reason_key)
        self.assertEqual(PRECHECK_FAIL_CARD_CONFIG_MISSING, result.ui_reason_code)
        self.assertEqual(RUN_FAIL_CARD_CONFIG_MISSING, result.run_failure_ui_reason_code)

    def test_run_replacer_preserves_structured_failure_metadata_from_line_runner(self) -> None:
        repo_root = Path("C:/repo")
        plan_result = ReplacerPlanResult(
            client_id="C1",
            requested_line="receipt",
            plans=(
                LinePlan(
                    line_id="receipt",
                    status="RUN",
                    reason="ready",
                    reason_key="ready",
                    target_files=("target.csv",),
                    ui_reason_code="PRECHECK_READY",
                    ui_reason_detail={"phase": "plan", "status": "RUN", "reason": "ready"},
                    details={
                        "client_layout_line_id": "receipt",
                        "client_dir": str(repo_root / "clients" / "C1" / "lines" / "receipt"),
                    },
                ),
            ),
        )

        with mock.patch.object(
            replacer_app,
            "run_receipt",
            side_effect=replacer_app.LineRunnerFailure(
                line_id="receipt",
                message="free text no longer matters",
                failure_key="target_ingest_failed",
                ui_reason_code=RUN_FAIL_TARGET_INGEST,
                ui_reason_detail={"phase": "run", "status": "failure", "failure_key": "target_ingest_failed"},
            ),
        ):
            with self.assertRaises(replacer_app.ReplacerRunFailedError) as ctx:
                replacer_app.run_replacer(repo_root, "C1", plan_result=plan_result)

        self.assertEqual("receipt", ctx.exception.line_id)
        self.assertEqual("target_ingest_failed", ctx.exception.failure_key)
        self.assertEqual(RUN_FAIL_TARGET_INGEST, ctx.exception.ui_reason_code)

    def test_cli_main_maps_structured_needs_review_to_exit_code_2(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        module = _load_replacer_script_module(repo_root)
        module.__file__ = str(
            repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
        )
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
                    run_id="RID003",
                    run_dir="C:/repo/clients/C1/lines/bank_statement/outputs/runs/RID003",
                    run_manifest_path="C:/repo/clients/C1/lines/bank_statement/outputs/runs/RID003/run_manifest.json",
                    changed_ratio=0.125,
                    output_file="foo.csv",
                    reasons=("bank_sub_fill_required_failed",),
                    details={
                        "bank_cache_update": {
                            "pairs_unique_used_total": 2,
                            "cache_path": "C:/repo/cache.json",
                        }
                    },
                ),
            ),
            stopped_early=True,
        )

        buf = io.StringIO()
        with mock.patch.object(module, "plan_replacer", return_value=plan_result):
            with mock.patch.object(module, "run_replacer", return_value=run_result):
                with mock.patch.object(sys, "argv", ["run_yayoi_replacer.py", "--client", "C1", "--line", "bank_statement", "--yes"]):
                    with contextlib.redirect_stdout(buf):
                        rc = module.main()

        out = buf.getvalue()
        self.assertEqual(2, rc, msg=out)
        self.assertIn("[PLAN] client=C1 line=bank_statement", out)
        self.assertIn("[OK] client=C1 run_id=RID003 inputs=1 outputs=1", out)
        self.assertIn("[OK] run_manifest=C:/repo/clients/C1/lines/bank_statement/outputs/runs/RID003/run_manifest.json", out)
        self.assertIn("[UI_REASON]", out)
        self.assertIn("strict-stop: Contract A failed", out)

    def test_cli_main_emits_plan_gate_ui_reason_from_structured_metadata_not_reason_text(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        module = _load_replacer_script_module(repo_root)
        module.__file__ = str(
            repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
        )
        plan_result = ReplacerPlanResult(
            client_id="C1",
            requested_line="credit_card_statement",
            plans=(
                LinePlan(
                    line_id="credit_card_statement",
                    status="FAIL",
                    reason="multiple target inputs",
                    reason_key="multiple_target_inputs",
                    target_files=("a.csv", "b.csv"),
                    ui_reason_code="PRECHECK_FAIL_MULTIPLE_TARGET_INPUTS",
                    ui_reason_detail={
                        "phase": "plan",
                        "status": "FAIL",
                        "reason": "multiple target inputs",
                        "reason_key": "multiple_target_inputs",
                    },
                    run_failure_ui_reason_code=RUN_FAIL_CARD_CONFIG_MISSING,
                ),
            ),
        )

        buf = io.StringIO()
        with mock.patch.object(module, "plan_replacer", return_value=plan_result):
            with mock.patch.object(module, "run_replacer") as mocked_run:
                with mock.patch.object(
                    sys,
                    "argv",
                    ["run_yayoi_replacer.py", "--client", "C1", "--line", "credit_card_statement", "--dry-run"],
                ):
                    with contextlib.redirect_stdout(buf):
                        rc = module.main()

        out = buf.getvalue()
        mocked_run.assert_not_called()
        self.assertEqual(1, rc, msg=out)
        parsed = parse_ui_reason_from_text(out, line_id="credit_card_statement")
        self.assertEqual(
            (
                RUN_FAIL_CARD_CONFIG_MISSING,
                {
                    "phase": "plan_gate",
                    "reason": "multiple target inputs",
                    "reason_key": "multiple_target_inputs",
                    "status": "FAIL",
                },
            ),
            parsed,
        )
        self.assertNotIn(RUN_FAIL_MULTIPLE_TARGET_INPUTS, out)

    def test_cli_main_emits_runtime_failure_ui_reason_from_structured_error_not_message_text(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        module = _load_replacer_script_module(repo_root)
        module.__file__ = str(
            repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
        )
        plan_result = ReplacerPlanResult(
            client_id="C1",
            requested_line="receipt",
            plans=(
                LinePlan(
                    line_id="receipt",
                    status="RUN",
                    reason="ready",
                    reason_key="ready",
                    target_files=("target.csv",),
                    ui_reason_code="PRECHECK_READY",
                    ui_reason_detail={"phase": "plan", "status": "RUN", "reason": "ready", "reason_key": "ready"},
                    details={
                        "client_layout_line_id": "receipt",
                        "client_dir": "C:/repo/clients/C1/lines/receipt",
                    },
                ),
            ),
        )

        buf = io.StringIO()
        with mock.patch.object(module, "plan_replacer", return_value=plan_result):
            with mock.patch.object(
                module,
                "run_replacer",
                side_effect=module.ReplacerRunFailedError(
                    line_id="receipt",
                    message="client_cache 更新に失敗しました",
                    failure_key="target_ingest_failed",
                    ui_reason_code=RUN_FAIL_TARGET_INGEST,
                    ui_reason_detail={
                        "phase": "run",
                        "status": "failure",
                        "failure_key": "target_ingest_failed",
                    },
                ),
            ):
                with mock.patch.object(sys, "argv", ["run_yayoi_replacer.py", "--client", "C1", "--line", "receipt", "--yes"]):
                    with contextlib.redirect_stdout(buf):
                        rc = module.main()

        out = buf.getvalue()
        self.assertEqual(1, rc, msg=out)
        parsed = parse_ui_reason_from_text(out, line_id="receipt")
        self.assertEqual(
            (
                RUN_FAIL_TARGET_INGEST,
                {
                    "failure_key": "target_ingest_failed",
                    "phase": "run",
                    "status": "failure",
                },
            ),
            parsed,
        )
        self.assertIn("[ERROR] receipt run failed: client_cache 更新に失敗しました", out)

    def test_replacer_cli_script_source_has_no_run_failure_reason_helper_dependency(self) -> None:
        script_path = (
            Path(__file__).resolve().parents[1]
            / ".agents"
            / "skills"
            / "yayoi-replacer"
            / "scripts"
            / "run_yayoi_replacer.py"
        )
        source = script_path.read_text(encoding="utf-8")
        self.assertNotIn("run_failure_reason_code_for", source)


if __name__ == "__main__":
    unittest.main()
