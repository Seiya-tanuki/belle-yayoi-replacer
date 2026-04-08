from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4


def _write_yayoi_row(path: Path, *, summary: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [""] * 25
    cols[4] = "仮払金"
    cols[16] = summary
    path.write_bytes((",".join(cols) + "\n").encode("cp932"))


def _prepare_receipt_repo(repo_root: Path, client_id: str) -> None:
    client_dir = repo_root / "clients" / client_id / "lines" / "receipt"
    (client_dir / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
    (client_dir / "inputs" / "ledger_ref").mkdir(parents=True, exist_ok=True)
    (client_dir / "config").mkdir(parents=True, exist_ok=True)
    (client_dir / "config" / "category_overrides.json").write_text("{}", encoding="utf-8")
    (repo_root / "rulesets" / "receipt").mkdir(parents=True, exist_ok=True)
    (repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json").write_text(
        "{\"version\":\"1.15\"}\n",
        encoding="utf-8",
    )


class LocalUiReplacerServiceTests(unittest.TestCase):
    def test_parse_plan_output_supports_run_skip_fail(self) -> None:
        from belle.local_ui.services.replacer import parse_plan_output

        stdout = "\n".join(
            [
                "[PLAN] client=C1 line=all",
                "- receipt: RUN (single target input) target=[target.csv]",
                "- bank_statement: SKIP (no target input) target=[-]",
                "- credit_card_statement: FAIL (missing_cc_config) target=[card.csv]",
            ]
        )
        results = parse_plan_output(stdout, returncode=1)
        self.assertEqual(["receipt", "bank_statement", "credit_card_statement"], [result.line_id for result in results])
        self.assertEqual("準備OK", results[0].status_label)
        self.assertEqual("今回は対象ファイルがありません", results[1].status_label)
        self.assertEqual("このままでは進めません", results[2].status_label)
        self.assertEqual("PRECHECK_READY", results[0].ui_reason_code)
        self.assertEqual("PRECHECK_SKIP_NO_TARGET", results[1].ui_reason_code)
        self.assertEqual("PRECHECK_FAIL_CARD_CONFIG_MISSING", results[2].ui_reason_code)

    def test_parse_run_output_extracts_run_metadata(self) -> None:
        from belle.local_ui.services.replacer import parse_run_output

        stdout = "\n".join(
            [
                '[UI_REASON] {"code": "RUN_OK", "detail": {"phase": "run", "status": "success"}, "line_id": "receipt"}',
                "[OK] client=C1 run_id=RID123 inputs=1 outputs=1",
                "[OK] run_dir=C:/repo/clients/C1/lines/receipt/outputs/runs/RID123",
                "[OK] run_manifest=C:/repo/clients/C1/lines/receipt/outputs/runs/RID123/run_manifest.json",
                " - changed_ratio=0.125 output=foo.csv",
                "[OK] done client=C1",
                "- receipt: DONE run_id=RID123 changed_ratio=0.125",
            ]
        )
        result = parse_run_output(stdout, line_id="receipt", returncode=0)
        self.assertEqual("success", result.status)
        self.assertEqual("RID123", result.run_id)
        self.assertIn("run_manifest.json", result.run_manifest)
        self.assertEqual("0.125", result.changed_ratio)
        self.assertEqual("RUN_OK", result.ui_reason_code)

    def test_parse_run_output_exit_two_is_needs_review(self) -> None:
        from belle.local_ui.services.replacer import parse_run_output

        result = parse_run_output("[OK] client=C1 run_id=RID456 inputs=1 outputs=1", line_id="bank_statement", returncode=2)
        self.assertEqual("needs_review", result.status)
        self.assertEqual("処理は完了しましたが、確認が必要です", result.status_label)
        self.assertEqual("RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED", result.ui_reason_code)

    def test_parse_run_output_failure_uses_structured_reason_when_present(self) -> None:
        from belle.local_ui.services.replacer import parse_run_output

        stdout = "\n".join(
            [
                "[ERROR] credit_card_statement run failed: missing_cc_config: expected=C:/repo/x.json",
                '[UI_REASON] {"code": "RUN_FAIL_CARD_CONFIG_MISSING", "detail": {"error": "missing_cc_config: expected=C:/repo/x.json", "phase": "run", "status": "failure"}, "line_id": "credit_card_statement"}',
            ]
        )
        result = parse_run_output(stdout, line_id="credit_card_statement", returncode=1)
        self.assertEqual("failure", result.status)
        self.assertEqual("RUN_FAIL_CARD_CONFIG_MISSING", result.ui_reason_code)

    def test_parse_plan_output_none_is_handled_without_crash(self) -> None:
        from belle.local_ui.services.replacer import parse_plan_output

        results = parse_plan_output(None, returncode=1)
        self.assertEqual([], results)

    def test_parse_run_output_none_is_handled_without_crash(self) -> None:
        from belle.local_ui.services.replacer import parse_run_output

        result = parse_run_output(None, line_id="receipt", returncode=1)
        self.assertEqual("failure", result.status)
        self.assertEqual("RUN_FAIL_UNKNOWN", result.ui_reason_code)

    def test_build_replacer_command_includes_expected_flags(self) -> None:
        from belle.local_ui.services.replacer import build_replacer_command

        command = build_replacer_command("C1", "receipt", root=Path("C:/repo"), dry_run=True, confirm_yes=False)
        self.assertIn("--client", command)
        self.assertIn("C1", command)
        self.assertIn("--line", command)
        self.assertIn("receipt", command)
        self.assertIn("--dry-run", command)
        self.assertNotIn("--yes", command)

        command_yes = build_replacer_command("C1", "receipt", root=Path("C:/repo"), confirm_yes=True)
        self.assertIn("--yes", command_yes)

    def test_normalized_line_order_uses_fixed_execution_order(self) -> None:
        from belle.local_ui.services.replacer import normalized_line_order

        self.assertEqual(
            ["receipt", "bank_statement", "credit_card_statement"],
            normalized_line_order(["credit_card_statement", "receipt", "bank_statement"]),
        )

    def test_run_precheck_for_lines_integration_smoke(self) -> None:
        from belle.local_ui.services.replacer import run_precheck_for_lines, source_repo_root

        client_id = "C1"
        real_repo_root = source_repo_root()
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            _prepare_receipt_repo(temp_repo_root, client_id)
            _write_yayoi_row(
                temp_repo_root / "clients" / client_id / "lines" / "receipt" / "inputs" / "kari_shiwake" / "target.csv",
                summary="SMOKE SHOP",
            )
            script_src = real_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            script_dst = temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            script_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(script_src, script_dst)

            results = run_precheck_for_lines(client_id, ["receipt"], root=temp_repo_root)
            self.assertEqual(1, len(results))
            self.assertEqual("receipt", results[0].line_id)
            self.assertEqual("RUN", results[0].status)
            self.assertEqual(0, results[0].returncode)

    def test_run_precheck_for_lines_accepts_non_ascii_client_id(self) -> None:
        from belle.local_ui.services.replacer import run_precheck_for_lines, source_repo_root

        client_id = "神話"
        real_repo_root = source_repo_root()
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            _prepare_receipt_repo(temp_repo_root, client_id)
            _write_yayoi_row(
                temp_repo_root / "clients" / client_id / "lines" / "receipt" / "inputs" / "kari_shiwake" / "target.csv",
                summary="NON ASCII CLIENT",
            )
            script_src = real_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            script_dst = temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            script_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(script_src, script_dst)

            results = run_precheck_for_lines(client_id, ["receipt"], root=temp_repo_root)
            self.assertEqual(1, len(results))
            self.assertEqual("receipt", results[0].line_id)
            self.assertEqual("RUN", results[0].status)
            self.assertEqual(0, results[0].returncode)
            self.assertIn("client=神話", results[0].stdout)

    def test_run_precheck_for_lines_raises_session_fatal_when_output_missing(self) -> None:
        from belle.local_ui.services import replacer as replacer_service

        proc = subprocess.CompletedProcess(args=["python"], returncode=1, stdout=None, stderr="")
        with mock.patch.object(replacer_service, "_run_command", return_value=proc):
            with self.assertRaises(replacer_service.SessionFatalError) as ctx:
                replacer_service.run_precheck_for_lines("C1", ["receipt"], root=Path("C:/repo"))
        self.assertEqual("SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID", ctx.exception.ui_reason_code)
        self.assertEqual("precheck", ctx.exception.detail["phase"])
        self.assertEqual("receipt", ctx.exception.detail["origin_line_id"])
        self.assertTrue(ctx.exception.detail["stdout_was_none"])

    def test_run_selected_lines_raises_session_fatal_when_success_markers_missing(self) -> None:
        from belle.local_ui.services import replacer as replacer_service

        proc = subprocess.CompletedProcess(args=["python"], returncode=0, stdout="[OK] done client=C1\n", stderr="")
        with mock.patch.object(replacer_service, "_run_command", return_value=proc):
            with self.assertRaises(replacer_service.SessionFatalError) as ctx:
                replacer_service.run_selected_lines("C1", ["receipt"], root=Path("C:/repo"))
        self.assertEqual("SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID", ctx.exception.ui_reason_code)
        self.assertEqual("run", ctx.exception.detail["phase"])
        self.assertEqual("receipt", ctx.exception.detail["origin_line_id"])

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
            command=["python", "script.py"],
            returncode=1,
            stdout=None,
            stderr="",
            raw_error="run output did not contain required success markers",
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
        self.assertTrue(all(result.ui_reason_code == "SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID" for result in precheck_results))
        self.assertEqual(
            ["receipt", "bank_statement", "credit_card_statement"],
            [result.line_id for result in run_results],
        )
        self.assertTrue(all(result.status == "failure" for result in run_results))
        self.assertEqual("SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID", payload["ui_reason_code"])
        self.assertEqual("run", payload["detail"]["phase"])


if __name__ == "__main__":
    unittest.main()
