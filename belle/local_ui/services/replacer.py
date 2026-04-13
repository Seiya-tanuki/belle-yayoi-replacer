from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from belle.application import plan_replacer, run_replacer
from belle.application.errors import ReplacerRunFailedError
from belle.application.models import LinePlan, RunLineResult
from belle.local_ui.state import normalize_selected_lines
from belle.ui_reason_codes import (
    RUN_FAIL_UNKNOWN,
    SESSION_FATAL_APPLICATION_CALL_FAILED,
)

STATUS_LABELS = {
    "RUN": "準備OK",
    "SKIP": "今回は対象ファイルがありません",
    "FAIL": "このままでは進めません",
}

EXECUTION_LABELS = {
    "success": "処理が完了しました",
    "needs_review": "処理は完了しましたが、確認が必要です",
    "failure": "処理を完了できませんでした",
    "skipped": "今回は対象ファイルがありません",
}

SESSION_FATAL_REASON = (
    "想定外の問題が発生したため、このまま進めません。"
    "システムを再起動するため、まずコマンドプロンプト"
    "（システム起動時に表示されるテキストだけの黒い画面）を右上のバツボタンを押して終了してください。"
    "さらにこのブラウザも終了し、その後改めてデスクトップからシステムを起動してください。"
)
SESSION_FATAL_DETAIL_TEXT = (
    "想定外の問題が発生したため、今回の処理は完了できませんでした。\n"
    "システムを再起動するため、まずコマンドプロンプト"
    "（システム起動時に表示されるテキストだけの黒い画面）を右上のバツボタンを押して終了してください。\n"
    "さらにこのブラウザも終了し、その後改めてデスクトップからシステムを起動してください。\n"
    "再起動後も同じエラーコードが表示される場合は、管理者に連絡してください。"
)


@dataclass(frozen=True)
class PrecheckResult:
    line_id: str
    status: str
    status_label: str
    ui_reason_code: str
    ui_reason_detail: dict[str, object]
    reason: str
    target_files: list[str]
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class RunResult:
    line_id: str
    status: str
    status_label: str
    ui_reason_code: str
    ui_reason_detail: dict[str, object]
    returncode: int
    stdout: str
    stderr: str
    run_id: str
    run_dir: str
    run_manifest: str
    changed_ratio: str


class SessionFatalError(RuntimeError):
    def __init__(
        self,
        *,
        phase: str,
        line_id: str,
        raw_error: str,
        detail: dict[str, object] | None = None,
    ) -> None:
        message = str(raw_error or "unexpected replacer failure")
        super().__init__(message)
        self.phase = str(phase)
        self.line_id = str(line_id)
        self.raw_error = message
        self.ui_reason_code = SESSION_FATAL_APPLICATION_CALL_FAILED
        self.detail: dict[str, object] = {
            "phase": self.phase,
            "origin_line_id": self.line_id,
            "raw_error": self.raw_error,
        }
        if detail:
            self.detail.update(dict(detail))


def source_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def normalized_line_order(selected_lines: list[str]) -> list[str]:
    return normalize_selected_lines(selected_lines)


def _plan_stdout(plan: LinePlan) -> str:
    return f"{plan.line_id}: {plan.status} ({plan.reason})"


def _format_changed_ratio(value: float) -> str:
    return f"{float(value):.3f}"


def _precheck_result_from_plan(plan: LinePlan) -> PrecheckResult:
    return PrecheckResult(
        line_id=plan.line_id,
        status=plan.status,
        status_label=STATUS_LABELS[plan.status],
        ui_reason_code=plan.ui_reason_code,
        ui_reason_detail=dict(plan.ui_reason_detail),
        reason=plan.reason,
        target_files=list(plan.target_files),
        returncode=0 if plan.status in {"RUN", "SKIP"} else 1,
        stdout=_plan_stdout(plan),
        stderr="",
    )


def _run_result_from_line_result(result: RunLineResult) -> RunResult:
    status = str(result.outcome)
    return RunResult(
        line_id=result.line_id,
        status=status,
        status_label=EXECUTION_LABELS[status],
        ui_reason_code=result.ui_reason_code,
        ui_reason_detail=dict(result.ui_reason_detail),
        returncode=0 if status in {"success", "needs_review", "skipped"} else 1,
        stdout=str(result.reason or ""),
        stderr="",
        run_id=str(result.run_id or ""),
        run_dir=str(result.run_dir or ""),
        run_manifest=str(result.run_manifest_path or ""),
        changed_ratio=_format_changed_ratio(result.changed_ratio),
    )


def _run_result_from_failed_plan(plan: LinePlan) -> RunResult:
    return RunResult(
        line_id=plan.line_id,
        status="failure",
        status_label=EXECUTION_LABELS["failure"],
        ui_reason_code=plan.run_failure_ui_reason_code or RUN_FAIL_UNKNOWN,
        ui_reason_detail={"phase": "plan_gate", "status": plan.status, "reason": plan.reason, "reason_key": plan.reason_key},
        returncode=1,
        stdout=plan.reason,
        stderr="",
        run_id="",
        run_dir="",
        run_manifest="",
        changed_ratio="",
    )


def _run_result_from_skipped_plan(plan: LinePlan) -> RunResult:
    return RunResult(
        line_id=plan.line_id,
        status="skipped",
        status_label=EXECUTION_LABELS["skipped"],
        ui_reason_code=plan.ui_reason_code,
        ui_reason_detail={"phase": "run", "status": "skipped", "reason": plan.reason},
        returncode=0,
        stdout=plan.reason,
        stderr="",
        run_id="",
        run_dir="",
        run_manifest="",
        changed_ratio="",
    )


def run_precheck_for_lines(client_id: str, selected_lines: list[str], *, root: Path | None = None) -> list[PrecheckResult]:
    current_root = root or source_repo_root()
    results: list[PrecheckResult] = []
    for line_id in normalized_line_order(selected_lines):
        try:
            plan_result = plan_replacer(current_root, client_id, requested_line=line_id)
        except Exception as exc:
            raise SessionFatalError(
                phase="precheck",
                line_id=line_id,
                raw_error=f"precheck shared-layer call failed: {exc}",
            ) from exc
        results.extend(_precheck_result_from_plan(plan) for plan in plan_result.plans)
    return results


def run_selected_lines(client_id: str, selected_lines: list[str], *, root: Path | None = None) -> list[RunResult]:
    current_root = root or source_repo_root()
    results: list[RunResult] = []
    for line_id in normalized_line_order(selected_lines):
        try:
            plan_result = plan_replacer(current_root, client_id, requested_line=line_id)
        except Exception as exc:
            raise SessionFatalError(
                phase="run",
                line_id=line_id,
                raw_error=f"run preflight shared-layer call failed: {exc}",
            ) from exc

        if plan_result.has_failures:
            results.extend(_run_result_from_failed_plan(plan) for plan in plan_result.plans if plan.status == "FAIL")
            continue

        if not plan_result.runnable_plans:
            results.extend(_run_result_from_skipped_plan(plan) for plan in plan_result.plans if plan.status == "SKIP")
            continue

        try:
            run_result = run_replacer(current_root, client_id, plan_result=plan_result)
        except ReplacerRunFailedError as exc:
            results.extend(_run_result_from_line_result(result) for result in exc.partial_results)
            results.append(
                RunResult(
                    line_id=exc.line_id,
                    status="failure",
                    status_label=EXECUTION_LABELS["failure"],
                    ui_reason_code=exc.ui_reason_code or RUN_FAIL_UNKNOWN,
                    ui_reason_detail=dict(exc.ui_reason_detail) or {"phase": "run", "status": "failure", "error": str(exc)},
                    returncode=1,
                    stdout=str(exc),
                    stderr="",
                    run_id="",
                    run_dir="",
                    run_manifest="",
                    changed_ratio="",
                )
            )
            continue
        except Exception as exc:
            raise SessionFatalError(
                phase="run",
                line_id=line_id,
                raw_error=f"run shared-layer call failed: {exc}",
            ) from exc

        results.extend(_run_result_from_line_result(result) for result in run_result.line_results)
    return results


def session_fatal_payload(error: SessionFatalError) -> dict[str, object]:
    return {
        "ui_reason_code": error.ui_reason_code,
        "detail": dict(error.detail),
        "user_message": SESSION_FATAL_DETAIL_TEXT,
    }


def build_session_fatal_precheck_results(
    selected_lines: list[str],
    *,
    error: SessionFatalError,
) -> list[PrecheckResult]:
    log_text = f"{SESSION_FATAL_DETAIL_TEXT}\n\nエラーコード: {error.ui_reason_code}"
    return [
        PrecheckResult(
            line_id=line_id,
            status="FAIL",
            status_label=STATUS_LABELS["FAIL"],
            ui_reason_code=error.ui_reason_code,
            ui_reason_detail=dict(error.detail),
            reason=SESSION_FATAL_REASON,
            target_files=[],
            returncode=1,
            stdout=log_text,
            stderr="",
        )
        for line_id in normalized_line_order(selected_lines)
    ]


def build_session_fatal_run_results(
    selected_lines: list[str],
    *,
    error: SessionFatalError,
) -> list[RunResult]:
    return [
        RunResult(
            line_id=line_id,
            status="failure",
            status_label=EXECUTION_LABELS["failure"],
            ui_reason_code=error.ui_reason_code,
            ui_reason_detail=dict(error.detail),
            returncode=1,
            stdout="",
            stderr="",
            run_id="",
            run_dir="",
            run_manifest="",
            changed_ratio="",
        )
        for line_id in normalized_line_order(selected_lines)
    ]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def serialize_precheck_results(results: list[PrecheckResult]) -> list[dict[str, object]]:
    return [asdict(result) for result in results]


def serialize_run_results(results: list[RunResult]) -> list[dict[str, object]]:
    return [asdict(result) for result in results]
