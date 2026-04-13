from __future__ import annotations

from pathlib import Path

from belle.application.errors import (
    InvalidRequestedLineError,
    LineRunnerFailure,
    ReplacerPlanBlockedError,
    ReplacerRunFailedError,
)
from belle.application.models import (
    LinePlan,
    ReplacerPlanResult,
    ReplacerRunResult,
    RequestedLine,
    RunLineResult,
)
from belle.line_runners.bank_statement import plan_bank, run_bank
from belle.line_runners.credit_card_statement import plan_card, run_card
from belle.line_runners.receipt import plan_receipt, run_receipt
from belle.ui_reason_codes import (
    PRECHECK_FAIL_CARD_CONFIG_MISSING,
    RUN_FAIL_CARD_CONFIG_MISSING,
    RUN_FAIL_UNKNOWN,
)

LINE_ORDER = ("receipt", "bank_statement", "credit_card_statement")


def _selected_lines(requested_line: RequestedLine) -> tuple[str, ...]:
    if requested_line == "all":
        return LINE_ORDER
    if requested_line in LINE_ORDER:
        return (requested_line,)
    raise InvalidRequestedLineError(f"unsupported requested line: {requested_line}")


def _expected_cc_config_path(repo_root: Path, client_id: str) -> Path:
    return (
        repo_root
        / "clients"
        / client_id
        / "lines"
        / "credit_card_statement"
        / "config"
        / "credit_card_line_config.json"
    )


def _enforce_cc_config_required(repo_root: Path, client_id: str, plan: LinePlan) -> LinePlan:
    if plan.line_id != "credit_card_statement" or plan.status == "FAIL":
        return plan
    expected_path = _expected_cc_config_path(repo_root, client_id)
    if expected_path.exists():
        return plan
    details = dict(plan.details)
    details["expected_cc_config_path"] = str(expected_path)
    reason = f"missing_cc_config: expected={expected_path}"
    return LinePlan(
        line_id=plan.line_id,
        status="FAIL",
        reason=reason,
        reason_key="missing_cc_config",
        target_files=plan.target_files,
        ui_reason_code=PRECHECK_FAIL_CARD_CONFIG_MISSING,
        ui_reason_detail={"phase": "plan", "status": "FAIL", "reason": reason, "reason_key": "missing_cc_config"},
        run_failure_ui_reason_code=RUN_FAIL_CARD_CONFIG_MISSING,
        details=details,
    )


def plan_replacer(
    repo_root: Path,
    client_id: str,
    *,
    requested_line: RequestedLine,
) -> ReplacerPlanResult:
    plans: list[LinePlan] = []
    for line_id in _selected_lines(requested_line):
        if line_id == "receipt":
            plans.append(plan_receipt(repo_root, client_id))
        elif line_id == "bank_statement":
            plans.append(plan_bank(repo_root, client_id))
        else:
            plans.append(_enforce_cc_config_required(repo_root, client_id, plan_card(repo_root, client_id)))
    return ReplacerPlanResult(
        client_id=client_id,
        requested_line=requested_line,
        plans=tuple(plans),
    )


def run_replacer(
    repo_root: Path,
    client_id: str,
    *,
    plan_result: ReplacerPlanResult,
) -> ReplacerRunResult:
    if plan_result.has_failures:
        raise ReplacerPlanBlockedError(plan_result)

    results: list[RunLineResult] = []
    for plan in plan_result.runnable_plans:
        details = dict(plan.details)
        try:
            if plan.line_id == "receipt":
                raw_layout = str(details.get("client_layout_line_id") or "")
                if raw_layout != "receipt":
                    raise RuntimeError(f"invalid receipt layout marker: {raw_layout}")
                client_dir_raw = str(details.get("client_dir") or "")
                if not client_dir_raw:
                    raise RuntimeError("missing client_dir in receipt plan")
                line_result = run_receipt(
                    repo_root,
                    client_id,
                    client_layout_line_id=raw_layout,
                    client_dir=Path(client_dir_raw),
                )
            elif plan.line_id == "bank_statement":
                client_dir_raw = str(details.get("client_dir") or "")
                if not client_dir_raw:
                    raise RuntimeError("missing client_dir in bank_statement plan")
                line_result = run_bank(
                    repo_root,
                    client_id,
                    client_dir=Path(client_dir_raw),
                )
            else:
                line_result = run_card(repo_root, client_id)
        except Exception as exc:
            failure_key = "unknown"
            ui_reason_code = RUN_FAIL_UNKNOWN
            ui_reason_detail = {"phase": "run", "status": "failure", "failure_key": failure_key, "error": str(exc)}
            if isinstance(exc, LineRunnerFailure):
                failure_key = exc.failure_key
                ui_reason_code = exc.ui_reason_code
                ui_reason_detail = dict(exc.ui_reason_detail)
            raise ReplacerRunFailedError(
                line_id=plan.line_id,
                message=str(exc),
                failure_key=failure_key,
                ui_reason_code=ui_reason_code,
                ui_reason_detail=ui_reason_detail,
                partial_results=tuple(results),
            ) from exc

        results.append(line_result)
        if line_result.needs_review:
            return ReplacerRunResult(
                client_id=client_id,
                requested_line=plan_result.requested_line,
                plan_result=plan_result,
                line_results=tuple(results),
                stopped_early=True,
            )

    return ReplacerRunResult(
        client_id=client_id,
        requested_line=plan_result.requested_line,
        plan_result=plan_result,
        line_results=tuple(results),
        stopped_early=False,
    )
