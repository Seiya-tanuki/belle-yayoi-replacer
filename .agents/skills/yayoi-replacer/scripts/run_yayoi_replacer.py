#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path as _Path

_REPO_ROOT = _Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

import argparse
from pathlib import Path

from belle.application import (
    LinePlan,
    ReplacerPlanResult,
    ReplacerRunFailedError,
    ReplacerRunResult,
    RunLineResult,
    plan_replacer,
    run_replacer,
)
from belle.ui_reason_codes import RUN_FAIL_UNKNOWN, build_ui_reason_event

LINE_ID_CARD = "credit_card_statement"


def _format_target_files(target_files: tuple[str, ...]) -> str:
    if not target_files:
        return "-"
    return ", ".join(target_files)


def _print_plan(plan_result: ReplacerPlanResult) -> None:
    print(f"[PLAN] client={plan_result.client_id} line={plan_result.requested_line}")
    for plan in plan_result.plans:
        print(
            f"- {plan.line_id}: {plan.status} ({plan.reason}) "
            f"target=[{_format_target_files(plan.target_files)}]"
        )
        print(
            build_ui_reason_event(
                plan.ui_reason_code,
                line_id=plan.line_id,
                detail=plan.ui_reason_detail,
            )
        )


def _find_result(run_result: ReplacerRunResult, line_id: str) -> RunLineResult | None:
    for result in run_result.line_results:
        if result.line_id == line_id:
            return result
    return None


def _print_run_result(client_id: str, result: RunLineResult) -> None:
    print(
        f"[OK] client={client_id} run_id={result.run_id} "
        f"inputs={result.input_count} outputs={result.output_count}"
    )
    print(f"[OK] run_dir={result.run_dir}")
    print(f"[OK] run_manifest={result.run_manifest_path}")
    if result.line_id == "bank_statement":
        bank_cache_update = result.details.get("bank_cache_update")
        if isinstance(bank_cache_update, dict):
            print(
                "[OK] bank_cache"
                f" pairs_used={int(bank_cache_update.get('pairs_unique_used_total') or 0)}"
                f" cache={bank_cache_update.get('cache_path') or ''}"
            )
    print(f" - changed_ratio={result.changed_ratio:.3f} output={result.output_file}")
    if result.warnings:
        print("[WARN] " + " | ".join(str(v) for v in result.warnings))
    print(
        build_ui_reason_event(
            result.ui_reason_code,
            line_id=result.line_id,
            detail=result.ui_reason_detail,
        )
    )
    if result.needs_review and result.reason:
        print(result.reason)


def _plan_gate_failure_detail(plan: LinePlan) -> dict[str, object]:
    detail = dict(plan.ui_reason_detail)
    if not detail:
        detail = {"status": plan.status, "reason": plan.reason}
    detail["phase"] = "plan_gate"
    detail.setdefault("status", plan.status)
    detail.setdefault("reason", plan.reason)
    if plan.reason_key:
        detail.setdefault("reason_key", plan.reason_key)
    return detail


def _run_failure_detail(exc: ReplacerRunFailedError) -> dict[str, object]:
    return dict(exc.ui_reason_detail) or {"phase": "run", "status": "failure", "error": str(exc)}


def _confirm_or_exit(*, force_yes: bool) -> int:
    if force_yes:
        return 0
    if not sys.stdin or not sys.stdin.isatty():
        print("[ERROR] non-interactive run requires --yes (or use --dry-run)")
        return 2
    try:
        ans = input("Proceed with RUN lines? [y/N] ").strip().lower()
    except EOFError:
        print("[ERROR] non-interactive run requires --yes (or use --dry-run)")
        return 2
    if ans in {"y", "yes"}:
        return 0
    return 2


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True, help="Client ID under clients/<CLIENT_ID>/")
    ap.add_argument(
        "--line",
        default="all",
        choices=["receipt", "bank_statement", LINE_ID_CARD, "all"],
        help="Document processing line_id",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print PLAN and exit")
    ap.add_argument("--yes", action="store_true", help="Skip interactive confirmation")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    client_id = str(args.client or "").strip()
    if not client_id:
        print("[ERROR] 置換を実行するクライアントのディレクトリ名（--client）を指定してください。")
        print("例: $yayoi-replacer --client <CLIENT_ID>")
        return 2

    plan_result = plan_replacer(
        repo_root,
        client_id,
        requested_line=args.line,
    )
    _print_plan(plan_result)

    fail_plans = [plan for plan in plan_result.plans if plan.status == "FAIL"]
    if fail_plans:
        for plan in fail_plans:
            print(
                build_ui_reason_event(
                    plan.run_failure_ui_reason_code or RUN_FAIL_UNKNOWN,
                    line_id=plan.line_id,
                    detail=_plan_gate_failure_detail(plan),
                )
            )
        print("[ERROR] PLAN contains FAIL. Fix inputs/config and rerun (use --dry-run to only inspect).")
        return 1

    if args.dry_run:
        return 0

    if not plan_result.runnable_plans:
        print("[OK] nothing to do")
        return 0

    confirm_rc = _confirm_or_exit(force_yes=bool(args.yes))
    if confirm_rc != 0:
        return confirm_rc

    try:
        run_result = run_replacer(
            repo_root,
            client_id,
            plan_result=plan_result,
        )
    except ReplacerRunFailedError as exc:
        partial_result = ReplacerRunResult(
            client_id=client_id,
            requested_line=plan_result.requested_line,
            plan_result=plan_result,
            line_results=exc.partial_results,
            stopped_early=True,
        )
        for line_result in partial_result.line_results:
            _print_run_result(client_id, line_result)
        print(f"[ERROR] {exc.line_id} run failed: {exc}")
        print(
            build_ui_reason_event(
                exc.ui_reason_code or RUN_FAIL_UNKNOWN,
                line_id=exc.line_id,
                detail=_run_failure_detail(exc),
            )
        )
        return 1

    for result in run_result.line_results:
        _print_run_result(client_id, result)

    if run_result.has_needs_review:
        return 2

    print(f"[OK] done client={client_id}")
    for plan in plan_result.plans:
        if plan.status == "RUN":
            result = _find_result(run_result, plan.line_id)
            if result is None:
                continue
            print(
                f"- {plan.line_id}: DONE run_id={result.run_id} "
                f"changed_ratio={result.changed_ratio:.3f}"
            )
        elif plan.status == "SKIP":
            print(f"- {plan.line_id}: SKIPPED {plan.reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
