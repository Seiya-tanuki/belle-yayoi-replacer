#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path as _Path

_REPO_ROOT = _Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

import argparse
from pathlib import Path

from belle.line_runners import (
    LinePlan,
    plan_bank,
    plan_card,
    plan_receipt,
    run_bank,
    run_card,
    run_receipt,
)
from belle.ui_reason_codes import (
    RUN_OK,
    build_ui_reason_event,
    precheck_reason_code_for,
    run_failure_reason_code_for,
)

LINE_ID_CARD = "credit_card_statement"
LINE_ORDER = ["receipt", "bank_statement", LINE_ID_CARD]


def _resolve_config_path(repo_root: Path, config_arg: str) -> Path:
    path = Path(config_arg)
    if path.is_absolute():
        return path
    return repo_root / path


def _format_target_files(target_files: list[str]) -> str:
    if not target_files:
        return "-"
    return ", ".join(target_files)


def _print_plan(client_id: str, requested_line: str, plans: list[LinePlan]) -> None:
    print(f"[PLAN] client={client_id} line={requested_line}")
    for plan in plans:
        print(
            f"- {plan.line_id}: {plan.status} ({plan.reason}) "
            f"target=[{_format_target_files(plan.target_files)}]"
        )
        print(
            build_ui_reason_event(
                precheck_reason_code_for(plan.line_id, plan.status, plan.reason),
                line_id=plan.line_id,
                detail={"phase": "plan", "status": plan.status, "reason": plan.reason},
            )
        )


def _find_plan(plans: list[LinePlan], line_id: str) -> LinePlan | None:
    for plan in plans:
        if plan.line_id == line_id:
            return plan
    return None


def _expected_cc_config_path(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / LINE_ID_CARD / "config" / "credit_card_line_config.json"


def _enforce_cc_config_required(repo_root: Path, client_id: str, plan: LinePlan) -> LinePlan:
    if plan.line_id != LINE_ID_CARD or plan.status == "FAIL":
        return plan
    expected_path = _expected_cc_config_path(repo_root, client_id)
    if expected_path.exists():
        return plan
    details = dict(plan.details or {})
    details["expected_cc_config_path"] = str(expected_path)
    return LinePlan(
        line_id=plan.line_id,
        status="FAIL",
        reason=f"missing_cc_config: expected={expected_path}",
        target_files=list(plan.target_files),
        details=details,
    )


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
    ap.add_argument(
        "--config",
        help="Replacer config JSON path",
        default="rulesets/receipt/replacer_config_v1_15.json",
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

    config_path = _resolve_config_path(repo_root, args.config)
    selected_lines = LINE_ORDER if args.line == "all" else [args.line]

    plans: list[LinePlan] = []
    for line_id in selected_lines:
        if line_id == "receipt":
            plans.append(plan_receipt(repo_root, client_id, config_path=config_path))
            continue
        if line_id == "bank_statement":
            plans.append(plan_bank(repo_root, client_id))
            continue
        card_plan = plan_card(repo_root, client_id)
        plans.append(_enforce_cc_config_required(repo_root, client_id, card_plan))

    _print_plan(client_id, args.line, plans)

    fail_plans = [p for p in plans if p.status == "FAIL"]
    if fail_plans:
        for plan in fail_plans:
            print(
                build_ui_reason_event(
                    run_failure_reason_code_for(plan.line_id, plan.reason),
                    line_id=plan.line_id,
                    detail={"phase": "plan_gate", "status": plan.status, "reason": plan.reason},
                )
            )
        print("[ERROR] PLAN contains FAIL. Fix inputs/config and rerun (use --dry-run to only inspect).")
        return 1

    if args.dry_run:
        return 0

    run_plans = [p for p in plans if p.status == "RUN"]
    if not run_plans:
        print("[OK] nothing to do")
        return 0

    confirm_rc = _confirm_or_exit(force_yes=bool(args.yes))
    if confirm_rc != 0:
        return confirm_rc

    outcomes: dict[str, dict[str, object]] = {}
    for line_id in LINE_ORDER:
        plan = _find_plan(run_plans, line_id)
        if plan is None:
            continue

        details = plan.details or {}
        try:
            if line_id == "receipt":
                raw_layout = details.get("client_layout_line_id")
                if raw_layout != "receipt":
                    raise RuntimeError(f"invalid receipt layout marker: {raw_layout}")
                client_dir_raw = str(details.get("client_dir") or "")
                if not client_dir_raw:
                    raise RuntimeError("missing client_dir in receipt plan")
                outcomes[line_id] = run_receipt(
                    repo_root,
                    client_id,
                    client_layout_line_id=raw_layout,
                    client_dir=Path(client_dir_raw),
                    config_path=config_path,
                )
            elif line_id == "bank_statement":
                client_dir_raw = str(details.get("client_dir") or "")
                if not client_dir_raw:
                    raise RuntimeError("missing client_dir in bank_statement plan")
                outcomes[line_id] = run_bank(
                    repo_root,
                    client_id,
                    client_dir=Path(client_dir_raw),
                )
            else:
                outcomes[line_id] = run_card(repo_root, client_id)
            print(
                build_ui_reason_event(
                    RUN_OK,
                    line_id=line_id,
                    detail={"phase": "run", "status": "success"},
                )
            )
        except Exception as exc:
            print(f"[ERROR] {line_id} run failed: {exc}")
            print(
                build_ui_reason_event(
                    run_failure_reason_code_for(line_id, str(exc)),
                    line_id=line_id,
                    detail={"phase": "run", "status": "failure", "error": str(exc)},
                )
            )
            return 1

    print(f"[OK] done client={client_id}")
    for plan in plans:
        if plan.status == "RUN":
            out = outcomes.get(plan.line_id) or {}
            changed_ratio = float(out.get("changed_ratio") or 0.0)
            print(
                f"- {plan.line_id}: DONE run_id={out.get('run_id', '')} "
                f"changed_ratio={changed_ratio:.3f}"
            )
        elif plan.status == "SKIP":
            print(f"- {plan.line_id}: SKIPPED {plan.reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
