#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from belle.application.collect import (
    ALL_LINE_ID,
    ALL_MODE_LINE_ORDER,
    CollectPlan,
    CollectRequest,
    CollectResult,
    JST,
    LINE_ARG_CHOICES,
    execute_collect_plan,
    normalize_client_ids,
    normalize_run_refs,
    parse_jst_date,
    parse_time_range,
    prepare_collect_plan,
)

PREVIEW_LIMIT = 200


def _confirm() -> bool:
    answer = input("この条件で収集ZIPを作成しますか? (y/N) ").strip().lower()
    return answer in {"y", "yes"}


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Collect per-run deliverables across clients and export a single zip under exports/collect/."
    )
    ap.add_argument(
        "--line",
        choices=LINE_ARG_CHOICES,
        default=ALL_LINE_ID,
        help="Document processing line_id. Default: all",
    )
    ap.add_argument("--date", help="JST date filter (YYYY-MM-DD). Default: today JST.", default=None)
    ap.add_argument(
        "--client",
        help="Comma-separated client IDs filter. Default: all clients except TEMPLATE.",
        default=None,
    )
    ap.add_argument(
        "--time",
        dest="time_range",
        help="JST time range filter (HH:MM-HH:MM). Default: full day.",
        default=None,
    )
    ap.add_argument(
        "--run-ref",
        action="append",
        dest="run_refs",
        help="Exact run reference(s) in CLIENT_ID:RUN_ID format. Repeatable or comma-separated.",
        default=None,
    )
    ap.add_argument("--yes", action="store_true", help="Skip confirmation prompt.")
    return ap.parse_args(list(argv) if argv is not None else None)


def _preview_header(plan: CollectPlan) -> tuple[str, str]:
    client_label = ",".join(plan.filter_client_ids) if plan.filter_client_ids else "ALL"
    time_label = plan.filter_time_range if plan.filter_time_range else "full-day"
    return client_label, time_label


def _print_preview(plan: CollectPlan) -> None:
    client_label, time_label = _preview_header(plan)
    line = plan.lines[0]
    print("[INFO] 収集条件")
    print(f"  - line: {line.line_id}")
    print(f"  - date(JST): {plan.target_jst_date}")
    print(f"  - clients: {client_label}")
    print(f"  - time(JST): {time_label}")
    print(f"  - selected_clients: {len(plan.selected_client_ids)}")
    print(f"  - matched_runs: {line.matched_count}")
    print(f"  - collected_runs: {line.collected_count}")
    print(f"  - skipped_incomplete_runs: {line.skipped_incomplete_count}")
    if line.skipped_invalid_run_id_count > 0:
        print(f"  - skipped_invalid_run_id_count: {line.skipped_invalid_run_id_count}")

    print("[INFO] Preview (client_id | run_id | layout | replaced_count | report_count | manifest_count)")
    for idx, run in enumerate(line.collected_runs):
        if idx >= PREVIEW_LIMIT:
            break
        print(
            f"{run.client_id} | {run.run_id} | {run.layout} | "
            f"{run.replaced_count} | {run.report_count} | {run.manifest_count}"
        )
    omitted = len(line.collected_runs) - PREVIEW_LIMIT
    if omitted > 0:
        print(f"[INFO] preview truncated: +{omitted}")


def _print_preview_all(plan: CollectPlan) -> None:
    client_label, time_label = _preview_header(plan)
    print("[INFO] 収集条件")
    print(f"  - line: {ALL_LINE_ID}")
    print(f"  - date(JST): {plan.target_jst_date}")
    print(f"  - clients: {client_label}")
    print(f"  - time(JST): {time_label}")
    print(f"  - selected_clients: {len(plan.selected_client_ids)}")
    print(f"  - target_lines: {','.join(ALL_MODE_LINE_ORDER)}")
    print("[INFO] line summary")
    for line in plan.lines:
        run_refs = list(line.collected_run_refs)
        if len(run_refs) > 12:
            run_ref_text = ", ".join(run_refs[:12]) + f", ... (+{len(run_refs) - 12})"
        else:
            run_ref_text = ", ".join(run_refs) if run_refs else "-"
        status = "included" if line.collected_runs else "skipped(no eligible runs)"
        print(
            f"  - {line.line_id}: status={status}, matched={line.matched_count}, "
            f"collected={line.collected_count}, skipped_incomplete={line.skipped_incomplete_count}, "
            f"invalid_run_id={line.skipped_invalid_run_id_count}"
        )
        print(f"    run_ids: {run_ref_text}")


def _render_success(result: CollectResult) -> None:
    summary = result.manifest_summary
    print("[OK] 収集ZIPを作成しました。")
    print(f"[OK] ZIP: {result.zip_path}")
    print(f"[OK] LATEST: {result.latest_path}")
    print(
        "[OK] 件数: runs={runs}, csv={csv}, reports={reports}, manifests={manifests}, items={items}".format(
            runs=summary.get("collected_runs", 0),
            csv=summary.get("csv_files", 0),
            reports=summary.get("report_files", 0),
            manifests=summary.get("manifest_files", 0),
            items=summary.get("items", 0),
        )
    )


def _request_from_args(args: argparse.Namespace, default_jst_date: str) -> CollectRequest:
    client_raw = args.client
    date_raw = args.date
    time_raw = args.time_range
    run_ref_raw = args.run_refs

    if not args.yes:
        if not run_ref_raw and not sys.stdin.isatty():
            raise ValueError("interactive input unavailable. Use --yes.")
        if not run_ref_raw and client_raw is None:
            client_raw = input("対象クライアントID (カンマ区切り。空で全件): ").strip()
        if not run_ref_raw and date_raw is None:
            date_raw = input(f"対象日付(JST, YYYY-MM-DD。空で{default_jst_date}): ").strip()
        if not run_ref_raw and time_raw is None:
            time_raw = input("対象時間帯(JST, HH:MM-HH:MM。空で終日): ").strip()

    requested_run_refs = normalize_run_refs(run_ref_raw)
    if requested_run_refs:
        return CollectRequest(line_id=args.line, requested_run_refs=requested_run_refs)

    target_jst_date = parse_jst_date(date_raw, default_date=datetime.strptime(default_jst_date, "%Y-%m-%d").date())
    parsed_time_range = parse_time_range(time_raw)
    return CollectRequest(
        line_id=args.line,
        target_jst_date=target_jst_date.isoformat(),
        client_ids=normalize_client_ids(client_raw),
        time_range=parsed_time_range[2] if parsed_time_range is not None else "",
    )


def main(argv: Optional[Sequence[str]] = None, *, repo_root: Optional[Path] = None) -> int:
    args = _parse_args(argv)
    repo = repo_root or Path(__file__).resolve().parents[4]
    default_jst_date = datetime.now(JST).date().isoformat()

    try:
        request = _request_from_args(args, default_jst_date)
        plan = prepare_collect_plan(repo, request)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    if plan.ui_reason_code == "COLLECT_FAIL_MISSING_RUN_REFS":
        print(f"[ERROR] {plan.message}", file=sys.stderr)
        return 2

    if plan.line_id == ALL_LINE_ID:
        _print_preview_all(plan)
    else:
        _print_preview(plan)

    if not plan.can_collect:
        if plan.status == "skip":
            print(f"[INFO] {plan.message}")
            return 0
        print(f"[ERROR] {plan.message}", file=sys.stderr)
        return 1

    if not args.yes and not _confirm():
        print("[INFO] user canceled. skip.")
        return 0

    try:
        result = execute_collect_plan(repo, plan)
    except Exception as exc:
        print(f"[ERROR] unexpected collect failure: {exc}", file=sys.stderr)
        return 1

    if not result.ok:
        if result.ui_reason_code == "COLLECT_FAIL_MISSING_RUN_REFS":
            print("[ERROR] requested run_refs not found in collected ZIP", file=sys.stderr)
            return 2
        print(f"[ERROR] collect failed: {result.ui_reason_code}", file=sys.stderr)
        return 1

    _render_success(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
