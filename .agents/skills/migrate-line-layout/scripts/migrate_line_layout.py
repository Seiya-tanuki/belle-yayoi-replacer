#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from belle.lines import validate_line_id
from belle.migration import (
    MigrationError,
    MigrationSafetyError,
    migrate_legacy_pending_to_receipt,
    migrate_receipt_client_layout,
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_iso(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _utc_compact(ts: datetime) -> str:
    return ts.strftime("%Y%m%d_%H%M%S")


def _parse_bool(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError("expected one of: true/false")


def _discover_clients(repo_root: Path) -> list[str]:
    clients_dir = repo_root / "clients"
    if not clients_dir.exists():
        return []
    values: list[str] = []
    for path in sorted(clients_dir.iterdir(), key=lambda p: p.name):
        if not path.is_dir():
            continue
        if path.name == "TEMPLATE":
            continue
        values.append(path.name)
    return values


def _resolve_client_ids(repo_root: Path, raw: str | None) -> list[str]:
    if raw is None:
        return []
    token = str(raw).strip()
    if not token:
        raise ValueError("--client requires a non-empty value")
    if token.upper() == "ALL":
        return _discover_clients(repo_root)
    return [token]


def _format_client_plan(result: dict) -> str:
    op_count = len(result.get("operations", []))
    return (
        f"[PLAN] client={result.get('client_id')} status={result.get('status')} "
        f"legacy_dirs={len(result.get('legacy_dirs', []))} ops={op_count} "
        f"mode={result.get('mode')} reason={result.get('reason')}"
    )


def _format_pending_plan(result: dict) -> str:
    op_count = len(result.get("operations", []))
    return (
        f"[PLAN] pending status={result.get('status')} ops={op_count} "
        f"skipped_locks={len(result.get('skipped_locks', []))} "
        f"reason={result.get('reason')}"
    )


def _format_client_apply(result: dict) -> str:
    op_count = len(result.get("operations", []))
    return (
        f"[OK] client={result.get('client_id')} status={result.get('status')} "
        f"ops={op_count} mode={result.get('mode')}"
    )


def _format_pending_apply(result: dict) -> str:
    op_count = len(result.get("operations", []))
    return (
        f"[OK] pending status={result.get('status')} "
        f"ops={op_count} mode={result.get('mode')}"
    )


def _write_run_report(
    *,
    repo_root: Path,
    args: argparse.Namespace,
    started_at: datetime,
    finished_at: datetime,
    client_results: list[dict],
    pending_result: dict | None,
    errors: list[str],
    exit_code: int,
) -> Path:
    out_dir = repo_root / "exports" / "migrations"
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / f"migrate_line_layout_{_utc_compact(finished_at)}.md"

    summary = {
        "schema": "belle.migrate_line_layout_report.v1",
        "started_at_utc": _utc_iso(started_at),
        "finished_at_utc": _utc_iso(finished_at),
        "argv": {
            "client": args.client,
            "migrate_pending": bool(args.migrate_pending),
            "mode": args.mode,
            "apply": bool(args.apply),
            "dry_run": bool(args.dry_run),
            "line": args.line,
        },
        "summary": {
            "clients_requested": len(client_results),
            "client_planned_or_applied": sum(1 for row in client_results if row.get("status") in {"planned", "applied"}),
            "client_noop": sum(1 for row in client_results if row.get("status") == "noop"),
            "pending_requested": bool(args.migrate_pending),
            "pending_status": pending_result.get("status") if pending_result is not None else None,
            "errors": len(errors),
            "exit_code": exit_code,
        },
        "client_results": client_results,
        "pending_result": pending_result,
        "errors": errors,
    }

    body = [
        "# migrate-line-layout report",
        "",
        f"- started_at_utc: {_utc_iso(started_at)}",
        f"- finished_at_utc: {_utc_iso(finished_at)}",
        f"- line: {args.line}",
        f"- mode: {args.mode}",
        f"- apply: {bool(args.apply)}",
        f"- dry_run: {bool(args.dry_run)}",
        f"- exit_code: {exit_code}",
        "",
        "```json",
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
    ]
    report_path.write_text("\n".join(body), encoding="utf-8", newline="\n")
    return report_path


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safely migrate legacy receipt client layout and/or legacy lexicon pending to line-scoped paths."
    )
    parser.add_argument(
        "--client",
        default=None,
        help="Target client ID or ALL. Required for client layout migration.",
    )
    parser.add_argument(
        "--migrate-pending",
        action="store_true",
        help="Also migrate legacy lexicon/pending files into lexicon/receipt/pending.",
    )
    parser.add_argument("--mode", choices=("copy", "move"), default="copy", help="Migration mode.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply filesystem changes. Without this flag, no changes are made.",
    )
    parser.add_argument(
        "--dry-run",
        type=_parse_bool,
        default=True,
        help="true|false. Default true. Use --apply --dry-run false to execute.",
    )
    parser.add_argument("--line", default="receipt", help="Line ID. Phase 2 supports receipt only.")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(__file__).resolve().parents[4]
    started_at = _now_utc()

    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2
    if line_id != "receipt":
        print("[ERROR] only --line receipt is supported in Phase 2", file=sys.stderr)
        return 2
    if args.apply and args.dry_run:
        print("[ERROR] --apply requires --dry-run false", file=sys.stderr)
        return 2
    if args.client is None and not args.migrate_pending:
        print("[ERROR] either --client or --migrate-pending is required", file=sys.stderr)
        return 2

    client_results: list[dict] = []
    pending_result: dict | None = None
    errors: list[str] = []
    stop_due_to_safety = False

    try:
        client_ids = _resolve_client_ids(repo_root, args.client)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    if args.client is not None and args.client.strip().upper() == "ALL" and not client_ids:
        print("[INFO] no clients found under clients/ (excluding TEMPLATE)")

    if client_ids:
        for client_id in client_ids:
            try:
                result = migrate_receipt_client_layout(
                    repo_root=repo_root,
                    client_id=client_id,
                    mode=args.mode,
                    apply=bool(args.apply),
                    dry_run=bool(args.dry_run),
                )
                client_results.append(result)
                if args.dry_run:
                    print(_format_client_plan(result))
                else:
                    print(_format_client_apply(result))
            except MigrationSafetyError as exc:
                message = f"client={client_id} safety_blocked: {exc}"
                errors.append(message)
                print(f"[SAFE-EXIT] {message}", file=sys.stderr)
                stop_due_to_safety = True
                break
            except MigrationError as exc:
                message = f"client={client_id} migration_failed: {exc}"
                errors.append(message)
                print(f"[ERROR] {message}", file=sys.stderr)
                break
            except Exception as exc:  # pragma: no cover - defensive
                message = f"client={client_id} unexpected_error: {type(exc).__name__}: {exc}"
                errors.append(message)
                print(f"[ERROR] {message}", file=sys.stderr)
                break

    if args.migrate_pending and not stop_due_to_safety and not errors:
        try:
            pending_result = migrate_legacy_pending_to_receipt(
                repo_root=repo_root,
                mode=args.mode,
                apply=bool(args.apply),
                dry_run=bool(args.dry_run),
            )
            if args.dry_run:
                print(_format_pending_plan(pending_result))
            else:
                print(_format_pending_apply(pending_result))
        except MigrationSafetyError as exc:
            message = f"pending safety_blocked: {exc}"
            errors.append(message)
            print(f"[SAFE-EXIT] {message}", file=sys.stderr)
        except MigrationError as exc:
            message = f"pending migration_failed: {exc}"
            errors.append(message)
            print(f"[ERROR] {message}", file=sys.stderr)
        except Exception as exc:  # pragma: no cover - defensive
            message = f"pending unexpected_error: {type(exc).__name__}: {exc}"
            errors.append(message)
            print(f"[ERROR] {message}", file=sys.stderr)

    exit_code = 1 if errors else 0
    finished_at = _now_utc()
    report_path = _write_run_report(
        repo_root=repo_root,
        args=args,
        started_at=started_at,
        finished_at=finished_at,
        client_results=client_results,
        pending_result=pending_result,
        errors=errors,
        exit_code=exit_code,
    )

    if args.dry_run:
        print(
            "[SUMMARY] dry-run clients={clients} pending={pending} errors={errors}".format(
                clients=len(client_results),
                pending=1 if pending_result is not None else 0,
                errors=len(errors),
            )
        )
    else:
        print(
            "[SUMMARY] applied clients={clients} pending={pending} errors={errors}".format(
                clients=len(client_results),
                pending=1 if pending_result is not None else 0,
                errors=len(errors),
            )
        )
    print(f"[INFO] report: {report_path}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
