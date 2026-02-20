#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path as _Path

_REPO_ROOT = _Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from belle.lines import line_asset_paths, validate_line_id
from belle.lexicon_manager import LABEL_QUEUE_COLUMNS, apply_label_queue_adds


def ensure_pending_workspace(pending_dir: Path, queue_csv: Path, applied_log: Path, lock_dir: Path) -> bool:
    pending_dir.mkdir(parents=True, exist_ok=True)
    lock_dir.mkdir(parents=True, exist_ok=True)
    applied_log.touch(exist_ok=True)
    if queue_csv.exists():
        return False
    with queue_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(LABEL_QUEUE_COLUMNS)
    return True


def exit_code_from_summary_errors(errors: list[str]) -> int:
    return 1 if errors else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--line", default="receipt", help="Document processing line_id")
    ap.add_argument("--learned-weight", type=float, default=0.85)
    ap.add_argument("--show-paths", action="store_true", help="Print lexicon/<line_id>/pending read-write paths and continue")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        return 2
    if line_id != "receipt":
        print(f"[ERROR] This skill is receipt-only. {line_id} is not supported.")
        return 2

    assets = line_asset_paths(repo_root, line_id)
    lexicon_path = assets["lexicon_path"]
    pending_dir = assets["pending_dir"]
    queue_csv = pending_dir / "label_queue.csv"
    queue_state = pending_dir / "label_queue_state.json"
    applied_log = pending_dir / "applied_log.jsonl"
    lock_dir = pending_dir / "locks"

    created_queue = ensure_pending_workspace(pending_dir, queue_csv, applied_log, lock_dir)
    if args.show_paths:
        print(
            f"[PATHS] lexicon={lexicon_path} queue_csv={queue_csv} queue_state={queue_state} "
            f"applied_log={applied_log} lock={lock_dir / 'label_queue.lock'}"
        )
    if created_queue:
        print("[INFO] label_queue.csv was initialized. No rows to apply.")
        return 0

    summary = apply_label_queue_adds(
        lexicon_path=lexicon_path,
        queue_csv_path=queue_csv,
        queue_state_path=queue_state,
        applied_log_path=applied_log,
        learned_weight=float(args.learned_weight),
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = {
        "schema": "belle.lexicon_apply_run.v1",
        "version": "1.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "line_id": line_id,
        "result": {
            "added": summary.added,
            "skipped": summary.skipped,
            "removed_from_queue": summary.removed_from_queue,
            "errors": summary.errors,
        },
        "paths": {
            "lexicon": str(lexicon_path),
            "label_queue_csv": str(queue_csv),
            "applied_log": str(applied_log),
            "label_queue_lock": str(lock_dir / "label_queue.lock"),
        },
    }
    (pending_dir / f"apply_run_{ts}.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[OK] added={summary.added} skipped={summary.skipped} removed_from_queue={summary.removed_from_queue}")
    if summary.errors:
        print("[ERR] " + " | ".join(summary.errors))
    return exit_code_from_summary_errors(summary.errors)


if __name__ == "__main__":
    sys.exit(main())
