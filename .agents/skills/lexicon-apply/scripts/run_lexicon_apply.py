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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--learned-weight", type=float, default=0.85)
    ap.add_argument("--show-paths", action="store_true", help="Print lexicon/pending read-write paths and continue")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    pending_dir = repo_root / "lexicon" / "pending"
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
        print("[INFO] label_queue.csv が未作成だったため初期化しました。適用対象はありません。")
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
    (pending_dir / f"apply_run_{ts}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] added={summary.added} skipped={summary.skipped} removed_from_queue={summary.removed_from_queue}")
    if summary.errors:
        print("[ERR] " + " | ".join(summary.errors))
    return 0


if __name__ == "__main__":
    sys.exit(main())
