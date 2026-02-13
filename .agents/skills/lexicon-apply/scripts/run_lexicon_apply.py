#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path as _Path
_REPO_ROOT = _Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from belle.lexicon_manager import apply_label_queue_adds


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--learned-weight", type=float, default=0.85)
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    pending_dir = repo_root / "lexicon" / "pending"
    queue_csv = pending_dir / "label_queue.csv"
    queue_state = pending_dir / "label_queue_state.json"
    applied_log = pending_dir / "applied_log.jsonl"

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
        },
    }
    pending_dir.mkdir(parents=True, exist_ok=True)
    (pending_dir / f"apply_run_{ts}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] added={summary.added} skipped={summary.skipped} removed_from_queue={summary.removed_from_queue}")
    if summary.errors:
        print("[ERR] " + " | ".join(summary.errors))


if __name__ == "__main__":
    main()
