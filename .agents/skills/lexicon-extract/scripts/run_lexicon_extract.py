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

from belle.lexicon import load_lexicon
from belle.lexicon_manager import LABEL_QUEUE_COLUMNS, extract_unknown_terms_update_queue
from belle.ingest import ingest_csv_dir, save_manifest
from belle.paths import (
    ensure_client_system_dirs,
    get_artifacts_telemetry_dir,
    get_client_root,
    get_ledger_train_ingested_path,
)


def _list_train_files_with_txt(dir_path: Path):
    files = list(dir_path.glob("*.csv")) + list(dir_path.glob("*.txt"))
    return sorted({p.resolve(): p for p in files}.values(), key=lambda p: p.name)


def find_client_id_auto(repo_root: Path) -> str:
    clients_dir = repo_root / "clients"
    cands = []
    for tdir in clients_dir.iterdir():
        if not tdir.is_dir() or tdir.name == "TEMPLATE":
            continue
        tr = tdir / "inputs" / "ledger_train"
        if tr.exists() and _list_train_files_with_txt(tr):
            cands.append(tdir.name)
    if len(cands) == 1:
        return cands[0]
    if not cands:
        raise SystemExit("Could not auto-detect client: no clients/<CLIENT_ID>/inputs/ledger_train/*.csv or *.txt found.")
    raise SystemExit(f"Could not auto-detect client: multiple candidates found: {cands}. Use --client.")


def ensure_pending_workspace(pending_dir: Path, queue_csv: Path, queue_state: Path) -> None:
    pending_dir.mkdir(parents=True, exist_ok=True)
    if not queue_csv.exists():
        with queue_csv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(LABEL_QUEUE_COLUMNS)
    if not queue_state.exists():
        queue_state.write_text(
            json.dumps({"version": "1.0", "clients_by_norm_key": {}}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", default=None)
    ap.add_argument("--config", default="rulesets/replacer_config_v1_15.json", help="Used for dummy_summary_exact")
    ap.add_argument("--min-count-per-run", type=int, default=1)
    ap.add_argument("--show-paths", action="store_true", help="Print queue/state paths and continue")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    pending_dir = repo_root / "lexicon" / "pending"
    queue_csv = pending_dir / "label_queue.csv"
    queue_state = pending_dir / "label_queue_state.json"
    ensure_pending_workspace(pending_dir, queue_csv, queue_state)
    if args.show_paths:
        print(f"[PATHS] queue_csv={queue_csv} queue_state={queue_state}")

    client_id = args.client or find_client_id_auto(repo_root)

    client_dir = get_client_root(repo_root, client_id)
    train_dir = client_dir / "inputs" / "ledger_train"
    ensure_client_system_dirs(repo_root, client_id)
    telemetry_dir = get_artifacts_telemetry_dir(repo_root, client_id)
    telemetry_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = get_ledger_train_ingested_path(repo_root, client_id)

    # Load shared lexicon
    lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")

    # Ingest training files (rename + sha256 manifest)
    manifest, new_shas_csv, dup_shas_csv = ingest_csv_dir(
        dir_path=train_dir,
        manifest_path=manifest_path,
        client_id=client_id,
        kind="ledger_train",
        allow_rename=True,
        include_glob="*.csv",
    )
    manifest, new_shas_txt, dup_shas_txt = ingest_csv_dir(
        dir_path=train_dir,
        manifest_path=manifest_path,
        client_id=client_id,
        kind="ledger_train",
        allow_rename=True,
        include_glob="*.txt",
    )
    new_shas = new_shas_csv + new_shas_txt
    dup_shas = dup_shas_csv + dup_shas_txt

    # Determine which ingested shas are not yet processed to label_queue
    ingested = manifest.get("ingested") or {}
    ingested_order = manifest.get("ingested_order") or list(ingested.keys())

    to_process = []
    for sha in ingested_order:
        ent = ingested.get(sha) or {}
        if ent.get("processed_to_label_queue_at"):
            continue
        stored = ent.get("stored_name")
        if not stored:
            continue
        p = train_dir / stored
        if p.exists():
            to_process.append((sha, p))

    # Dummy summary contract (from config)
    config_path = (repo_root / args.config) if not Path(args.config).is_absolute() else Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    dummy = (config.get("csv_contract") or {}).get("dummy_summary_exact") or "##DUMMY_OCR_UNREADABLE##"

    # Run extraction
    summary = extract_unknown_terms_update_queue(
        client_id=client_id,
        ledger_train_files=[p for _, p in to_process],
        lex=lex,
        queue_csv_path=queue_csv,
        queue_state_path=queue_state,
        dummy_summary_exact=dummy,
        min_count_per_run=int(args.min_count_per_run),
    )

    # Mark processed shas
    now = datetime.now(timezone.utc).isoformat()
    for sha, _p in to_process:
        if sha in ingested:
            ingested[sha]["processed_to_label_queue_at"] = now
    save_manifest(manifest_path, manifest)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = {
        "schema": "belle.lexicon_extract_run.v1",
        "version": "1.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "ingest": {"new_shas": len(new_shas), "dup_shas": len(dup_shas), "processed_files": len(to_process)},
        "queue": {
            "new_norm_keys": summary.new_norm_keys,
            "updated_norm_keys": summary.updated_norm_keys,
            "terms_observed": summary.terms_observed,
            "rows_scanned": summary.rows_scanned,
        },
        "paths": {
            "label_queue_csv": str(queue_csv),
            "label_queue_state": str(queue_state),
            "train_ingest_manifest": str(manifest_path),
        },
    }
    (telemetry_dir / f"lexicon_extract_run_{ts}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] client={client_id} processed_files={len(to_process)} new_keys={summary.new_norm_keys} updated_keys={summary.updated_norm_keys}")
    if summary.warnings:
        print("[WARN] " + " | ".join(summary.warnings))


if __name__ == "__main__":
    main()

