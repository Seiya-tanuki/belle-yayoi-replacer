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

from belle.lexicon import load_lexicon
from belle.build_client_cache import ensure_client_cache_updated


def find_client_id_auto(repo_root: Path) -> str:
    clients_dir = repo_root / "clients"
    cands = []
    for tdir in clients_dir.iterdir():
        if not tdir.is_dir() or tdir.name == "TEMPLATE":
            continue
        ref = tdir / "inputs" / "ledger_ref"
        if ref.exists() and (list(ref.glob("*.csv")) or list(ref.glob("*.txt"))):
            cands.append(tdir.name)
    if len(cands) == 1:
        return cands[0]
    if not cands:
        raise SystemExit("Could not auto-detect client: no ledger_ref *.csv or *.txt files found.")
    raise SystemExit(f"Could not auto-detect client: multiple candidates found: {cands}. Use --client.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", default=None)
    ap.add_argument("--config", default="rulesets/replacer_config_v1_15.json", help="Replacer config JSON (thresholds reused)")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    client_id = args.client or find_client_id_auto(repo_root)

    client_dir = repo_root / "clients" / client_id
    artifacts_dir = client_dir / "artifacts"
    reports_dir = artifacts_dir / "reports"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")
    config_path = (repo_root / args.config) if not Path(args.config).is_absolute() else Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))

    tm, summary = ensure_client_cache_updated(
        repo_root=repo_root,
        client_id=client_id,
        lex=lex,
        config=config,
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_manifest = {
        "schema": "belle.client_cache_update_run.v1",
        "version": "1.15",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "summary": {
            "ingested_new_files": len(summary.ingested_new_files),
            "applied_new_files": len(summary.applied_new_files),
            "rows_total_added": summary.rows_total_added,
            "rows_used_added": summary.rows_used_added,
            "warnings": summary.warnings,
        },
        "paths": {
            "client_cache": summary.client_cache_path,
            "ingest_manifest": summary.ingest_manifest_path,
        },
    }
    (reports_dir / f"client_cache_update_run_{ts}.json").write_text(json.dumps(out_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] client={client_id} applied_new_files={len(summary.applied_new_files)} t_numbers={len(tm.t_numbers)} t_by_cat={len(tm.t_numbers_by_category)} vendor_keys={len(tm.vendor_keys)} categories={len(tm.categories)}")
    if summary.warnings:
        print("[WARN] " + " | ".join(summary.warnings))


if __name__ == "__main__":
    main()

