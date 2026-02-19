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
from belle.lines import is_line_implemented, line_asset_paths, validate_line_id
from belle.build_client_cache import ensure_client_cache_updated
from belle.paths import (
    ensure_client_system_dirs,
    get_artifacts_telemetry_dir,
    get_client_root,
)


def _has_ingested_manifest_entries(client_dir: Path) -> bool:
    manifest_path = client_dir / "artifacts" / "ingest" / "ledger_ref_ingested.json"
    if not manifest_path.exists():
        return False
    try:
        obj = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    ingested = obj.get("ingested") or {}
    return isinstance(ingested, dict) and bool(ingested)


def _resolve_client_layout(repo_root: Path, client_id: str, line_id: str) -> tuple[str | None, Path]:
    line_dir = get_client_root(repo_root, client_id, line_id=line_id)
    if line_dir.exists():
        return line_id, line_dir
    if line_id == "receipt":
        legacy_dir = get_client_root(repo_root, client_id)
        if legacy_dir.exists():
            return None, legacy_dir
    raise SystemExit(f"client dir not found: {line_dir}")


def find_client_id_auto(repo_root: Path, line_id: str) -> tuple[str, str | None]:
    clients_dir = repo_root / "clients"
    cands = []
    for tdir in clients_dir.iterdir():
        if not tdir.is_dir() or tdir.name == "TEMPLATE":
            continue
        try:
            client_layout_line_id, client_dir = _resolve_client_layout(repo_root, tdir.name, line_id)
        except SystemExit:
            continue
        ref = client_dir / "inputs" / "ledger_ref"
        has_inbox_files = ref.exists() and (list(ref.glob("*.csv")) or list(ref.glob("*.txt")))
        if has_inbox_files or _has_ingested_manifest_entries(client_dir):
            cands.append((tdir.name, client_layout_line_id))
    if len(cands) == 1:
        return cands[0]
    if not cands:
        raise SystemExit(
            "Could not auto-detect client: no ledger_ref inbox files or ingest manifest entries found."
        )
    candidate_ids = [client_id for client_id, _ in cands]
    raise SystemExit(f"Could not auto-detect client: multiple candidates found: {candidate_ids}. Use --client.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", default=None)
    ap.add_argument("--line", default="receipt", help="Document processing line_id")
    ap.add_argument(
        "--config",
        default="rulesets/receipt/replacer_config_v1_15.json",
        help="Replacer config JSON (thresholds reused)",
    )
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(2)
    if not is_line_implemented(line_id):
        print("[ERROR] line is unimplemented in Phase 1")
        raise SystemExit(2)

    if args.client:
        client_id = args.client
        client_layout_line_id, _ = _resolve_client_layout(repo_root, client_id, line_id)
    else:
        client_id, client_layout_line_id = find_client_id_auto(repo_root, line_id)

    if client_layout_line_id is None:
        print(f"[WARN] legacy client layout detected (no lines/{line_id}/). Using legacy paths for this run.")

    ensure_client_system_dirs(repo_root, client_id, line_id=client_layout_line_id)
    telemetry_dir = get_artifacts_telemetry_dir(repo_root, client_id, line_id=client_layout_line_id)
    telemetry_dir.mkdir(parents=True, exist_ok=True)

    asset_paths = line_asset_paths(repo_root, line_id)
    lex = load_lexicon(asset_paths["lexicon_path"])
    config_path = (repo_root / args.config) if not Path(args.config).is_absolute() else Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))

    tm, summary = ensure_client_cache_updated(
        repo_root=repo_root,
        client_id=client_id,
        lex=lex,
        config=config,
        line_id=client_layout_line_id,
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_manifest = {
        "schema": "belle.client_cache_update_run.v1",
        "version": "1.15",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "line_id": line_id,
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
    (telemetry_dir / f"client_cache_update_run_{ts}.json").write_text(json.dumps(out_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] client={client_id} applied_new_files={len(summary.applied_new_files)} t_numbers={len(tm.t_numbers)} t_by_cat={len(tm.t_numbers_by_category)} vendor_keys={len(tm.vendor_keys)} categories={len(tm.categories)}")
    if summary.warnings:
        print("[WARN] " + " | ".join(summary.warnings))


if __name__ == "__main__":
    main()

