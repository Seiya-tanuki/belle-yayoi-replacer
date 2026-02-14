#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path as _Path
_REPO_ROOT = _Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from belle.lexicon import load_lexicon
from belle.defaults import (
    generate_full_category_overrides,
    load_category_defaults,
    load_category_overrides,
    merge_effective_defaults,
)
from belle.build_client_cache import ensure_client_cache_updated
from belle.lexicon_manager import ensure_lexicon_candidates_updated_from_ledger_ref
from belle.paths import (
    ensure_client_system_dirs,
    get_category_overrides_path,
    get_client_root,
    get_latest_path,
    make_run_dir,
)
from belle.replacer import replace_yayoi_csv


def _list_input_files_with_txt(dir_path: Path):
    files = list(dir_path.glob("*.csv")) + list(dir_path.glob("*.txt"))
    return sorted({p.resolve(): p for p in files}.values(), key=lambda p: p.name)


def find_client_id_auto(repo_root: Path) -> str:
    clients_dir = repo_root / "clients"
    cands = []
    if not clients_dir.exists():
        raise SystemExit("clients/ directory not found.")
    for tdir in clients_dir.iterdir():
        if not tdir.is_dir() or tdir.name == "TEMPLATE":
            continue
        inp = tdir / "inputs" / "kari_shiwake"
        if inp.exists() and _list_input_files_with_txt(inp):
            cands.append(tdir.name)
    if len(cands) == 1:
        return cands[0]
    if not cands:
        raise SystemExit("Could not auto-detect client: no clients/<CLIENT_ID>/inputs/kari_shiwake/*.csv or *.txt found.")
    raise SystemExit(f"Could not auto-detect client: multiple candidates found: {cands}. Use --client.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", help="Client ID under clients/<CLIENT_ID>/", default=None)
    ap.add_argument("--config", help="Replacer config JSON path", default="rulesets/replacer_config_v1_15.json")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    client_id = args.client or find_client_id_auto(repo_root)

    client_dir = get_client_root(repo_root, client_id)
    if not client_dir.exists():
        raise SystemExit(f"client dir not found: {client_dir}")

    ensure_client_system_dirs(repo_root, client_id)

    in_dir = client_dir / "inputs" / "kari_shiwake"

    input_files = _list_input_files_with_txt(in_dir)
    if not input_files:
        raise SystemExit(f"No input files found in {in_dir} (expected *.csv or *.txt).")

    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    defaults_path = repo_root / "defaults" / "category_defaults.json"
    config_path = (repo_root / args.config) if not Path(args.config).is_absolute() else Path(args.config)
    overrides_path = get_category_overrides_path(repo_root, client_id)

    lex = load_lexicon(lexicon_path)
    global_defaults = load_category_defaults(defaults_path)
    lexicon_category_keys = set(lex.categories_by_key.keys())

    if not overrides_path.exists():
        generate_full_category_overrides(
            path=overrides_path,
            client_id=client_id,
            global_defaults=global_defaults,
            lexicon_category_keys=lexicon_category_keys,
        )

    try:
        override_debit_accounts = load_category_overrides(
            path=overrides_path,
            lexicon_category_keys=lexicon_category_keys,
        )
    except ValueError as exc:
        print(f"[ERROR] category_overrides.json が不正です: {overrides_path}")
        print(f"[ERROR] {exc}")
        return 1

    defaults = merge_effective_defaults(global_defaults, override_debit_accounts)
    config = json.loads(config_path.read_text(encoding="utf-8"))

    # Ensure client_cache cache is updated BEFORE replacement.
    try:
        tm, tm_summary = ensure_client_cache_updated(
            repo_root=repo_root,
            client_id=client_id,
            lex=lex,
            config=config,
        )
    except Exception as exc:
        print(f"[ERROR] client_cache 更新に失敗しました: {exc}")
        return 1

    # Fail-closed: autogrow must succeed before run dir creation.
    try:
        lock_timeout_sec = int(os.environ.get("BELLE_LABEL_QUEUE_LOCK_TIMEOUT_SEC", "120"))
        lock_stale_sec = int(os.environ.get("BELLE_LABEL_QUEUE_LOCK_STALE_SEC", "120"))
        autogrow_summary = ensure_lexicon_candidates_updated_from_ledger_ref(
            repo_root=repo_root,
            client_id=client_id,
            lex=lex,
            config=config,
            ingest_inputs=False,
            processed_version="autogrow.v1",
            lock_timeout_sec=lock_timeout_sec,
            lock_stale_sec=lock_stale_sec,
        )
    except Exception as exc:
        print(f"[ERROR] label_queue 自動更新に失敗しました。出力は作成しません: {exc}")
        return 1

    run_id, run_dir = make_run_dir(repo_root, client_id)
    latest_path = get_latest_path(repo_root, client_id)

    run_manifest = {
        "schema": "belle.replacer_run.v2",
        "version": str(config.get("version") or "1.15"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "client_cache_update": {
            "applied_new_files": len(tm_summary.applied_new_files),
            "rows_used_added": tm_summary.rows_used_added,
            "warnings": tm_summary.warnings,
        },
        "lexicon_autogrow": {
            "processed_files": autogrow_summary.processed_files,
            "processed_rows": autogrow_summary.processed_rows,
            "unclassified_rows_seen": autogrow_summary.unclassified_rows_seen,
            "new_keys": autogrow_summary.new_keys,
            "updated_keys": autogrow_summary.updated_keys,
            "skipped_by_reason": autogrow_summary.skipped_by_reason,
            "warnings": autogrow_summary.warnings,
        },
        "inputs": [str(p) for p in input_files],
        "outputs": [],
    }

    warnings = []

    for idx, in_path in enumerate(input_files, start=1):
        out_path = run_dir / f"{in_path.stem}_replaced_{run_id}.csv"
        if out_path.exists():
            out_path = run_dir / f"{in_path.stem}_replaced_{run_id}_{idx:02d}.csv"
        mf = replace_yayoi_csv(
            in_path=in_path,
            out_path=out_path,
            lex=lex,
            client_cache=tm,
            defaults=defaults,
            config=config,
            run_dir=run_dir,
        )
        run_manifest["outputs"].append(mf)

        # Simple sanity warnings: if T numbers exist but no T route used.
        rows_with_t = int(mf.get("analysis", {}).get("rows_with_t_number", 0))
        t_routes_used = int(mf.get("analysis", {}).get("rows_using_t_routes", 0))
        if rows_with_t > 0 and t_routes_used == 0 and len(tm.t_numbers) > 0:
            warnings.append(f"t_number_present_but_unused: file={in_path.name}")

    if warnings:
        run_manifest["warnings"] = warnings

    run_manifest_path = run_dir / "run_manifest.json"
    run_manifest_path.write_text(json.dumps(run_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(f"{run_id}\n", encoding="utf-8")

    print(f"[OK] client={client_id} run_id={run_id} inputs={len(input_files)} outputs={len(run_manifest['outputs'])}")
    print(f"[OK] run_dir={run_dir}")
    print(f"[OK] run_manifest={run_manifest_path}")
    for o in run_manifest["outputs"]:
        print(f" - changed_ratio={o['changed_ratio']:.3f} output={o['output_file']}")
    if warnings:
        print("[WARN] " + " | ".join(warnings))
    return 0


if __name__ == "__main__":
    sys.exit(main())

