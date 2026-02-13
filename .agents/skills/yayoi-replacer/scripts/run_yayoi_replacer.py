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
from belle.defaults import load_category_defaults
from belle.build_client_cache import ensure_client_cache_updated
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", help="Client ID under clients/<CLIENT_ID>/", default=None)
    ap.add_argument("--config", help="Replacer config JSON path", default="rulesets/replacer_config_v1_15.json")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    client_id = args.client or find_client_id_auto(repo_root)

    client_dir = repo_root / "clients" / client_id
    if not client_dir.exists():
        raise SystemExit(f"client dir not found: {client_dir}")

    in_dir = client_dir / "inputs" / "kari_shiwake"
    out_dir = client_dir / "outputs"
    reports_dir = client_dir / "artifacts" / "reports"
    artifacts_dir = client_dir / "artifacts"

    out_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    input_files = _list_input_files_with_txt(in_dir)
    if not input_files:
        raise SystemExit(f"No input files found in {in_dir} (expected *.csv or *.txt).")

    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    defaults_path = repo_root / "defaults" / "category_defaults.json"
    config_path = (repo_root / args.config) if not Path(args.config).is_absolute() else Path(args.config)

    lex = load_lexicon(lexicon_path)
    defaults = load_category_defaults(defaults_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))

    # Ensure client_cache cache is updated BEFORE replacement.
    tm, tm_summary = ensure_client_cache_updated(
        repo_root=repo_root,
        client_id=client_id,
        lex=lex,
        config=config,
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    run_manifest = {
        "schema": "belle.replacer_run.v2",
        "version": str(config.get("version") or "1.15"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "client_cache_update": {
            "applied_new_files": len(tm_summary.applied_new_files),
            "rows_used_added": tm_summary.rows_used_added,
            "warnings": tm_summary.warnings,
        },
        "inputs": [str(p) for p in input_files],
        "outputs": [],
    }

    warnings = []

    for in_path in input_files:
        out_path = out_dir / f"{in_path.stem}_replaced_{ts}.csv"
        mf = replace_yayoi_csv(
            in_path=in_path,
            out_path=out_path,
            lex=lex,
            client_cache=tm,
            defaults=defaults,
            config=config,
            reports_dir=reports_dir,
        )
        run_manifest["outputs"].append(mf)

        # Simple sanity warnings: if T numbers exist but no T route used.
        rows_with_t = int(mf.get("analysis", {}).get("rows_with_t_number", 0))
        t_routes_used = int(mf.get("analysis", {}).get("rows_using_t_routes", 0))
        if rows_with_t > 0 and t_routes_used == 0 and len(tm.t_numbers) > 0:
            warnings.append(f"t_number_present_but_unused: file={in_path.name}")

    if warnings:
        run_manifest["warnings"] = warnings

    (reports_dir / f"run_manifest_{ts}.json").write_text(json.dumps(run_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] client={client_id} inputs={len(input_files)} outputs={len(run_manifest['outputs'])}")
    for o in run_manifest["outputs"]:
        print(f" - changed_ratio={o['changed_ratio']:.3f} output={o['output_file']}")
    if warnings:
        print("[WARN] " + " | ".join(warnings))


if __name__ == "__main__":
    main()

