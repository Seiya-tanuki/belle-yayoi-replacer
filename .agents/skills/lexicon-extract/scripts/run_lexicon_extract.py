#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path as _Path

_REPO_ROOT = _Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

import argparse
import json
from pathlib import Path

from belle.lexicon import load_lexicon
from belle.lexicon_manager import ensure_lexicon_candidates_updated_from_ledger_ref
from belle.paths import get_client_root


def _list_ref_files_with_txt(dir_path: Path):
    files = list(dir_path.glob("*.csv")) + list(dir_path.glob("*.txt"))
    return sorted({p.resolve(): p for p in files}.values(), key=lambda p: p.name)


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


def find_client_id_auto(repo_root: Path) -> str:
    clients_dir = repo_root / "clients"
    cands = []
    for tdir in clients_dir.iterdir():
        if not tdir.is_dir() or tdir.name == "TEMPLATE":
            continue
        ref = tdir / "inputs" / "ledger_ref"
        has_inbox_files = ref.exists() and bool(_list_ref_files_with_txt(ref))
        if has_inbox_files or _has_ingested_manifest_entries(tdir):
            cands.append(tdir.name)
    if len(cands) == 1:
        return cands[0]
    if not cands:
        raise SystemExit(
            "Could not auto-detect client: no ledger_ref inbox files or ingest manifest entries found."
        )
    raise SystemExit(f"Could not auto-detect client: multiple candidates found: {cands}. Use --client.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", default=None)
    ap.add_argument("--config", default="rulesets/replacer_config_v1_15.json", help="Used for dummy summary contract")
    ap.add_argument("--show-paths", action="store_true", help="Print paths and continue")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    client_id = args.client or find_client_id_auto(repo_root)

    client_dir = get_client_root(repo_root, client_id)
    if not client_dir.exists():
        raise SystemExit(f"client dir not found: {client_dir}")

    config_path = (repo_root / args.config) if not Path(args.config).is_absolute() else Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))

    if args.show_paths:
        print(f"[PATHS] ledger_ref_dir={client_dir / 'inputs' / 'ledger_ref'}")
        print(f"[PATHS] manifest={client_dir / 'artifacts' / 'ingest' / 'ledger_ref_ingested.json'}")
        print(f"[PATHS] queue_csv={repo_root / 'lexicon' / 'pending' / 'label_queue.csv'}")
        print(f"[PATHS] queue_state={repo_root / 'lexicon' / 'pending' / 'label_queue_state.json'}")
        print(f"[PATHS] lock={repo_root / 'lexicon' / 'pending' / 'locks' / 'label_queue.lock'}")

    lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")
    summary = ensure_lexicon_candidates_updated_from_ledger_ref(
        repo_root=repo_root,
        client_id=client_id,
        lex=lex,
        config=config,
        ingest_inputs=True,
        processed_version="lexicon-extract.v1",
    )

    print(
        "[OK] client={client} processed_files={files} processed_rows={rows} "
        "new_keys={new_keys} updated_keys={updated_keys}".format(
            client=client_id,
            files=summary.processed_files,
            rows=summary.processed_rows,
            new_keys=summary.new_keys,
            updated_keys=summary.updated_keys,
        )
    )
    if summary.warnings:
        print("[WARN] " + " | ".join(summary.warnings))
    return 0


if __name__ == "__main__":
    sys.exit(main())
