#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path as _Path

_REPO_ROOT = _Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

import argparse
import json
from pathlib import Path

from belle.ingest import list_discoverable_files
from belle.lexicon import load_lexicon
from belle.lines import line_mode_independent_asset_paths, validate_line_id
from belle.lexicon_manager import ensure_lexicon_candidates_updated_from_ledger_ref
from belle.paths import get_client_root


def _list_ref_files_with_txt(dir_path: Path):
    return list_discoverable_files(dir_path, allowed_extensions={".csv", ".txt"})


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


def find_client_id_auto(repo_root: Path, line_id: str) -> tuple[str, str | None, Path]:
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
        has_inbox_files = ref.exists() and bool(_list_ref_files_with_txt(ref))
        if has_inbox_files or _has_ingested_manifest_entries(client_dir):
            cands.append((tdir.name, client_layout_line_id, client_dir))
    if len(cands) == 1:
        return cands[0]
    if not cands:
        raise SystemExit(
            "Could not auto-detect client: no ledger_ref inbox files or ingest manifest entries found."
        )
    candidate_ids = [client_id for client_id, _, _ in cands]
    raise SystemExit(f"Could not auto-detect client: multiple candidates found: {candidate_ids}. Use --client.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", default=None)
    ap.add_argument("--line", default="receipt", help="Document processing line_id")
    ap.add_argument(
        "--config",
        default="rulesets/receipt/replacer_config_v1_15.json",
        help="Used for dummy summary contract",
    )
    ap.add_argument("--show-paths", action="store_true", help="Print paths and continue")
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

    if args.client:
        client_id = args.client
        client_layout_line_id, client_dir = _resolve_client_layout(repo_root, client_id, line_id)
    else:
        client_id, client_layout_line_id, client_dir = find_client_id_auto(repo_root, line_id)

    if client_layout_line_id is None:
        print(f"[WARN] legacy client layout detected (no lines/{line_id}/). Using legacy paths for this run.")

    config_path = (repo_root / args.config) if not Path(args.config).is_absolute() else Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    asset_paths = line_mode_independent_asset_paths(repo_root, line_id)
    pending_dir = asset_paths["pending_dir"]

    if args.show_paths:
        print(f"[PATHS] ledger_ref_dir={client_dir / 'inputs' / 'ledger_ref'}")
        print(f"[PATHS] manifest={client_dir / 'artifacts' / 'ingest' / 'ledger_ref_ingested.json'}")
        print(f"[PATHS] queue_csv={pending_dir / 'label_queue.csv'}")
        print(f"[PATHS] queue_state={pending_dir / 'label_queue_state.json'}")
        print(f"[PATHS] lock={pending_dir / 'locks' / 'label_queue.lock'}")

    lex = load_lexicon(asset_paths["lexicon_path"])
    summary = ensure_lexicon_candidates_updated_from_ledger_ref(
        repo_root=repo_root,
        client_id=client_id,
        lex=lex,
        config=config,
        ingest_inputs=True,
        processed_version="lexicon-extract.v1",
        line_id=line_id,
        client_line_id=client_layout_line_id,
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
