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
from typing import Any

from belle.lexicon import load_lexicon
from belle.lines import is_line_implemented, line_asset_paths, validate_line_id
from belle.ingest import ingest_single_file
from belle.defaults import (
    generate_full_category_overrides,
    load_category_defaults,
    load_category_overrides,
    merge_effective_defaults,
)
from belle.build_client_cache import ensure_client_cache_updated
from belle.io_atomic import atomic_write_text
from belle.lexicon_manager import ensure_lexicon_candidates_updated_from_ledger_ref
from belle.paths import (
    build_input_artifact_prefix,
    ensure_client_system_dirs,
    get_category_overrides_path,
    get_client_cache_path,
    get_client_root,
    get_kari_shiwake_ingest_dir,
    get_kari_shiwake_ingested_path,
    get_latest_path,
    make_run_dir,
)
from belle.replacer import replace_yayoi_csv
try:
    from belle.build_bank_cache import ensure_bank_client_cache_updated, load_bank_line_config
except ImportError:  # pragma: no cover - compatibility guard
    ensure_bank_client_cache_updated = None
    load_bank_line_config = None
try:
    from belle.bank_replacer import replace_bank_yayoi_csv
except ImportError:  # pragma: no cover - compatibility guard
    replace_bank_yayoi_csv = None


def _list_kari_shiwake_input_files(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    files = []
    for p in dir_path.iterdir():
        if not p.is_file():
            continue
        if p.name == ".gitkeep":
            continue
        if p.name.endswith(".tmp"):
            continue
        files.append(p)
    return sorted(files, key=lambda x: x.name)


def _resolve_client_layout(
    repo_root: Path,
    client_id: str,
    line_id: str,
) -> tuple[str | None, Path]:
    line_dir = get_client_root(repo_root, client_id, line_id=line_id)
    if line_dir.exists():
        return line_id, line_dir
    if line_id == "receipt":
        legacy_dir = get_client_root(repo_root, client_id)
        if legacy_dir.exists():
            print(f"[WARN] legacy client layout detected (no lines/{line_id}/). Using legacy paths for this run.")
            return None, legacy_dir
    raise SystemExit(f"client dir not found: {line_dir}")


def _ingest_single_kari_input(
    *,
    repo_root: Path,
    client_id: str,
    client_layout_line_id: str | None,
    client_dir: Path,
    line_id: str,
) -> tuple[Any, str] | None:
    in_dir = client_dir / "inputs" / "kari_shiwake"
    if client_layout_line_id is None:
        input_dir_label = f"clients/{client_id}/inputs/kari_shiwake/"
    else:
        input_dir_label = f"clients/{client_id}/lines/{line_id}/inputs/kari_shiwake/"
    input_files = _list_kari_shiwake_input_files(in_dir)

    if not input_files:
        print(
            "[ERROR] 置換対象の仮仕訳CSVが見つかりません。"
            f"{input_dir_label} に1ファイル配置してください。"
        )
        return None
    if len(input_files) >= 2:
        print("[ERROR] 置換対象の仮仕訳CSVが複数あります。1ファイルにしてください:")
        for p in input_files:
            print(f"  - {p.name}")
        return None

    kari_input = input_files[0]
    try:
        _kari_manifest, kari_ingest = ingest_single_file(
            source_path=kari_input,
            store_dir=get_kari_shiwake_ingest_dir(repo_root, client_id, line_id=client_layout_line_id),
            manifest_path=get_kari_shiwake_ingested_path(repo_root, client_id, line_id=client_layout_line_id),
            client_id=client_id,
            kind="kari_shiwake",
            manifest_schema="belle.kari_shiwake_ingest.v1",
        )
        return kari_ingest, input_dir_label
    except Exception as exc:
        print(f"[ERROR] 仮仕訳CSVの取り込みに失敗しました: {exc}")
        return None


def _load_bank_runtime_config(repo_root: Path, client_id: str) -> dict[str, Any]:
    if callable(load_bank_line_config):
        return load_bank_line_config(repo_root, client_id)
    config_path = repo_root / "clients" / client_id / "lines" / "bank_statement" / "config" / "bank_line_config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"bank_line_config.json not found: {config_path}")
    obj = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"bank_line_config.json must be a JSON object: {config_path}")
    return obj


def _run_receipt(
    *,
    repo_root: Path,
    client_id: str,
    line_id: str,
    client_layout_line_id: str | None,
    client_dir: Path,
    args: argparse.Namespace,
) -> int:
    ensure_client_system_dirs(repo_root, client_id, line_id=client_layout_line_id)

    ingested = _ingest_single_kari_input(
        repo_root=repo_root,
        client_id=client_id,
        client_layout_line_id=client_layout_line_id,
        client_dir=client_dir,
        line_id=line_id,
    )
    if ingested is None:
        return 1
    kari_ingest, _input_dir_label = ingested

    asset_paths = line_asset_paths(repo_root, line_id)
    lexicon_path = asset_paths["lexicon_path"]
    defaults_path = asset_paths["defaults_path"]
    config_path = (repo_root / args.config) if not Path(args.config).is_absolute() else Path(args.config)
    overrides_path = get_category_overrides_path(repo_root, client_id, line_id=client_layout_line_id)

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
            line_id=client_layout_line_id,
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
            line_id=line_id,
            client_line_id=client_layout_line_id,
        )
    except Exception as exc:
        print(f"[ERROR] label_queue 自動更新に失敗しました。出力は作成しません: {exc}")
        return 1

    run_id, run_dir = make_run_dir(repo_root, client_id, line_id=client_layout_line_id)
    latest_path = get_latest_path(repo_root, client_id, line_id=client_layout_line_id)

    run_manifest = {
        "schema": "belle.replacer_run.v2",
        "version": str(config.get("version") or "1.15"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "line_id": line_id,
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
        "inputs": {
            "kari_shiwake": {
                "original_name": kari_ingest.original_name,
                "stored_name": kari_ingest.stored_name,
                "sha256": kari_ingest.sha256,
            }
        },
        "outputs": [],
    }

    warnings = []

    input_stem = Path(kari_ingest.original_name).stem or kari_ingest.stored_path.stem
    in_path = kari_ingest.stored_path
    out_path = run_dir / f"{input_stem}_replaced_{run_id}.csv"
    artifact_prefix = build_input_artifact_prefix(
        in_path=Path(kari_ingest.original_name),
        input_index=1,
        run_id=run_id,
    )
    mf = replace_yayoi_csv(
        in_path=in_path,
        out_path=out_path,
        lex=lex,
        client_cache=tm,
        defaults=defaults,
        config=config,
        run_dir=run_dir,
        artifact_prefix=artifact_prefix,
    )
    run_manifest["outputs"].append(mf)

    # Simple sanity warnings: if T numbers exist but no T route used.
    rows_with_t = int(mf.get("analysis", {}).get("rows_with_t_number", 0))
    t_routes_used = int(mf.get("analysis", {}).get("rows_using_t_routes", 0))
    if rows_with_t > 0 and t_routes_used == 0 and len(tm.t_numbers) > 0:
        warnings.append(f"t_number_present_but_unused: file={kari_ingest.original_name}")

    if warnings:
        run_manifest["warnings"] = warnings

    run_manifest_path = run_dir / "run_manifest.json"
    atomic_write_text(
        run_manifest_path,
        json.dumps(run_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(latest_path, f"{run_id}\n", encoding="utf-8")

    print(f"[OK] client={client_id} run_id={run_id} inputs=1 outputs={len(run_manifest['outputs'])}")
    print(f"[OK] run_dir={run_dir}")
    print(f"[OK] run_manifest={run_manifest_path}")
    for o in run_manifest["outputs"]:
        print(f" - changed_ratio={o['changed_ratio']:.3f} output={o['output_file']}")
    if warnings:
        print("[WARN] " + " | ".join(warnings))
    return 0


def _run_bank_statement(
    *,
    repo_root: Path,
    client_id: str,
    line_id: str,
    client_layout_line_id: str | None,
    client_dir: Path,
) -> int:
    if client_layout_line_id is None:
        print("[ERROR] bank_statement does not support legacy client layout.")
        return 2
    if ensure_bank_client_cache_updated is None or replace_bank_yayoi_csv is None:
        print("[ERROR] bank_statement runtime modules are unavailable.")
        return 1

    ensure_client_system_dirs(repo_root, client_id, line_id=client_layout_line_id)
    ingested = _ingest_single_kari_input(
        repo_root=repo_root,
        client_id=client_id,
        client_layout_line_id=client_layout_line_id,
        client_dir=client_dir,
        line_id=line_id,
    )
    if ingested is None:
        return 1
    kari_ingest, _input_dir_label = ingested

    try:
        cache_update = ensure_bank_client_cache_updated(repo_root, client_id)
    except Exception as exc:
        print(f"[ERROR] bank client_cache 更新に失敗しました: {exc}")
        return 1

    try:
        bank_config = _load_bank_runtime_config(repo_root, client_id)
    except Exception as exc:
        print(f"[ERROR] bank_line_config 読み込みに失敗しました: {exc}")
        return 1

    run_id, run_dir = make_run_dir(repo_root, client_id, line_id=client_layout_line_id)
    latest_path = get_latest_path(repo_root, client_id, line_id=client_layout_line_id)
    cache_path = Path(
        str(
            cache_update.get("cache_path")
            or get_client_cache_path(repo_root, client_id, line_id=client_layout_line_id)
        )
    )

    input_stem = Path(kari_ingest.original_name).stem or kari_ingest.stored_path.stem
    out_path = run_dir / f"{input_stem}_replaced_{run_id}.csv"
    artifact_prefix = build_input_artifact_prefix(
        in_path=Path(kari_ingest.original_name),
        input_index=1,
        run_id=run_id,
    )
    bank_output_manifest = replace_bank_yayoi_csv(
        in_path=kari_ingest.stored_path,
        out_path=out_path,
        cache_path=cache_path,
        config=bank_config,
        run_dir=run_dir,
        artifact_prefix=artifact_prefix,
    )

    run_manifest = {
        "schema": "belle.bank_replacer_skill_run.v1",
        "version": str(bank_config.get("version") or "0.1"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "line_id": line_id,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "bank_cache_update": {
            "applied_pair_ids": int(len(cache_update.get("applied_pair_ids") or [])),
            "skipped_pair_ids": int(len(cache_update.get("skipped_pair_ids") or [])),
            "pairs_unique_used_total": int(cache_update.get("pairs_unique_used_total") or 0),
            "sign_mismatch_skipped_total": int(cache_update.get("sign_mismatch_skipped_total") or 0),
            "warnings": list(cache_update.get("warnings") or []),
            "cache_path": str(cache_path),
        },
        "inputs": {
            "kari_shiwake": {
                "original_name": kari_ingest.original_name,
                "stored_name": kari_ingest.stored_name,
                "sha256": kari_ingest.sha256,
            }
        },
        "outputs": [bank_output_manifest],
    }
    run_manifest_path = run_dir / "run_manifest.json"
    atomic_write_text(
        run_manifest_path,
        json.dumps(run_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(latest_path, f"{run_id}\n", encoding="utf-8")

    print(f"[OK] client={client_id} run_id={run_id} inputs=1 outputs=1")
    print(f"[OK] run_dir={run_dir}")
    print(f"[OK] run_manifest={run_manifest_path}")
    print(
        "[OK] bank_cache"
        f" pairs_used={run_manifest['bank_cache_update']['pairs_unique_used_total']}"
        f" cache={cache_path}"
    )
    print(
        f" - changed_ratio={bank_output_manifest.get('changed_ratio', 0.0):.3f}"
        f" output={bank_output_manifest.get('output_file', '')}"
    )
    warnings = run_manifest["bank_cache_update"]["warnings"]
    if warnings:
        print("[WARN] " + " | ".join(str(v) for v in warnings))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", help="Client ID under clients/<CLIENT_ID>/", default=None)
    ap.add_argument("--line", help="Document processing line_id", default="receipt")
    ap.add_argument(
        "--config",
        help="Replacer config JSON path",
        default="rulesets/receipt/replacer_config_v1_15.json",
    )
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    client_id = (args.client or "").strip()
    if not client_id:
        print("[ERROR] 置換を実行するクライアントのディレクトリ名（--client）を指定してください。")
        print("例: $yayoi-replacer --client <CLIENT_ID>")
        return 2

    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        return 2
    if not is_line_implemented(line_id):
        print(f"[ERROR] line is unimplemented: {line_id}")
        return 2

    client_layout_line_id, client_dir = _resolve_client_layout(repo_root, client_id, line_id)
    if line_id == "bank_statement":
        return _run_bank_statement(
            repo_root=repo_root,
            client_id=client_id,
            line_id=line_id,
            client_layout_line_id=client_layout_line_id,
            client_dir=client_dir,
        )
    return _run_receipt(
        repo_root=repo_root,
        client_id=client_id,
        line_id=line_id,
        client_layout_line_id=client_layout_line_id,
        client_dir=client_dir,
        args=args,
    )


if __name__ == "__main__":
    sys.exit(main())

