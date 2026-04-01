# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from belle.build_client_cache import ensure_client_cache_updated
from belle.defaults import (
    generate_full_category_overrides,
    load_category_defaults,
    merge_effective_defaults,
    try_load_category_overrides,
)
from belle.ingest import ingest_single_file
from belle.lexicon import load_lexicon
from belle.lexicon_manager import ensure_lexicon_candidates_updated_from_ledger_ref
from belle.lines import line_asset_paths
from belle.paths import (
    build_input_artifact_prefix,
    ensure_client_system_dirs,
    get_category_overrides_path,
    get_kari_shiwake_ingest_dir,
    get_kari_shiwake_ingested_path,
    get_latest_path,
    make_run_dir,
)
from belle.replacer import replace_yayoi_csv
from belle.runner_io import update_latest_run_id, write_text_atomic
from belle.ui_reason_codes import RUN_OK

from .common import LinePlan, compute_target_file_status, list_input_files, resolve_client_layout

LINE_ID_RECEIPT = "receipt"


def plan_receipt(repo_root: Path, client_id: str, *, config_path: Path) -> LinePlan:
    details: dict[str, object] = {"config_path": str(config_path)}
    try:
        client_layout_line_id, client_dir = resolve_client_layout(repo_root, client_id, LINE_ID_RECEIPT)
    except FileNotFoundError as exc:
        return LinePlan(
            line_id=LINE_ID_RECEIPT,
            status="FAIL",
            reason=str(exc),
            target_files=[],
            details=details,
        )

    details.update(
        {
            "layout": "legacy" if client_layout_line_id is None else "line",
            "client_layout_line_id": client_layout_line_id,
            "client_dir": str(client_dir),
        }
    )

    status, reason, target_files = compute_target_file_status(client_dir)
    if status in {"SKIP", "FAIL"}:
        return LinePlan(
            line_id=LINE_ID_RECEIPT,
            status=status,
            reason=reason,
            target_files=target_files,
            details=details,
        )

    if not config_path.exists():
        return LinePlan(
            line_id=LINE_ID_RECEIPT,
            status="FAIL",
            reason=f"config not found: {config_path}",
            target_files=target_files,
            details=details,
        )

    return LinePlan(
        line_id=LINE_ID_RECEIPT,
        status="RUN",
        reason="ready",
        target_files=target_files,
        details=details,
    )


def _ingest_single_kari_input(
    *,
    repo_root: Path,
    client_id: str,
    client_layout_line_id: str | None,
    client_dir: Path,
) -> Any:
    in_dir = client_dir / "inputs" / "kari_shiwake"
    input_files = list_input_files(in_dir, allowed_extensions={".csv"})
    if len(input_files) != 1:
        raise RuntimeError(
            "receipt target input must be exactly one file under inputs/kari_shiwake "
            f"(current={len(input_files)})"
        )
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
    except Exception as exc:
        raise RuntimeError(f"仮仕訳CSVの取り込みに失敗しました: {exc}") from exc
    return kari_ingest


def run_receipt(
    repo_root: Path,
    client_id: str,
    *,
    client_layout_line_id: str | None,
    client_dir: Path,
    config_path: Path,
) -> dict[str, object]:
    if client_layout_line_id is None:
        print("[WARN] legacy client layout detected (no lines/receipt/). Using legacy paths for this run.")

    ensure_client_system_dirs(repo_root, client_id, line_id=client_layout_line_id)

    asset_paths = line_asset_paths(repo_root, LINE_ID_RECEIPT)
    lexicon_path = asset_paths["lexicon_path"]
    defaults_path = asset_paths["defaults_path"]
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

    override_debit_accounts, category_overrides_warnings = try_load_category_overrides(
        path=overrides_path,
        lexicon_category_keys=lexicon_category_keys,
    )

    defaults = merge_effective_defaults(global_defaults, override_debit_accounts)
    config = json.loads(config_path.read_text(encoding="utf-8"))

    try:
        tm, tm_summary = ensure_client_cache_updated(
            repo_root=repo_root,
            client_id=client_id,
            lex=lex,
            config=config,
            line_id=client_layout_line_id,
        )
    except Exception as exc:
        raise RuntimeError(f"client_cache 更新に失敗しました: {exc}") from exc

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
            line_id=LINE_ID_RECEIPT,
            client_line_id=client_layout_line_id,
        )
    except Exception as exc:
        raise RuntimeError(f"label_queue 自動更新に失敗しました。出力は作成しません: {exc}") from exc

    run_id, run_dir = make_run_dir(repo_root, client_id, line_id=client_layout_line_id)
    latest_path = get_latest_path(repo_root, client_id, line_id=client_layout_line_id)
    kari_ingest = _ingest_single_kari_input(
        repo_root=repo_root,
        client_id=client_id,
        client_layout_line_id=client_layout_line_id,
        client_dir=client_dir,
    )

    run_manifest = {
        "schema": "belle.replacer_run.v2",
        "version": str(config.get("version") or "1.15"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "line_id": LINE_ID_RECEIPT,
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
        "category_overrides": {
            "path": str(overrides_path),
            "applied_count": len(override_debit_accounts),
            "expected_count": len(lexicon_category_keys),
            "warnings": category_overrides_warnings,
        },
        "ui_reason_code": RUN_OK,
        "ui_reason_detail": {"line_id": LINE_ID_RECEIPT},
        "outputs": [],
    }

    warnings: list[str] = list(category_overrides_warnings)

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

    rows_with_t = int(mf.get("analysis", {}).get("rows_with_t_number", 0))
    t_routes_used = int(mf.get("analysis", {}).get("rows_using_t_routes", 0))
    if rows_with_t > 0 and t_routes_used == 0 and len(tm.t_numbers) > 0:
        warnings.append(f"t_number_present_but_unused: file={kari_ingest.original_name}")

    if warnings:
        run_manifest["warnings"] = warnings

    run_manifest_path = run_dir / "run_manifest.json"
    write_text_atomic(
        run_manifest_path,
        json.dumps(run_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    update_latest_run_id(latest_path, run_id)

    print(f"[OK] client={client_id} run_id={run_id} inputs=1 outputs={len(run_manifest['outputs'])}")
    print(f"[OK] run_dir={run_dir}")
    print(f"[OK] run_manifest={run_manifest_path}")
    print(f" - changed_ratio={mf['changed_ratio']:.3f} output={mf['output_file']}")
    if warnings:
        print("[WARN] " + " | ".join(warnings))

    return {
        "line_id": LINE_ID_RECEIPT,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "run_manifest_path": str(run_manifest_path),
        "changed_ratio": float(mf.get("changed_ratio") or 0.0),
        "output_file": str(mf.get("output_file") or ""),
        "warnings": warnings,
    }
