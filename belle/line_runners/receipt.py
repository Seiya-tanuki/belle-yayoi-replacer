# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from belle.application.models import RunLineResult
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
from belle.receipt_config import load_receipt_line_config, receipt_line_config_path
from belle.replacer import replace_yayoi_csv
from belle.runner_io import update_latest_run_id, write_text_atomic
from belle.tax_postprocess import (
    get_yayoi_tax_config_path,
    load_yayoi_tax_postprocess_config,
)
from belle.ui_reason_codes import (
    PRECHECK_FAIL_CLIENT_DIR_NOT_FOUND,
    PRECHECK_FAIL_MULTIPLE_TARGET_INPUTS,
    PRECHECK_FAIL_RECEIPT_CONFIG_MISSING,
    PRECHECK_READY,
    PRECHECK_SKIP_NO_TARGET,
    RUN_FAIL_MULTIPLE_TARGET_INPUTS,
    RUN_FAIL_RECEIPT_CLIENT_CACHE_UPDATE,
    RUN_FAIL_RECEIPT_LEXICON_AUTOGROW,
    RUN_FAIL_TARGET_INGEST,
    RUN_FAIL_UNKNOWN,
    RUN_OK,
)

from .common import (
    build_line_plan,
    compute_target_file_status,
    list_input_files,
    raise_line_runner_failure,
    resolve_client_layout,
)

LINE_ID_RECEIPT = "receipt"


def plan_receipt(repo_root: Path, client_id: str):
    details: dict[str, object] = {}
    try:
        client_layout_line_id, client_dir = resolve_client_layout(repo_root, client_id, LINE_ID_RECEIPT)
    except FileNotFoundError as exc:
        return build_line_plan(
            line_id=LINE_ID_RECEIPT,
            status="FAIL",
            reason=str(exc),
            reason_key="client_dir_not_found",
            ui_reason_code=PRECHECK_FAIL_CLIENT_DIR_NOT_FOUND,
            target_files=[],
            run_failure_ui_reason_code=RUN_FAIL_UNKNOWN,
            details=details,
        )

    details.update(
        {
            "layout": "line",
            "client_layout_line_id": client_layout_line_id,
            "client_dir": str(client_dir),
        }
    )
    config_path = receipt_line_config_path(client_dir)
    details["config_path"] = str(config_path)

    status, reason_key, reason, target_files = compute_target_file_status(client_dir)
    if status in {"SKIP", "FAIL"}:
        return build_line_plan(
            line_id=LINE_ID_RECEIPT,
            status=status,
            reason=reason,
            reason_key=reason_key,
            ui_reason_code=PRECHECK_SKIP_NO_TARGET if status == "SKIP" else PRECHECK_FAIL_MULTIPLE_TARGET_INPUTS,
            target_files=target_files,
            run_failure_ui_reason_code="" if status == "SKIP" else RUN_FAIL_MULTIPLE_TARGET_INPUTS,
            details=details,
        )

    if not config_path.exists():
        return build_line_plan(
            line_id=LINE_ID_RECEIPT,
            status="FAIL",
            reason=f"config not found: {config_path}",
            reason_key="receipt_config_missing",
            ui_reason_code=PRECHECK_FAIL_RECEIPT_CONFIG_MISSING,
            target_files=target_files,
            run_failure_ui_reason_code=RUN_FAIL_UNKNOWN,
            details=details,
        )

    return build_line_plan(
        line_id=LINE_ID_RECEIPT,
        status="RUN",
        reason="ready",
        reason_key="ready",
        ui_reason_code=PRECHECK_READY,
        target_files=target_files,
        details=details,
    )


def _ingest_single_kari_input(
    *,
    repo_root: Path,
    client_id: str,
    client_layout_line_id: str,
    client_dir: Path,
) -> Any:
    in_dir = client_dir / "inputs" / "kari_shiwake"
    input_files = list_input_files(in_dir, allowed_extensions={".csv"})
    if len(input_files) != 1:
        raise_line_runner_failure(
            line_id=LINE_ID_RECEIPT,
            message=(
                "receipt target input must be exactly one file under inputs/kari_shiwake "
                f"(current={len(input_files)})"
            ),
            failure_key="target_input_count_invalid",
            ui_reason_code=RUN_FAIL_UNKNOWN if len(input_files) <= 1 else RUN_FAIL_MULTIPLE_TARGET_INPUTS,
            detail={"input_count": len(input_files)},
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
        raise_line_runner_failure(
            line_id=LINE_ID_RECEIPT,
            message=f"仮仕訳CSVの取り込みに失敗しました: {exc}",
            failure_key="target_ingest_failed",
            ui_reason_code=RUN_FAIL_TARGET_INGEST,
        )
    return kari_ingest


def run_receipt(
    repo_root: Path,
    client_id: str,
    *,
    client_layout_line_id: str,
    client_dir: Path,
) -> RunLineResult:
    if client_layout_line_id != LINE_ID_RECEIPT:
        raise RuntimeError(f"invalid receipt layout marker: {client_layout_line_id}")

    ensure_client_system_dirs(repo_root, client_id, line_id=client_layout_line_id)
    config_path = receipt_line_config_path(client_dir)
    config = load_receipt_line_config(client_dir)
    yayoi_tax_config = load_yayoi_tax_postprocess_config(repo_root, client_id)
    yayoi_tax_config_path = get_yayoi_tax_config_path(repo_root, client_id)

    asset_paths = line_asset_paths(
        repo_root,
        LINE_ID_RECEIPT,
        bookkeeping_mode=yayoi_tax_config.bookkeeping_mode,
    )
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

    overrides_by_category, category_overrides_warnings = try_load_category_overrides(
        path=overrides_path,
        lexicon_category_keys=lexicon_category_keys,
    )

    defaults = merge_effective_defaults(global_defaults, overrides_by_category)

    try:
        tm, tm_summary = ensure_client_cache_updated(
            repo_root=repo_root,
            client_id=client_id,
            lex=lex,
            config=config,
            line_id=client_layout_line_id,
        )
    except Exception as exc:
        raise_line_runner_failure(
            line_id=LINE_ID_RECEIPT,
            message=f"client_cache 更新に失敗しました: {exc}",
            failure_key="receipt_client_cache_update_failed",
            ui_reason_code=RUN_FAIL_RECEIPT_CLIENT_CACHE_UPDATE,
        )

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
        raise_line_runner_failure(
            line_id=LINE_ID_RECEIPT,
            message=f"label_queue 自動更新に失敗しました。出力は作成しません: {exc}",
            failure_key="receipt_lexicon_autogrow_failed",
            ui_reason_code=RUN_FAIL_RECEIPT_LEXICON_AUTOGROW,
        )

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
            "applied_count": len(overrides_by_category),
            "expected_count": len(lexicon_category_keys),
            "warnings": category_overrides_warnings,
        },
        "yayoi_tax_config": {
            "path": str(yayoi_tax_config_path),
            "enabled": bool(yayoi_tax_config.enabled),
            "bookkeeping_mode": str(yayoi_tax_config.bookkeeping_mode),
            "rounding_mode": str(yayoi_tax_config.rounding_mode),
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
        yayoi_tax_config=yayoi_tax_config,
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

    return RunLineResult.success(
        line_id=LINE_ID_RECEIPT,
        ui_reason_code=RUN_OK,
        ui_reason_detail={"phase": "run", "status": "success"},
        run_id=run_id,
        run_dir=str(run_dir),
        run_manifest_path=str(run_manifest_path),
        changed_ratio=float(mf.get("changed_ratio") or 0.0),
        output_file=str(mf.get("output_file") or ""),
        warnings=tuple(warnings),
        input_count=1,
        output_count=len(run_manifest["outputs"]),
        details={"outputs": list(run_manifest["outputs"])},
    )
