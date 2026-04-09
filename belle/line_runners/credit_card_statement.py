# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from belle.build_cc_cache import ensure_cc_client_cache_updated, load_credit_card_line_config
from belle.cc_replacer import replace_credit_card_yayoi_csv
from belle.defaults import (
    generate_full_category_overrides,
    load_category_defaults,
    merge_effective_defaults,
    try_load_category_overrides,
)
from belle.ingest import ingest_single_file
from belle.lexicon import load_lexicon
from belle.lines import line_asset_paths
from belle.paths import (
    build_input_artifact_prefix,
    ensure_client_system_dirs,
    get_category_overrides_path,
    get_client_cache_path,
    get_kari_shiwake_ingest_dir,
    get_kari_shiwake_ingested_path,
    get_latest_path,
    make_run_dir,
)
from belle.runner_io import update_latest_run_id, write_text_atomic
from belle.tax_postprocess import (
    get_yayoi_tax_config_path,
    load_yayoi_tax_postprocess_config,
)
from belle.ui_reason_codes import (
    RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED,
    RUN_OK,
    build_ui_reason_event,
)

from .common import LinePlan, compute_target_file_status, list_input_files, resolve_client_layout

LINE_ID_CARD = "credit_card_statement"


def _credit_card_line_config_path(client_dir: Path) -> Path:
    return client_dir / "config" / "credit_card_line_config.json"


def _missing_cc_config_reason(client_dir: Path) -> str:
    return f"missing_cc_config: expected={_credit_card_line_config_path(client_dir)}"


def plan_card(repo_root: Path, client_id: str) -> LinePlan:
    details: dict[str, object] = {}
    try:
        client_layout_line_id, client_dir = resolve_client_layout(repo_root, client_id, LINE_ID_CARD)
    except FileNotFoundError as exc:
        return LinePlan(
            line_id=LINE_ID_CARD,
            status="FAIL",
            reason=str(exc),
            target_files=[],
            details=details,
        )

    if client_layout_line_id is None:
        return LinePlan(
            line_id=LINE_ID_CARD,
            status="FAIL",
            reason="credit_card_statement does not support legacy client layout",
            target_files=[],
            details=details,
        )

    details.update(
        {
            "client_layout_line_id": client_layout_line_id,
            "client_dir": str(client_dir),
        }
    )

    status, reason, target_files = compute_target_file_status(client_dir)
    if status in {"SKIP", "FAIL"}:
        return LinePlan(
            line_id=LINE_ID_CARD,
            status=status,
            reason=reason,
            target_files=target_files,
            details=details,
        )

    cc_config_path = _credit_card_line_config_path(client_dir)
    if not cc_config_path.exists():
        return LinePlan(
            line_id=LINE_ID_CARD,
            status="FAIL",
            reason=_missing_cc_config_reason(client_dir),
            target_files=target_files,
            details=details,
        )

    return LinePlan(
        line_id=LINE_ID_CARD,
        status="RUN",
        reason="ready",
        target_files=target_files,
        details=details,
    )


def _ingest_single_kari_input(*, repo_root: Path, client_id: str, client_dir: Path) -> Any:
    input_files = list_input_files(
        client_dir / "inputs" / "kari_shiwake",
        allowed_extensions={".csv"},
    )
    if len(input_files) != 1:
        raise RuntimeError(
            "credit_card_statement target input must be exactly one file under inputs/kari_shiwake "
            f"(current={len(input_files)})"
        )
    kari_input = input_files[0]
    try:
        _kari_manifest, kari_ingest = ingest_single_file(
            source_path=kari_input,
            store_dir=get_kari_shiwake_ingest_dir(repo_root, client_id, line_id=LINE_ID_CARD),
            manifest_path=get_kari_shiwake_ingested_path(repo_root, client_id, line_id=LINE_ID_CARD),
            client_id=client_id,
            kind="kari_shiwake",
            manifest_schema="belle.kari_shiwake_ingest.v1",
        )
    except Exception as exc:
        raise RuntimeError(f"仮仕訳CSVの取り込みに失敗しました: {exc}") from exc
    return kari_ingest


def _write_run_manifest(run_manifest_path: Path, run_manifest: dict[str, Any]) -> None:
    run_manifest_text = json.dumps(run_manifest, ensure_ascii=False, indent=2)
    write_text_atomic(run_manifest_path, run_manifest_text, encoding="utf-8")


def run_card(repo_root: Path, client_id: str) -> dict[str, object]:
    try:
        client_layout_line_id, client_dir = resolve_client_layout(repo_root, client_id, LINE_ID_CARD)
    except FileNotFoundError as exc:
        raise RuntimeError(str(exc)) from exc
    if client_layout_line_id is None:
        raise RuntimeError("credit_card_statement does not support legacy client layout")

    status, reason, target_files = compute_target_file_status(client_dir)
    if status == "SKIP":
        print(f"[OK] {LINE_ID_CARD}: SKIP ({reason})")
        return {
            "line_id": LINE_ID_CARD,
            "exit_status": "SKIP",
            "reasons": [reason],
            "target_files": target_files,
        }
    if status == "FAIL":
        raise RuntimeError(reason)

    cc_config_path = _credit_card_line_config_path(client_dir)
    if not cc_config_path.exists():
        raise RuntimeError(_missing_cc_config_reason(client_dir))

    ensure_client_system_dirs(repo_root, client_id, line_id=LINE_ID_CARD)
    yayoi_tax_config = load_yayoi_tax_postprocess_config(repo_root, client_id)
    yayoi_tax_config_path = get_yayoi_tax_config_path(repo_root, client_id)
    asset_paths = line_asset_paths(
        repo_root,
        LINE_ID_CARD,
        bookkeeping_mode=yayoi_tax_config.bookkeeping_mode,
    )
    try:
        _cache, cache_update_summary = ensure_cc_client_cache_updated(repo_root, client_id)
    except Exception as exc:
        raise RuntimeError(f"credit card client_cache 更新に失敗しました: {exc}") from exc

    kari_ingest = _ingest_single_kari_input(repo_root=repo_root, client_id=client_id, client_dir=client_dir)

    run_id, run_dir = make_run_dir(repo_root, client_id, line_id=LINE_ID_CARD)
    latest_path = get_latest_path(repo_root, client_id, line_id=LINE_ID_CARD)
    config = load_credit_card_line_config(repo_root, client_id)
    cache_path = Path(
        str(cache_update_summary.get("cache_path") or get_client_cache_path(repo_root, client_id, line_id=LINE_ID_CARD))
    )
    lexicon_path = asset_paths["lexicon_path"]
    defaults_path = asset_paths["defaults_path"]
    lex = load_lexicon(lexicon_path)
    global_defaults = load_category_defaults(defaults_path)
    lexicon_category_keys = set(lex.categories_by_key.keys())
    overrides_path = get_category_overrides_path(repo_root, client_id, line_id=LINE_ID_CARD)
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
    effective_defaults = merge_effective_defaults(global_defaults, overrides_by_category)

    input_stem = Path(kari_ingest.original_name).stem or kari_ingest.stored_path.stem
    out_path = run_dir / f"{input_stem}_replaced_{run_id}.csv"
    artifact_prefix = build_input_artifact_prefix(
        in_path=Path(kari_ingest.original_name),
        input_index=1,
        run_id=run_id,
    )
    replacer_manifest = replace_credit_card_yayoi_csv(
        in_path=kari_ingest.stored_path,
        out_path=out_path,
        cache_path=cache_path,
        config=config,
        run_dir=run_dir,
        artifact_prefix=artifact_prefix,
        lex=lex,
        defaults=effective_defaults,
        yayoi_tax_config=yayoi_tax_config,
    )

    reports_obj = replacer_manifest.get("reports") if isinstance(replacer_manifest, dict) else {}
    if not isinstance(reports_obj, dict):
        reports_obj = {}
    replacer_manifest_path = str(reports_obj.get("manifest_json") or "")
    strict_stop = bool(replacer_manifest.get("payable_sub_fill_required_failed"))
    reasons: list[str] = []
    warnings: list[str] = list(category_overrides_warnings)
    exit_status = "OK"
    if strict_stop:
        exit_status = "FAIL"
        reasons.append("payable_sub_fill_required_failed")
        file_inference = replacer_manifest.get("file_card_inference")
        if isinstance(file_inference, dict):
            infer_status = str(file_inference.get("status") or "").strip()
            if infer_status:
                reasons.append(f"file_card_inference_status={infer_status}")

    run_manifest = {
        "schema": "belle.cc_runner_manifest.v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "line_id": LINE_ID_CARD,
        "client_id": client_id,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "cache_update_summary": cache_update_summary,
        "target_input": {
            "original_name": kari_ingest.original_name,
            "stored_name": kari_ingest.stored_name,
            "stored_path": str(kari_ingest.stored_path),
            "sha256": kari_ingest.sha256,
            "status": kari_ingest.status,
        },
        "yayoi_tax_config": {
            "path": str(yayoi_tax_config_path),
            "enabled": bool(yayoi_tax_config.enabled),
            "bookkeeping_mode": str(yayoi_tax_config.bookkeeping_mode),
            "rounding_mode": str(yayoi_tax_config.rounding_mode),
        },
        "category_overrides": {
            "path": str(overrides_path),
            "applied_count": len(overrides_by_category),
            "expected_count": len(lexicon_category_keys),
            "warnings": category_overrides_warnings,
        },
        "replacer_manifest_path": replacer_manifest_path,
        "strict_stop_applied": strict_stop,
        "exit_status": exit_status,
        "reasons": reasons,
        "ui_reason_code": RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED if strict_stop else RUN_OK,
        "ui_reason_detail": {
            "line_id": LINE_ID_CARD,
            "strict_stop_applied": strict_stop,
            "reasons": reasons,
        },
    }
    if warnings:
        run_manifest["warnings"] = warnings
    run_manifest_path = run_dir / "run_manifest.json"
    _write_run_manifest(run_manifest_path, run_manifest)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    update_latest_run_id(latest_path, run_id)

    print(f"[OK] client={client_id} run_id={run_id} inputs=1 outputs=1")
    print(f"[OK] run_dir={run_dir}")
    print(f"[OK] run_manifest={run_manifest_path}")
    print(
        f" - changed_ratio={float(replacer_manifest.get('changed_ratio') or 0.0):.3f}"
        f" output={replacer_manifest.get('output_file', '')}"
    )
    if warnings:
        print("[WARN] " + " | ".join(warnings))

    if strict_stop:
        print(
            build_ui_reason_event(
                RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED,
                line_id=LINE_ID_CARD,
                detail={"strict_stop_applied": True, "reasons": reasons},
            )
        )
        print(
            "[ERROR] strict-stop: Contract A failed "
            "(payable_sub_fill_required_failed=True)."
        )
        raise SystemExit(2)

    return {
        "line_id": LINE_ID_CARD,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "run_manifest_path": str(run_manifest_path),
        "changed_ratio": float(replacer_manifest.get("changed_ratio") or 0.0),
        "output_file": str(replacer_manifest.get("output_file") or ""),
        "warnings": warnings,
    }
