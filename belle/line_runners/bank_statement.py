# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from belle.paths import (
    build_input_artifact_prefix,
    ensure_client_system_dirs,
    get_client_cache_path,
    get_kari_shiwake_ingest_dir,
    get_kari_shiwake_ingested_path,
    get_latest_path,
    make_run_dir,
)
from belle.runner_io import update_latest_run_id, write_json_atomic, write_text_atomic

try:
    from belle.build_bank_cache import ensure_bank_client_cache_updated, load_bank_line_config
except ImportError:  # pragma: no cover - compatibility guard
    ensure_bank_client_cache_updated = None
    load_bank_line_config = None

try:
    from belle.bank_replacer import replace_bank_yayoi_csv
except ImportError:  # pragma: no cover - compatibility guard
    replace_bank_yayoi_csv = None

from belle.ingest import ingest_single_file

from .common import LinePlan, compute_target_file_status, list_input_files, resolve_client_layout

LINE_ID_BANK = "bank_statement"


def _list_training_files(dir_path: Path, *, allowed_exts: set[str]) -> list[Path]:
    return list_input_files(dir_path, allowed_extensions=allowed_exts)


def _inspect_training_pair_state(client_dir: Path) -> tuple[str, str, dict[str, object]]:
    ocr_dir = client_dir / "inputs" / "training" / "ocr_kari_shiwake"
    ref_dir = client_dir / "inputs" / "training" / "reference_yayoi"

    ocr_files = _list_training_files(ocr_dir, allowed_exts={".csv"})
    ref_files = _list_training_files(ref_dir, allowed_exts={".csv", ".txt"})
    ocr_count = len(ocr_files)
    ref_count = len(ref_files)

    detail_obj: dict[str, object] = {
        "training_pair_state": "none",
        "training_pair_reason": "no training inputs",
        "training_ocr_count": int(ocr_count),
        "training_reference_count": int(ref_count),
        "training_ocr_files": [p.name for p in ocr_files],
        "training_reference_files": [p.name for p in ref_files],
        "training_ocr_dir": str(ocr_dir),
        "training_reference_dir": str(ref_dir),
    }

    if ocr_count == 0 and ref_count == 0:
        return "none", "no training inputs", detail_obj

    if ocr_count >= 2:
        reason = f"training OCR count must be <=1 (current={ocr_count})"
        detail_obj["training_pair_state"] = "fail"
        detail_obj["training_pair_reason"] = reason
        return "fail", reason, detail_obj

    if ref_count >= 2:
        reason = f"training reference count must be <=1 (current={ref_count})"
        detail_obj["training_pair_state"] = "fail"
        detail_obj["training_pair_reason"] = reason
        return "fail", reason, detail_obj

    if ocr_count != ref_count:
        reason = (
            "training pair is incomplete: provide exactly one OCR and one reference together "
            f"(ocr={ocr_count}, reference={ref_count})"
        )
        detail_obj["training_pair_state"] = "fail"
        detail_obj["training_pair_reason"] = reason
        return "fail", reason, detail_obj

    detail_obj["training_pair_state"] = "pair"
    detail_obj["training_pair_reason"] = "single training pair detected"
    return "pair", "single training pair detected", detail_obj


def plan_bank(repo_root: Path, client_id: str) -> LinePlan:
    details: dict[str, object] = {}
    try:
        client_layout_line_id, client_dir = resolve_client_layout(repo_root, client_id, LINE_ID_BANK)
    except FileNotFoundError as exc:
        return LinePlan(
            line_id=LINE_ID_BANK,
            status="FAIL",
            reason=str(exc),
            target_files=[],
            details=details,
        )

    if client_layout_line_id is None:
        return LinePlan(
            line_id=LINE_ID_BANK,
            status="FAIL",
            reason="bank_statement does not support legacy client layout",
            target_files=[],
            details=details,
        )

    config_path = client_dir / "config" / "bank_line_config.json"
    details.update(
        {
            "client_layout_line_id": client_layout_line_id,
            "client_dir": str(client_dir),
            "config_path": str(config_path),
        }
    )

    status, reason, target_files = compute_target_file_status(client_dir)
    if status in {"SKIP", "FAIL"}:
        return LinePlan(
            line_id=LINE_ID_BANK,
            status=status,
            reason=reason,
            target_files=target_files,
            details=details,
        )

    if not config_path.exists():
        return LinePlan(
            line_id=LINE_ID_BANK,
            status="FAIL",
            reason=f"bank_line_config.json not found: {config_path}",
            target_files=target_files,
            details=details,
        )

    training_state, training_reason, training_details = _inspect_training_pair_state(client_dir)
    details.update(training_details)
    details["training_pair_summary"] = training_reason
    if training_state == "fail":
        return LinePlan(
            line_id=LINE_ID_BANK,
            status="FAIL",
            reason=training_reason,
            target_files=target_files,
            details=details,
        )

    return LinePlan(
        line_id=LINE_ID_BANK,
        status="RUN",
        reason="ready",
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
    input_files = list_input_files(
        client_dir / "inputs" / "kari_shiwake",
        allowed_extensions={".csv"},
    )
    if len(input_files) != 1:
        raise RuntimeError(
            "bank_statement target input must be exactly one file under inputs/kari_shiwake "
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


def _load_bank_runtime_config(repo_root: Path, client_id: str) -> dict[str, Any]:
    if callable(load_bank_line_config):
        return load_bank_line_config(repo_root, client_id)
    config_path = repo_root / "clients" / client_id / "lines" / LINE_ID_BANK / "config" / "bank_line_config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"bank_line_config.json not found: {config_path}")
    obj = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"bank_line_config.json must be a JSON object: {config_path}")
    return obj


def run_bank(
    repo_root: Path,
    client_id: str,
    *,
    client_dir: Path,
) -> dict[str, object]:
    if ensure_bank_client_cache_updated is None or replace_bank_yayoi_csv is None:
        raise RuntimeError("bank_statement runtime modules are unavailable")

    client_layout_line_id = LINE_ID_BANK
    ensure_client_system_dirs(repo_root, client_id, line_id=client_layout_line_id)
    kari_ingest = _ingest_single_kari_input(
        repo_root=repo_root,
        client_id=client_id,
        client_layout_line_id=client_layout_line_id,
        client_dir=client_dir,
    )

    try:
        cache_update = ensure_bank_client_cache_updated(repo_root, client_id)
    except Exception as exc:
        raise RuntimeError(f"bank client_cache 更新に失敗しました: {exc}") from exc

    try:
        bank_config = _load_bank_runtime_config(repo_root, client_id)
    except Exception as exc:
        raise RuntimeError(f"bank_line_config 読み込みに失敗しました: {exc}") from exc

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
    reports_obj = bank_output_manifest.get("reports") if isinstance(bank_output_manifest, dict) else {}
    if not isinstance(reports_obj, dict):
        reports_obj = {}
    replacer_manifest_path = str(reports_obj.get("manifest_json") or "")
    strict_stop = bool(bank_output_manifest.get("bank_sub_fill_required_failed"))
    reasons: list[str] = []
    exit_status = "OK"
    if strict_stop:
        exit_status = "FAIL"
        reasons.append("bank_sub_fill_required_failed")
        file_inference = bank_output_manifest.get("file_bank_sub_inference")
        if isinstance(file_inference, dict):
            infer_status = str(file_inference.get("status") or "").strip()
            if infer_status:
                reasons.append(f"file_bank_sub_inference_status={infer_status}")

    run_manifest = {
        "schema": "belle.bank_replacer_skill_run.v2",
        "version": str(bank_config.get("version") or "0.1"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "line_id": LINE_ID_BANK,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "bank_cache_update": {
            "applied_pair_set_count": int(len(cache_update.get("applied_pair_set_ids") or [])),
            "skipped_pair_set_count": int(len(cache_update.get("skipped_pair_set_ids") or [])),
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
        "replacer_manifest_path": replacer_manifest_path,
        "strict_stop_applied": strict_stop,
        "exit_status": exit_status,
        "reasons": reasons,
        "outputs": [bank_output_manifest],
    }
    run_manifest_path = run_dir / "run_manifest.json"
    # Preserve historical formatting parity: run_manifest.json has no trailing newline.
    run_manifest_text = json.dumps(run_manifest, ensure_ascii=False, indent=2)
    write_text_atomic(run_manifest_path, run_manifest_text, encoding="utf-8")
    update_latest_run_id(latest_path, run_id)

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
    if strict_stop:
        print(
            "[ERROR] strict-stop: Contract A failed "
            "(bank_sub_fill_required_failed=True)."
        )
        raise SystemExit(2)

    return {
        "line_id": LINE_ID_BANK,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "run_manifest_path": str(run_manifest_path),
        "changed_ratio": float(bank_output_manifest.get("changed_ratio") or 0.0),
        "output_file": str(bank_output_manifest.get("output_file") or ""),
        "warnings": warnings,
        "bank_cache_update": run_manifest["bank_cache_update"],
    }
