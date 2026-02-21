# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from belle.io_atomic import atomic_write_text
from belle.paths import (
    build_input_artifact_prefix,
    ensure_client_system_dirs,
    get_client_cache_path,
    get_kari_shiwake_ingest_dir,
    get_kari_shiwake_ingested_path,
    get_latest_path,
    make_run_dir,
)

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


def _count_teacher_reference(client_dir: Path) -> tuple[int, str]:
    manifest_path = client_dir / "artifacts" / "ingest" / "training_reference_ingested.json"
    if manifest_path.exists():
        obj = json.loads(manifest_path.read_text(encoding="utf-8"))
        ingested = obj.get("ingested")
        if not isinstance(ingested, dict):
            raise ValueError(f"ingested must be object in {manifest_path}")
        shas = {str(k).strip() for k in ingested.keys() if str(k).strip()}
        return len(shas), "ingested_manifest"

    teacher_dir = client_dir / "inputs" / "training" / "reference_yayoi"
    files = list_input_files(teacher_dir)
    return len(files), "inputs_dir"


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

    try:
        teacher_count, teacher_source = _count_teacher_reference(client_dir)
    except Exception as exc:
        return LinePlan(
            line_id=LINE_ID_BANK,
            status="FAIL",
            reason=f"teacher reference check failed: {exc}",
            target_files=target_files,
            details=details,
        )

    details["teacher_count"] = teacher_count
    details["teacher_source"] = teacher_source
    if teacher_count != 1:
        return LinePlan(
            line_id=LINE_ID_BANK,
            status="FAIL",
            reason=f"teacher reference count must be exactly 1 (current={teacher_count}, source={teacher_source})",
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
    input_files = list_input_files(client_dir / "inputs" / "kari_shiwake")
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

    run_manifest = {
        "schema": "belle.bank_replacer_skill_run.v1",
        "version": str(bank_config.get("version") or "0.1"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "client_id": client_id,
        "line_id": LINE_ID_BANK,
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
