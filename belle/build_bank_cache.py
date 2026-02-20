# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from .bank_cache import (
    BankClientCache,
    BankLabel,
    LabelStatsEntry,
    ValueStatsEntry,
    LINE_ID_BANK_STATEMENT,
    ROUTE_KANA_SIGN,
    ROUTE_KANA_SIGN_AMOUNT,
    SCHEMA_BANK_CLIENT_CACHE_V0,
    load_bank_cache,
    make_bank_label_id,
    save_bank_cache,
)
from .bank_pairing import build_training_pairs
from .ingest import ingest_csv_dir


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_stored_path(line_root: Path, default_store_dir: Path, entry: Dict[str, Any]) -> Optional[Path]:
    stored_relpath = str(entry.get("stored_relpath") or "").strip()
    if stored_relpath:
        return line_root / Path(stored_relpath)
    stored_name = str(entry.get("stored_name") or "").strip()
    if not stored_name:
        return None
    return default_store_dir / stored_name


def _ensure_stats_entry(stats_map: Dict[str, LabelStatsEntry], key: str) -> LabelStatsEntry:
    if key not in stats_map:
        stats_map[key] = LabelStatsEntry.empty()
    return stats_map[key]


def _ensure_value_stats_entry(stats_map: Dict[str, ValueStatsEntry], key: str) -> ValueStatsEntry:
    if key not in stats_map:
        stats_map[key] = ValueStatsEntry.empty()
    return stats_map[key]


def _normalize_threshold_route(route_obj: Any, *, min_count: int, min_p_majority: float) -> Dict[str, Any]:
    src = route_obj if isinstance(route_obj, dict) else {}
    return {
        "min_count": int(src.get("min_count", min_count)),
        "min_p_majority": float(src.get("min_p_majority", src.get("p_majority", min_p_majority))),
    }


def load_bank_line_config(repo_root: Path, client_id: str) -> Dict[str, Any]:
    line_root = repo_root / "clients" / client_id / "lines" / LINE_ID_BANK_STATEMENT
    cfg_path = line_root / "config" / "bank_line_config.json"
    raw: Dict[str, Any] = {}
    if cfg_path.exists():
        obj = json.loads(cfg_path.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            raise ValueError(f"bank_line_config.json must be a JSON object: {cfg_path}")
        raw = obj

    thresholds_raw = raw.get("thresholds") if isinstance(raw.get("thresholds"), dict) else {}
    return {
        "schema": str(raw.get("schema") or "belle.bank_line_config.v0"),
        "version": str(raw.get("version") or "0.1"),
        "placeholder_account_name": str(raw.get("placeholder_account_name") or "仮払金"),
        "bank_account_name": str(raw.get("bank_account_name") or "普通預金"),
        "bank_account_subaccount": str(raw.get("bank_account_subaccount") or ""),
        "thresholds": {
            ROUTE_KANA_SIGN_AMOUNT: _normalize_threshold_route(
                thresholds_raw.get(ROUTE_KANA_SIGN_AMOUNT),
                min_count=2,
                min_p_majority=0.85,
            ),
            ROUTE_KANA_SIGN: _normalize_threshold_route(
                thresholds_raw.get(ROUTE_KANA_SIGN),
                min_count=3,
                min_p_majority=0.80,
            ),
        },
    }


def ensure_bank_client_cache_updated(repo_root: Path, client_id: str) -> Dict[str, Any]:
    line_root = repo_root / "clients" / client_id / "lines" / LINE_ID_BANK_STATEMENT

    ocr_training_dir = line_root / "inputs" / "training" / "ocr_kari_shiwake"
    ref_training_dir = line_root / "inputs" / "training" / "reference_yayoi"
    training_ocr_store_dir = line_root / "artifacts" / "ingest" / "training_ocr"
    training_ref_store_dir = line_root / "artifacts" / "ingest" / "training_reference"
    cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
    training_ocr_ingested_path = line_root / "artifacts" / "ingest" / "training_ocr_ingested.json"
    training_ref_ingested_path = line_root / "artifacts" / "ingest" / "training_reference_ingested.json"

    for d in [
        ocr_training_dir,
        ref_training_dir,
        training_ocr_store_dir,
        training_ref_store_dir,
        cache_path.parent,
        training_ocr_ingested_path.parent,
        training_ref_ingested_path.parent,
    ]:
        d.mkdir(parents=True, exist_ok=True)

    manifest_ocr, new_ocr_shas, dup_ocr_shas = ingest_csv_dir(
        dir_path=ocr_training_dir,
        store_dir=training_ocr_store_dir,
        manifest_path=training_ocr_ingested_path,
        client_id=client_id,
        kind="training_ocr",
        allow_rename=True,
        include_glob="*.csv",
        relpath_base_dir=line_root,
    )
    manifest_ref, new_ref_csv_shas, dup_ref_csv_shas = ingest_csv_dir(
        dir_path=ref_training_dir,
        store_dir=training_ref_store_dir,
        manifest_path=training_ref_ingested_path,
        client_id=client_id,
        kind="training_reference",
        allow_rename=True,
        include_glob="*.csv",
        relpath_base_dir=line_root,
    )
    manifest_ref, new_ref_txt_shas, dup_ref_txt_shas = ingest_csv_dir(
        dir_path=ref_training_dir,
        store_dir=training_ref_store_dir,
        manifest_path=training_ref_ingested_path,
        client_id=client_id,
        kind="training_reference",
        allow_rename=True,
        include_glob="*.txt",
        relpath_base_dir=line_root,
    )

    config = load_bank_line_config(repo_root, client_id)
    thresholds = config.get("thresholds") if isinstance(config.get("thresholds"), dict) else {}

    ref_ingested = manifest_ref.get("ingested") if isinstance(manifest_ref.get("ingested"), dict) else {}
    ref_order_raw = manifest_ref.get("ingested_order") if isinstance(manifest_ref.get("ingested_order"), list) else []
    ref_order = [str(sha) for sha in ref_order_raw if str(sha) in ref_ingested]
    if not ref_order:
        ref_order = [str(sha) for sha in ref_ingested.keys()]
    ref_unique_sha_list = sorted(set(ref_order))
    if len(ref_unique_sha_list) != 1:
        count = len(ref_unique_sha_list)
        raise SystemExit(
            "bank_statement training reference requires exactly one ingested file in "
            f"{training_ref_ingested_path}. current_count={count}. "
            "Leave only one canonical teacher file under inputs/training/reference_yayoi/ and rerun."
        )

    ref_sha = str(ref_unique_sha_list[0])
    ref_entry = ref_ingested.get(ref_sha) or {}
    ref_path = _resolve_stored_path(line_root, training_ref_store_dir, ref_entry)
    if ref_path is None or not ref_path.exists():
        raise SystemExit(
            f"bank_statement training reference file is missing for sha={ref_sha}. "
            f"expected path from manifest={training_ref_ingested_path}"
        )

    cache = load_bank_cache(cache_path)
    if not cache.client_id:
        cache.client_id = str(client_id)
    if not cache.line_id:
        cache.line_id = LINE_ID_BANK_STATEMENT
    if not cache.created_at:
        cache.created_at = _now_utc_iso()
    if not cache.decision_thresholds:
        cache.decision_thresholds = thresholds
    cache.stats.setdefault(ROUTE_KANA_SIGN_AMOUNT, {})
    cache.stats.setdefault(ROUTE_KANA_SIGN, {})
    cache.bank_account_subaccount_stats.setdefault(ROUTE_KANA_SIGN_AMOUNT, {})
    cache.bank_account_subaccount_stats.setdefault(ROUTE_KANA_SIGN, {})

    ocr_ingested = manifest_ocr.get("ingested") if isinstance(manifest_ocr.get("ingested"), dict) else {}
    ocr_order_raw = manifest_ocr.get("ingested_order") if isinstance(manifest_ocr.get("ingested_order"), list) else []
    ocr_order = [str(sha) for sha in ocr_order_raw if str(sha) in ocr_ingested]
    if not ocr_order:
        ocr_order = [str(sha) for sha in ocr_ingested.keys()]

    warnings: List[str] = []
    applied_pair_ids: List[str] = []
    skipped_pair_ids: List[str] = []
    pairs_unique_used_total = 0
    sign_mismatch_skipped_total = 0

    for ocr_sha in ocr_order:
        ocr_entry = ocr_ingested.get(ocr_sha) or {}
        pair_id = f"pair:{ocr_sha}:{ref_sha}"
        if pair_id in (cache.applied_training_sets or {}):
            skipped_pair_ids.append(pair_id)
            continue

        ocr_path = _resolve_stored_path(line_root, training_ocr_store_dir, ocr_entry)
        if ocr_path is None or not ocr_path.exists():
            warnings.append(f"missing_training_ocr_file: sha={ocr_sha}")
            continue

        pairs, metrics = build_training_pairs(
            ocr_csv_path=ocr_path,
            ref_csv_path=ref_path,
            config=config,
        )
        now = _now_utc_iso()
        for pair in pairs:
            ocr = pair.get("ocr") or {}
            teacher = pair.get("teacher") or {}
            sign = str(pair.get("sign") or ocr.get("sign") or "")
            amount = int(pair.get("amount") or ocr.get("amount") or 0)
            kana_key = str(ocr.get("kana_key") or "")
            if not kana_key or not sign or amount <= 0:
                continue

            corrected_summary = str(teacher.get("corrected_summary") or "")
            counter_account = str(teacher.get("counter_account") or "")
            counter_subaccount = str(teacher.get("counter_subaccount") or "")
            counter_tax_division = str(teacher.get("counter_tax_division") or "")
            if not corrected_summary or not counter_account:
                continue

            label_id = make_bank_label_id(
                corrected_summary=corrected_summary,
                counter_account=counter_account,
                counter_subaccount=counter_subaccount,
                counter_tax_division=counter_tax_division,
            )
            label = cache.labels.get(label_id)
            if label is None:
                label = BankLabel(
                    corrected_summary=corrected_summary,
                    counter_account=counter_account,
                    counter_subaccount=counter_subaccount,
                    counter_tax_division=counter_tax_division,
                    first_seen_at=now,
                    last_seen_at=now,
                    count_total=0,
                    examples=[],
                )
                cache.labels[label_id] = label
            if not label.first_seen_at:
                label.first_seen_at = now
            label.last_seen_at = now
            label.count_total = int(label.count_total) + 1

            key_strong = f"{kana_key}|{sign}|{amount}"
            key_weak = f"{kana_key}|{sign}"
            _ensure_stats_entry(cache.stats[ROUTE_KANA_SIGN_AMOUNT], key_strong).add_label(label_id)
            _ensure_stats_entry(cache.stats[ROUTE_KANA_SIGN], key_weak).add_label(label_id)

            bank_subaccount = str(teacher.get("bank_subaccount") or "").strip()
            if bank_subaccount:
                _ensure_value_stats_entry(
                    cache.bank_account_subaccount_stats[ROUTE_KANA_SIGN_AMOUNT],
                    key_strong,
                ).update(bank_subaccount)
                _ensure_value_stats_entry(
                    cache.bank_account_subaccount_stats[ROUTE_KANA_SIGN],
                    key_weak,
                ).update(bank_subaccount)

        pairs_unique_used_total += int(metrics.get("pairs_unique_used") or 0)
        sign_mismatch_skipped_total += int(metrics.get("sign_mismatch_skipped") or 0)

        ocr_stored_name = str(ocr_entry.get("stored_name") or ocr_path.name)
        ocr_stored_relpath = str(ocr_entry.get("stored_relpath") or "")
        ref_stored_name = str(ref_entry.get("stored_name") or ref_path.name)
        ref_stored_relpath = str(ref_entry.get("stored_relpath") or "")

        cache.applied_training_sets[pair_id] = {
            "applied_at": now,
            "training_ocr_sha256_set": [str(ocr_sha)],
            "training_reference_sha256_set": [str(ref_sha)],
            "training_ocr_sha256": str(ocr_sha),
            "training_reference_sha256": str(ref_sha),
            "training_ocr_stored_name": ocr_stored_name,
            "training_ocr_stored_relpath": ocr_stored_relpath,
            "training_reference_stored_name": ref_stored_name,
            "training_reference_stored_relpath": ref_stored_relpath,
            "rows_total_ocr": int(metrics.get("rows_total_ocr") or 0),
            "rows_valid_ocr": int(metrics.get("rows_valid_ocr") or 0),
            "rows_total_reference": int(metrics.get("rows_total_reference") or 0),
            "ref_rows_valid": int(metrics.get("ref_rows_valid") or 0),
            "ocr_dup_keys": int(metrics.get("ocr_dup_keys") or 0),
            "ref_dup_keys": int(metrics.get("ref_dup_keys") or 0),
            "pairs_unique_used": int(metrics.get("pairs_unique_used") or 0),
            "pairs_missing_skipped": int(metrics.get("pairs_missing_skipped") or 0),
            "sign_mismatch_skipped": int(metrics.get("sign_mismatch_skipped") or 0),
            # Compatibility aliases with BANK_CLIENT_CACHE_SPEC wording.
            "pairs_used": int(metrics.get("pairs_unique_used") or 0),
            "pairs_skipped_collision": int(metrics.get("ocr_dup_keys") or 0) + int(metrics.get("ref_dup_keys") or 0),
            "pairs_skipped_missing": int(metrics.get("pairs_missing_skipped") or 0),
        }
        applied_pair_ids.append(pair_id)

    cache.schema = SCHEMA_BANK_CLIENT_CACHE_V0
    cache.version = "0.1"
    cache.client_id = str(client_id)
    cache.line_id = LINE_ID_BANK_STATEMENT
    cache.append_only = True
    cache.updated_at = _now_utc_iso()
    if not cache.decision_thresholds:
        cache.decision_thresholds = thresholds

    save_bank_cache(cache_path, cache)

    return {
        "client_id": str(client_id),
        "line_id": LINE_ID_BANK_STATEMENT,
        "cache_path": str(cache_path),
        "training_ocr_ingest_manifest_path": str(training_ocr_ingested_path),
        "training_reference_ingest_manifest_path": str(training_ref_ingested_path),
        "ingested_new_training_ocr_shas": [str(v) for v in new_ocr_shas],
        "ingested_duplicate_training_ocr_shas": [str(v) for v in dup_ocr_shas],
        "ingested_new_training_reference_shas": [str(v) for v in (new_ref_csv_shas + new_ref_txt_shas)],
        "ingested_duplicate_training_reference_shas": [str(v) for v in (dup_ref_csv_shas + dup_ref_txt_shas)],
        "reference_sha256": ref_sha,
        "applied_pair_ids": applied_pair_ids,
        "skipped_pair_ids": skipped_pair_ids,
        "pairs_unique_used_total": int(pairs_unique_used_total),
        "sign_mismatch_skipped_total": int(sign_mismatch_skipped_total),
        "labels_total": int(len(cache.labels)),
        "stats_kana_sign_amount_keys": int(len(cache.stats.get(ROUTE_KANA_SIGN_AMOUNT, {}))),
        "stats_kana_sign_keys": int(len(cache.stats.get(ROUTE_KANA_SIGN, {}))),
        "bank_subaccount_stats_kana_sign_amount_keys": int(
            len(cache.bank_account_subaccount_stats.get(ROUTE_KANA_SIGN_AMOUNT, {}))
        ),
        "bank_subaccount_stats_kana_sign_keys": int(
            len(cache.bank_account_subaccount_stats.get(ROUTE_KANA_SIGN, {}))
        ),
        "warnings": warnings,
    }
