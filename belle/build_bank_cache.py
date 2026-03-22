# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
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
from .ingest import ingest_single_file, load_manifest_strict, sha256_file


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


_TRAINING_OCR_EXTS = {".csv"}
_TRAINING_REFERENCE_EXTS = {".csv", ".txt"}


def _list_training_files(dir_path: Path, *, allowed_exts: set[str]) -> List[Path]:
    if not dir_path.exists():
        return []
    files: List[Path] = []
    for p in sorted(dir_path.iterdir(), key=lambda v: v.name):
        if not p.is_file():
            continue
        if p.name == ".gitkeep":
            continue
        if p.name.endswith(".tmp"):
            continue
        if p.suffix.lower() not in allowed_exts:
            continue
        files.append(p)
    return files


def _load_ingested_entries_or_empty(manifest_path: Path) -> Dict[str, Dict[str, Any]]:
    if not manifest_path.exists():
        return {}
    try:
        obj = load_manifest_strict(manifest_path)
    except Exception as exc:
        raise SystemExit(f"failed to read ingest manifest (fail-closed): {manifest_path}: {exc}") from exc
    raw = obj.get("ingested")
    if not isinstance(raw, dict):
        raise SystemExit(f"invalid ingest manifest shape (missing ingested object): {manifest_path}")
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in raw.items():
        sha = str(k).strip()
        if not sha:
            continue
        out[sha] = v if isinstance(v, dict) else {}
    return out


def _to_relpath_from_line_root(line_root: Path, path: Path) -> str:
    try:
        return path.relative_to(line_root).as_posix()
    except ValueError:
        return str(path)


def _entry_stored_meta(
    *,
    entry: Dict[str, Any],
    fallback_store_dir: Path,
    fallback_stored_name: str,
    line_root: Path,
) -> Dict[str, str]:
    stored_name = str(entry.get("stored_name") or fallback_stored_name).strip()
    stored_relpath = str(entry.get("stored_relpath") or "").strip()
    if not stored_relpath and stored_name:
        stored_relpath = _to_relpath_from_line_root(line_root, fallback_store_dir / stored_name)
    return {
        "stored_name": stored_name,
        "stored_relpath": stored_relpath,
    }


def _pair_set_sha256(ocr_sha256: str, ref_sha256: str) -> str:
    payload = f"ocr={ocr_sha256}|ref={ref_sha256}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _prepare_cache(
    *,
    cache_path: Path,
    client_id: str,
    thresholds: Dict[str, Any],
) -> BankClientCache:
    cache = load_bank_cache(cache_path)
    if not cache.client_id:
        cache.client_id = str(client_id)
    if not cache.line_id:
        cache.line_id = LINE_ID_BANK_STATEMENT
    if not cache.created_at:
        cache.created_at = _now_utc_iso()
    if not cache.decision_thresholds:
        cache.decision_thresholds = thresholds
    if not isinstance(cache.applied_training_sets, dict):
        cache.applied_training_sets = {}
    cache.stats.setdefault(ROUTE_KANA_SIGN_AMOUNT, {})
    cache.stats.setdefault(ROUTE_KANA_SIGN, {})
    cache.bank_account_subaccount_stats.setdefault(ROUTE_KANA_SIGN_AMOUNT, {})
    cache.bank_account_subaccount_stats.setdefault(ROUTE_KANA_SIGN, {})
    return cache


def _finalize_cache_meta(cache: BankClientCache, *, client_id: str, thresholds: Dict[str, Any]) -> None:
    cache.schema = SCHEMA_BANK_CLIENT_CACHE_V0
    cache.version = "0.1"
    cache.client_id = str(client_id)
    cache.line_id = LINE_ID_BANK_STATEMENT
    cache.append_only = True
    cache.updated_at = _now_utc_iso()
    if not cache.decision_thresholds:
        cache.decision_thresholds = thresholds


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


_FILE_LEVEL_BANK_SUB_INFERENCE_KEY = "file_level_bank_sub_inference"
_FILE_LEVEL_BANK_SUB_MIN_VOTES_DEFAULT = 3
_FILE_LEVEL_BANK_SUB_MIN_P_MAJORITY_DEFAULT = 0.9


def _normalize_file_level_bank_sub_inference(threshold_obj: Any) -> Dict[str, Any]:
    src = threshold_obj if isinstance(threshold_obj, dict) else {}

    try:
        min_votes = int(src.get("min_votes", _FILE_LEVEL_BANK_SUB_MIN_VOTES_DEFAULT))
    except Exception:
        min_votes = _FILE_LEVEL_BANK_SUB_MIN_VOTES_DEFAULT
    if min_votes < 1:
        min_votes = _FILE_LEVEL_BANK_SUB_MIN_VOTES_DEFAULT

    try:
        min_p_majority = float(
            src.get("min_p_majority", _FILE_LEVEL_BANK_SUB_MIN_P_MAJORITY_DEFAULT)
        )
    except Exception:
        min_p_majority = _FILE_LEVEL_BANK_SUB_MIN_P_MAJORITY_DEFAULT
    if min_p_majority <= 0.0 or min_p_majority > 1.0:
        min_p_majority = _FILE_LEVEL_BANK_SUB_MIN_P_MAJORITY_DEFAULT

    return {
        "min_votes": int(min_votes),
        "min_p_majority": float(min_p_majority),
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
    loaded: Dict[str, Any] = {
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
            _FILE_LEVEL_BANK_SUB_INFERENCE_KEY: _normalize_file_level_bank_sub_inference(
                thresholds_raw.get(_FILE_LEVEL_BANK_SUB_INFERENCE_KEY)
            ),
        },
    }
    bank_side_subaccount = raw.get("bank_side_subaccount")
    if isinstance(bank_side_subaccount, dict):
        loaded["bank_side_subaccount"] = bank_side_subaccount
    return loaded


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

    config = load_bank_line_config(repo_root, client_id)
    thresholds = config.get("thresholds") if isinstance(config.get("thresholds"), dict) else {}

    ocr_inputs = _list_training_files(ocr_training_dir, allowed_exts=_TRAINING_OCR_EXTS)
    ref_inputs = _list_training_files(ref_training_dir, allowed_exts=_TRAINING_REFERENCE_EXTS)
    ocr_count = len(ocr_inputs)
    ref_count = len(ref_inputs)

    if ocr_count == 0 and ref_count == 0:
        return {
            "client_id": str(client_id),
            "line_id": LINE_ID_BANK_STATEMENT,
            "cache_path": str(cache_path),
            "training_ocr_ingest_manifest_path": str(training_ocr_ingested_path),
            "training_reference_ingest_manifest_path": str(training_ref_ingested_path),
            "training_input_state": "none",
            "training_ocr_input_count": int(ocr_count),
            "training_reference_input_count": int(ref_count),
            "applied_pair_set_ids": [],
            "skipped_pair_set_ids": [],
            "pairs_unique_used_total": 0,
            "sign_mismatch_skipped_total": 0,
            "ingested_new_training_ocr_shas": [],
            "ingested_duplicate_training_ocr_shas": [],
            "ingested_new_training_reference_shas": [],
            "ingested_duplicate_training_reference_shas": [],
            "warnings": [],
        }

    if ocr_count >= 2:
        raise SystemExit(
            "bank_statement training OCR input must be at most one *.csv per run. "
            f"current_count={ocr_count} dir={ocr_training_dir}"
        )
    if ref_count >= 2:
        raise SystemExit(
            "bank_statement training reference input must be at most one (*.csv or *.txt) per run. "
            f"current_count={ref_count} dir={ref_training_dir}"
        )
    if ocr_count != ref_count:
        raise SystemExit(
            "bank_statement training pair is incomplete (fail-closed): "
            f"ocr_count={ocr_count}, reference_count={ref_count}. "
            "Provide exactly one OCR and one reference file together."
        )

    ocr_input_path = ocr_inputs[0]
    ref_input_path = ref_inputs[0]
    ocr_sha = sha256_file(ocr_input_path)
    ref_sha = sha256_file(ref_input_path)
    pair_set_id = _pair_set_sha256(ocr_sha, ref_sha)

    cache = _prepare_cache(
        cache_path=cache_path,
        client_id=client_id,
        thresholds=thresholds,
    )

    applied_pair_set_ids: List[str] = []
    skipped_pair_set_ids: List[str] = []
    warnings: List[str] = []

    if pair_set_id in cache.applied_training_sets:
        skipped_pair_set_ids.append(pair_set_id)

        _manifest_ocr, ocr_ingest_result = ingest_single_file(
            source_path=ocr_input_path,
            store_dir=training_ocr_store_dir,
            manifest_path=training_ocr_ingested_path,
            client_id=client_id,
            kind="training_ocr",
        )
        _manifest_ref, ref_ingest_result = ingest_single_file(
            source_path=ref_input_path,
            store_dir=training_ref_store_dir,
            manifest_path=training_ref_ingested_path,
            client_id=client_id,
            kind="training_reference",
        )

        ocr_ingested_after = _load_ingested_entries_or_empty(training_ocr_ingested_path)
        ref_ingested_after = _load_ingested_entries_or_empty(training_ref_ingested_path)
        ocr_meta = _entry_stored_meta(
            entry=ocr_ingested_after.get(ocr_sha) or {},
            fallback_store_dir=training_ocr_store_dir,
            fallback_stored_name=ocr_ingest_result.stored_name,
            line_root=line_root,
        )
        ref_meta = _entry_stored_meta(
            entry=ref_ingested_after.get(ref_sha) or {},
            fallback_store_dir=training_ref_store_dir,
            fallback_stored_name=ref_ingest_result.stored_name,
            line_root=line_root,
        )

        existing_entry = cache.applied_training_sets.get(pair_set_id)
        if isinstance(existing_entry, dict):
            changed = False
            for key, value in {
                "training_ocr_stored_name": ocr_meta["stored_name"],
                "training_ocr_stored_relpath": ocr_meta["stored_relpath"],
                "training_reference_stored_name": ref_meta["stored_name"],
                "training_reference_stored_relpath": ref_meta["stored_relpath"],
            }.items():
                if value and str(existing_entry.get(key) or "") != value:
                    existing_entry[key] = value
                    changed = True
            if changed:
                _finalize_cache_meta(cache, client_id=client_id, thresholds=thresholds)
                save_bank_cache(cache_path, cache)

        ingested_new_training_ocr_shas = [ocr_sha] if ocr_ingest_result.status == "ingested" else []
        ingested_dup_training_ocr_shas = [ocr_sha] if ocr_ingest_result.status != "ingested" else []
        ingested_new_training_ref_shas = [ref_sha] if ref_ingest_result.status == "ingested" else []
        ingested_dup_training_ref_shas = [ref_sha] if ref_ingest_result.status != "ingested" else []

        return {
            "client_id": str(client_id),
            "line_id": LINE_ID_BANK_STATEMENT,
            "cache_path": str(cache_path),
            "training_ocr_ingest_manifest_path": str(training_ocr_ingested_path),
            "training_reference_ingest_manifest_path": str(training_ref_ingested_path),
            "training_input_state": "pair",
            "training_ocr_input_count": int(ocr_count),
            "training_reference_input_count": int(ref_count),
            "reference_sha256": ref_sha,
            "pair_set_sha256": pair_set_id,
            "applied_pair_set_ids": [],
            "skipped_pair_set_ids": skipped_pair_set_ids,
            "pairs_unique_used_total": 0,
            "sign_mismatch_skipped_total": 0,
            "labels_total": int(len(cache.labels)),
            "stats_kana_sign_amount_keys": int(len(cache.stats.get(ROUTE_KANA_SIGN_AMOUNT, {}))),
            "stats_kana_sign_keys": int(len(cache.stats.get(ROUTE_KANA_SIGN, {}))),
            "bank_subaccount_stats_kana_sign_amount_keys": int(
                len(cache.bank_account_subaccount_stats.get(ROUTE_KANA_SIGN_AMOUNT, {}))
            ),
            "bank_subaccount_stats_kana_sign_keys": int(
                len(cache.bank_account_subaccount_stats.get(ROUTE_KANA_SIGN, {}))
            ),
            "ingested_new_training_ocr_shas": ingested_new_training_ocr_shas,
            "ingested_duplicate_training_ocr_shas": ingested_dup_training_ocr_shas,
            "ingested_new_training_reference_shas": ingested_new_training_ref_shas,
            "ingested_duplicate_training_reference_shas": ingested_dup_training_ref_shas,
            "warnings": warnings,
        }

    ocr_ingested_before = _load_ingested_entries_or_empty(training_ocr_ingested_path)
    ref_ingested_before = _load_ingested_entries_or_empty(training_ref_ingested_path)
    ocr_known = ocr_sha in ocr_ingested_before
    ref_known = ref_sha in ref_ingested_before

    if ocr_known != ref_known:
        raise SystemExit(
            "bank_statement training pair rejected (fail-closed): one-side-only new pair is not allowed. "
            f"ocr_sha_known={ocr_known} ref_sha_known={ref_known}. "
            "Use a fresh OCR+reference pair for the same period."
        )
    if ocr_known and ref_known:
        raise SystemExit(
            "bank_statement training state is inconsistent (fail-closed): both OCR/reference SHA are already "
            "ingested but this pair_set is not applied in client_cache. "
            "Reset cache/manifests consistently or provide a new training pair."
        )

    pairs, metrics = build_training_pairs(
        ocr_csv_path=ocr_input_path,
        ref_csv_path=ref_input_path,
        config=config,
    )

    pairs_unique_used = int(metrics.get("pairs_unique_used") or 0)
    if pairs_unique_used == 0:
        raise SystemExit(
            "bank_statement training pairing produced zero usable pairs (fail-closed). "
            "No cache/ingest updates were made; keep inbox files for debugging."
        )

    now = _now_utc_iso()
    for pair in pairs:
        ocr = pair.get("ocr") or {}
        teacher = pair.get("teacher") or {}
        sign = str(pair.get("sign") or ocr.get("sign") or "")
        amount = int(pair.get("amount") or ocr.get("amount") or 0)
        kana_key = str(ocr.get("kana_key") or "")
        if not kana_key or sign not in {"debit", "credit"} or amount <= 0:
            raise SystemExit("invalid training pair emitted by build_training_pairs (ocr side)")

        corrected_summary = str(teacher.get("corrected_summary") or "")
        counter_account = str(teacher.get("counter_account") or "")
        counter_subaccount = str(teacher.get("counter_subaccount") or "")
        counter_tax_division = str(teacher.get("counter_tax_division") or "")
        if not corrected_summary or not counter_account:
            raise SystemExit("invalid training pair emitted by build_training_pairs (teacher side)")

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

    cache.applied_training_sets[pair_set_id] = {
        "applied_at": now,
        "pair_set_sha256": pair_set_id,
        "training_ocr_sha256_set": [str(ocr_sha)],
        "training_reference_sha256_set": [str(ref_sha)],
        "training_ocr_sha256": str(ocr_sha),
        "training_reference_sha256": str(ref_sha),
        "training_ocr_original_name": ocr_input_path.name,
        "training_reference_original_name": ref_input_path.name,
        "rows_total_ocr": int(metrics.get("rows_total_ocr") or 0),
        "rows_valid_ocr": int(metrics.get("rows_valid_ocr") or 0),
        "rows_total_reference": int(metrics.get("rows_total_reference") or 0),
        "ref_rows_valid": int(metrics.get("ref_rows_valid") or 0),
        "ocr_dup_keys": int(metrics.get("ocr_dup_keys") or 0),
        "ref_dup_keys": int(metrics.get("ref_dup_keys") or 0),
        "pairs_unique_used": int(metrics.get("pairs_unique_used") or 0),
        "pairs_missing_skipped": int(metrics.get("pairs_missing_skipped") or 0),
        "sign_mismatch_skipped": int(metrics.get("sign_mismatch_skipped") or 0),
        "pairs_used": int(metrics.get("pairs_unique_used") or 0),
        "pairs_skipped_collision": int(metrics.get("ocr_dup_keys") or 0) + int(metrics.get("ref_dup_keys") or 0),
        "pairs_skipped_missing": int(metrics.get("pairs_missing_skipped") or 0),
    }

    _finalize_cache_meta(cache, client_id=client_id, thresholds=thresholds)
    save_bank_cache(cache_path, cache)

    _manifest_ocr_after, ocr_ingest_result = ingest_single_file(
        source_path=ocr_input_path,
        store_dir=training_ocr_store_dir,
        manifest_path=training_ocr_ingested_path,
        client_id=client_id,
        kind="training_ocr",
    )
    _manifest_ref_after, ref_ingest_result = ingest_single_file(
        source_path=ref_input_path,
        store_dir=training_ref_store_dir,
        manifest_path=training_ref_ingested_path,
        client_id=client_id,
        kind="training_reference",
    )

    ocr_ingested_after = _load_ingested_entries_or_empty(training_ocr_ingested_path)
    ref_ingested_after = _load_ingested_entries_or_empty(training_ref_ingested_path)
    ocr_meta = _entry_stored_meta(
        entry=ocr_ingested_after.get(ocr_sha) or {},
        fallback_store_dir=training_ocr_store_dir,
        fallback_stored_name=ocr_ingest_result.stored_name,
        line_root=line_root,
    )
    ref_meta = _entry_stored_meta(
        entry=ref_ingested_after.get(ref_sha) or {},
        fallback_store_dir=training_ref_store_dir,
        fallback_stored_name=ref_ingest_result.stored_name,
        line_root=line_root,
    )

    applied_entry = cache.applied_training_sets.get(pair_set_id)
    if isinstance(applied_entry, dict):
        applied_entry["training_ocr_stored_name"] = ocr_meta["stored_name"]
        applied_entry["training_ocr_stored_relpath"] = ocr_meta["stored_relpath"]
        applied_entry["training_reference_stored_name"] = ref_meta["stored_name"]
        applied_entry["training_reference_stored_relpath"] = ref_meta["stored_relpath"]

    _finalize_cache_meta(cache, client_id=client_id, thresholds=thresholds)
    save_bank_cache(cache_path, cache)

    applied_pair_set_ids.append(pair_set_id)

    ingested_new_training_ocr_shas = [ocr_sha] if ocr_ingest_result.status == "ingested" else []
    ingested_dup_training_ocr_shas = [ocr_sha] if ocr_ingest_result.status != "ingested" else []
    ingested_new_training_ref_shas = [ref_sha] if ref_ingest_result.status == "ingested" else []
    ingested_dup_training_ref_shas = [ref_sha] if ref_ingest_result.status != "ingested" else []

    return {
        "client_id": str(client_id),
        "line_id": LINE_ID_BANK_STATEMENT,
        "cache_path": str(cache_path),
        "training_ocr_ingest_manifest_path": str(training_ocr_ingested_path),
        "training_reference_ingest_manifest_path": str(training_ref_ingested_path),
        "training_input_state": "pair",
        "training_ocr_input_count": int(ocr_count),
        "training_reference_input_count": int(ref_count),
        "reference_sha256": ref_sha,
        "pair_set_sha256": pair_set_id,
        "applied_pair_set_ids": applied_pair_set_ids,
        "skipped_pair_set_ids": skipped_pair_set_ids,
        "pairs_unique_used_total": int(metrics.get("pairs_unique_used") or 0),
        "sign_mismatch_skipped_total": int(metrics.get("sign_mismatch_skipped") or 0),
        "labels_total": int(len(cache.labels)),
        "stats_kana_sign_amount_keys": int(len(cache.stats.get(ROUTE_KANA_SIGN_AMOUNT, {}))),
        "stats_kana_sign_keys": int(len(cache.stats.get(ROUTE_KANA_SIGN, {}))),
        "bank_subaccount_stats_kana_sign_amount_keys": int(
            len(cache.bank_account_subaccount_stats.get(ROUTE_KANA_SIGN_AMOUNT, {}))
        ),
        "bank_subaccount_stats_kana_sign_keys": int(
            len(cache.bank_account_subaccount_stats.get(ROUTE_KANA_SIGN, {}))
        ),
        "ingested_new_training_ocr_shas": ingested_new_training_ocr_shas,
        "ingested_duplicate_training_ocr_shas": ingested_dup_training_ocr_shas,
        "ingested_new_training_reference_shas": ingested_new_training_ref_shas,
        "ingested_duplicate_training_reference_shas": ingested_dup_training_ref_shas,
        "warnings": warnings,
    }
