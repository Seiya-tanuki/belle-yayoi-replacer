# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import json
import re
import unicodedata

from .cc_cache import CCClientCache, LINE_ID_CC, SCHEMA_CC_CLIENT_CACHE_V0, ValueStatsEntry
from .client_cache import StatsEntry
from .stats_utils import ensure_stats_entry, ensure_value_stats_entry
from .ingest import ingest_csv_dir
from .paths import (
    ensure_client_system_dirs,
    get_client_cache_path,
    get_client_root,
    get_ledger_ref_ingest_dir,
    get_ledger_ref_ingested_path,
    resolve_ledger_ref_stored_path,
)
from .yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_SUMMARY,
)
from .yayoi_csv import read_yayoi_csv, token_to_text

_WHITESPACE_RE = re.compile(r"\s+")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
    return bool(default)


def _normalize_str_list(value: Any, default: List[str]) -> List[str]:
    if not isinstance(value, list):
        return list(default)
    out: List[str] = []
    for item in value:
        s = str(item or "").strip()
        if s:
            out.append(s)
    if not out:
        return list(default)
    return sorted(set(out))


def _normalize_candidate_config(obj: Any) -> Dict[str, Any]:
    src = obj if isinstance(obj, dict) else {}
    manual_allow = _normalize_str_list(src.get("manual_allow"), default=[])
    return {
        "min_total_count": _as_int(src.get("min_total_count", src.get("min_rows", 5)), 5),
        "min_unique_merchants": _as_int(src.get("min_unique_merchants", 3), 3),
        "min_unique_counter_accounts": _as_int(src.get("min_unique_counter_accounts", 2), 2),
        "manual_allow": manual_allow,
    }


def _normalize_partial_match_config(obj: Any) -> Dict[str, Any]:
    src = obj if isinstance(obj, dict) else {}
    direction = str(src.get("direction") or "cache_key_in_input").strip()
    if direction != "cache_key_in_input":
        direction = "cache_key_in_input"

    min_match_len = _as_int(src.get("min_match_len", 4), 4)
    if min_match_len < 1:
        min_match_len = 4

    min_stats_sample_total = _as_int(src.get("min_stats_sample_total", 10), 10)
    if min_stats_sample_total < 1:
        min_stats_sample_total = 10

    min_stats_p_majority = _as_float(src.get("min_stats_p_majority", 0.95), 0.95)
    if min_stats_p_majority < 0.0 or min_stats_p_majority > 1.0:
        min_stats_p_majority = 0.95

    return {
        "enabled": _as_bool(src.get("enabled"), True),
        "direction": direction,
        "require_unique_longest": _as_bool(src.get("require_unique_longest"), True),
        "min_match_len": int(min_match_len),
        "min_stats_sample_total": int(min_stats_sample_total),
        "min_stats_p_majority": float(min_stats_p_majority),
    }


def load_credit_card_line_config(repo_root: Path, client_id: str) -> Dict[str, Any]:
    line_root = repo_root / "clients" / client_id / "lines" / LINE_ID_CC
    cfg_path = line_root / "config" / "credit_card_line_config.json"

    if not cfg_path.exists():
        raise FileNotFoundError(f"missing_cc_config: expected={cfg_path}")

    try:
        obj = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to parse credit_card_line_config.json (fail-closed): {cfg_path}: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"credit_card_line_config.json must be a JSON object (fail-closed): {cfg_path}")
    raw: Dict[str, Any] = obj

    thresholds_raw = raw.get("thresholds") if isinstance(raw.get("thresholds"), dict) else {}
    merchant_key_account_raw = (
        thresholds_raw.get("merchant_key_account")
        if isinstance(thresholds_raw.get("merchant_key_account"), dict)
        else {}
    )
    file_level_raw = (
        thresholds_raw.get("file_level_card_inference")
        if isinstance(thresholds_raw.get("file_level_card_inference"), dict)
        else {}
    )

    training_raw = raw.get("training") if isinstance(raw.get("training"), dict) else {}
    merchant_key_norm_raw = (
        raw.get("merchant_key_normalization")
        if isinstance(raw.get("merchant_key_normalization"), dict)
        else {}
    )
    candidate_raw = (
        raw.get("candidate_extraction")
        if isinstance(raw.get("candidate_extraction"), dict)
        else {}
    )
    partial_match_raw = raw.get("partial_match") if isinstance(raw.get("partial_match"), dict) else {}

    loaded: Dict[str, Any] = {
        "schema": str(raw.get("schema") or "belle.credit_card_line_config.v0"),
        "version": str(raw.get("version") or "0.1"),
        "placeholder_account_name": str(raw.get("placeholder_account_name") or "仮払金"),
        "payable_account_name": str(raw.get("payable_account_name") or "未払金"),
        "merchant_key_normalization": {
            "nfkc": bool(merchant_key_norm_raw.get("nfkc", True)),
            "trim": bool(merchant_key_norm_raw.get("trim", True)),
            "collapse_spaces": bool(merchant_key_norm_raw.get("collapse_spaces", True)),
            "strip_digits": bool(merchant_key_norm_raw.get("strip_digits", True)),
            "strip_symbols": bool(merchant_key_norm_raw.get("strip_symbols", True)),
            "split_on_slash": bool(merchant_key_norm_raw.get("split_on_slash", True)),
            "uppercase_ascii": bool(merchant_key_norm_raw.get("uppercase_ascii", True)),
        },
        "training": {
            "exclude_counter_accounts": _normalize_str_list(
                training_raw.get("exclude_counter_accounts"),
                default=["普通預金", "当座預金"],
            ),
        },
        "thresholds": {
            "merchant_key_account": {
                "min_count": _as_int(merchant_key_account_raw.get("min_count", 3), 3),
                "min_p_majority": _as_float(merchant_key_account_raw.get("min_p_majority", 0.9), 0.9),
            },
            "file_level_card_inference": {
                "min_votes": _as_int(file_level_raw.get("min_votes", 3), 3),
                "min_p_majority": _as_float(file_level_raw.get("min_p_majority", 0.9), 0.9),
            },
        },
        "candidate_extraction": _normalize_candidate_config(candidate_raw),
        "partial_match": _normalize_partial_match_config(partial_match_raw),
    }
    return loaded


def merchant_key_from_summary(summary: str, config: Dict[str, Any]) -> str:
    s = str(summary or "")
    if not s.strip():
        return ""

    norm_cfg = (
        config.get("merchant_key_normalization")
        if isinstance(config.get("merchant_key_normalization"), dict)
        else {}
    )

    if bool(norm_cfg.get("nfkc", True)):
        s = unicodedata.normalize("NFKC", s)

    if bool(norm_cfg.get("split_on_slash", True)):
        if "/" in s:
            s = s.split("/", 1)[0]

    if bool(norm_cfg.get("trim", True)):
        s = s.strip()
    if not s:
        return ""

    if bool(norm_cfg.get("collapse_spaces", True)):
        s = _WHITESPACE_RE.sub(" ", s).strip()

    if bool(norm_cfg.get("uppercase_ascii", True)):
        s = "".join(ch.upper() if "a" <= ch <= "z" else ch for ch in s)

    strip_digits = bool(norm_cfg.get("strip_digits", True))
    strip_symbols = bool(norm_cfg.get("strip_symbols", True))

    out_chars: List[str] = []
    for ch in s:
        cat = unicodedata.category(ch)
        cat_head = cat[0] if cat else ""
        if cat_head == "Z":
            continue
        if strip_digits and cat_head == "N":
            continue
        if strip_symbols and cat_head in {"P", "S"}:
            continue
        if cat_head == "C":
            continue
        out_chars.append(ch)
    key = "".join(out_chars).strip()
    return key


def _thresholds_snapshot(config: Dict[str, Any]) -> Dict[str, Any]:
    thresholds = config.get("thresholds") if isinstance(config.get("thresholds"), dict) else {}
    candidate_extraction = (
        config.get("candidate_extraction")
        if isinstance(config.get("candidate_extraction"), dict)
        else {}
    )
    training = config.get("training") if isinstance(config.get("training"), dict) else {}
    partial_match = (
        config.get("partial_match") if isinstance(config.get("partial_match"), dict) else {}
    )
    return {
        "merchant_key_account": thresholds.get("merchant_key_account") or {},
        "file_level_card_inference": thresholds.get("file_level_card_inference") or {},
        "candidate_extraction": candidate_extraction,
        "exclude_counter_accounts": training.get("exclude_counter_accounts") or [],
        "partial_match": _normalize_partial_match_config(partial_match),
    }


def _ensure_stats_entry(stats_map: Dict[str, StatsEntry], key: str) -> StatsEntry:
    return ensure_stats_entry(stats_map, key)


def _ensure_value_stats_entry(stats_map: Dict[str, ValueStatsEntry], key: str) -> ValueStatsEntry:
    return ensure_value_stats_entry(stats_map, key)


def _canonical_counter_set(value: Any) -> List[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    out: List[str] = []
    for v in value:
        s = str(v or "").strip()
        if s:
            out.append(s)
    return sorted(set(out))


def _recompute_card_subaccount_candidates(
    *,
    cache: CCClientCache,
    candidate_cfg: Dict[str, Any],
    per_sub_counter_sets_new: Dict[str, Set[str]],
) -> None:
    existing_candidates = (
        cache.card_subaccount_candidates if isinstance(cache.card_subaccount_candidates, dict) else {}
    )
    counter_sets: Dict[str, Set[str]] = {}
    for subaccount, entry in existing_candidates.items():
        if not isinstance(entry, dict):
            continue
        seen = set(_canonical_counter_set(entry.get("counter_accounts_seen")))
        if seen:
            counter_sets[str(subaccount)] = seen

    for subaccount, seen in per_sub_counter_sets_new.items():
        if subaccount not in counter_sets:
            counter_sets[subaccount] = set()
        counter_sets[subaccount].update(seen)

    unique_merchants_by_sub: Dict[str, int] = {}
    for merchant_key, stats_entry in cache.merchant_key_payable_sub_stats.items():
        del merchant_key
        for subaccount, count in stats_entry.value_counts.items():
            if int(count) <= 0:
                continue
            unique_merchants_by_sub[subaccount] = int(unique_merchants_by_sub.get(subaccount, 0)) + 1

    total_count_by_sub = dict(cache.payable_sub_global_stats.value_counts)
    manual_allow = set(_normalize_str_list(candidate_cfg.get("manual_allow"), default=[]))

    all_subaccounts: Set[str] = set(total_count_by_sub.keys())
    all_subaccounts.update(unique_merchants_by_sub.keys())
    all_subaccounts.update(counter_sets.keys())
    all_subaccounts.update(manual_allow)

    min_total_count = _as_int(candidate_cfg.get("min_total_count", 5), 5)
    min_unique_merchants = _as_int(candidate_cfg.get("min_unique_merchants", 3), 3)
    min_unique_counter_accounts = _as_int(candidate_cfg.get("min_unique_counter_accounts", 2), 2)

    rebuilt: Dict[str, Dict[str, Any]] = {}
    for subaccount in sorted(str(v) for v in all_subaccounts if str(v)):
        total_count = _as_int(total_count_by_sub.get(subaccount, 0), 0)
        unique_merchants = _as_int(unique_merchants_by_sub.get(subaccount, 0), 0)
        counter_seen_sorted = sorted(counter_sets.get(subaccount) or set())
        unique_counter_accounts = len(counter_seen_sorted)

        by_thresholds = (
            total_count >= min_total_count
            and unique_merchants >= min_unique_merchants
            and unique_counter_accounts >= min_unique_counter_accounts
        )
        is_manual = subaccount in manual_allow
        is_candidate = bool(by_thresholds or is_manual)

        notes: List[str] = []
        if is_manual:
            notes.append("manual_allow")

        entry: Dict[str, Any] = {
            "total_count": int(total_count),
            "unique_merchants": int(unique_merchants),
            "unique_counter_accounts": int(unique_counter_accounts),
            "is_candidate": is_candidate,
            "counter_accounts_seen": counter_seen_sorted,
        }
        if notes:
            entry["notes"] = notes
        rebuilt[subaccount] = entry

    cache.card_subaccount_candidates = rebuilt


def ensure_cc_client_cache_updated(repo_root: Path, client_id: str) -> Tuple[CCClientCache, Dict[str, Any]]:
    ensure_client_system_dirs(repo_root, client_id, line_id=LINE_ID_CC)
    line_root = get_client_root(repo_root, client_id, line_id=LINE_ID_CC)

    ledger_ref_inbox_dir = line_root / "inputs" / "ledger_ref"
    ledger_ref_store_dir = get_ledger_ref_ingest_dir(repo_root, client_id, line_id=LINE_ID_CC)
    ingest_manifest_path = get_ledger_ref_ingested_path(repo_root, client_id, line_id=LINE_ID_CC)
    cache_path = get_client_cache_path(repo_root, client_id, line_id=LINE_ID_CC)

    for d in [ledger_ref_inbox_dir, ledger_ref_store_dir, ingest_manifest_path.parent, cache_path.parent]:
        d.mkdir(parents=True, exist_ok=True)

    config = load_credit_card_line_config(repo_root, client_id)
    thresholds_snapshot = _thresholds_snapshot(config)
    payable_account_name = str(config.get("payable_account_name") or "").strip()
    exclude_counter_accounts = set(
        _normalize_str_list(
            ((config.get("training") or {}).get("exclude_counter_accounts")),
            default=["普通預金", "当座預金"],
        )
    )
    candidate_cfg = config.get("candidate_extraction") if isinstance(config.get("candidate_extraction"), dict) else {}

    manifest, new_shas_csv, dup_shas_csv = ingest_csv_dir(
        dir_path=ledger_ref_inbox_dir,
        store_dir=ledger_ref_store_dir,
        manifest_path=ingest_manifest_path,
        client_id=client_id,
        kind="ledger_ref",
        allow_rename=True,
        include_glob="*.csv",
        relpath_base_dir=line_root,
    )
    manifest, new_shas_txt, dup_shas_txt = ingest_csv_dir(
        dir_path=ledger_ref_inbox_dir,
        store_dir=ledger_ref_store_dir,
        manifest_path=ingest_manifest_path,
        client_id=client_id,
        kind="ledger_ref",
        allow_rename=True,
        include_glob="*.txt",
        relpath_base_dir=line_root,
    )
    new_shas = [str(v) for v in (new_shas_csv + new_shas_txt)]
    dup_shas = [str(v) for v in (dup_shas_csv + dup_shas_txt)]

    cache = CCClientCache.load(cache_path)
    if not cache.client_id:
        cache.client_id = str(client_id)
    if not cache.created_at:
        cache.created_at = _now_utc_iso()
    if not cache.line_id:
        cache.line_id = LINE_ID_CC
    if not isinstance(cache.applied_ledger_ref_sha256, dict):
        cache.applied_ledger_ref_sha256 = {}
    if not isinstance(cache.merchant_key_account_stats, dict):
        cache.merchant_key_account_stats = {}
    if not isinstance(cache.merchant_key_payable_sub_stats, dict):
        cache.merchant_key_payable_sub_stats = {}
    if not isinstance(cache.card_subaccount_candidates, dict):
        cache.card_subaccount_candidates = {}
    if not cache.decision_thresholds:
        cache.decision_thresholds = thresholds_snapshot
    elif not isinstance(cache.decision_thresholds.get("partial_match"), dict):
        cache.decision_thresholds = dict(cache.decision_thresholds)
        cache.decision_thresholds["partial_match"] = thresholds_snapshot.get("partial_match") or {}

    warnings: List[str] = []
    applied_new_shas: List[str] = []
    rows_total_added = 0
    rows_used_added = 0
    per_sub_counter_sets_new: Dict[str, Set[str]] = {}

    ingested = manifest.get("ingested") if isinstance(manifest.get("ingested"), dict) else {}
    ingested_order_raw = manifest.get("ingested_order")
    if isinstance(ingested_order_raw, list):
        ingested_order = [str(v) for v in ingested_order_raw]
    else:
        ingested_order = [str(v) for v in ingested.keys()]

    for sha in ingested_order:
        if sha in cache.applied_ledger_ref_sha256:
            continue
        entry = ingested.get(sha)
        if not isinstance(entry, dict):
            warnings.append(f"invalid_manifest_entry: sha={sha}")
            continue

        stored_path = resolve_ledger_ref_stored_path(repo_root, client_id, entry, line_id=LINE_ID_CC)
        if stored_path is None:
            warnings.append(f"missing_stored_path: sha={sha}")
            continue
        if not stored_path.exists():
            warnings.append(f"missing_ingested_file: sha={sha} expected={stored_path}")
            continue

        stored_name = str(entry.get("stored_name") or stored_path.name)
        stored_relpath = str(entry.get("stored_relpath") or "").strip()
        if not stored_relpath:
            try:
                stored_relpath = stored_path.relative_to(line_root).as_posix()
            except ValueError:
                stored_relpath = stored_name

        csv_obj = read_yayoi_csv(stored_path)
        rows_total = 0
        rows_used = 0

        for row in csv_obj.rows:
            rows_total += 1

            summary = token_to_text(row.tokens[COL_SUMMARY], csv_obj.encoding).strip()
            if not summary:
                continue

            debit_account = token_to_text(row.tokens[COL_DEBIT_ACCOUNT], csv_obj.encoding).strip()
            debit_subaccount = token_to_text(row.tokens[COL_DEBIT_SUBACCOUNT], csv_obj.encoding).strip()
            credit_account = token_to_text(row.tokens[COL_CREDIT_ACCOUNT], csv_obj.encoding).strip()
            credit_subaccount = token_to_text(row.tokens[COL_CREDIT_SUBACCOUNT], csv_obj.encoding).strip()

            payable_subaccount = ""
            counter_account = ""
            if debit_account == payable_account_name and credit_account != payable_account_name:
                payable_subaccount = debit_subaccount
                counter_account = credit_account
            elif credit_account == payable_account_name and debit_account != payable_account_name:
                payable_subaccount = credit_subaccount
                counter_account = debit_account
            else:
                continue

            if not payable_subaccount:
                continue
            if not counter_account:
                continue
            if counter_account in exclude_counter_accounts:
                continue

            merchant_key = merchant_key_from_summary(summary, config)
            if not merchant_key:
                continue

            rows_used += 1
            _ensure_stats_entry(cache.merchant_key_account_stats, merchant_key).add_account(counter_account)
            _ensure_value_stats_entry(cache.merchant_key_payable_sub_stats, merchant_key).update(payable_subaccount)
            cache.payable_sub_global_stats.update(payable_subaccount)

            if payable_subaccount not in per_sub_counter_sets_new:
                per_sub_counter_sets_new[payable_subaccount] = set()
            per_sub_counter_sets_new[payable_subaccount].add(counter_account)

        rows_total_added += int(rows_total)
        rows_used_added += int(rows_used)
        cache.applied_ledger_ref_sha256[sha] = {
            "applied_at": _now_utc_iso(),
            "stored_name": stored_name,
            "stored_relpath": stored_relpath,
            "rows_total": int(rows_total),
            "rows_used": int(rows_used),
        }
        applied_new_shas.append(sha)

    _recompute_card_subaccount_candidates(
        cache=cache,
        candidate_cfg=candidate_cfg,
        per_sub_counter_sets_new=per_sub_counter_sets_new,
    )

    cache.schema = SCHEMA_CC_CLIENT_CACHE_V0
    cache.version = "0.1"
    cache.client_id = str(client_id)
    cache.line_id = LINE_ID_CC
    cache.append_only = True
    cache.updated_at = _now_utc_iso()
    if not cache.decision_thresholds:
        cache.decision_thresholds = thresholds_snapshot
    elif not isinstance(cache.decision_thresholds.get("partial_match"), dict):
        cache.decision_thresholds = dict(cache.decision_thresholds)
        cache.decision_thresholds["partial_match"] = thresholds_snapshot.get("partial_match") or {}

    cache.save(cache_path)

    summary: Dict[str, Any] = {
        "client_id": str(client_id),
        "line_id": LINE_ID_CC,
        "ledger_ref_dir": str(ledger_ref_inbox_dir),
        "cache_path": str(cache_path),
        "ingest_manifest_path": str(ingest_manifest_path),
        "ingested_new_files": int(len(new_shas)),
        "ingested_duplicate_files": int(len(dup_shas)),
        "applied_new_files": int(len(applied_new_shas)),
        "ingested_new_sha256": new_shas,
        "ingested_duplicate_sha256": dup_shas,
        "applied_new_sha256": applied_new_shas,
        "rows_total_added": int(rows_total_added),
        "rows_used_added": int(rows_used_added),
        "warnings": warnings,
    }
    return cache, summary
