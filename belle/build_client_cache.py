# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Client cache updater (append-only).

This module implements the "client_cache is a cache" design:
- ledger_ref/ is append-only batches (CSV).
- We ingest (sha256 + move+rename into artifacts/ingest/ledger_ref) into a manifest.
- client_cache.json grows by applying only not-yet-applied batches.

The replacer MUST call the ensure/update function before using client_cache,
so that receipt account and tax-division evidence are always up to date.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .lines import is_line_implemented, validate_line_id
from .yayoi_columns import COL_DEBIT_ACCOUNT, COL_DEBIT_TAX_DIVISION, COL_SUMMARY
from .yayoi_csv import read_yayoi_csv, token_to_text
from .text import extract_t_number, vendor_key_from_summary
from .lexicon import Lexicon, match_summary
from .client_cache import (
    CLIENT_CACHE_SCHEMA_V2,
    CLIENT_CACHE_VERSION_V2,
    ClientCache,
    StatsEntry,
    TaxStatsEntry,
)
from .stats_utils import ensure_stats_entry
from .ingest import ingest_csv_dir
from .paths import (
    ensure_client_system_dirs,
    get_client_cache_path,
    get_client_root,
    get_ledger_ref_ingest_dir,
    get_ledger_ref_ingested_path,
    resolve_ledger_ref_stored_path,
)


@dataclass
class ClientCacheUpdateSummary:
    client_id: str
    ledger_ref_dir: str
    client_cache_path: str
    ingest_manifest_path: str
    ingested_new_files: List[str]
    ingested_duplicate_files: List[str]
    applied_new_files: List[str]
    rows_total_added: int
    rows_used_added: int
    updated_at: str
    warnings: List[str]


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_dummy_summary(config: Dict[str, Any]) -> str:
    return (config.get("csv_contract") or {}).get("dummy_summary_exact") or "##DUMMY_OCR_UNREADABLE##"


def _account_thresholds_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    thresholds = config.get("thresholds") or {}
    return {
        "t_number": {
            "min_count": int(thresholds.get("t_number_min_count", 3)),
            "min_p_majority": float(thresholds.get("t_number_p_majority_min", 0.70)),
        },
        "t_number_x_category": {
            "min_count": int(thresholds.get("t_number_x_category_min_count", 2)),
            "min_p_majority": float(thresholds.get("t_number_x_category_p_majority_min", 0.75)),
        },
        "vendor_key": {
            "min_count": int(thresholds.get("vendor_key_min_count", 3)),
            "min_p_majority": float(thresholds.get("vendor_key_p_majority_min", 0.70)),
        },
        "category": {
            "min_count": int(thresholds.get("category_min_count", 3)),
            "min_p_majority": float(thresholds.get("category_p_majority_min", 0.70)),
        },
    }


def _tax_thresholds_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    raw = config.get("tax_division_thresholds")
    section = raw if isinstance(raw, dict) else {}
    return {
        "t_number_x_category_target_account": {
            "min_count": int((section.get("t_number_x_category_target_account") or {}).get("min_count", 2)),
            "min_p_majority": float(
                (section.get("t_number_x_category_target_account") or {}).get("min_p_majority", 0.75)
            ),
        },
        "t_number_target_account": {
            "min_count": int((section.get("t_number_target_account") or {}).get("min_count", 3)),
            "min_p_majority": float((section.get("t_number_target_account") or {}).get("min_p_majority", 0.70)),
        },
        "vendor_key_target_account": {
            "min_count": int((section.get("vendor_key_target_account") or {}).get("min_count", 3)),
            "min_p_majority": float((section.get("vendor_key_target_account") or {}).get("min_p_majority", 0.70)),
        },
        "category_target_account": {
            "min_count": int((section.get("category_target_account") or {}).get("min_count", 3)),
            "min_p_majority": float((section.get("category_target_account") or {}).get("min_p_majority", 0.70)),
        },
        "global_target_account": {
            "min_count": int((section.get("global_target_account") or {}).get("min_count", 3)),
            "min_p_majority": float((section.get("global_target_account") or {}).get("min_p_majority", 0.70)),
        },
    }


def _decision_thresholds_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "account": _account_thresholds_from_config(config),
        "tax_division": _tax_thresholds_from_config(config),
    }


def _ensure_stats_entry(d: Dict[str, StatsEntry], key: str) -> StatsEntry:
    return ensure_stats_entry(d, key)


def _ensure_nested_stats(d: Dict[str, Dict[str, StatsEntry]], k1: str, k2: str) -> StatsEntry:
    if k1 not in d:
        d[k1] = {}
    if k2 not in d[k1]:
        d[k1][k2] = StatsEntry.empty()
    return d[k1][k2]


def _ensure_tax_stats_entry(d: Dict[str, TaxStatsEntry], key: str) -> TaxStatsEntry:
    if key not in d:
        d[key] = TaxStatsEntry.empty()
    return d[key]


def _ensure_nested_tax_stats(d: Dict[str, Dict[str, TaxStatsEntry]], k1: str, k2: str) -> TaxStatsEntry:
    if k1 not in d:
        d[k1] = {}
    if k2 not in d[k1]:
        d[k1][k2] = TaxStatsEntry.empty()
    return d[k1][k2]


def _ensure_t_category_account_tax_stats(
    d: Dict[str, Dict[str, Dict[str, TaxStatsEntry]]],
    t_number: str,
    category_key: str,
    debit_account: str,
) -> TaxStatsEntry:
    if t_number not in d:
        d[t_number] = {}
    if category_key not in d[t_number]:
        d[t_number][category_key] = {}
    if debit_account not in d[t_number][category_key]:
        d[t_number][category_key][debit_account] = TaxStatsEntry.empty()
    return d[t_number][category_key][debit_account]


def apply_ledger_ref_file_append_only(
    *,
    tm: ClientCache,
    ledger_ref_csv: Path,
    lex: Lexicon,
    dummy_summary_exact: str,
) -> Tuple[int, int]:
    """
    Apply a single ledger_ref CSV (append-only): adds counts into client distributions.

    Returns (row_total, row_used).
    """
    csv_obj = read_yayoi_csv(ledger_ref_csv)
    row_total = 0
    row_used = 0

    for row in csv_obj.rows:
        row_total += 1
        summary = token_to_text(row.tokens[COL_SUMMARY], csv_obj.encoding)
        debit_account = token_to_text(row.tokens[COL_DEBIT_ACCOUNT], csv_obj.encoding)

        if not summary or summary == dummy_summary_exact:
            continue
        if not debit_account:
            continue

        row_used += 1
        tm.global_stats.add_account(debit_account)

        t_number = extract_t_number(summary)
        if t_number:
            _ensure_stats_entry(tm.t_numbers, t_number).add_account(debit_account)

        vendor_key = vendor_key_from_summary(summary)
        if vendor_key:
            _ensure_stats_entry(tm.vendor_keys, vendor_key).add_account(debit_account)

        match = match_summary(lex, summary)
        category_key = match.category_key
        if category_key:
            _ensure_stats_entry(tm.categories, category_key).add_account(debit_account)

        if t_number and category_key:
            _ensure_nested_stats(tm.t_numbers_by_category, t_number, category_key).add_account(debit_account)

        debit_tax_division = token_to_text(row.tokens[COL_DEBIT_TAX_DIVISION], csv_obj.encoding).strip()
        if not debit_tax_division:
            continue

        _ensure_tax_stats_entry(tm.tax_global_by_account, debit_account).add_tax_division(debit_tax_division)

        if t_number:
            _ensure_nested_tax_stats(tm.tax_t_numbers_by_account, t_number, debit_account).add_tax_division(
                debit_tax_division
            )
        if vendor_key:
            _ensure_nested_tax_stats(tm.tax_vendor_keys_by_account, vendor_key, debit_account).add_tax_division(
                debit_tax_division
            )
        if category_key:
            _ensure_nested_tax_stats(tm.tax_categories_by_account, category_key, debit_account).add_tax_division(
                debit_tax_division
            )
        if t_number and category_key:
            _ensure_t_category_account_tax_stats(
                tm.tax_t_numbers_by_category_and_account,
                t_number,
                category_key,
                debit_account,
            ).add_tax_division(debit_tax_division)

    return row_total, row_used


def ensure_client_cache_updated(
    *,
    repo_root: Path,
    client_id: str,
    lex: Lexicon,
    config: Dict[str, Any],
    line_id: Optional[str] = None,
) -> Tuple[ClientCache, ClientCacheUpdateSummary]:
    """
    Ensure client_cache cache is up-to-date with append-only ledger_ref batches.
    This will:
    - ingest ledger_ref inbox files (sha256+move+rename) into artifacts/ingest/ledger_ref_ingested.json
    - load/create artifacts/cache/client_cache.json
    - apply only not-yet-applied batches into client_cache (append-only)
    """
    if line_id is not None:
        line_id = validate_line_id(line_id)
        if not is_line_implemented(line_id):
            raise ValueError(f"line is unimplemented in Phase 1: {line_id}")

    ensure_client_system_dirs(repo_root, client_id, line_id=line_id)
    client_dir = get_client_root(repo_root, client_id, line_id=line_id)
    ledger_ref_inbox_dir = client_dir / "inputs" / "ledger_ref"
    ledger_ref_store_dir = get_ledger_ref_ingest_dir(repo_root, client_id, line_id=line_id)
    client_cache_path = get_client_cache_path(repo_root, client_id, line_id=line_id)
    ingest_manifest_path = get_ledger_ref_ingested_path(repo_root, client_id, line_id=line_id)

    warnings: List[str] = []
    dummy_summary_exact = _get_dummy_summary(config)
    decision_thresholds = _decision_thresholds_from_config(config)

    manifest, new_shas_csv, dup_shas_csv = ingest_csv_dir(
        dir_path=ledger_ref_inbox_dir,
        store_dir=ledger_ref_store_dir,
        manifest_path=ingest_manifest_path,
        client_id=client_id,
        kind="ledger_ref",
        allow_rename=True,
        include_glob="*.csv",
        relpath_base_dir=client_dir,
    )
    manifest, new_shas_txt, dup_shas_txt = ingest_csv_dir(
        dir_path=ledger_ref_inbox_dir,
        store_dir=ledger_ref_store_dir,
        manifest_path=ingest_manifest_path,
        client_id=client_id,
        kind="ledger_ref",
        allow_rename=True,
        include_glob="*.txt",
        relpath_base_dir=client_dir,
    )
    new_shas = new_shas_csv + new_shas_txt
    dup_shas = dup_shas_csv + dup_shas_txt

    if client_cache_path.exists():
        tm = ClientCache.load(client_cache_path)
        tm.decision_thresholds = tm.decision_thresholds or decision_thresholds
    else:
        tm = ClientCache.empty(client_id, thresholds=decision_thresholds)

    applied_new: List[str] = []
    rows_total_added = 0
    rows_used_added = 0

    ingested = manifest.get("ingested") or {}
    ingested_order = manifest.get("ingested_order") or list(ingested.keys())

    for sha in ingested_order:
        if sha in (tm.applied_ledger_ref_sha256 or {}):
            continue
        entry = ingested.get(sha) or {}
        stored_path = resolve_ledger_ref_stored_path(repo_root, client_id, entry, line_id=line_id)
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
                stored_relpath = stored_path.relative_to(client_dir).as_posix()
            except ValueError:
                stored_relpath = stored_name

        rows_total, rows_used = apply_ledger_ref_file_append_only(
            tm=tm,
            ledger_ref_csv=stored_path,
            lex=lex,
            dummy_summary_exact=dummy_summary_exact,
        )
        rows_total_added += int(rows_total)
        rows_used_added += int(rows_used)
        tm.applied_ledger_ref_sha256[str(sha)] = {
            "applied_at": _now_utc_iso(),
            "stored_name": stored_name,
            "stored_relpath": stored_relpath,
            "rows_total": int(rows_total),
            "rows_used": int(rows_used),
        }
        applied_new.append(str(sha))

    tm.updated_at = _now_utc_iso()
    tm.version = CLIENT_CACHE_VERSION_V2
    tm.schema = CLIENT_CACHE_SCHEMA_V2
    tm.append_only = True
    if not tm.decision_thresholds:
        tm.decision_thresholds = decision_thresholds

    tm.save(client_cache_path)

    summary = ClientCacheUpdateSummary(
        client_id=client_id,
        ledger_ref_dir=str(ledger_ref_inbox_dir),
        client_cache_path=str(client_cache_path),
        ingest_manifest_path=str(ingest_manifest_path),
        ingested_new_files=[str(s) for s in new_shas],
        ingested_duplicate_files=[str(s) for s in dup_shas],
        applied_new_files=applied_new,
        rows_total_added=int(rows_total_added),
        rows_used_added=int(rows_used_added),
        updated_at=tm.updated_at,
        warnings=warnings,
    )

    return tm, summary
