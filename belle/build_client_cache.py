# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Client cache updater (append-only).

This module implements the "client_cache is a cache" design:
- ledger_ref/ is append-only batches (CSV).
- We ingest (sha256 + move+rename into artifacts/ingest/ledger_ref) into a manifest.
- client_cache.json grows by applying only not-yet-applied batches.

The replacer MUST call the ensure/update function before using client_cache,
so that T-number and T×category evidence are always up to date.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from .lines import is_line_implemented, validate_line_id
from .yayoi_csv import read_yayoi_csv, token_to_text
from .text import extract_t_number, vendor_key_from_summary
from .lexicon import Lexicon, match_summary
from .client_cache import ClientCache, StatsEntry
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


def _thresholds_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    thr = config.get("thresholds") or {}
    return {
        "t_number": {
            "min_count": int(thr.get("t_number_min_count", 3)),
            "min_p_majority": float(thr.get("t_number_p_majority_min", 0.70)),
        },
        "t_number_x_category": {
            "min_count": int(thr.get("t_number_x_category_min_count", 2)),
            "min_p_majority": float(thr.get("t_number_x_category_p_majority_min", 0.75)),
        },
        "vendor_key": {
            "min_count": int(thr.get("vendor_key_min_count", 3)),
            "min_p_majority": float(thr.get("vendor_key_p_majority_min", 0.70)),
        },
        "category": {
            "min_count": int(thr.get("category_min_count", 3)),
            "min_p_majority": float(thr.get("category_p_majority_min", 0.70)),
        },
    }


def _ensure_stats_entry(d: Dict[str, StatsEntry], key: str) -> StatsEntry:
    return ensure_stats_entry(d, key)


def _ensure_nested_stats(d: Dict[str, Dict[str, StatsEntry]], k1: str, k2: str) -> StatsEntry:
    if k1 not in d:
        d[k1] = {}
    if k2 not in d[k1]:
        d[k1][k2] = StatsEntry.empty()
    return d[k1][k2]


def apply_ledger_ref_file_append_only(
    *,
    tm: ClientCache,
    ledger_ref_csv: Path,
    lex: Lexicon,
    dummy_summary_exact: str,
) -> Tuple[int, int]:
    """
    Apply a single ledger_ref CSV (append-only): adds counts into tm distributions.

    Returns (row_total, row_used).
    """
    csv = read_yayoi_csv(ledger_ref_csv)
    row_total = 0
    row_used = 0

    for row in csv.rows:
        row_total += 1
        summary = token_to_text(row.tokens[16], csv.encoding)  # 摘要 (17th col)
        debit = token_to_text(row.tokens[4], csv.encoding)     # 借方勘定科目 (5th col)

        if not summary or summary == dummy_summary_exact:
            continue
        if not debit:
            continue

        row_used += 1

        # global distribution
        tm.global_stats.add_account(debit)

        # T-number
        tnum = extract_t_number(summary)
        if tnum:
            _ensure_stats_entry(tm.t_numbers, tnum).add_account(debit)

        # vendor key
        vkey = vendor_key_from_summary(summary)
        if vkey:
            _ensure_stats_entry(tm.vendor_keys, vkey).add_account(debit)

        # category (from lexicon)
        m = match_summary(lex, summary)
        cat_key = m.category_key
        if cat_key:
            _ensure_stats_entry(tm.categories, cat_key).add_account(debit)

        # T-number × category
        if tnum and cat_key:
            _ensure_nested_stats(tm.t_numbers_by_category, tnum, cat_key).add_account(debit)

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
    dummy = _get_dummy_summary(config)
    thresholds = _thresholds_from_config(config)

    # Ingest (rename + manifest)
    # Accept both .csv and .txt for ledger_ref inputs.
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

    # Load/create client_cache
    if client_cache_path.exists():
        tm = ClientCache.load(client_cache_path)
        # keep thresholds auditable but do not break compatibility
        tm.decision_thresholds = tm.decision_thresholds or thresholds
    else:
        tm = ClientCache.empty(client_id, thresholds=thresholds)

    # Apply only not-yet-applied ingested shas (append-only)
    applied_new: List[str] = []
    rows_total_added = 0
    rows_used_added = 0

    ingested = manifest.get("ingested") or {}
    ingested_order = manifest.get("ingested_order") or list(ingested.keys())

    for sha in ingested_order:
        if sha in (tm.applied_ledger_ref_sha256 or {}):
            continue
        entry = ingested.get(sha) or {}
        p = resolve_ledger_ref_stored_path(repo_root, client_id, entry, line_id=line_id)
        if p is None:
            warnings.append(f"missing_stored_path: sha={sha}")
            continue
        if not p.exists():
            warnings.append(f"missing_ingested_file: sha={sha} expected={p}")
            continue

        stored_name = str(entry.get("stored_name") or p.name)
        stored_relpath = str(entry.get("stored_relpath") or "").strip()
        if not stored_relpath:
            try:
                stored_relpath = p.relative_to(client_dir).as_posix()
            except ValueError:
                stored_relpath = stored_name

        rt, ru = apply_ledger_ref_file_append_only(
            tm=tm,
            ledger_ref_csv=p,
            lex=lex,
            dummy_summary_exact=dummy,
        )
        rows_total_added += int(rt)
        rows_used_added += int(ru)
        tm.applied_ledger_ref_sha256[str(sha)] = {
            "applied_at": _now_utc_iso(),
            "stored_name": stored_name,
            "stored_relpath": stored_relpath,
            "rows_total": int(rt),
            "rows_used": int(ru),
        }
        applied_new.append(str(sha))

    tm.updated_at = _now_utc_iso()
    tm.version = "1.15"
    tm.schema = "belle.client_cache.v1"
    tm.append_only = True
    if not tm.decision_thresholds:
        tm.decision_thresholds = thresholds

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

