# -*- coding: utf-8 -*-
from __future__ import annotations

import csv as csv_lib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from belle.defaults import CategoryDefaults
from .build_cc_cache import merchant_key_from_summary
from .cc_cache import CCClientCache, ValueStatsEntry
from .client_cache import StatsEntry
from belle.fs_utils import sha256_file_chunked
from belle.lexicon import Lexicon, match_summary
from .paths import get_input_manifest_path, get_review_report_path
from .tax_postprocess import (
    TaxPostprocessSideResult,
    YayoiTaxPostprocessConfig,
    apply_yayoi_tax_postprocess,
    build_tax_postprocess_manifest,
    default_yayoi_tax_postprocess_config,
)
from .yayoi_text import safe_cell_text, set_cell_text
from .yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_CREDIT_TAX_AMOUNT,
    COL_CREDIT_TAX_DIVISION,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_DEBIT_TAX_AMOUNT,
    COL_DEBIT_TAX_DIVISION,
    COL_SUMMARY,
)
from .yayoi_csv import read_yayoi_csv, write_yayoi_csv

_NONE_EVIDENCE = "none"
_FILE_INFERRED_EVIDENCE = "file_inferred"
_ROUTE_TAX_EXACT = "merchant_key_target_account_exact"
_ROUTE_TAX_PARTIAL = "merchant_key_target_account_partial"
_ROUTE_TAX_CATEGORY_DEFAULT = "category_default"
_ROUTE_TAX_GLOBAL_FALLBACK = "global_fallback"
_PLACEHOLDER_DEFAULT = "\u4eee\u6255\u91d1"
_PAYABLE_DEFAULT = "\u672a\u6255\u91d1"
_SPACES_RE = re.compile(r"[ \u3000]+")


@dataclass
class CCFileCardInference:
    status: str
    inferred_payable_subaccount: Optional[str]
    votes_total: int
    top_value: Optional[str]
    top_count: int
    p_majority: float
    reasons: List[str]
    votes_partial_used: int
    partial_examples: List[Tuple[str, str]]


@dataclass
class CCRowDecision:
    row_index_1b: int
    merchant_key: str
    placeholder_side: str
    payable_side: str
    payable_side_detected: str
    changed: bool
    account_changed: int
    evidence_type: str
    lookup_key: str
    sample_total: int
    p_majority: float
    top_count: int
    predicted_account: str
    account_partial_match_used: bool
    payable_account_before_raw: str
    payable_account_after_canonical: str
    payable_account_rewritten: bool
    payable_account_rewrite_reason: str
    canonical_payable_status: str
    canonical_payable_required_failed: bool
    payable_sub_before: str
    payable_sub_after: str
    payable_sub_changed: bool
    payable_sub_evidence: str
    reasons: List[str]
    debit_account_before: str
    debit_account_after: str
    credit_account_before: str
    credit_account_after: str
    debit_sub_before: str
    debit_sub_after: str
    credit_sub_before: str
    credit_sub_after: str
    category_key: str
    category_label: str
    lexicon_quality: str
    matched_needle: str
    is_learned_signal: bool
    target_tax_side: str
    target_tax_division_before: str
    target_tax_division_after: str
    target_tax_division_changed: bool
    tax_evidence_type: str
    tax_lookup_key: str
    tax_confidence: float
    tax_sample_total: int
    tax_p_majority: float
    tax_reasons: List[str]


@dataclass
class CCTaxDecision:
    target_tax_side: str
    target_tax_division_before: str
    target_tax_division_after: str
    changed: bool
    evidence_type: str
    lookup_key: str
    confidence: float
    sample_total: int
    p_majority: float
    top_count: int
    reasons: List[str]


def sha256_file(path: Path) -> str:
    return sha256_file_chunked(path)


def normalize_name(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text or "")
    return _SPACES_RE.sub("", normalized).strip()


def _safe_text(tokens: Sequence[bytes], idx: int, encoding: str) -> str:
    return safe_cell_text(tokens, idx, encoding)


def _set_text(tokens: List[bytes], idx: int, encoding: str, new_text: str) -> None:
    if idx < 0 or idx >= len(tokens):
        return
    set_cell_text(tokens, idx, encoding, new_text)


def _clone_row_tokens(rows: Sequence[Sequence[bytes]]) -> List[List[bytes]]:
    return [list(tokens) for tokens in rows]


def _row_changed(original_tokens: List[bytes], final_tokens: List[bytes]) -> bool:
    return list(original_tokens) != list(final_tokens)


def _tax_result_map(summary) -> Dict[Tuple[int, str], TaxPostprocessSideResult]:
    return {(result.row_index_1b, result.side): result for result in summary.side_results}


def _tax_review_cells(
    *,
    row_index_1b: int,
    encoding: str,
    pre_tax_tokens: List[bytes],
    final_tokens: List[bytes],
    side_results: Dict[Tuple[int, str], TaxPostprocessSideResult],
) -> List[str]:
    values: List[str] = []
    for side, tax_amount_idx in (("debit", COL_DEBIT_TAX_AMOUNT), ("credit", COL_CREDIT_TAX_AMOUNT)):
        result = side_results[(row_index_1b, side)]
        values.extend(
            [
                _safe_text(pre_tax_tokens, tax_amount_idx, encoding),
                _safe_text(final_tokens, tax_amount_idx, encoding),
                result.status,
                "" if result.rate_percent is None else str(result.rate_percent),
                result.calc_mode,
            ]
        )
    return values


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


def _thresholds_root(config: Dict[str, Any], cache: CCClientCache) -> Dict[str, Any]:
    cfg_thresholds = config.get("thresholds")
    if isinstance(cfg_thresholds, dict):
        return cfg_thresholds
    cache_thresholds = cache.decision_thresholds
    if isinstance(cache_thresholds, dict):
        return cache_thresholds
    return {}


def _resolve_merchant_account_thresholds(config: Dict[str, Any], cache: CCClientCache) -> Dict[str, float]:
    thresholds = _thresholds_root(config, cache)
    raw = thresholds.get("merchant_key_account") if isinstance(thresholds.get("merchant_key_account"), dict) else {}
    return {
        "min_count": float(_as_int(raw.get("min_count"), 3)),
        "min_p_majority": float(_as_float(raw.get("min_p_majority"), 0.9)),
    }


def _resolve_merchant_payable_sub_thresholds(config: Dict[str, Any], cache: CCClientCache) -> Dict[str, float]:
    thresholds = _thresholds_root(config, cache)
    account_raw = (
        thresholds.get("merchant_key_account")
        if isinstance(thresholds.get("merchant_key_account"), dict)
        else {}
    )
    payable_raw = (
        thresholds.get("merchant_key_payable_subaccount")
        if isinstance(thresholds.get("merchant_key_payable_subaccount"), dict)
        else {}
    )
    return {
        "min_count": float(_as_int(payable_raw.get("min_count"), _as_int(account_raw.get("min_count"), 3))),
        "min_p_majority": float(
            _as_float(
                payable_raw.get("min_p_majority"),
                _as_float(account_raw.get("min_p_majority"), 0.9),
            )
        ),
    }


def _resolve_file_level_thresholds(config: Dict[str, Any], cache: CCClientCache) -> Dict[str, float]:
    thresholds = _thresholds_root(config, cache)
    raw = (
        thresholds.get("file_level_card_inference")
        if isinstance(thresholds.get("file_level_card_inference"), dict)
        else {}
    )
    return {
        "min_votes": float(_as_int(raw.get("min_votes"), 3)),
        "min_p_majority": float(_as_float(raw.get("min_p_majority"), 0.9)),
    }


def _resolve_tax_division_thresholds(config: Dict[str, Any], cache: CCClientCache) -> Dict[str, Dict[str, float]]:
    raw = (
        config.get("tax_division_thresholds")
        if isinstance(config.get("tax_division_thresholds"), dict)
        else {}
    )
    if not raw:
        cache_thresholds = cache.decision_thresholds if isinstance(cache.decision_thresholds, dict) else {}
        if isinstance(cache_thresholds.get("tax_division_thresholds"), dict):
            raw = cache_thresholds.get("tax_division_thresholds") or {}
    exact_raw = raw.get(_ROUTE_TAX_EXACT) if isinstance(raw.get(_ROUTE_TAX_EXACT), dict) else {}
    partial_raw = raw.get(_ROUTE_TAX_PARTIAL) if isinstance(raw.get(_ROUTE_TAX_PARTIAL), dict) else {}
    return {
        _ROUTE_TAX_EXACT: {
            "min_count": float(_as_int(exact_raw.get("min_count"), 3)),
            "min_p_majority": float(_as_float(exact_raw.get("min_p_majority"), 0.9)),
        },
        _ROUTE_TAX_PARTIAL: {
            "min_count": float(_as_int(partial_raw.get("min_count"), 3)),
            "min_p_majority": float(_as_float(partial_raw.get("min_p_majority"), 0.9)),
        },
    }


def _resolve_partial_match_settings(config: Dict[str, Any], cache: CCClientCache) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "enabled": True,
        "direction": "cache_key_in_input",
        "require_unique_longest": True,
        "min_match_len": 4,
        "min_stats_sample_total": 10,
        "min_stats_p_majority": 0.95,
    }

    src: Dict[str, Any] = {}
    cfg_partial = config.get("partial_match")
    if isinstance(cfg_partial, dict):
        src = cfg_partial
    else:
        thresholds = _thresholds_root(config, cache)
        threshold_partial = thresholds.get("partial_match")
        if isinstance(threshold_partial, dict):
            src = threshold_partial

    direction = str(src.get("direction") or defaults["direction"]).strip()
    if direction != "cache_key_in_input":
        direction = str(defaults["direction"])

    min_match_len = _as_int(src.get("min_match_len"), int(defaults["min_match_len"]))
    if min_match_len < 1:
        min_match_len = int(defaults["min_match_len"])

    min_stats_sample_total = _as_int(
        src.get("min_stats_sample_total"),
        int(defaults["min_stats_sample_total"]),
    )
    if min_stats_sample_total < 1:
        min_stats_sample_total = int(defaults["min_stats_sample_total"])

    min_stats_p_majority = _as_float(
        src.get("min_stats_p_majority"),
        float(defaults["min_stats_p_majority"]),
    )
    if min_stats_p_majority < 0.0 or min_stats_p_majority > 1.0:
        min_stats_p_majority = float(defaults["min_stats_p_majority"])

    return {
        "enabled": _as_bool(src.get("enabled"), bool(defaults["enabled"])),
        "direction": direction,
        "require_unique_longest": _as_bool(
            src.get("require_unique_longest"),
            bool(defaults["require_unique_longest"]),
        ),
        "min_match_len": int(min_match_len),
        "min_stats_sample_total": int(min_stats_sample_total),
        "min_stats_p_majority": float(min_stats_p_majority),
    }


def resolve_partial_match_key(
    input_key: str,
    candidate_keys: Iterable[str],
    min_len: int,
) -> Optional[str]:
    key = str(input_key or "")
    if not key:
        return None
    min_len_i = int(min_len)
    if min_len_i < 1:
        min_len_i = 1

    matched: Set[str] = set()
    for candidate in candidate_keys:
        c = str(candidate or "").strip()
        if not c:
            continue
        if len(c) < min_len_i:
            continue
        if c in key:
            matched.add(c)

    if not matched:
        return None

    max_len = max(len(v) for v in matched)
    longest = sorted(v for v in matched if len(v) == max_len)
    if len(longest) != 1:
        return None
    return longest[0]


def _build_eligible_account_partial_keys(
    cache: CCClientCache,
    *,
    min_stats_sample_total: int,
    min_stats_p_majority: float,
) -> Set[str]:
    eligible: Set[str] = set()
    for key, stats_entry in (cache.merchant_key_account_stats or {}).items():
        if isinstance(stats_entry, dict):
            stats_entry = StatsEntry.from_obj(stats_entry)
        if not isinstance(stats_entry, StatsEntry):
            continue
        k = str(key or "").strip()
        if not k:
            continue
        if int(stats_entry.sample_total) < int(min_stats_sample_total):
            continue
        if float(stats_entry.p_majority) < float(min_stats_p_majority):
            continue
        if not str(stats_entry.top_account or "").strip():
            continue
        eligible.add(k)
    return eligible


def _build_eligible_payable_partial_keys(
    cache: CCClientCache,
    *,
    min_stats_sample_total: int,
    min_stats_p_majority: float,
) -> Set[str]:
    eligible: Set[str] = set()
    for key, stats_entry in (cache.merchant_key_payable_sub_stats or {}).items():
        if isinstance(stats_entry, dict):
            stats_entry = ValueStatsEntry.from_obj(stats_entry)
        if not isinstance(stats_entry, ValueStatsEntry):
            continue
        k = str(key or "").strip()
        if not k:
            continue
        if int(stats_entry.sample_total) < int(min_stats_sample_total):
            continue
        if float(stats_entry.p_majority) < float(min_stats_p_majority):
            continue
        if not str(stats_entry.top_value or "").strip():
            continue
        eligible.add(k)
    return eligible


def _load_cc_tax_stats_entry(
    cache: CCClientCache,
    lookup_key: str,
    target_account: str,
) -> Optional[ValueStatsEntry]:
    tax_map = cache.merchant_key_target_account_tax_stats if isinstance(cache.merchant_key_target_account_tax_stats, dict) else {}
    by_account = tax_map.get(str(lookup_key or ""))
    if not isinstance(by_account, dict):
        return None
    entry = by_account.get(str(target_account or ""))
    if isinstance(entry, dict):
        entry = ValueStatsEntry.from_obj(entry)
    return entry if isinstance(entry, ValueStatsEntry) else None


def _tax_threshold_pass(
    *,
    stats: ValueStatsEntry,
    min_count: int,
    min_p_majority: float,
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    if int(stats.sample_total) < int(min_count):
        reasons.append("tax_min_count_not_met")
    if float(stats.p_majority) < float(min_p_majority):
        reasons.append("tax_p_majority_not_met")
    if not str(stats.top_value or "").strip():
        reasons.append("tax_top_missing")
    if int(stats.top_count) <= 0:
        reasons.append("tax_top_count_invalid")
    counts = stats.value_counts if isinstance(stats.value_counts, dict) else {}
    if counts and int(stats.top_count) > 0:
        tie_count = sum(1 for _, cnt in counts.items() if int(cnt) == int(stats.top_count))
        if tie_count != 1:
            reasons.append("tax_top_tie")
    return (len(reasons) == 0, reasons)


def _target_tax_side_from_placeholder_side(placeholder_side: str) -> str:
    if placeholder_side in {"debit", "credit"}:
        return placeholder_side
    return ""


def _target_tax_col_from_side(side: str) -> Optional[int]:
    if side == "debit":
        return COL_DEBIT_TAX_DIVISION
    if side == "credit":
        return COL_CREDIT_TAX_DIVISION
    return None


def _append_partial_example(examples: List[Tuple[str, str]], input_key: str, matched_key: str) -> None:
    input_s = str(input_key or "").strip()
    matched_s = str(matched_key or "").strip()
    if not input_s or not matched_s:
        return
    pair = (input_s, matched_s)
    if pair in examples:
        return
    if len(examples) >= 10:
        return
    examples.append(pair)


def _is_candidate_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _pick_top_votes(votes: Dict[str, int]) -> Tuple[Optional[str], int, int]:
    if not votes:
        return None, 0, 0
    sorted_votes = sorted(((str(k), int(v)) for k, v in votes.items()), key=lambda kv: (-kv[1], kv[0]))
    top_value, top_count = sorted_votes[0]
    tie_count = sum(1 for _, cnt in sorted_votes if int(cnt) == int(top_count))
    return top_value, int(top_count), int(tie_count)


def infer_file_payable_subaccount(
    rows: Sequence[Any],
    encoding: str,
    cache: CCClientCache,
    config: Dict[str, Any],
    *,
    partial_match_settings: Optional[Dict[str, Any]] = None,
    eligible_payable_partial_keys: Optional[Iterable[str]] = None,
) -> CCFileCardInference:
    reasons: List[str] = []
    candidate_map = cache.card_subaccount_candidates if isinstance(cache.card_subaccount_candidates, dict) else {}
    eligible = {
        str(subaccount).strip()
        for subaccount, meta in candidate_map.items()
        if str(subaccount).strip() and isinstance(meta, dict) and _is_candidate_enabled(meta.get("is_candidate"))
    }

    if not eligible:
        reasons.append("no_candidates_flagged")
        return CCFileCardInference(
            status="SKIP",
            inferred_payable_subaccount=None,
            votes_total=0,
            top_value=None,
            top_count=0,
            p_majority=0.0,
            reasons=reasons,
            votes_partial_used=0,
            partial_examples=[],
        )

    row_thresholds = _resolve_merchant_payable_sub_thresholds(config, cache)
    row_min_count = int(row_thresholds.get("min_count", 3))
    row_min_p_majority = float(row_thresholds.get("min_p_majority", 0.9))
    votes: Dict[str, int] = {}
    votes_partial_used = 0
    partial_examples: List[Tuple[str, str]] = []
    partial_cfg = (
        partial_match_settings
        if isinstance(partial_match_settings, dict)
        else _resolve_partial_match_settings(config, cache)
    )
    partial_enabled = (
        bool(partial_cfg.get("enabled"))
        and str(partial_cfg.get("direction") or "") == "cache_key_in_input"
        and bool(partial_cfg.get("require_unique_longest", True))
    )
    partial_min_match_len = int(_as_int(partial_cfg.get("min_match_len"), 4))
    partial_candidates = {
        str(v or "").strip()
        for v in (eligible_payable_partial_keys or [])
        if str(v or "").strip()
    }

    for row in rows:
        tokens = getattr(row, "tokens", [])
        summary = _safe_text(tokens, COL_SUMMARY, encoding)
        merchant_key = merchant_key_from_summary(summary, config)
        if not merchant_key:
            continue
        lookup_key = merchant_key
        partial_matched_key: Optional[str] = None
        stats_entry = cache.merchant_key_payable_sub_stats.get(lookup_key)
        if stats_entry is None and partial_enabled:
            matched = resolve_partial_match_key(merchant_key, partial_candidates, partial_min_match_len)
            if matched:
                lookup_key = matched
                partial_matched_key = matched
                stats_entry = cache.merchant_key_payable_sub_stats.get(lookup_key)
        if isinstance(stats_entry, dict):
            stats_entry = ValueStatsEntry.from_obj(stats_entry)
        if not isinstance(stats_entry, ValueStatsEntry):
            continue

        if int(stats_entry.sample_total) < row_min_count:
            continue
        if float(stats_entry.p_majority) < row_min_p_majority:
            continue

        row_top = str(stats_entry.top_value or "").strip()
        if not row_top:
            continue
        if row_top not in eligible:
            continue
        votes[row_top] = int(votes.get(row_top, 0)) + 1
        if partial_matched_key is not None:
            votes_partial_used += 1
            _append_partial_example(partial_examples, merchant_key, partial_matched_key)

    votes_total = int(sum(votes.values()))
    if votes_total <= 0:
        reasons.append("no_votes")
        return CCFileCardInference(
            status="SKIP",
            inferred_payable_subaccount=None,
            votes_total=0,
            top_value=None,
            top_count=0,
            p_majority=0.0,
            reasons=reasons,
            votes_partial_used=int(votes_partial_used),
            partial_examples=list(partial_examples),
        )

    top_value, top_count, tie_count = _pick_top_votes(votes)
    p_majority = float(top_count / votes_total) if votes_total > 0 else 0.0

    file_thresholds = _resolve_file_level_thresholds(config, cache)
    min_votes = int(file_thresholds.get("min_votes", 3))
    min_p_majority = float(file_thresholds.get("min_p_majority", 0.9))

    fail_reasons: List[str] = []
    if tie_count != 1:
        fail_reasons.append("top_tie")
    if votes_total < min_votes:
        fail_reasons.append("below_min_votes")
    if p_majority < min_p_majority:
        fail_reasons.append("below_min_p_majority")

    if fail_reasons:
        return CCFileCardInference(
            status="FAIL",
            inferred_payable_subaccount=None,
            votes_total=votes_total,
            top_value=top_value,
            top_count=top_count,
            p_majority=p_majority,
            reasons=[*reasons, *fail_reasons],
            votes_partial_used=int(votes_partial_used),
            partial_examples=list(partial_examples),
        )

    return CCFileCardInference(
        status="OK",
        inferred_payable_subaccount=top_value,
        votes_total=votes_total,
        top_value=top_value,
        top_count=top_count,
        p_majority=p_majority,
        reasons=reasons,
        votes_partial_used=int(votes_partial_used),
        partial_examples=list(partial_examples),
    )


def _detect_side(debit_account: str, credit_account: str, target_account: str) -> str:
    debit_match = debit_account == target_account
    credit_match = credit_account == target_account
    if debit_match and not credit_match:
        return "debit"
    if credit_match and not debit_match:
        return "credit"
    if debit_match and credit_match:
        return "ambiguous"
    return "none"


def _detect_side_from_candidates(
    debit_account: str,
    credit_account: str,
    candidate_accounts: Iterable[str],
) -> str:
    normalized_candidates = {
        normalize_name(str(account or ""))
        for account in candidate_accounts
        if normalize_name(str(account or ""))
    }
    if not normalized_candidates:
        return "none"
    debit_match = debit_account in normalized_candidates
    credit_match = credit_account in normalized_candidates
    if debit_match and not credit_match:
        return "debit"
    if credit_match and not debit_match:
        return "credit"
    if debit_match and credit_match:
        return "ambiguous"
    return "none"


def _target_payable_placeholder_names(config: Dict[str, Any]) -> List[str]:
    raw = config.get("target_payable_placeholder_names")
    names: List[str] = []
    if isinstance(raw, list):
        for value in raw:
            text = str(value or "").strip()
            if text:
                names.append(text)
    if names:
        return names

    bridge_name = str(config.get("payable_account_name") or _PAYABLE_DEFAULT).strip()
    return [bridge_name] if bridge_name else [_PAYABLE_DEFAULT]


def _canonical_payable_snapshot(cache: CCClientCache) -> Dict[str, Any]:
    raw = cache.canonical_payable if isinstance(cache.canonical_payable, dict) else {}
    reasons_raw = raw.get("reasons") if isinstance(raw.get("reasons"), list) else []
    value_counts_raw = raw.get("value_counts") if isinstance(raw.get("value_counts"), dict) else {}
    return {
        "status": str(raw.get("status") or "EMPTY").strip() or "EMPTY",
        "account_name": str(raw.get("account_name") or "").strip(),
        "sample_total": int(_as_int(raw.get("sample_total"), 0)),
        "top_count": int(_as_int(raw.get("top_count"), 0)),
        "p_majority": float(_as_float(raw.get("p_majority"), 0.0)),
        "value_counts": {str(k): int(_as_int(v, 0)) for k, v in value_counts_raw.items()},
        "reasons": [str(reason) for reason in reasons_raw if str(reason or "").strip()],
    }


def _detect_payable_side(
    *,
    debit_account: str,
    credit_account: str,
    config: Dict[str, Any],
    cache: CCClientCache,
) -> Tuple[str, Dict[str, Any]]:
    canonical_payable = _canonical_payable_snapshot(cache)
    candidate_accounts = list(_target_payable_placeholder_names(config))
    if canonical_payable["status"] == "OK" and canonical_payable["account_name"]:
        candidate_accounts.append(str(canonical_payable["account_name"]))
    return (
        _detect_side_from_candidates(
            debit_account=debit_account,
            credit_account=credit_account,
            candidate_accounts=candidate_accounts,
        ),
        canonical_payable,
    )


def _account_col_from_side(side: str) -> Optional[int]:
    if side == "debit":
        return COL_DEBIT_ACCOUNT
    if side == "credit":
        return COL_CREDIT_ACCOUNT
    return None


def _subaccount_col_from_side(side: str) -> Optional[int]:
    if side == "debit":
        return COL_DEBIT_SUBACCOUNT
    if side == "credit":
        return COL_CREDIT_SUBACCOUNT
    return None


def _account_threshold_pass(
    *,
    stats: StatsEntry,
    min_count: int,
    min_p_majority: float,
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    if int(stats.sample_total) < int(min_count):
        reasons.append("account_min_count_not_met")
    if float(stats.p_majority) < float(min_p_majority):
        reasons.append("account_p_majority_not_met")
    top_account = str(stats.top_account or "").strip()
    if not top_account:
        reasons.append("account_top_missing")
    if int(stats.top_count) <= 0:
        reasons.append("account_top_count_invalid")
    counts = stats.debit_account_counts if isinstance(stats.debit_account_counts, dict) else {}
    if counts and int(stats.top_count) > 0:
        tie_count = sum(1 for _, cnt in counts.items() if int(cnt) == int(stats.top_count))
        if tie_count != 1:
            reasons.append("account_top_tie")
    return (len(reasons) == 0, reasons)


def decide_cc_tax(
    *,
    tokens: Sequence[bytes],
    encoding: str,
    cache: CCClientCache,
    config: Dict[str, Any],
    decision: CCRowDecision,
    defaults_opt: Optional[CategoryDefaults],
    partial_match_settings: Dict[str, Any],
    eligible_account_partial_keys: Optional[Iterable[str]] = None,
) -> CCTaxDecision:
    target_tax_side = _target_tax_side_from_placeholder_side(decision.placeholder_side)
    tax_col = _target_tax_col_from_side(target_tax_side)
    before = _safe_text(tokens, tax_col, encoding) if tax_col is not None else ""
    predicted_account = str(decision.predicted_account or "").strip()
    merchant_key = str(decision.merchant_key or "").strip()
    tax_thresholds = _resolve_tax_division_thresholds(config, cache)

    base = CCTaxDecision(
        target_tax_side=target_tax_side,
        target_tax_division_before=before,
        target_tax_division_after=before,
        changed=False,
        evidence_type=_NONE_EVIDENCE,
        lookup_key=merchant_key,
        confidence=0.0,
        sample_total=0,
        p_majority=0.0,
        top_count=0,
        reasons=[],
    )

    if tax_col is None:
        base.reasons.append("target_tax_side_not_determined")
        return base
    if not predicted_account:
        base.reasons.append("predicted_account_empty")
        return base
    if not merchant_key:
        base.reasons.append("merchant_key_empty")
        return base

    exact_entry = _load_cc_tax_stats_entry(cache, merchant_key, predicted_account)
    if exact_entry is not None:
        base.sample_total = int(exact_entry.sample_total)
        base.p_majority = float(exact_entry.p_majority)
        base.top_count = int(exact_entry.top_count)
        passed, reasons = _tax_threshold_pass(
            stats=exact_entry,
            min_count=int((tax_thresholds.get(_ROUTE_TAX_EXACT) or {}).get("min_count", 3)),
            min_p_majority=float((tax_thresholds.get(_ROUTE_TAX_EXACT) or {}).get("min_p_majority", 0.9)),
        )
        if passed:
            chosen_tax = str(exact_entry.top_value or "").strip()
            return CCTaxDecision(
                target_tax_side=target_tax_side,
                target_tax_division_before=before,
                target_tax_division_after=chosen_tax or before,
                changed=(chosen_tax != before) if chosen_tax else False,
                evidence_type=_ROUTE_TAX_EXACT,
                lookup_key=merchant_key,
                confidence=float(exact_entry.p_majority),
                sample_total=int(exact_entry.sample_total),
                p_majority=float(exact_entry.p_majority),
                top_count=int(exact_entry.top_count),
                reasons=["tax_exact_selected"],
            )
        base.reasons.extend(reasons)
    else:
        base.reasons.append("tax_exact_stats_not_found")

    partial_cfg = partial_match_settings if isinstance(partial_match_settings, dict) else {}
    partial_enabled = (
        bool(partial_cfg.get("enabled"))
        and str(partial_cfg.get("direction") or "") == "cache_key_in_input"
        and bool(partial_cfg.get("require_unique_longest", True))
    )
    partial_min_match_len = int(_as_int(partial_cfg.get("min_match_len"), 4))
    partial_lookup_key = ""
    if partial_enabled:
        if decision.account_partial_match_used and str(decision.lookup_key or "").strip():
            partial_lookup_key = str(decision.lookup_key or "").strip()
            if partial_lookup_key == merchant_key:
                partial_lookup_key = ""
        elif not decision.account_partial_match_used:
            partial_lookup_key = str(
                resolve_partial_match_key(
                    merchant_key,
                    eligible_account_partial_keys or [],
                    partial_min_match_len,
                )
                or ""
            ).strip()
    if partial_lookup_key:
        partial_entry = _load_cc_tax_stats_entry(cache, partial_lookup_key, predicted_account)
        if partial_entry is not None:
            passed, reasons = _tax_threshold_pass(
                stats=partial_entry,
                min_count=int((tax_thresholds.get(_ROUTE_TAX_PARTIAL) or {}).get("min_count", 3)),
                min_p_majority=float((tax_thresholds.get(_ROUTE_TAX_PARTIAL) or {}).get("min_p_majority", 0.9)),
            )
            if passed:
                chosen_tax = str(partial_entry.top_value or "").strip()
                return CCTaxDecision(
                    target_tax_side=target_tax_side,
                    target_tax_division_before=before,
                    target_tax_division_after=chosen_tax or before,
                    changed=(chosen_tax != before) if chosen_tax else False,
                    evidence_type=_ROUTE_TAX_PARTIAL,
                    lookup_key=partial_lookup_key,
                    confidence=float(partial_entry.p_majority),
                    sample_total=int(partial_entry.sample_total),
                    p_majority=float(partial_entry.p_majority),
                    top_count=int(partial_entry.top_count),
                    reasons=["tax_partial_selected"],
                )
            base.sample_total = int(partial_entry.sample_total)
            base.p_majority = float(partial_entry.p_majority)
            base.top_count = int(partial_entry.top_count)
            base.reasons.extend(reasons)
        else:
            base.reasons.append("tax_partial_stats_not_found")
    else:
        base.reasons.append("tax_partial_not_applicable")

    if defaults_opt is not None and decision.category_key:
        rule = defaults_opt.defaults.get(decision.category_key)
        if rule is not None:
            target_tax_division = str(rule.target_tax_division or "").strip()
            if target_tax_division:
                return CCTaxDecision(
                    target_tax_side=target_tax_side,
                    target_tax_division_before=before,
                    target_tax_division_after=target_tax_division,
                    changed=target_tax_division != before,
                    evidence_type=_ROUTE_TAX_CATEGORY_DEFAULT,
                    lookup_key="",
                    confidence=float(rule.confidence),
                    sample_total=0,
                    p_majority=0.0,
                    top_count=0,
                    reasons=["tax_category_default_applied"],
                )
            base.reasons.append("tax_category_default_blank")
        else:
            base.reasons.append("tax_category_default_missing")
    else:
        base.reasons.append("tax_category_not_available")

    if defaults_opt is not None:
        global_tax_division = str(defaults_opt.global_fallback.target_tax_division or "").strip()
        if global_tax_division:
            return CCTaxDecision(
                target_tax_side=target_tax_side,
                target_tax_division_before=before,
                target_tax_division_after=global_tax_division,
                changed=global_tax_division != before,
                evidence_type=_ROUTE_TAX_GLOBAL_FALLBACK,
                lookup_key="",
                confidence=float(defaults_opt.global_fallback.confidence),
                sample_total=0,
                p_majority=0.0,
                top_count=0,
                reasons=["tax_global_fallback_applied"],
            )
        base.reasons.append("tax_global_fallback_blank")
    else:
        base.reasons.append("tax_defaults_unavailable")

    return base


def decide_cc_row(
    tokens: Sequence[bytes],
    encoding: str,
    cache: CCClientCache,
    config: Dict[str, Any],
    inferred_payable_subaccount_opt: Optional[str],
    lex_opt: Optional[Lexicon] = None,
    defaults_opt: Optional[CategoryDefaults] = None,
    *,
    partial_match_settings: Optional[Dict[str, Any]] = None,
    eligible_account_partial_keys: Optional[Iterable[str]] = None,
) -> Tuple[List[bytes], CCRowDecision]:
    new_tokens = list(tokens)
    summary = _safe_text(tokens, COL_SUMMARY, encoding)
    debit_account_before = _safe_text(tokens, COL_DEBIT_ACCOUNT, encoding)
    credit_account_before = _safe_text(tokens, COL_CREDIT_ACCOUNT, encoding)
    debit_sub_before = _safe_text(tokens, COL_DEBIT_SUBACCOUNT, encoding)
    credit_sub_before = _safe_text(tokens, COL_CREDIT_SUBACCOUNT, encoding)

    placeholder_account_name = str(config.get("placeholder_account_name") or _PLACEHOLDER_DEFAULT)

    debit_key = normalize_name(debit_account_before)
    credit_key = normalize_name(credit_account_before)
    placeholder_key = normalize_name(placeholder_account_name)

    placeholder_side = _detect_side(debit_key, credit_key, placeholder_key)
    payable_side, canonical_payable = _detect_payable_side(
        debit_account=debit_key,
        credit_account=credit_key,
        config=config,
        cache=cache,
    )
    merchant_key = merchant_key_from_summary(summary, config)
    partial_cfg = (
        partial_match_settings
        if isinstance(partial_match_settings, dict)
        else _resolve_partial_match_settings(config, cache)
    )
    partial_enabled = (
        bool(partial_cfg.get("enabled"))
        and str(partial_cfg.get("direction") or "") == "cache_key_in_input"
        and bool(partial_cfg.get("require_unique_longest", True))
    )
    partial_min_match_len = int(_as_int(partial_cfg.get("min_match_len"), 4))
    partial_candidates = {
        str(v or "").strip()
        for v in (eligible_account_partial_keys or [])
        if str(v or "").strip()
    }

    decision = CCRowDecision(
        row_index_1b=0,
        merchant_key=merchant_key,
        placeholder_side=placeholder_side,
        payable_side=payable_side,
        payable_side_detected=payable_side,
        changed=False,
        account_changed=0,
        evidence_type=_NONE_EVIDENCE,
        lookup_key=merchant_key,
        sample_total=0,
        p_majority=0.0,
        top_count=0,
        predicted_account="",
        account_partial_match_used=False,
        payable_account_before_raw="",
        payable_account_after_canonical="",
        payable_account_rewritten=False,
        payable_account_rewrite_reason="",
        canonical_payable_status=str(canonical_payable.get("status") or "EMPTY"),
        canonical_payable_required_failed=False,
        payable_sub_before="",
        payable_sub_after="",
        payable_sub_changed=False,
        payable_sub_evidence=_NONE_EVIDENCE,
        reasons=[],
        debit_account_before=debit_account_before,
        debit_account_after=debit_account_before,
        credit_account_before=credit_account_before,
        credit_account_after=credit_account_before,
        debit_sub_before=debit_sub_before,
        debit_sub_after=debit_sub_before,
        credit_sub_before=credit_sub_before,
        credit_sub_after=credit_sub_before,
        category_key="",
        category_label="",
        lexicon_quality="",
        matched_needle="",
        is_learned_signal=False,
        target_tax_side=_target_tax_side_from_placeholder_side(placeholder_side),
        target_tax_division_before="",
        target_tax_division_after="",
        target_tax_division_changed=False,
        tax_evidence_type=_NONE_EVIDENCE,
        tax_lookup_key="",
        tax_confidence=0.0,
        tax_sample_total=0,
        tax_p_majority=0.0,
        tax_reasons=[],
    )

    if placeholder_side == "none":
        decision.reasons.append("placeholder_side_none")
    elif placeholder_side == "ambiguous":
        decision.reasons.append("placeholder_side_ambiguous")
    else:
        account_thresholds = _resolve_merchant_account_thresholds(config, cache)
        min_count = int(account_thresholds.get("min_count", 3))
        min_p_majority = float(account_thresholds.get("min_p_majority", 0.9))

        if not merchant_key:
            decision.reasons.append("merchant_key_empty")
        else:
            lookup_key = merchant_key
            stats = cache.merchant_key_account_stats.get(lookup_key)
            if stats is None and partial_enabled:
                matched = resolve_partial_match_key(merchant_key, partial_candidates, partial_min_match_len)
                if matched:
                    lookup_key = matched
                    stats = cache.merchant_key_account_stats.get(lookup_key)
                    decision.lookup_key = lookup_key
                    decision.account_partial_match_used = True
                    decision.reasons.append("partial_match_used")
            if isinstance(stats, dict):
                stats = StatsEntry.from_obj(stats)
            if not isinstance(stats, StatsEntry):
                decision.reasons.append("account_stats_not_found")
            else:
                decision.sample_total = int(stats.sample_total)
                decision.p_majority = float(stats.p_majority)
                decision.top_count = int(stats.top_count)
                decision.predicted_account = str(stats.top_account or "").strip()

                passed, reasons = _account_threshold_pass(
                    stats=stats,
                    min_count=min_count,
                    min_p_majority=min_p_majority,
                )
                if not passed:
                    decision.reasons.extend(reasons)
                else:
                    target_col = COL_DEBIT_ACCOUNT if placeholder_side == "debit" else COL_CREDIT_ACCOUNT
                    _set_text(new_tokens, target_col, encoding, decision.predicted_account)
                    decision.evidence_type = "merchant_key"
                    if new_tokens[target_col] != tokens[target_col]:
                        decision.account_changed = 1
                        decision.reasons.append("account_replaced")
                    else:
                        decision.reasons.append("account_same_as_current")

        if decision.evidence_type == _NONE_EVIDENCE and lex_opt is not None and defaults_opt is not None:
            match = match_summary(lex_opt, summary)
            decision.category_key = str(match.category_key or "")
            decision.category_label = str(match.category_label or "")
            decision.lexicon_quality = str(match.quality or "")
            decision.matched_needle = str(match.matched_needle or "")
            decision.is_learned_signal = bool(match.is_learned_signal)

            matched_key = decision.category_key
            if matched_key and matched_key in defaults_opt.defaults:
                rule = defaults_opt.defaults[matched_key]
                target_col = COL_DEBIT_ACCOUNT if placeholder_side == "debit" else COL_CREDIT_ACCOUNT
                _set_text(new_tokens, target_col, encoding, rule.target_account)
                decision.evidence_type = "category_default"
                decision.predicted_account = rule.target_account
                if match.quality == "ambiguous":
                    decision.reasons.append("category_match_ambiguous")
                decision.reasons.append("category_default_applied")
                if new_tokens[target_col] != tokens[target_col]:
                    decision.account_changed = 1
                    decision.reasons.append("account_replaced")
                else:
                    decision.reasons.append("account_same_as_current")
            elif match.quality == "none":
                decision.reasons.append("category_match_none")
            else:
                decision.reasons.append("category_no_rule")

    payable_account_col = _account_col_from_side(payable_side)
    payable_sub_col = _subaccount_col_from_side(payable_side)
    payable_sub_before = ""
    if payable_side == "debit":
        decision.payable_account_before_raw = debit_account_before
        decision.payable_account_after_canonical = debit_account_before
        payable_sub_before = debit_sub_before
    elif payable_side == "credit":
        decision.payable_account_before_raw = credit_account_before
        decision.payable_account_after_canonical = credit_account_before
        payable_sub_before = credit_sub_before
    elif payable_side == "ambiguous":
        decision.payable_account_rewrite_reason = "payable_side_ambiguous"
        decision.reasons.append("payable_side_ambiguous")
    else:
        decision.payable_account_rewrite_reason = "payable_side_none"
        decision.reasons.append("payable_side_none")

    if payable_account_col is not None:
        canonical_account_name = str(canonical_payable.get("account_name") or "").strip()
        if decision.canonical_payable_status == "OK" and canonical_account_name:
            _set_text(new_tokens, payable_account_col, encoding, canonical_account_name)
            decision.payable_account_after_canonical = _safe_text(new_tokens, payable_account_col, encoding)
            if decision.payable_account_before_raw == decision.payable_account_after_canonical:
                decision.payable_account_rewrite_reason = "already_canonical"
                decision.reasons.append("payable_account_already_canonical")
            else:
                decision.payable_account_rewritten = True
                decision.payable_account_rewrite_reason = "raw_placeholder_to_canonical"
                decision.reasons.append("payable_account_rewritten")
        else:
            decision.canonical_payable_required_failed = True
            decision.payable_account_rewrite_reason = "canonical_payable_not_ok"
            decision.reasons.append("canonical_payable_not_ok")

    decision.payable_sub_before = payable_sub_before
    decision.payable_sub_after = payable_sub_before

    if payable_sub_col is not None and decision.canonical_payable_required_failed:
        decision.reasons.append("payable_sub_skipped_canonical_payable_not_ok")
    elif payable_sub_col is not None and payable_sub_before == "":
        inferred_value = str(inferred_payable_subaccount_opt or "").strip()
        if inferred_value:
            _set_text(new_tokens, payable_sub_col, encoding, inferred_value)
            decision.payable_sub_evidence = _FILE_INFERRED_EVIDENCE
            decision.reasons.append("payable_sub_filled")
        else:
            decision.reasons.append("file_card_inference_not_ok")
    elif payable_sub_col is not None and payable_sub_before != "":
        decision.reasons.append("payable_sub_already_present")

    decision.debit_account_after = _safe_text(new_tokens, COL_DEBIT_ACCOUNT, encoding)
    decision.credit_account_after = _safe_text(new_tokens, COL_CREDIT_ACCOUNT, encoding)
    decision.debit_sub_after = _safe_text(new_tokens, COL_DEBIT_SUBACCOUNT, encoding)
    decision.credit_sub_after = _safe_text(new_tokens, COL_CREDIT_SUBACCOUNT, encoding)

    if payable_side == "debit":
        decision.payable_account_after_canonical = decision.debit_account_after
        decision.payable_sub_after = decision.debit_sub_after
    elif payable_side == "credit":
        decision.payable_account_after_canonical = decision.credit_account_after
        decision.payable_sub_after = decision.credit_sub_after

    tax_decision = decide_cc_tax(
        tokens=tokens,
        encoding=encoding,
        cache=cache,
        config=config,
        decision=decision,
        defaults_opt=defaults_opt,
        partial_match_settings=partial_cfg,
        eligible_account_partial_keys=partial_candidates,
    )
    decision.target_tax_side = tax_decision.target_tax_side
    decision.target_tax_division_before = tax_decision.target_tax_division_before
    decision.target_tax_division_after = tax_decision.target_tax_division_after
    decision.target_tax_division_changed = bool(tax_decision.changed)
    decision.tax_evidence_type = tax_decision.evidence_type
    decision.tax_lookup_key = tax_decision.lookup_key
    decision.tax_confidence = float(tax_decision.confidence)
    decision.tax_sample_total = int(tax_decision.sample_total)
    decision.tax_p_majority = float(tax_decision.p_majority)
    decision.tax_reasons = list(tax_decision.reasons)
    if tax_decision.changed:
        tax_col = _target_tax_col_from_side(tax_decision.target_tax_side)
        if tax_col is not None:
            _set_text(new_tokens, tax_col, encoding, tax_decision.target_tax_division_after)

    decision.payable_sub_changed = decision.payable_sub_before != decision.payable_sub_after
    decision.changed = bool(
        decision.account_changed
        or decision.payable_account_rewritten
        or decision.payable_sub_changed
        or decision.target_tax_division_changed
    )
    return new_tokens, decision


def replace_credit_card_yayoi_csv(
    in_path: Path,
    out_path: Path,
    cache_path: Path,
    config: Dict[str, Any],
    run_dir: Path,
    artifact_prefix: Optional[str] = None,
    lex: Optional[Lexicon] = None,
    defaults: Optional[CategoryDefaults] = None,
    yayoi_tax_config: Optional[YayoiTaxPostprocessConfig] = None,
) -> Dict[str, Any]:
    csv_obj = read_yayoi_csv(in_path)
    original_row_tokens = _clone_row_tokens([row.tokens for row in csv_obj.rows])
    cache = CCClientCache.load(cache_path)
    partial_match_settings = _resolve_partial_match_settings(config, cache)
    partial_enabled = (
        bool(partial_match_settings.get("enabled"))
        and str(partial_match_settings.get("direction") or "") == "cache_key_in_input"
        and bool(partial_match_settings.get("require_unique_longest", True))
    )
    partial_min_match_len = int(_as_int(partial_match_settings.get("min_match_len"), 4))
    partial_min_stats_sample_total = int(_as_int(partial_match_settings.get("min_stats_sample_total"), 10))
    partial_min_stats_p_majority = float(_as_float(partial_match_settings.get("min_stats_p_majority"), 0.95))

    eligible_account_partial_keys: Set[str] = set()
    eligible_payable_partial_keys: Set[str] = set()
    if partial_enabled:
        eligible_account_partial_keys = _build_eligible_account_partial_keys(
            cache,
            min_stats_sample_total=partial_min_stats_sample_total,
            min_stats_p_majority=partial_min_stats_p_majority,
        )
        eligible_payable_partial_keys = _build_eligible_payable_partial_keys(
            cache,
            min_stats_sample_total=partial_min_stats_sample_total,
            min_stats_p_majority=partial_min_stats_p_majority,
        )

    file_inference = infer_file_payable_subaccount(
        rows=csv_obj.rows,
        encoding=csv_obj.encoding,
        cache=cache,
        config=config,
        partial_match_settings=partial_match_settings,
        eligible_payable_partial_keys=eligible_payable_partial_keys,
    )
    inferred_subaccount = (
        str(file_inference.inferred_payable_subaccount or "").strip()
        if file_inference.status == "OK"
        else None
    )

    decisions: List[CCRowDecision] = []
    account_changed_count = 0
    payable_sub_changed_count = 0
    tax_division_changed_count = 0
    evidence_counts: Dict[str, int] = {}
    payable_sub_evidence_counts: Dict[str, int] = {}
    tax_route_counts: Dict[str, int] = {}
    tax_unresolved_count = 0
    tax_partial_match_applied_count = 0
    tax_target_side_counts: Dict[str, int] = {}
    account_partial_rows_used = 0
    votes_partial_used = int(file_inference.votes_partial_used)
    partial_examples: List[Tuple[str, str]] = []
    canonical_payable_snapshot = _canonical_payable_snapshot(cache)
    canonical_payable_rewrite_count = 0
    canonical_payable_noop_count = 0
    canonical_payable_required_failed_count = 0
    canonical_payable_status_counts: Dict[str, int] = {}
    canonical_payable_rewrite_reason_counts: Dict[str, int] = {}
    for input_key, matched_key in file_inference.partial_examples:
        _append_partial_example(partial_examples, input_key, matched_key)

    for row_index_1b, row in enumerate(csv_obj.rows, start=1):
        new_tokens, decision = decide_cc_row(
            tokens=row.tokens,
            encoding=csv_obj.encoding,
            cache=cache,
            config=config,
            inferred_payable_subaccount_opt=inferred_subaccount,
            lex_opt=lex,
            defaults_opt=defaults,
            partial_match_settings=partial_match_settings,
            eligible_account_partial_keys=eligible_account_partial_keys,
        )
        decision.row_index_1b = int(row_index_1b)
        if list(row.tokens) != new_tokens:
            row.tokens = new_tokens
        decisions.append(decision)
        account_changed_count += int(decision.account_changed)
        if "partial_match_used" in decision.reasons:
            account_partial_rows_used += 1
            _append_partial_example(partial_examples, decision.merchant_key, decision.lookup_key)
        if decision.payable_account_rewritten:
            canonical_payable_rewrite_count += 1
        elif decision.payable_account_rewrite_reason == "already_canonical":
            canonical_payable_noop_count += 1
        if decision.canonical_payable_required_failed:
            canonical_payable_required_failed_count += 1
        if decision.payable_sub_changed:
            payable_sub_changed_count += 1
        if decision.target_tax_division_changed:
            tax_division_changed_count += 1
        evidence_counts[decision.evidence_type] = evidence_counts.get(decision.evidence_type, 0) + 1
        payable_sub_evidence_counts[decision.payable_sub_evidence] = (
            payable_sub_evidence_counts.get(decision.payable_sub_evidence, 0) + 1
        )
        canonical_payable_status_counts[decision.canonical_payable_status] = (
            canonical_payable_status_counts.get(decision.canonical_payable_status, 0) + 1
        )
        canonical_reason = str(decision.payable_account_rewrite_reason or "").strip()
        if canonical_reason:
            canonical_payable_rewrite_reason_counts[canonical_reason] = (
                canonical_payable_rewrite_reason_counts.get(canonical_reason, 0) + 1
            )
        if decision.tax_evidence_type == _NONE_EVIDENCE:
            tax_unresolved_count += 1
        else:
            tax_route_counts[decision.tax_evidence_type] = tax_route_counts.get(decision.tax_evidence_type, 0) + 1
            if decision.tax_evidence_type == _ROUTE_TAX_PARTIAL:
                tax_partial_match_applied_count += 1
            if decision.target_tax_side:
                tax_target_side_counts[decision.target_tax_side] = (
                    tax_target_side_counts.get(decision.target_tax_side, 0) + 1
                )

    pre_tax_row_tokens = _clone_row_tokens([row.tokens for row in csv_obj.rows])
    tax_summary = apply_yayoi_tax_postprocess(
        csv_obj,
        yayoi_tax_config or default_yayoi_tax_postprocess_config(),
    )
    tax_side_results = _tax_result_map(tax_summary)
    changed_count = 0
    for original_tokens, row, decision in zip(original_row_tokens, csv_obj.rows, decisions):
        decision.changed = _row_changed(original_tokens, row.tokens)
        if decision.changed:
            changed_count += 1

    write_yayoi_csv(csv_obj, out_path)

    if artifact_prefix:
        report_path = get_review_report_path(run_dir, artifact_prefix)
        manifest_path = get_input_manifest_path(run_dir, artifact_prefix)
    else:
        report_path = run_dir / f"{in_path.stem}_review_report.csv"
        manifest_path = run_dir / f"{in_path.stem}_manifest.json"

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_header = [
        "row_index_1b",
        "merchant_key",
        "placeholder_side",
        "payable_side",
        "payable_side_detected",
        "changed",
        "account_changed",
        "evidence_type",
        "lookup_key",
        "sample_total",
        "p_majority",
        "top_count",
        "predicted_account",
        "payable_account_before_raw",
        "payable_account_after_canonical",
        "payable_account_rewritten",
        "payable_account_rewrite_reason",
        "canonical_payable_status",
        "canonical_payable_required_failed",
        "payable_sub_before",
        "payable_sub_after",
        "payable_sub_changed",
        "payable_sub_evidence",
        "debit_account_before",
        "debit_account_after",
        "credit_account_before",
        "credit_account_after",
        "debit_sub_before",
        "debit_sub_after",
        "credit_sub_before",
        "credit_sub_after",
        "reasons",
        "category_key",
        "category_label",
        "lexicon_quality",
        "matched_needle",
        "is_learned_signal",
        "target_tax_side",
        "target_tax_division_before",
        "target_tax_division_after",
        "target_tax_division_changed",
        "tax_evidence_type",
        "tax_lookup_key",
        "tax_confidence",
        "tax_sample_total",
        "tax_p_majority",
        "tax_reasons",
        "debit_tax_amount_before",
        "debit_tax_amount_after",
        "debit_tax_fill_status",
        "debit_tax_rate",
        "debit_tax_calc_mode",
        "credit_tax_amount_before",
        "credit_tax_amount_after",
        "credit_tax_fill_status",
        "credit_tax_rate",
        "credit_tax_calc_mode",
    ]
    with report_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv_lib.writer(fh, dialect="excel", lineterminator="\r\n", quoting=csv_lib.QUOTE_MINIMAL)
        writer.writerow(report_header)
        for decision, pre_tax_tokens, row in zip(decisions, pre_tax_row_tokens, csv_obj.rows):
            writer.writerow(
                [
                    str(decision.row_index_1b),
                    decision.merchant_key,
                    decision.placeholder_side,
                    decision.payable_side,
                    decision.payable_side_detected,
                    "1" if decision.changed else "0",
                    str(int(decision.account_changed)),
                    decision.evidence_type,
                    decision.lookup_key,
                    str(decision.sample_total),
                    f"{decision.p_majority:.6f}",
                    str(decision.top_count),
                    decision.predicted_account,
                    decision.payable_account_before_raw,
                    decision.payable_account_after_canonical,
                    "1" if decision.payable_account_rewritten else "0",
                    decision.payable_account_rewrite_reason,
                    decision.canonical_payable_status,
                    "1" if decision.canonical_payable_required_failed else "0",
                    decision.payable_sub_before,
                    decision.payable_sub_after,
                    "1" if decision.payable_sub_changed else "0",
                    decision.payable_sub_evidence,
                    decision.debit_account_before,
                    decision.debit_account_after,
                    decision.credit_account_before,
                    decision.credit_account_after,
                    decision.debit_sub_before,
                    decision.debit_sub_after,
                    decision.credit_sub_before,
                    decision.credit_sub_after,
                    " | ".join(decision.reasons),
                    decision.category_key,
                    decision.category_label,
                    decision.lexicon_quality,
                    decision.matched_needle,
                    "1" if decision.is_learned_signal else "0",
                    decision.target_tax_side,
                    decision.target_tax_division_before,
                    decision.target_tax_division_after,
                    "1" if decision.target_tax_division_changed else "0",
                    decision.tax_evidence_type,
                    decision.tax_lookup_key,
                    f"{decision.tax_confidence:.4f}",
                    str(decision.tax_sample_total),
                    f"{decision.tax_p_majority:.6f}",
                    " | ".join(decision.tax_reasons),
                    *_tax_review_cells(
                        row_index_1b=decision.row_index_1b,
                        encoding=csv_obj.encoding,
                        pre_tax_tokens=pre_tax_tokens,
                        final_tokens=row.tokens,
                        side_results=tax_side_results,
                    ),
                ]
            )

    row_count = len(csv_obj.rows)
    canonical_payable_required_failed = bool(canonical_payable_required_failed_count)
    payable_sub_fill_required_failed = (
        file_inference.status != "OK"
        and any(
            decision.payable_side in {"debit", "credit"} and str(decision.payable_sub_after or "").strip() == ""
            for decision in decisions
        )
    )

    manifest: Dict[str, Any] = {
        "schema": "belle.cc_replacer_run_manifest.v1",
        "version": str(config.get("version") or "0.1"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "line_id": "credit_card_statement",
        "input_file": str(in_path),
        "input_sha256": sha256_file(in_path),
        "output_file": str(out_path),
        "output_sha256": sha256_file(out_path),
        "cache_file": str(cache_path),
        "cache_sha256": sha256_file(cache_path) if cache_path.exists() else "",
        "row_count": int(row_count),
        "changed_count": int(changed_count),
        "changed_ratio": (changed_count / row_count) if row_count else 0.0,
        "account_changed_count": int(account_changed_count),
        "tax_division_changed_count": int(tax_division_changed_count),
        "payable_sub_changed_count": int(payable_sub_changed_count),
        "evidence_counts": evidence_counts,
        "payable_sub_evidence_counts": payable_sub_evidence_counts,
        "canonical_payable_required_failed": bool(canonical_payable_required_failed),
        "canonical_payable": {
            "cache_snapshot": canonical_payable_snapshot,
            "rewrite_count": int(canonical_payable_rewrite_count),
            "noop_count": int(canonical_payable_noop_count),
            "required_failed_count": int(canonical_payable_required_failed_count),
            "status_counts": canonical_payable_status_counts,
            "rewrite_reason_counts": canonical_payable_rewrite_reason_counts,
        },
        "file_card_inference": {
            "status": file_inference.status,
            "inferred_payable_subaccount": file_inference.inferred_payable_subaccount,
            "votes_total": int(file_inference.votes_total),
            "top_value": file_inference.top_value,
            "top_count": int(file_inference.top_count),
            "p_majority": float(file_inference.p_majority),
            "reasons": list(file_inference.reasons),
        },
        "partial_match": {
            "enabled": bool(partial_enabled),
            "min_match_len": int(partial_min_match_len),
            "min_stats_sample_total": int(partial_min_stats_sample_total),
            "min_stats_p_majority": float(partial_min_stats_p_majority),
            "account_partial_rows_used": int(account_partial_rows_used),
            "votes_partial_used": int(votes_partial_used),
            "examples": [
                {"input_key": input_key, "matched_key": matched_key}
                for input_key, matched_key in partial_examples[:10]
            ],
        },
        "payable_sub_fill_required_failed": bool(payable_sub_fill_required_failed),
        "tax_division_replacement": {
            "changed_count": int(tax_division_changed_count),
            "route_counts": tax_route_counts,
            "unresolved_count": int(tax_unresolved_count),
            "partial_match_applied_count": int(tax_partial_match_applied_count),
            "category_default_applied_count": int(tax_route_counts.get(_ROUTE_TAX_CATEGORY_DEFAULT) or 0),
            "global_fallback_applied_count": int(tax_route_counts.get(_ROUTE_TAX_GLOBAL_FALLBACK) or 0),
            "target_side_counts": tax_target_side_counts,
        },
        "tax_postprocess": build_tax_postprocess_manifest(tax_summary),
        "reports": {
            "review_report_csv": str(report_path),
            "manifest_json": str(manifest_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
