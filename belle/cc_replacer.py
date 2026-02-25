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

from .build_cc_cache import merchant_key_from_summary
from .cc_cache import CCClientCache, ValueStatsEntry
from .client_cache import StatsEntry
from belle.fs_utils import sha256_file_chunked
from .paths import get_input_manifest_path, get_review_report_path
from .yayoi_text import safe_cell_text, set_cell_text
from .yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_SUMMARY,
)
from .yayoi_csv import read_yayoi_csv, write_yayoi_csv

_NONE_EVIDENCE = "none"
_FILE_INFERRED_EVIDENCE = "file_inferred"
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
    changed: bool
    account_changed: int
    evidence_type: str
    lookup_key: str
    sample_total: int
    p_majority: float
    top_count: int
    predicted_account: str
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


def _collect_observed_payable_subaccounts(cache: CCClientCache) -> List[str]:
    observed: set[str] = set()

    global_counts = (
        cache.payable_sub_global_stats.value_counts
        if isinstance(getattr(cache.payable_sub_global_stats, "value_counts", None), dict)
        else {}
    )
    for value, count in global_counts.items():
        s = str(value or "").strip()
        if s and int(count) > 0:
            observed.add(s)

    for entry in (cache.merchant_key_payable_sub_stats or {}).values():
        if isinstance(entry, dict):
            entry = ValueStatsEntry.from_obj(entry)
        if not isinstance(entry, ValueStatsEntry):
            continue
        if entry.top_value:
            observed.add(str(entry.top_value).strip())
        for value, count in (entry.value_counts or {}).items():
            s = str(value or "").strip()
            if s and int(count) > 0:
                observed.add(s)

    return sorted(v for v in observed if v)


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
        eligible = set(_collect_observed_payable_subaccounts(cache))
        reasons.append("no_candidates_flagged_fallback")

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


def decide_cc_row(
    tokens: Sequence[bytes],
    encoding: str,
    cache: CCClientCache,
    config: Dict[str, Any],
    inferred_payable_subaccount_opt: Optional[str],
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
    payable_account_name = str(config.get("payable_account_name") or _PAYABLE_DEFAULT)

    debit_key = normalize_name(debit_account_before)
    credit_key = normalize_name(credit_account_before)
    placeholder_key = normalize_name(placeholder_account_name)
    payable_key = normalize_name(payable_account_name)

    placeholder_side = _detect_side(debit_key, credit_key, placeholder_key)
    payable_side = _detect_side(debit_key, credit_key, payable_key)
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
        changed=False,
        account_changed=0,
        evidence_type=_NONE_EVIDENCE,
        lookup_key=merchant_key,
        sample_total=0,
        p_majority=0.0,
        top_count=0,
        predicted_account="",
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

    payable_sub_col: Optional[int] = None
    payable_sub_before = ""
    if payable_side == "debit":
        payable_sub_col = COL_DEBIT_SUBACCOUNT
        payable_sub_before = debit_sub_before
    elif payable_side == "credit":
        payable_sub_col = COL_CREDIT_SUBACCOUNT
        payable_sub_before = credit_sub_before
    elif payable_side == "ambiguous":
        decision.reasons.append("payable_side_ambiguous")
    else:
        decision.reasons.append("payable_side_none")

    decision.payable_sub_before = payable_sub_before
    decision.payable_sub_after = payable_sub_before

    if payable_sub_col is not None and payable_sub_before == "":
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
        decision.payable_sub_after = decision.debit_sub_after
    elif payable_side == "credit":
        decision.payable_sub_after = decision.credit_sub_after

    decision.payable_sub_changed = decision.payable_sub_before != decision.payable_sub_after
    decision.changed = bool(decision.account_changed or decision.payable_sub_changed)
    return new_tokens, decision


def replace_credit_card_yayoi_csv(
    in_path: Path,
    out_path: Path,
    cache_path: Path,
    config: Dict[str, Any],
    run_dir: Path,
    artifact_prefix: Optional[str] = None,
) -> Dict[str, Any]:
    csv_obj = read_yayoi_csv(in_path)
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
    changed_count = 0
    account_changed_count = 0
    payable_sub_changed_count = 0
    evidence_counts: Dict[str, int] = {}
    payable_sub_evidence_counts: Dict[str, int] = {}
    account_partial_rows_used = 0
    votes_partial_used = int(file_inference.votes_partial_used)
    partial_examples: List[Tuple[str, str]] = []
    for input_key, matched_key in file_inference.partial_examples:
        _append_partial_example(partial_examples, input_key, matched_key)

    for row_index_1b, row in enumerate(csv_obj.rows, start=1):
        new_tokens, decision = decide_cc_row(
            tokens=row.tokens,
            encoding=csv_obj.encoding,
            cache=cache,
            config=config,
            inferred_payable_subaccount_opt=inferred_subaccount,
            partial_match_settings=partial_match_settings,
            eligible_account_partial_keys=eligible_account_partial_keys,
        )
        decision.row_index_1b = int(row_index_1b)
        if list(row.tokens) != new_tokens:
            row.tokens = new_tokens
            changed_count += 1
        decisions.append(decision)
        account_changed_count += int(decision.account_changed)
        if "partial_match_used" in decision.reasons:
            account_partial_rows_used += 1
            _append_partial_example(partial_examples, decision.merchant_key, decision.lookup_key)
        if decision.payable_sub_changed:
            payable_sub_changed_count += 1
        evidence_counts[decision.evidence_type] = evidence_counts.get(decision.evidence_type, 0) + 1
        payable_sub_evidence_counts[decision.payable_sub_evidence] = (
            payable_sub_evidence_counts.get(decision.payable_sub_evidence, 0) + 1
        )

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
        "changed",
        "account_changed",
        "evidence_type",
        "lookup_key",
        "sample_total",
        "p_majority",
        "top_count",
        "predicted_account",
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
    ]
    with report_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv_lib.writer(fh, dialect="excel", lineterminator="\r\n", quoting=csv_lib.QUOTE_MINIMAL)
        writer.writerow(report_header)
        for decision in decisions:
            writer.writerow(
                [
                    str(decision.row_index_1b),
                    decision.merchant_key,
                    decision.placeholder_side,
                    decision.payable_side,
                    "1" if decision.changed else "0",
                    str(int(decision.account_changed)),
                    decision.evidence_type,
                    decision.lookup_key,
                    str(decision.sample_total),
                    f"{decision.p_majority:.6f}",
                    str(decision.top_count),
                    decision.predicted_account,
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
                ]
            )

    row_count = len(csv_obj.rows)
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
        "payable_sub_changed_count": int(payable_sub_changed_count),
        "evidence_counts": evidence_counts,
        "payable_sub_evidence_counts": payable_sub_evidence_counts,
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
        "reports": {
            "review_report_csv": str(report_path),
            "manifest_json": str(manifest_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
