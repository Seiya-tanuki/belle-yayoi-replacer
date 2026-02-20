# -*- coding: utf-8 -*-
from __future__ import annotations

import csv as csv_lib
import hashlib
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .bank_cache import (
    BankClientCache,
    BankLabel,
    LabelStatsEntry,
    ROUTE_KANA_SIGN,
    ROUTE_KANA_SIGN_AMOUNT,
    ValueStatsEntry,
    load_bank_cache,
)
from .bank_pairing import (
    derive_effective_sign,
    derive_sign_from_accounts,
    extract_sign_from_memo,
    normalize_kana_key,
    parse_amount,
)
from .paths import get_input_manifest_path, get_review_report_path
from .yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_CREDIT_TAX_DIVISION,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_DEBIT_TAX_DIVISION,
    COL_SUMMARY,
)
from .yayoi_csv import read_yayoi_csv, text_to_token, token_to_text, write_yayoi_csv

_NONE_EVIDENCE = "none"
_BANK_SUB_STRONG_EVIDENCE = "bank_sub_kana_sign_amount"
_BANK_SUB_WEAK_EVIDENCE = "bank_sub_kana_sign"
_PLACEHOLDER_DEFAULT = "仮払金"
_BANK_ACCOUNT_DEFAULT = "普通預金"
_AMOUNT_RE = re.compile(r"[+-]?\d+")


@dataclass
class BankRowDecision:
    row_index_1b: int
    kana_key: str
    sign: str
    amount: Optional[int]
    placeholder_side: str
    changed: bool
    summary_before: str
    summary_after: str
    debit_account_before: str
    debit_account_after: str
    debit_sub_before: str
    debit_sub_after: str
    debit_tax_before: str
    debit_tax_after: str
    credit_account_before: str
    credit_account_after: str
    credit_sub_before: str
    credit_sub_after: str
    credit_tax_before: str
    credit_tax_after: str
    bank_side: str
    bank_sub_before: str
    bank_sub_after: str
    bank_sub_changed: bool
    bank_sub_evidence: str
    bank_sub_sample_total: int
    bank_sub_p_majority: float
    bank_sub_top_count: int
    evidence_type: str
    lookup_key: str
    sample_total: int
    p_majority: float
    top_count: int
    label_id: Optional[str]
    confidence: float
    priority: str
    reasons: List[str]


@dataclass
class _RouteEval:
    route: str
    lookup_key: str
    selected: bool
    label_id: Optional[str]
    label: Optional[BankLabel]
    sample_total: int
    p_majority: float
    top_count: int
    reasons: List[str]


@dataclass
class _BankSideSubaccountConfig:
    enabled: bool
    weak_enabled: bool
    weak_min_count: int


@dataclass
class _BankSubEval:
    maybe_value: Optional[str]
    evidence_type: str
    sample_total: int
    p_majority: float
    top_count: int
    reasons: List[str]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def confidence(strength: float, p_majority: float, sample_total: int) -> float:
    sample_factor = min(1.0, math.log(sample_total + 1, 10) / math.log(50, 10))
    raw = float(strength) * (0.7 * float(p_majority) + 0.3 * sample_factor)
    return max(0.0, min(1.0, raw))


def _safe_text(tokens: Sequence[bytes], idx: int, encoding: str) -> str:
    if idx < 0 or idx >= len(tokens):
        return ""
    tok = tokens[idx]
    if isinstance(tok, bytes):
        return token_to_text(tok, encoding)
    return str(tok)


def _normalize_name_for_match(text: str) -> str:
    s = unicodedata.normalize("NFKC", text or "")
    return re.sub(r"[ \u3000]+", "", s).strip()


def _parse_amount_cell(text: str) -> Optional[int]:
    s = unicodedata.normalize("NFKC", text or "").strip()
    if not s:
        return None
    s = s.replace(",", "")
    s = re.sub(r"[ \u3000]", "", s)
    if s.startswith("(") and s.endswith(")") and len(s) > 2:
        s = "-" + s[1:-1]
    m = _AMOUNT_RE.fullmatch(s)
    if not m:
        return None
    try:
        return abs(int(s))
    except Exception:
        return None


def _normalize_threshold_route(route_obj: Any, *, min_count: int, min_p_majority: float) -> Dict[str, Any]:
    src = route_obj if isinstance(route_obj, dict) else {}
    return {
        "min_count": int(src.get("min_count", min_count)),
        "min_p_majority": float(src.get("min_p_majority", src.get("p_majority", min_p_majority))),
    }


def _resolve_thresholds(config: Dict[str, Any], cache: BankClientCache) -> Dict[str, Dict[str, Any]]:
    thresholds = config.get("thresholds") if isinstance(config.get("thresholds"), dict) else None
    if thresholds is None:
        cache_thr = cache.decision_thresholds if isinstance(cache.decision_thresholds, dict) else {}
        thresholds = cache_thr
    if not isinstance(thresholds, dict):
        thresholds = {}

    return {
        ROUTE_KANA_SIGN_AMOUNT: _normalize_threshold_route(
            thresholds.get(ROUTE_KANA_SIGN_AMOUNT),
            min_count=2,
            min_p_majority=0.85,
        ),
        ROUTE_KANA_SIGN: _normalize_threshold_route(
            thresholds.get(ROUTE_KANA_SIGN),
            min_count=3,
            min_p_majority=0.80,
        ),
    }


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
    return default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _resolve_bank_side_subaccount_config(config: Dict[str, Any]) -> _BankSideSubaccountConfig:
    section = config.get("bank_side_subaccount")
    section_obj = section if isinstance(section, dict) else {}
    weak_min_count = max(3, _as_int(section_obj.get("weak_min_count"), 3))
    return _BankSideSubaccountConfig(
        enabled=_as_bool(section_obj.get("enabled"), True),
        weak_enabled=_as_bool(section_obj.get("weak_enabled"), True),
        weak_min_count=weak_min_count,
    )


def _load_bank_sub_stats_entry(
    *,
    cache: BankClientCache,
    route: str,
    lookup_key: str,
) -> Optional[ValueStatsEntry]:
    route_stats = cache.bank_account_subaccount_stats.get(route)
    if not isinstance(route_stats, dict):
        return None
    entry = route_stats.get(lookup_key)
    if isinstance(entry, dict):
        entry = ValueStatsEntry.from_obj(entry)
    return entry if isinstance(entry, ValueStatsEntry) else None


def _evaluate_bank_sub_entry(
    *,
    entry: Optional[ValueStatsEntry],
    route: str,
    evidence_type: str,
    min_count: Optional[int] = None,
) -> _BankSubEval:
    if not isinstance(entry, ValueStatsEntry):
        return _BankSubEval(
            maybe_value=None,
            evidence_type=_NONE_EVIDENCE,
            sample_total=0,
            p_majority=0.0,
            top_count=0,
            reasons=[f"bank_sub:{route}_stats_not_found"],
        )

    sample_total = int(entry.sample_total)
    p_majority = float(entry.p_majority)
    top_count = int(entry.top_count)
    top_value = str(entry.top_value or "").strip()

    reasons: List[str] = []
    if not top_value:
        reasons.append(f"bank_sub:{route}_top_value_missing")
    if sample_total <= 0 or top_count <= 0:
        reasons.append(f"bank_sub:{route}_counts_invalid")
    if min_count is not None and sample_total < int(min_count):
        reasons.append(f"bank_sub:{route}_min_count_not_met")
    if sample_total > 0 and top_count != sample_total:
        reasons.append(f"bank_sub:{route}_not_deterministic")

    if reasons:
        return _BankSubEval(
            maybe_value=None,
            evidence_type=_NONE_EVIDENCE,
            sample_total=sample_total,
            p_majority=p_majority,
            top_count=top_count,
            reasons=reasons,
        )

    return _BankSubEval(
        maybe_value=top_value,
        evidence_type=evidence_type,
        sample_total=sample_total,
        p_majority=p_majority,
        top_count=top_count,
        reasons=[f"bank_sub:{route}_selected"],
    )


def _select_bank_side_subaccount(
    *,
    cache: BankClientCache,
    bank_side: str,
    key_strong: str,
    key_weak: str,
    cfg: _BankSideSubaccountConfig,
) -> _BankSubEval:
    if not bank_side:
        return _BankSubEval(
            maybe_value=None,
            evidence_type=_NONE_EVIDENCE,
            sample_total=0,
            p_majority=0.0,
            top_count=0,
            reasons=["bank_sub:bank_side_not_determined"],
        )
    if not cfg.enabled:
        return _BankSubEval(
            maybe_value=None,
            evidence_type=_NONE_EVIDENCE,
            sample_total=0,
            p_majority=0.0,
            top_count=0,
            reasons=["bank_sub:disabled"],
        )

    strong_entry = _load_bank_sub_stats_entry(
        cache=cache,
        route=ROUTE_KANA_SIGN_AMOUNT,
        lookup_key=key_strong,
    )
    strong_eval = _evaluate_bank_sub_entry(
        entry=strong_entry,
        route=ROUTE_KANA_SIGN_AMOUNT,
        evidence_type=_BANK_SUB_STRONG_EVIDENCE,
    )
    if strong_eval.maybe_value:
        return strong_eval

    if not cfg.weak_enabled:
        return _BankSubEval(
            maybe_value=None,
            evidence_type=_NONE_EVIDENCE,
            sample_total=strong_eval.sample_total,
            p_majority=strong_eval.p_majority,
            top_count=strong_eval.top_count,
            reasons=[*strong_eval.reasons, "bank_sub:weak_disabled"],
        )

    weak_entry = _load_bank_sub_stats_entry(
        cache=cache,
        route=ROUTE_KANA_SIGN,
        lookup_key=key_weak,
    )
    weak_eval = _evaluate_bank_sub_entry(
        entry=weak_entry,
        route=ROUTE_KANA_SIGN,
        evidence_type=_BANK_SUB_WEAK_EVIDENCE,
        min_count=cfg.weak_min_count,
    )
    if weak_eval.maybe_value:
        return _BankSubEval(
            maybe_value=weak_eval.maybe_value,
            evidence_type=weak_eval.evidence_type,
            sample_total=weak_eval.sample_total,
            p_majority=weak_eval.p_majority,
            top_count=weak_eval.top_count,
            reasons=[*strong_eval.reasons, *weak_eval.reasons],
        )

    sample_total = weak_eval.sample_total if weak_eval.sample_total > 0 else strong_eval.sample_total
    p_majority = weak_eval.p_majority if weak_eval.sample_total > 0 else strong_eval.p_majority
    top_count = weak_eval.top_count if weak_eval.sample_total > 0 else strong_eval.top_count
    return _BankSubEval(
        maybe_value=None,
        evidence_type=_NONE_EVIDENCE,
        sample_total=sample_total,
        p_majority=p_majority,
        top_count=top_count,
        reasons=[*strong_eval.reasons, *weak_eval.reasons],
    )


def _evaluate_route(
    *,
    cache: BankClientCache,
    route: str,
    lookup_key: str,
    min_count: int,
    min_p_majority: float,
) -> _RouteEval:
    stats_map = cache.stats.get(route) if isinstance(cache.stats.get(route), dict) else {}
    entry = stats_map.get(lookup_key)
    if isinstance(entry, dict):
        entry = LabelStatsEntry.from_obj(entry)
    if not isinstance(entry, LabelStatsEntry):
        return _RouteEval(
            route=route,
            lookup_key=lookup_key,
            selected=False,
            label_id=None,
            label=None,
            sample_total=0,
            p_majority=0.0,
            top_count=0,
            reasons=[f"{route}:stats_not_found"],
        )

    reasons: List[str] = []
    sample_total = int(entry.sample_total)
    p_majority = float(entry.p_majority)
    top_count = int(entry.top_count)
    top_label_id = str(entry.top_label_id) if entry.top_label_id else None

    if sample_total < int(min_count):
        reasons.append(f"{route}:min_count_not_met")
    if p_majority < float(min_p_majority):
        reasons.append(f"{route}:p_majority_not_met")
    if not top_label_id:
        reasons.append(f"{route}:top_label_missing")
    if top_count <= 0:
        reasons.append(f"{route}:top_count_invalid")

    if top_count > 0:
        tie_count = sum(1 for _, cnt in (entry.label_counts or {}).items() if int(cnt) == top_count)
        if tie_count != 1:
            reasons.append(f"{route}:top_tie")

    label = cache.labels.get(top_label_id or "")
    if top_label_id and label is None:
        reasons.append(f"{route}:label_missing")

    if reasons:
        return _RouteEval(
            route=route,
            lookup_key=lookup_key,
            selected=False,
            label_id=top_label_id,
            label=label,
            sample_total=sample_total,
            p_majority=p_majority,
            top_count=top_count,
            reasons=reasons,
        )

    return _RouteEval(
        route=route,
        lookup_key=lookup_key,
        selected=True,
        label_id=top_label_id,
        label=label,
        sample_total=sample_total,
        p_majority=p_majority,
        top_count=top_count,
        reasons=[f"{route}:selected"],
    )


def _default_decision(*, tokens: Sequence[bytes], encoding: str) -> BankRowDecision:
    summary = _safe_text(tokens, COL_SUMMARY, encoding)
    debit_account = _safe_text(tokens, COL_DEBIT_ACCOUNT, encoding)
    debit_sub = _safe_text(tokens, COL_DEBIT_SUBACCOUNT, encoding)
    debit_tax = _safe_text(tokens, COL_DEBIT_TAX_DIVISION, encoding)
    credit_account = _safe_text(tokens, COL_CREDIT_ACCOUNT, encoding)
    credit_sub = _safe_text(tokens, COL_CREDIT_SUBACCOUNT, encoding)
    credit_tax = _safe_text(tokens, COL_CREDIT_TAX_DIVISION, encoding)
    return BankRowDecision(
        row_index_1b=0,
        kana_key="",
        sign="",
        amount=None,
        placeholder_side="",
        changed=False,
        summary_before=summary,
        summary_after=summary,
        debit_account_before=debit_account,
        debit_account_after=debit_account,
        debit_sub_before=debit_sub,
        debit_sub_after=debit_sub,
        debit_tax_before=debit_tax,
        debit_tax_after=debit_tax,
        credit_account_before=credit_account,
        credit_account_after=credit_account,
        credit_sub_before=credit_sub,
        credit_sub_after=credit_sub,
        credit_tax_before=credit_tax,
        credit_tax_after=credit_tax,
        bank_side="",
        bank_sub_before="",
        bank_sub_after="",
        bank_sub_changed=False,
        bank_sub_evidence=_NONE_EVIDENCE,
        bank_sub_sample_total=0,
        bank_sub_p_majority=0.0,
        bank_sub_top_count=0,
        evidence_type=_NONE_EVIDENCE,
        lookup_key="",
        sample_total=0,
        p_majority=0.0,
        top_count=0,
        label_id=None,
        confidence=0.0,
        priority="HIGH",
        reasons=[],
    )


def _finalize_decision(
    *,
    tokens: Sequence[bytes],
    new_tokens: Sequence[bytes],
    dec: BankRowDecision,
    encoding: str,
) -> None:
    dec.summary_after = _safe_text(new_tokens, COL_SUMMARY, encoding)
    dec.debit_account_after = _safe_text(new_tokens, COL_DEBIT_ACCOUNT, encoding)
    dec.debit_sub_after = _safe_text(new_tokens, COL_DEBIT_SUBACCOUNT, encoding)
    dec.debit_tax_after = _safe_text(new_tokens, COL_DEBIT_TAX_DIVISION, encoding)
    dec.credit_account_after = _safe_text(new_tokens, COL_CREDIT_ACCOUNT, encoding)
    dec.credit_sub_after = _safe_text(new_tokens, COL_CREDIT_SUBACCOUNT, encoding)
    dec.credit_tax_after = _safe_text(new_tokens, COL_CREDIT_TAX_DIVISION, encoding)

    if dec.bank_side == "debit":
        dec.bank_sub_after = dec.debit_sub_after
    elif dec.bank_side == "credit":
        dec.bank_sub_after = dec.credit_sub_after
    else:
        dec.bank_sub_after = dec.bank_sub_before
    dec.bank_sub_changed = dec.bank_sub_before != dec.bank_sub_after

    target_indexes = [
        COL_SUMMARY,
        COL_DEBIT_ACCOUNT,
        COL_DEBIT_SUBACCOUNT,
        COL_DEBIT_TAX_DIVISION,
        COL_CREDIT_ACCOUNT,
        COL_CREDIT_SUBACCOUNT,
        COL_CREDIT_TAX_DIVISION,
    ]
    dec.changed = any(new_tokens[idx] != tokens[idx] for idx in target_indexes)


def decide_bank_row(
    tokens: Sequence[bytes],
    encoding: str,
    cache: BankClientCache,
    config: Dict[str, Any],
) -> Tuple[List[bytes], BankRowDecision]:
    new_tokens = list(tokens)
    dec = _default_decision(tokens=tokens, encoding=encoding)

    placeholder_account_name = str(config.get("placeholder_account_name") or _PLACEHOLDER_DEFAULT)
    bank_account_name = str(config.get("bank_account_name") or _BANK_ACCOUNT_DEFAULT)
    bank_account_subaccount = str(config.get("bank_account_subaccount") or "")

    debit_account_key = _normalize_name_for_match(dec.debit_account_before)
    credit_account_key = _normalize_name_for_match(dec.credit_account_before)
    placeholder_key = _normalize_name_for_match(placeholder_account_name)
    bank_key = _normalize_name_for_match(bank_account_name)
    if debit_account_key == bank_key and credit_account_key != bank_key:
        dec.bank_side = "debit"
        dec.bank_sub_before = dec.debit_sub_before
        dec.bank_sub_after = dec.debit_sub_before
    elif credit_account_key == bank_key and debit_account_key != bank_key:
        dec.bank_side = "credit"
        dec.bank_sub_before = dec.credit_sub_before
        dec.bank_sub_after = dec.credit_sub_before

    if debit_account_key == placeholder_key and credit_account_key == bank_key:
        dec.placeholder_side = "debit"
    elif credit_account_key == placeholder_key and debit_account_key == bank_key:
        dec.placeholder_side = "credit"
    else:
        dec.reasons.append("placeholder_side_not_determined")
        return new_tokens, dec

    sign_from_accounts = derive_sign_from_accounts(
        tokens,
        encoding,
        bank_account_name=bank_account_name,
        bank_account_subaccount=bank_account_subaccount,
    )
    sign_from_memo = extract_sign_from_memo(tokens, encoding)
    if sign_from_accounts and sign_from_memo and sign_from_accounts != sign_from_memo:
        dec.reasons.append("sign_mismatch")
        return new_tokens, dec

    sign = derive_effective_sign(
        tokens,
        encoding,
        bank_account_name=bank_account_name,
        bank_account_subaccount=bank_account_subaccount,
    )
    if not sign:
        dec.reasons.append("sign_not_determined")
        return new_tokens, dec
    dec.sign = str(sign)

    amount = parse_amount(tokens, encoding)
    if amount is None:
        dec.reasons.append("amount_not_determined")
        return new_tokens, dec

    placeholder_amount_text = (
        _safe_text(tokens, COL_DEBIT_AMOUNT, encoding)
        if dec.placeholder_side == "debit"
        else _safe_text(tokens, COL_CREDIT_AMOUNT, encoding)
    )
    placeholder_amount = _parse_amount_cell(placeholder_amount_text)
    if placeholder_amount is None or placeholder_amount <= 0:
        dec.reasons.append("placeholder_amount_not_determined")
        return new_tokens, dec
    if int(amount) != int(placeholder_amount):
        dec.reasons.append("amount_mismatch")
        return new_tokens, dec
    dec.amount = int(placeholder_amount)

    dec.kana_key = normalize_kana_key(dec.summary_before)
    if not dec.kana_key:
        dec.reasons.append("kana_key_empty")
        return new_tokens, dec

    key_strong = f"{dec.kana_key}|{dec.sign}|{dec.amount}"
    key_weak = f"{dec.kana_key}|{dec.sign}"
    thresholds = _resolve_thresholds(config, cache)
    bank_sub_cfg = _resolve_bank_side_subaccount_config(config)
    strong_thr = thresholds.get(ROUTE_KANA_SIGN_AMOUNT) or {}
    weak_thr = thresholds.get(ROUTE_KANA_SIGN) or {}

    bank_sub_eval = _select_bank_side_subaccount(
        cache=cache,
        bank_side=dec.bank_side,
        key_strong=key_strong,
        key_weak=key_weak,
        cfg=bank_sub_cfg,
    )
    dec.reasons.extend(bank_sub_eval.reasons)
    dec.bank_sub_sample_total = int(bank_sub_eval.sample_total)
    dec.bank_sub_p_majority = float(bank_sub_eval.p_majority)
    dec.bank_sub_top_count = int(bank_sub_eval.top_count)

    selected_bank_sub = str(bank_sub_eval.maybe_value or "").strip()
    if selected_bank_sub and dec.bank_side:
        bank_sub_col = COL_DEBIT_SUBACCOUNT if dec.bank_side == "debit" else COL_CREDIT_SUBACCOUNT
        new_tokens[bank_sub_col] = text_to_token(
            selected_bank_sub,
            encoding,
            template_token=tokens[bank_sub_col],
        )
        dec.bank_sub_evidence = bank_sub_eval.evidence_type
        route_name = ROUTE_KANA_SIGN_AMOUNT
        if dec.bank_sub_evidence == _BANK_SUB_WEAK_EVIDENCE:
            route_name = ROUTE_KANA_SIGN
        if selected_bank_sub == dec.bank_sub_before:
            dec.reasons.append(f"bank_sub:{route_name}_same_as_current")
        else:
            dec.reasons.append(f"bank_sub:{route_name}_applied")

    strong_eval = _evaluate_route(
        cache=cache,
        route=ROUTE_KANA_SIGN_AMOUNT,
        lookup_key=key_strong,
        min_count=int(strong_thr.get("min_count", 2)),
        min_p_majority=float(strong_thr.get("min_p_majority", 0.85)),
    )
    weak_eval = _evaluate_route(
        cache=cache,
        route=ROUTE_KANA_SIGN,
        lookup_key=key_weak,
        min_count=int(weak_thr.get("min_count", 3)),
        min_p_majority=float(weak_thr.get("min_p_majority", 0.80)),
    )

    selected: Optional[_RouteEval]
    if strong_eval.selected:
        selected = strong_eval
    elif weak_eval.selected:
        selected = weak_eval
    else:
        selected = None

    if selected is None:
        dec.lookup_key = key_strong
        if strong_eval.sample_total > 0:
            dec.sample_total = int(strong_eval.sample_total)
            dec.p_majority = float(strong_eval.p_majority)
            dec.top_count = int(strong_eval.top_count)
        elif weak_eval.sample_total > 0:
            dec.sample_total = int(weak_eval.sample_total)
            dec.p_majority = float(weak_eval.p_majority)
            dec.top_count = int(weak_eval.top_count)
        dec.reasons.extend(strong_eval.reasons)
        dec.reasons.extend(weak_eval.reasons)
        _finalize_decision(tokens=tokens, new_tokens=new_tokens, dec=dec, encoding=encoding)
        return new_tokens, dec

    label = selected.label
    if label is None or not selected.label_id:
        dec.reasons.extend(selected.reasons)
        dec.reasons.append("selected_label_missing")
        _finalize_decision(tokens=tokens, new_tokens=new_tokens, dec=dec, encoding=encoding)
        return new_tokens, dec

    dec.evidence_type = selected.route
    dec.lookup_key = selected.lookup_key
    dec.sample_total = int(selected.sample_total)
    dec.p_majority = float(selected.p_majority)
    dec.top_count = int(selected.top_count)
    dec.label_id = str(selected.label_id)
    if selected.route == ROUTE_KANA_SIGN:
        dec.reasons.extend(strong_eval.reasons)
    dec.reasons.extend(selected.reasons)
    if selected.route == ROUTE_KANA_SIGN_AMOUNT:
        dec.confidence = confidence(0.98, dec.p_majority, dec.sample_total)
        dec.priority = "LOW" if dec.p_majority >= 0.90 else "MED"
    else:
        dec.confidence = confidence(0.82, dec.p_majority, dec.sample_total)
        dec.priority = "MED"

    new_tokens[COL_SUMMARY] = text_to_token(
        label.corrected_summary,
        encoding,
        template_token=tokens[COL_SUMMARY],
    )

    if dec.placeholder_side == "debit":
        new_tokens[COL_DEBIT_ACCOUNT] = text_to_token(
            label.counter_account,
            encoding,
            template_token=tokens[COL_DEBIT_ACCOUNT],
        )
        new_tokens[COL_DEBIT_SUBACCOUNT] = text_to_token(
            label.counter_subaccount,
            encoding,
            template_token=tokens[COL_DEBIT_SUBACCOUNT],
        )
        new_tokens[COL_DEBIT_TAX_DIVISION] = text_to_token(
            label.counter_tax_division,
            encoding,
            template_token=tokens[COL_DEBIT_TAX_DIVISION],
        )
    else:
        new_tokens[COL_CREDIT_ACCOUNT] = text_to_token(
            label.counter_account,
            encoding,
            template_token=tokens[COL_CREDIT_ACCOUNT],
        )
        new_tokens[COL_CREDIT_SUBACCOUNT] = text_to_token(
            label.counter_subaccount,
            encoding,
            template_token=tokens[COL_CREDIT_SUBACCOUNT],
        )
        new_tokens[COL_CREDIT_TAX_DIVISION] = text_to_token(
            label.counter_tax_division,
            encoding,
            template_token=tokens[COL_CREDIT_TAX_DIVISION],
        )

    _finalize_decision(tokens=tokens, new_tokens=new_tokens, dec=dec, encoding=encoding)
    if not dec.changed:
        dec.reasons.append("selected_label_same_as_current")

    return new_tokens, dec


def replace_bank_yayoi_csv(
    in_path: Path,
    out_path: Path,
    cache_path: Path,
    config: Dict[str, Any],
    run_dir: Path,
    artifact_prefix: Optional[str] = None,
) -> Dict[str, Any]:
    csv_obj = read_yayoi_csv(in_path)
    cache = load_bank_cache(cache_path)
    thresholds = _resolve_thresholds(config, cache)

    decisions: List[BankRowDecision] = []
    changed_count = 0
    evidence_counts: Dict[str, int] = {}
    bank_side_subaccount_changed_count = 0
    bank_side_subaccount_evidence_counts: Dict[str, int] = {}

    for row_index_1b, row in enumerate(csv_obj.rows, start=1):
        new_tokens, decision = decide_bank_row(
            tokens=row.tokens,
            encoding=csv_obj.encoding,
            cache=cache,
            config=config,
        )
        decision.row_index_1b = int(row_index_1b)
        if list(row.tokens) != new_tokens:
            changed_count += 1
            row.tokens = new_tokens
        decisions.append(decision)
        evidence_counts[decision.evidence_type] = evidence_counts.get(decision.evidence_type, 0) + 1
        if decision.bank_sub_changed:
            bank_side_subaccount_changed_count += 1
            evidence_bucket = decision.bank_sub_evidence
            if decision.bank_sub_evidence == _BANK_SUB_STRONG_EVIDENCE:
                evidence_bucket = "strong"
            elif decision.bank_sub_evidence == _BANK_SUB_WEAK_EVIDENCE:
                evidence_bucket = "weak"
            bank_side_subaccount_evidence_counts[evidence_bucket] = (
                bank_side_subaccount_evidence_counts.get(evidence_bucket, 0) + 1
            )

    write_yayoi_csv(csv_obj, out_path)

    if artifact_prefix:
        report_path = get_review_report_path(run_dir, artifact_prefix)
        manifest_path = get_input_manifest_path(run_dir, artifact_prefix)
    else:
        report_path = run_dir / f"{in_path.stem}_review_report.csv"
        manifest_path = run_dir / f"{in_path.stem}_manifest.json"

    report_path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "row_index_1b",
        "kana_key",
        "sign",
        "amount",
        "placeholder_side",
        "changed",
        "evidence_type",
        "lookup_key",
        "sample_total",
        "p_majority",
        "top_count",
        "label_id",
        "confidence",
        "priority",
        "summary_before",
        "summary_after",
        "debit_account_before",
        "debit_account_after",
        "debit_sub_before",
        "debit_sub_after",
        "debit_tax_before",
        "debit_tax_after",
        "credit_account_before",
        "credit_account_after",
        "credit_sub_before",
        "credit_sub_after",
        "credit_tax_before",
        "credit_tax_after",
        "reasons",
        "bank_side",
        "bank_sub_before",
        "bank_sub_after",
        "bank_sub_changed",
        "bank_sub_evidence",
        "bank_sub_sample_total",
        "bank_sub_p_majority",
        "bank_sub_top_count",
    ]
    with report_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv_lib.writer(f, dialect="excel", lineterminator="\r\n", quoting=csv_lib.QUOTE_MINIMAL)
        writer.writerow(header)
        for d in decisions:
            writer.writerow(
                [
                    str(d.row_index_1b),
                    d.kana_key,
                    d.sign,
                    "" if d.amount is None else str(d.amount),
                    d.placeholder_side,
                    "1" if d.changed else "0",
                    d.evidence_type,
                    d.lookup_key,
                    str(d.sample_total),
                    f"{d.p_majority:.6f}",
                    str(d.top_count),
                    d.label_id or "",
                    f"{d.confidence:.4f}",
                    d.priority,
                    d.summary_before,
                    d.summary_after,
                    d.debit_account_before,
                    d.debit_account_after,
                    d.debit_sub_before,
                    d.debit_sub_after,
                    d.debit_tax_before,
                    d.debit_tax_after,
                    d.credit_account_before,
                    d.credit_account_after,
                    d.credit_sub_before,
                    d.credit_sub_after,
                    d.credit_tax_before,
                    d.credit_tax_after,
                    " | ".join(d.reasons),
                    d.bank_side,
                    d.bank_sub_before,
                    d.bank_sub_after,
                    "1" if d.bank_sub_changed else "0",
                    d.bank_sub_evidence,
                    str(d.bank_sub_sample_total),
                    f"{d.bank_sub_p_majority:.6f}",
                    str(d.bank_sub_top_count),
                ]
            )

    row_count = len(csv_obj.rows)
    manifest = {
        "schema": "belle.bank_replacer_run_manifest.v1",
        "version": str(config.get("version") or "0.1"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "line_id": "bank_statement",
        "input_file": str(in_path),
        "input_sha256": sha256_file(in_path),
        "output_file": str(out_path),
        "output_sha256": sha256_file(out_path),
        "cache_file": str(cache_path),
        "cache_sha256": sha256_file(cache_path) if cache_path.exists() else "",
        "row_count": int(row_count),
        "changed_count": int(changed_count),
        "changed_ratio": (changed_count / row_count) if row_count else 0.0,
        "evidence_counts": evidence_counts,
        "bank_side_subaccount_changed_count": int(bank_side_subaccount_changed_count),
        "bank_side_subaccount_evidence_counts": bank_side_subaccount_evidence_counts,
        "decision_thresholds": thresholds,
        "reports": {
            "review_report_csv": str(report_path),
            "manifest_json": str(manifest_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
