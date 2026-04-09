# -*- coding: utf-8 -*-
from __future__ import annotations

import csv as csv_lib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from belle.fs_utils import sha256_file_chunked

from .tax_postprocess import (
    TaxPostprocessSideResult,
    YayoiTaxPostprocessConfig,
    apply_yayoi_tax_postprocess,
    build_tax_postprocess_manifest,
    default_yayoi_tax_postprocess_config,
)
from .yayoi_columns import COL_CREDIT_TAX_AMOUNT, COL_DEBIT_TAX_AMOUNT, COL_DEBIT_TAX_DIVISION
from .yayoi_csv import read_yayoi_csv, write_yayoi_csv, token_to_text, text_to_token, YayoiCSV
from .text import extract_t_number, vendor_key_from_summary
from .lexicon import Lexicon, match_summary
from .client_cache import ClientCache, TaxStatsEntry
from .defaults import CategoryDefaults
from .paths import get_input_manifest_path, get_review_report_path

ROUTE_TAX_T_NUMBER_X_CATEGORY_TARGET_ACCOUNT = "t_number_x_category_target_account"
ROUTE_TAX_T_NUMBER_TARGET_ACCOUNT = "t_number_target_account"
ROUTE_TAX_VENDOR_KEY_TARGET_ACCOUNT = "vendor_key_target_account"
ROUTE_TAX_CATEGORY_TARGET_ACCOUNT = "category_target_account"
ROUTE_TAX_GLOBAL_TARGET_ACCOUNT = "global_target_account"
ROUTE_TAX_CATEGORY_DEFAULT = "category_default"
ROUTE_TAX_GLOBAL_FALLBACK = "global_fallback"
ROUTE_TAX_NONE = "none"


@dataclass
class TaxDivisionDecision:
    debit_tax_division_before: str
    debit_tax_division_after: str
    changed: bool
    evidence_type: str
    confidence: float
    sample_total: int
    p_majority: float
    reasons: List[str]


@dataclass
class RowDecision:
    row_index_1b: int
    summary: str
    debit_before: str
    debit_after: str
    changed: bool
    evidence_type: str
    confidence: float
    priority: str
    reasons: List[str]
    t_number: Optional[str]
    vendor_key: Optional[str]
    category_key: Optional[str]
    category_label: Optional[str]
    lexicon_quality: Optional[str]
    matched_needle: Optional[str]
    is_learned_signal: bool
    debit_tax_division_before: str
    debit_tax_division_after: str
    debit_tax_division_changed: bool
    tax_evidence_type: str
    tax_confidence: float
    tax_sample_total: int
    tax_p_majority: float
    tax_reasons: List[str]


@dataclass
class _TaxRouteEval:
    route: str
    selected: bool
    tax_division: Optional[str]
    sample_total: int
    p_majority: float
    reasons: List[str]


def sha256_file(path: Path) -> str:
    return sha256_file_chunked(path)


def _confidence(strength: float, p_majority: float, sample_total: int) -> float:
    # sample_factor goes 0..1 around ~50 samples
    sample_factor = min(1.0, math.log(sample_total + 1, 10) / math.log(50, 10))
    conf = strength * (0.7 * p_majority + 0.3 * sample_factor)
    return max(0.0, min(1.0, conf))


def _clone_row_tokens(csv_obj: YayoiCSV) -> List[List[bytes]]:
    return [list(row.tokens) for row in csv_obj.rows]


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
                token_to_text(pre_tax_tokens[tax_amount_idx], encoding),
                token_to_text(final_tokens[tax_amount_idx], encoding),
                result.status,
                "" if result.rate_percent is None else str(result.rate_percent),
                result.calc_mode,
            ]
        )
    return values


def _account_thresholds(config: Dict[str, Any]) -> Dict[str, Any]:
    return config.get("thresholds") if isinstance(config.get("thresholds"), dict) else {}


def _account_confidence_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return config.get("confidence") if isinstance(config.get("confidence"), dict) else {}


def _tax_thresholds(config: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    raw = config.get("tax_division_thresholds")
    section = raw if isinstance(raw, dict) else {}

    def _route(name: str, *, min_count: int, min_p_majority: float) -> Dict[str, float]:
        route_obj = section.get(name) if isinstance(section.get(name), dict) else {}
        return {
            "min_count": float(route_obj.get("min_count", min_count)),
            "min_p_majority": float(route_obj.get("min_p_majority", min_p_majority)),
        }

    return {
        ROUTE_TAX_T_NUMBER_X_CATEGORY_TARGET_ACCOUNT: _route(
            ROUTE_TAX_T_NUMBER_X_CATEGORY_TARGET_ACCOUNT,
            min_count=2,
            min_p_majority=0.75,
        ),
        ROUTE_TAX_T_NUMBER_TARGET_ACCOUNT: _route(
            ROUTE_TAX_T_NUMBER_TARGET_ACCOUNT,
            min_count=3,
            min_p_majority=0.70,
        ),
        ROUTE_TAX_VENDOR_KEY_TARGET_ACCOUNT: _route(
            ROUTE_TAX_VENDOR_KEY_TARGET_ACCOUNT,
            min_count=3,
            min_p_majority=0.70,
        ),
        ROUTE_TAX_CATEGORY_TARGET_ACCOUNT: _route(
            ROUTE_TAX_CATEGORY_TARGET_ACCOUNT,
            min_count=3,
            min_p_majority=0.70,
        ),
        ROUTE_TAX_GLOBAL_TARGET_ACCOUNT: _route(
            ROUTE_TAX_GLOBAL_TARGET_ACCOUNT,
            min_count=3,
            min_p_majority=0.70,
        ),
    }


def _tax_confidence_config(config: Dict[str, Any]) -> Dict[str, Any]:
    raw = config.get("tax_division_confidence")
    section = raw if isinstance(raw, dict) else {}
    return {
        f"{ROUTE_TAX_T_NUMBER_X_CATEGORY_TARGET_ACCOUNT}_strength": float(
            section.get(f"{ROUTE_TAX_T_NUMBER_X_CATEGORY_TARGET_ACCOUNT}_strength", 0.97)
        ),
        f"{ROUTE_TAX_T_NUMBER_TARGET_ACCOUNT}_strength": float(
            section.get(f"{ROUTE_TAX_T_NUMBER_TARGET_ACCOUNT}_strength", 0.95)
        ),
        f"{ROUTE_TAX_VENDOR_KEY_TARGET_ACCOUNT}_strength": float(
            section.get(f"{ROUTE_TAX_VENDOR_KEY_TARGET_ACCOUNT}_strength", 0.85)
        ),
        f"{ROUTE_TAX_CATEGORY_TARGET_ACCOUNT}_strength": float(
            section.get(f"{ROUTE_TAX_CATEGORY_TARGET_ACCOUNT}_strength", 0.65)
        ),
        f"{ROUTE_TAX_GLOBAL_TARGET_ACCOUNT}_strength": float(
            section.get(f"{ROUTE_TAX_GLOBAL_TARGET_ACCOUNT}_strength", 0.55)
        ),
        "category_default_strength": float(section.get("category_default_strength", 0.55)),
        "global_fallback_strength": float(section.get("global_fallback_strength", 0.35)),
        "learned_weight_multiplier": float(section.get("learned_weight_multiplier", 0.85)),
    }


def _base_row_decision(
    *,
    summary: str,
    debit_before: str,
    debit_tax_division_before: str,
) -> RowDecision:
    return RowDecision(
        row_index_1b=0,
        summary=summary,
        debit_before=debit_before,
        debit_after=debit_before,
        changed=False,
        evidence_type=ROUTE_TAX_NONE,
        confidence=0.0,
        priority="HIGH",
        reasons=[],
        t_number=None,
        vendor_key=None,
        category_key=None,
        category_label=None,
        lexicon_quality=None,
        matched_needle=None,
        is_learned_signal=False,
        debit_tax_division_before=debit_tax_division_before,
        debit_tax_division_after=debit_tax_division_before,
        debit_tax_division_changed=False,
        tax_evidence_type=ROUTE_TAX_NONE,
        tax_confidence=0.0,
        tax_sample_total=0,
        tax_p_majority=0.0,
        tax_reasons=[],
    )


def decide_row(
    *,
    summary: str,
    debit_before: str,
    debit_tax_division_before: str,
    lex: Lexicon,
    client_cache: Optional[ClientCache],
    defaults: CategoryDefaults,
    config: Dict[str, Any],
) -> Tuple[str, RowDecision]:
    """
    Returns (debit_after, decision_meta)

    Deterministic decision order (highest -> lowest):
      1) T-number × category (client_cache stats, gated)
      2) T-number (client_cache stats, gated)
      3) vendor_key (client_cache stats, gated)
      4) category (client_cache stats, gated)
      5) category defaults (category_defaults.json)
      6) global fallback

    IMPORTANT:
    - T-number is extracted from 摘要 only.
    - Category is inferred from 摘要 using lexicon only.
    """
    thr = _account_thresholds(config)
    conf_cfg = _account_confidence_config(config)
    dummy = (config.get("csv_contract") or {}).get("dummy_summary_exact") or "##DUMMY_OCR_UNREADABLE##"
    dec = _base_row_decision(
        summary=summary,
        debit_before=debit_before,
        debit_tax_division_before=debit_tax_division_before,
    )

    if summary == dummy:
        dec.evidence_type = "dummy"
        dec.priority = "HIGH"
        dec.reasons = ["dummy_entry_detected"]
        return debit_before, dec

    tnum = extract_t_number(summary)
    vkey = vendor_key_from_summary(summary)
    m = match_summary(lex, summary)
    cat_key = m.category_key

    dec.t_number = tnum
    dec.vendor_key = vkey
    dec.category_key = cat_key
    dec.category_label = m.category_label
    dec.lexicon_quality = m.quality
    dec.matched_needle = m.matched_needle
    dec.is_learned_signal = m.is_learned_signal

    tm = client_cache

    # (1) T-number × category route (strongest when distribution is sharp)
    if tnum and cat_key and tm:
        inner = tm.t_numbers_by_category.get(tnum)
        if inner and cat_key in inner and inner[cat_key].top_account:
            st = inner[cat_key]
            min_count = int(thr.get("t_number_x_category_min_count", 2))
            pmin = float(thr.get("t_number_x_category_p_majority_min", 0.75))
            if st.sample_total >= min_count and st.p_majority >= pmin:
                debit_after = st.top_account
                strength = float(conf_cfg.get("t_number_x_category_strength", 0.97))
                confidence = _confidence(strength, st.p_majority, st.sample_total)
                # ambiguity in category -> higher review priority
                priority = "LOW"
                reasons = ["t_number_match", "category_match", "t_number_x_category_client_cache_match"]
                if m.quality == "ambiguous":
                    priority = "MED"
                    reasons.insert(0, "category_match_ambiguous")
                dec.debit_after = debit_after
                dec.changed = debit_after != debit_before
                dec.evidence_type = "t_number_x_category"
                dec.confidence = confidence
                dec.priority = priority
                dec.reasons = reasons
                return debit_after, dec

    # (2) T-number route
    if tnum and tm and tnum in tm.t_numbers and tm.t_numbers[tnum].top_account:
        st = tm.t_numbers[tnum]
        min_count = int(thr.get("t_number_min_count", 3))
        pmin = float(thr.get("t_number_p_majority_min", 0.70))
        if st.sample_total >= min_count and st.p_majority >= pmin:
            debit_after = st.top_account
            strength = float(conf_cfg.get("t_number_strength", 0.95))
            confidence = _confidence(strength, st.p_majority, st.sample_total)
            priority = "LOW" if st.p_majority >= 0.85 else "MED"
            reasons = ["t_number_match"]
            dec.debit_after = debit_after
            dec.changed = debit_after != debit_before
            dec.evidence_type = "t_number"
            dec.confidence = confidence
            dec.priority = priority
            dec.reasons = reasons
            return debit_after, dec

    # (3) vendor_key route
    if vkey and tm and vkey in tm.vendor_keys and tm.vendor_keys[vkey].top_account:
        st = tm.vendor_keys[vkey]
        min_count = int(thr.get("vendor_key_min_count", 3))
        pmin = float(thr.get("vendor_key_p_majority_min", 0.70))
        if st.sample_total >= min_count and st.p_majority >= pmin:
            debit_after = st.top_account
            strength = float(conf_cfg.get("vendor_key_strength", 0.85))
            confidence = _confidence(strength, st.p_majority, st.sample_total)
            priority = "LOW" if st.p_majority >= 0.85 and st.sample_total >= 10 else "MED"
            reasons = ["vendor_key_match"]
            dec.debit_after = debit_after
            dec.changed = debit_after != debit_before
            dec.evidence_type = "vendor_key"
            dec.confidence = confidence
            dec.priority = priority
            dec.reasons = reasons
            return debit_after, dec

    # (4) category route (client evidence)
    if cat_key and tm and cat_key in tm.categories and tm.categories[cat_key].top_account:
        st = tm.categories[cat_key]
        min_count = int(thr.get("category_min_count", 3))
        pmin = float(thr.get("category_p_majority_min", 0.70))
        if st.sample_total >= min_count and st.p_majority >= pmin:
            debit_after = st.top_account
            strength = float(conf_cfg.get("category_strength", 0.65))
            confidence = _confidence(strength, st.p_majority, st.sample_total)
            # learned signal reduces confidence
            if m.is_learned_signal:
                confidence *= float(conf_cfg.get("learned_weight_multiplier", 0.85))
            # ambiguity increases priority
            if m.quality == "ambiguous":
                priority = "HIGH"
                reasons = ["category_match_ambiguous", "category_client_cache_match"]
            else:
                priority = "MED"
                reasons = ["category_client_cache_match"]
            dec.debit_after = debit_after
            dec.changed = debit_after != debit_before
            dec.evidence_type = "category_client"
            dec.confidence = confidence
            dec.priority = priority
            dec.reasons = reasons
            return debit_after, dec

    # (5) category default route
    if cat_key and cat_key in defaults.defaults:
        rule = defaults.defaults[cat_key]
        debit_after = rule.target_account
        confidence = float(conf_cfg.get("default_strength", rule.confidence))
        if m.is_learned_signal:
            confidence *= float(conf_cfg.get("learned_weight_multiplier", 0.85))
        reasons = ["category_default_applied"]
        priority = rule.priority
        if m.quality == "ambiguous":
            priority = "HIGH"
            reasons.insert(0, "category_match_ambiguous")
        dec.debit_after = debit_after
        dec.changed = debit_after != debit_before
        dec.evidence_type = "category_default"
        dec.confidence = confidence
        dec.priority = priority
        dec.reasons = reasons
        return debit_after, dec

    # (6) global fallback
    gf = defaults.global_fallback
    debit_after = gf.target_account
    confidence = float(conf_cfg.get("global_fallback_strength", gf.confidence))
    reasons = ["global_fallback_applied"]
    dec.debit_after = debit_after
    dec.changed = debit_after != debit_before
    dec.evidence_type = "global_fallback"
    dec.confidence = confidence
    dec.priority = gf.priority
    dec.reasons = reasons
    return debit_after, dec


def _lookup_tax_stats_entry(
    *,
    client_cache: Optional[ClientCache],
    route: str,
    t_number: Optional[str],
    category_key: Optional[str],
    vendor_key: Optional[str],
    target_account: str,
) -> Optional[TaxStatsEntry]:
    if client_cache is None or not target_account:
        return None

    if route == ROUTE_TAX_T_NUMBER_X_CATEGORY_TARGET_ACCOUNT:
        if not t_number or not category_key:
            return None
        return (
            ((getattr(client_cache, "tax_t_numbers_by_category_and_account", {}).get(t_number) or {}).get(category_key) or {}).get(
                target_account
            )
        )
    if route == ROUTE_TAX_T_NUMBER_TARGET_ACCOUNT:
        if not t_number:
            return None
        return (getattr(client_cache, "tax_t_numbers_by_account", {}).get(t_number) or {}).get(target_account)
    if route == ROUTE_TAX_VENDOR_KEY_TARGET_ACCOUNT:
        if not vendor_key:
            return None
        return (getattr(client_cache, "tax_vendor_keys_by_account", {}).get(vendor_key) or {}).get(target_account)
    if route == ROUTE_TAX_CATEGORY_TARGET_ACCOUNT:
        if not category_key:
            return None
        return (getattr(client_cache, "tax_categories_by_account", {}).get(category_key) or {}).get(target_account)
    if route == ROUTE_TAX_GLOBAL_TARGET_ACCOUNT:
        return getattr(client_cache, "tax_global_by_account", {}).get(target_account)
    return None


def _evaluate_tax_route(
    *,
    route: str,
    entry: Optional[TaxStatsEntry],
    min_count: int,
    min_p_majority: float,
) -> _TaxRouteEval:
    if not isinstance(entry, TaxStatsEntry):
        return _TaxRouteEval(
            route=route,
            selected=False,
            tax_division=None,
            sample_total=0,
            p_majority=0.0,
            reasons=[f"tax:{route}_stats_not_found"],
        )

    sample_total = int(entry.sample_total)
    p_majority = float(entry.p_majority)
    top_tax_division = str(entry.top_tax_division or "").strip()
    reasons: List[str] = []
    if not top_tax_division:
        reasons.append(f"tax:{route}_top_tax_division_missing")
    if sample_total < int(min_count):
        reasons.append(f"tax:{route}_min_count_not_met")
    if p_majority < float(min_p_majority):
        reasons.append(f"tax:{route}_p_majority_not_met")

    if reasons:
        return _TaxRouteEval(
            route=route,
            selected=False,
            tax_division=top_tax_division or None,
            sample_total=sample_total,
            p_majority=p_majority,
            reasons=reasons,
        )

    return _TaxRouteEval(
        route=route,
        selected=True,
        tax_division=top_tax_division,
        sample_total=sample_total,
        p_majority=p_majority,
        reasons=[f"tax:{route}_selected"],
    )


def decide_tax_division(
    *,
    row_decision: RowDecision,
    client_cache: Optional[ClientCache],
    defaults: CategoryDefaults,
    config: Dict[str, Any],
) -> TaxDivisionDecision:
    before = row_decision.debit_tax_division_before
    target_account = str(row_decision.debit_after or "").strip()

    if row_decision.evidence_type == "dummy":
        return TaxDivisionDecision(
            debit_tax_division_before=before,
            debit_tax_division_after=before,
            changed=False,
            evidence_type=ROUTE_TAX_NONE,
            confidence=0.0,
            sample_total=0,
            p_majority=0.0,
            reasons=["tax:dummy_row_preserved"],
        )

    if not target_account:
        return TaxDivisionDecision(
            debit_tax_division_before=before,
            debit_tax_division_after=before,
            changed=False,
            evidence_type=ROUTE_TAX_NONE,
            confidence=0.0,
            sample_total=0,
            p_majority=0.0,
            reasons=["tax:target_account_missing"],
        )

    threshold_cfg = _tax_thresholds(config)
    confidence_cfg = _tax_confidence_config(config)
    miss_reasons: List[str] = []

    for route in (
        ROUTE_TAX_T_NUMBER_X_CATEGORY_TARGET_ACCOUNT,
        ROUTE_TAX_T_NUMBER_TARGET_ACCOUNT,
        ROUTE_TAX_VENDOR_KEY_TARGET_ACCOUNT,
        ROUTE_TAX_CATEGORY_TARGET_ACCOUNT,
        ROUTE_TAX_GLOBAL_TARGET_ACCOUNT,
    ):
        threshold = threshold_cfg.get(route) or {}
        route_eval = _evaluate_tax_route(
            route=route,
            entry=_lookup_tax_stats_entry(
                client_cache=client_cache,
                route=route,
                t_number=row_decision.t_number,
                category_key=row_decision.category_key,
                vendor_key=row_decision.vendor_key,
                target_account=target_account,
            ),
            min_count=int(threshold.get("min_count", 0)),
            min_p_majority=float(threshold.get("min_p_majority", 0.0)),
        )
        if route_eval.selected and route_eval.tax_division:
            confidence = _confidence(
                float(confidence_cfg.get(f"{route}_strength", 0.5)),
                route_eval.p_majority,
                route_eval.sample_total,
            )
            if row_decision.is_learned_signal and route in {
                ROUTE_TAX_T_NUMBER_X_CATEGORY_TARGET_ACCOUNT,
                ROUTE_TAX_CATEGORY_TARGET_ACCOUNT,
            }:
                confidence *= float(confidence_cfg.get("learned_weight_multiplier", 0.85))
            return TaxDivisionDecision(
                debit_tax_division_before=before,
                debit_tax_division_after=route_eval.tax_division,
                changed=route_eval.tax_division != before,
                evidence_type=route,
                confidence=confidence,
                sample_total=route_eval.sample_total,
                p_majority=route_eval.p_majority,
                reasons=[*route_eval.reasons, f"tax:target_account={target_account}"],
            )
        miss_reasons.extend(route_eval.reasons)

    category_key = row_decision.category_key
    if category_key and category_key in defaults.defaults:
        rule = defaults.defaults[category_key]
        target_tax_division = str(rule.target_tax_division or "").strip()
        if target_tax_division:
            confidence = float(confidence_cfg.get("category_default_strength", 0.55))
            if row_decision.is_learned_signal:
                confidence *= float(confidence_cfg.get("learned_weight_multiplier", 0.85))
            return TaxDivisionDecision(
                debit_tax_division_before=before,
                debit_tax_division_after=target_tax_division,
                changed=target_tax_division != before,
                evidence_type=ROUTE_TAX_CATEGORY_DEFAULT,
                confidence=confidence,
                sample_total=0,
                p_majority=0.0,
                reasons=["tax:category_default_applied", f"tax:target_account={target_account}"],
            )
        miss_reasons.append("tax:category_default_blank")
    else:
        miss_reasons.append("tax:category_default_unavailable")

    global_tax_division = str(defaults.global_fallback.target_tax_division or "").strip()
    if global_tax_division:
        return TaxDivisionDecision(
            debit_tax_division_before=before,
            debit_tax_division_after=global_tax_division,
            changed=global_tax_division != before,
            evidence_type=ROUTE_TAX_GLOBAL_FALLBACK,
            confidence=float(confidence_cfg.get("global_fallback_strength", 0.35)),
            sample_total=0,
            p_majority=0.0,
            reasons=["tax:global_fallback_applied", f"tax:target_account={target_account}"],
        )

    miss_reasons.append("tax:global_fallback_blank")
    return TaxDivisionDecision(
        debit_tax_division_before=before,
        debit_tax_division_after=before,
        changed=False,
        evidence_type=ROUTE_TAX_NONE,
        confidence=0.0,
        sample_total=0,
        p_majority=0.0,
        reasons=miss_reasons or ["tax:unresolved"],
    )


def replace_yayoi_csv(
    *,
    in_path: Path,
    out_path: Path,
    lex: Lexicon,
    client_cache: Optional[ClientCache],
    defaults: CategoryDefaults,
    config: Dict[str, Any],
    run_dir: Path,
    artifact_prefix: Optional[str] = None,
    yayoi_tax_config: Optional[YayoiTaxPostprocessConfig] = None,
) -> Dict[str, Any]:
    """
    Replace receipt debit account and debit-side tax division for a single input CSV.
    Produces:
    - out_path: replaced CSV
    - run_dir/<artifact_prefix>_review_report.csv
    - run_dir/<artifact_prefix>_manifest.json
      (fallback to <stem> if artifact_prefix is omitted)

    Returns manifest dict.
    """
    csv = read_yayoi_csv(in_path)
    original_row_tokens = _clone_row_tokens(csv)
    decisions: List[RowDecision] = []
    evidence_counter: Dict[str, int] = {}
    tax_route_counts: Dict[str, int] = {}
    tax_division_changed_count = 0
    tax_unresolved_count = 0

    for i, row in enumerate(csv.rows, start=1):
        summary = token_to_text(row.tokens[16], csv.encoding)
        debit_before = token_to_text(row.tokens[4], csv.encoding)
        debit_tax_division_before = token_to_text(row.tokens[COL_DEBIT_TAX_DIVISION], csv.encoding)

        debit_after, dec = decide_row(
            summary=summary,
            debit_before=debit_before,
            debit_tax_division_before=debit_tax_division_before,
            lex=lex,
            client_cache=client_cache,
            defaults=defaults,
            config=config,
        )
        dec.row_index_1b = i
        decisions.append(dec)
        evidence_counter[dec.evidence_type] = evidence_counter.get(dec.evidence_type, 0) + 1

        if debit_after != debit_before:
            # Replace token (preserve quoting style from original token)
            row.tokens[4] = text_to_token(debit_after, csv.encoding, template_token=row.tokens[4])

        tax_dec = decide_tax_division(
            row_decision=dec,
            client_cache=client_cache,
            defaults=defaults,
            config=config,
        )
        dec.debit_tax_division_before = tax_dec.debit_tax_division_before
        dec.debit_tax_division_after = tax_dec.debit_tax_division_after
        dec.debit_tax_division_changed = tax_dec.changed
        dec.tax_evidence_type = tax_dec.evidence_type
        dec.tax_confidence = tax_dec.confidence
        dec.tax_sample_total = tax_dec.sample_total
        dec.tax_p_majority = tax_dec.p_majority
        dec.tax_reasons = list(tax_dec.reasons)

        if tax_dec.evidence_type == ROUTE_TAX_NONE:
            tax_unresolved_count += 1
        else:
            tax_route_counts[tax_dec.evidence_type] = tax_route_counts.get(tax_dec.evidence_type, 0) + 1

        if tax_dec.changed:
            tax_division_changed_count += 1
            row.tokens[COL_DEBIT_TAX_DIVISION] = text_to_token(
                tax_dec.debit_tax_division_after,
                csv.encoding,
                template_token=row.tokens[COL_DEBIT_TAX_DIVISION],
            )

    pre_tax_row_tokens = _clone_row_tokens(csv)
    tax_summary = apply_yayoi_tax_postprocess(
        csv,
        yayoi_tax_config or default_yayoi_tax_postprocess_config(),
    )
    tax_side_results = _tax_result_map(tax_summary)
    changed_count = 0
    for original_tokens, row, decision in zip(original_row_tokens, csv.rows, decisions):
        decision.changed = _row_changed(original_tokens, row.tokens)
        if decision.changed:
            changed_count += 1

    # Write CSV
    write_yayoi_csv(csv, out_path)

    # Review report CSV (UTF-8 BOM for Excel)
    if artifact_prefix:
        report_path = get_review_report_path(run_dir, artifact_prefix)
        manifest_path = get_input_manifest_path(run_dir, artifact_prefix)
    else:
        report_path = run_dir / f"{in_path.stem}_review_report.csv"
        manifest_path = run_dir / f"{in_path.stem}_manifest.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "row_index_1b",
        "summary",
        "debit_before",
        "debit_after",
        "changed",
        "evidence_type",
        "confidence",
        "priority",
        "t_number",
        "vendor_key",
        "category_key",
        "category_label",
        "lexicon_quality",
        "matched_needle",
        "is_learned_signal",
        "reasons",
        "debit_tax_division_before",
        "debit_tax_division_after",
        "debit_tax_division_changed",
        "tax_evidence_type",
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
    with report_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv_lib.writer(f, dialect="excel", lineterminator="\r\n", quoting=csv_lib.QUOTE_MINIMAL)
        writer.writerow(header)
        for d, pre_tax_tokens, row in zip(decisions, pre_tax_row_tokens, csv.rows):
            writer.writerow(
                [
                    str(d.row_index_1b),
                    d.summary,
                    d.debit_before,
                    d.debit_after,
                    "1" if d.changed else "0",
                    d.evidence_type,
                    f"{d.confidence:.4f}",
                    d.priority,
                    d.t_number or "",
                    d.vendor_key or "",
                    d.category_key or "",
                    d.category_label or "",
                    d.lexicon_quality or "",
                    d.matched_needle or "",
                    "1" if d.is_learned_signal else "0",
                    " | ".join(d.reasons),
                    d.debit_tax_division_before,
                    d.debit_tax_division_after,
                    "1" if d.debit_tax_division_changed else "0",
                    d.tax_evidence_type,
                    f"{d.tax_confidence:.4f}",
                    str(d.tax_sample_total),
                    f"{d.tax_p_majority:.6f}",
                    " | ".join(d.tax_reasons),
                    *_tax_review_cells(
                        row_index_1b=d.row_index_1b,
                        encoding=csv.encoding,
                        pre_tax_tokens=pre_tax_tokens,
                        final_tokens=row.tokens,
                        side_results=tax_side_results,
                    ),
                ]
            )

    # Manifest JSON
        # Manifest JSON
    rows_with_t_number = sum(1 for d in decisions if d.t_number)
    rows_with_category = sum(1 for d in decisions if d.category_key)
    rows_using_t_routes = sum(1 for d in decisions if d.evidence_type in ("t_number", "t_number_x_category"))

    manifest = {
        "schema": "belle.replacer_run_manifest.v2",
        "version": str(config.get("version") or "1.16"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "input_file": str(in_path),
        "input_sha256": sha256_file(in_path),
        "output_file": str(out_path),
        "output_sha256": sha256_file(out_path),
        "reports": {
            "review_report_csv": str(report_path),
        },
        "row_count": len(csv.rows),
        "changed_count": changed_count,
        "changed_ratio": (changed_count / len(csv.rows)) if csv.rows else 0.0,
        "evidence_counts": evidence_counter,
        "analysis": {
            "rows_with_t_number": int(rows_with_t_number),
            "rows_with_category": int(rows_with_category),
            "rows_using_t_routes": int(rows_using_t_routes),
        },
        "tax_division_replacement": {
            "changed_count": int(tax_division_changed_count),
            "route_counts": tax_route_counts,
            "unresolved_count": int(tax_unresolved_count),
            "category_default_applied_count": int(tax_route_counts.get(ROUTE_TAX_CATEGORY_DEFAULT) or 0),
            "global_fallback_applied_count": int(tax_route_counts.get(ROUTE_TAX_GLOBAL_FALLBACK) or 0),
        },
        "tax_postprocess": build_tax_postprocess_manifest(tax_summary),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest["reports"]["manifest_json"] = str(manifest_path)

    return manifest

