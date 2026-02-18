# -*- coding: utf-8 -*-
from __future__ import annotations

import csv as csv_lib
import json
import math
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .yayoi_csv import read_yayoi_csv, write_yayoi_csv, token_to_text, text_to_token, YayoiCSV
from .text import extract_t_number, vendor_key_from_summary
from .lexicon import Lexicon, match_summary
from .client_cache import ClientCache
from .defaults import CategoryDefaults
from .paths import get_input_manifest_path, get_review_report_path


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


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _confidence(strength: float, p_majority: float, sample_total: int) -> float:
    # sample_factor goes 0..1 around ~50 samples
    sample_factor = min(1.0, math.log(sample_total + 1, 10) / math.log(50, 10))
    conf = strength * (0.7 * p_majority + 0.3 * sample_factor)
    return max(0.0, min(1.0, conf))


def decide_row(
    *,
    summary: str,
    debit_before: str,
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
    thr = config.get("thresholds") or {}
    conf_cfg = config.get("confidence") or {}
    dummy = (config.get("csv_contract") or {}).get("dummy_summary_exact") or "##DUMMY_OCR_UNREADABLE##"

    if summary == dummy:
        debit_after = debit_before
        dec = RowDecision(
            row_index_1b=0,
            summary=summary,
            debit_before=debit_before,
            debit_after=debit_after,
            changed=False,
            evidence_type="dummy",
            confidence=0.0,
            priority="HIGH",
            reasons=["dummy_entry_detected"],
            t_number=None,
            vendor_key=None,
            category_key=None,
            category_label=None,
            lexicon_quality=None,
            matched_needle=None,
            is_learned_signal=False,
        )
        return debit_after, dec

    tnum = extract_t_number(summary)
    vkey = vendor_key_from_summary(summary)
    m = match_summary(lex, summary)
    cat_key = m.category_key

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
                dec = RowDecision(
                    row_index_1b=0,
                    summary=summary,
                    debit_before=debit_before,
                    debit_after=debit_after,
                    changed=(debit_after != debit_before),
                    evidence_type="t_number_x_category",
                    confidence=confidence,
                    priority=priority,
                    reasons=reasons,
                    t_number=tnum,
                    vendor_key=vkey,
                    category_key=cat_key,
                    category_label=m.category_label,
                    lexicon_quality=m.quality,
                    matched_needle=m.matched_needle,
                    is_learned_signal=m.is_learned_signal,
                )
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
            dec = RowDecision(
                row_index_1b=0,
                summary=summary,
                debit_before=debit_before,
                debit_after=debit_after,
                changed=(debit_after != debit_before),
                evidence_type="t_number",
                confidence=confidence,
                priority=priority,
                reasons=reasons,
                t_number=tnum,
                vendor_key=vkey,
                category_key=cat_key,
                category_label=m.category_label,
                lexicon_quality=m.quality,
                matched_needle=m.matched_needle,
                is_learned_signal=m.is_learned_signal,
            )
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
            dec = RowDecision(
                row_index_1b=0,
                summary=summary,
                debit_before=debit_before,
                debit_after=debit_after,
                changed=(debit_after != debit_before),
                evidence_type="vendor_key",
                confidence=confidence,
                priority=priority,
                reasons=reasons,
                t_number=tnum,
                vendor_key=vkey,
                category_key=cat_key,
                category_label=m.category_label,
                lexicon_quality=m.quality,
                matched_needle=m.matched_needle,
                is_learned_signal=m.is_learned_signal,
            )
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
            dec = RowDecision(
                row_index_1b=0,
                summary=summary,
                debit_before=debit_before,
                debit_after=debit_after,
                changed=(debit_after != debit_before),
                evidence_type="category_client",
                confidence=confidence,
                priority=priority,
                reasons=reasons,
                t_number=tnum,
                vendor_key=vkey,
                category_key=cat_key,
                category_label=m.category_label,
                lexicon_quality=m.quality,
                matched_needle=m.matched_needle,
                is_learned_signal=m.is_learned_signal,
            )
            return debit_after, dec

    # (5) category default route
    if cat_key and cat_key in defaults.defaults:
        rule = defaults.defaults[cat_key]
        debit_after = rule.debit_account
        confidence = float(conf_cfg.get("default_strength", rule.confidence))
        if m.is_learned_signal:
            confidence *= float(conf_cfg.get("learned_weight_multiplier", 0.85))
        reasons = ["category_default_applied"]
        priority = rule.priority
        if m.quality == "ambiguous":
            priority = "HIGH"
            reasons.insert(0, "category_match_ambiguous")
        dec = RowDecision(
            row_index_1b=0,
            summary=summary,
            debit_before=debit_before,
            debit_after=debit_after,
            changed=(debit_after != debit_before),
            evidence_type="category_default",
            confidence=confidence,
            priority=priority,
            reasons=reasons,
            t_number=tnum,
            vendor_key=vkey,
            category_key=cat_key,
            category_label=m.category_label,
            lexicon_quality=m.quality,
            matched_needle=m.matched_needle,
            is_learned_signal=m.is_learned_signal,
        )
        return debit_after, dec

    # (6) global fallback
    gf = defaults.global_fallback
    debit_after = gf.debit_account
    confidence = float(conf_cfg.get("global_fallback_strength", gf.confidence))
    reasons = ["global_fallback_applied"]
    dec = RowDecision(
        row_index_1b=0,
        summary=summary,
        debit_before=debit_before,
        debit_after=debit_after,
        changed=(debit_after != debit_before),
        evidence_type="global_fallback",
        confidence=confidence,
        priority=gf.priority,
        reasons=reasons,
        t_number=tnum,
        vendor_key=vkey,
        category_key=cat_key,
        category_label=m.category_label,
        lexicon_quality=m.quality,
        matched_needle=m.matched_needle,
        is_learned_signal=m.is_learned_signal,
    )
    return debit_after, dec


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
) -> Dict[str, Any]:
    """
    Replace debit account for a single input CSV.
    Produces:
    - out_path: replaced CSV
    - run_dir/<artifact_prefix>_review_report.csv
    - run_dir/<artifact_prefix>_manifest.json
      (fallback to <stem> if artifact_prefix is omitted)

    Returns manifest dict.
    """
    csv = read_yayoi_csv(in_path)
    decisions: List[RowDecision] = []
    changed_count = 0
    evidence_counter: Dict[str, int] = {}

    for i, row in enumerate(csv.rows, start=1):
        summary = token_to_text(row.tokens[16], csv.encoding)
        debit_before = token_to_text(row.tokens[4], csv.encoding)

        debit_after, dec = decide_row(
            summary=summary,
            debit_before=debit_before,
            lex=lex,
            client_cache=client_cache,
            defaults=defaults,
            config=config,
        )
        dec.row_index_1b = i
        decisions.append(dec)
        evidence_counter[dec.evidence_type] = evidence_counter.get(dec.evidence_type, 0) + 1

        if debit_after != debit_before:
            changed_count += 1
            # Replace token (preserve quoting style from original token)
            row.tokens[4] = text_to_token(debit_after, csv.encoding, template_token=row.tokens[4])

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
    ]
    with report_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv_lib.writer(f, dialect="excel", lineterminator="\r\n", quoting=csv_lib.QUOTE_MINIMAL)
        writer.writerow(header)
        for d in decisions:
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
                ]
            )

    # Manifest JSON
        # Manifest JSON
    rows_with_t_number = sum(1 for d in decisions if d.t_number)
    rows_with_category = sum(1 for d in decisions if d.category_key)
    rows_using_t_routes = sum(1 for d in decisions if d.evidence_type in ("t_number", "t_number_x_category"))

    manifest = {
        "schema": "belle.replacer_run_manifest.v2",
        "version": str(config.get("version") or "1.15"),
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
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest["reports"]["manifest_json"] = str(manifest_path)

    return manifest

