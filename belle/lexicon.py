# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import json

from .text import normalize_n0, normalize_n1


@dataclass(frozen=True)
class Category:
    id: int
    key: str
    label: str
    kind: str
    precision_hint: float
    deprecated: bool
    negative_n0: Tuple[str, ...]
    negative_n1: Tuple[str, ...]


@dataclass(frozen=True)
class TermRow:
    field: str   # n0 / n1
    needle: str  # already normalized for the field
    category_id: int
    weight: float
    type: str    # S


@dataclass(frozen=True)
class MatchResult:
    category_id: Optional[int]
    category_key: Optional[str]
    category_label: Optional[str]
    quality: str           # none / ambiguous / clear
    matched_field: Optional[str]
    matched_needle: Optional[str]
    score: float
    second_score: float
    is_learned_signal: bool


@dataclass
class Lexicon:
    schema: str
    version: str
    categories_by_id: Dict[int, Category]
    categories_by_key: Dict[str, Category]
    terms_by_field: Dict[str, List[TermRow]]
    learned_weight_threshold: float = 0.95  # weight < threshold -> learned


def load_lexicon(path: Path) -> Lexicon:
    obj = json.loads(path.read_text(encoding="utf-8"))
    cats_by_id: Dict[int, Category] = {}
    cats_by_key: Dict[str, Category] = {}

    for c in obj["categories"]:
        neg = c.get("negative_terms", {}) or {}
        # Normalize negative terms into field space
        neg_n0 = tuple(normalize_n0(x) for x in (neg.get("n0") or []))
        neg_n1 = tuple(normalize_n1(x) for x in (neg.get("n1") or []))
        cat = Category(
            id=int(c["id"]),
            key=str(c["key"]),
            label=str(c.get("label") or ""),
            kind=str(c.get("kind") or ""),
            precision_hint=float(c.get("precision_hint") or 0.5),
            deprecated=bool(c.get("deprecated") or False),
            negative_n0=neg_n0,
            negative_n1=neg_n1,
        )
        cats_by_id[cat.id] = cat
        cats_by_key[cat.key] = cat

    terms_by_field: Dict[str, List[TermRow]] = {"n0": [], "n1": []}
    for row in obj["term_rows"]:
        field, needle, cat_id, weight, typ = row
        tr = TermRow(field=str(field), needle=str(needle), category_id=int(cat_id), weight=float(weight), type=str(typ))
        if tr.field not in terms_by_field:
            continue
        terms_by_field[tr.field].append(tr)

    # Sort by longer needles first for deterministic "longest-first" behavior
    for f in terms_by_field:
        terms_by_field[f].sort(key=lambda t: (len(t.needle), t.weight), reverse=True)

    learned_policy = obj.get("learned", {}).get("policy", {}) or {}
    thr = float(learned_policy.get("core_weight", 1.0)) - 0.05  # default 0.95

    return Lexicon(
        schema=str(obj.get("schema") or "belle.lexicon.v1"),
        version=str(obj.get("version") or ""),
        categories_by_id=cats_by_id,
        categories_by_key=cats_by_key,
        terms_by_field=terms_by_field,
        learned_weight_threshold=thr,
    )


def _has_negative(cat: Category, field: str, normalized_text: str) -> bool:
    if field == "n0":
        for n in cat.negative_n0:
            if n and n in normalized_text:
                return True
    if field == "n1":
        for n in cat.negative_n1:
            if n and n in normalized_text:
                return True
    return False


def match_summary(lex: Lexicon, summary: str) -> MatchResult:
    n0 = normalize_n0(summary)
    n1 = normalize_n1(summary)

    # best per category: (score, matched_field, matched_needle, is_learned)
    best: Dict[int, Tuple[float, str, str, bool]] = {}

    def consider(field: str, norm_text: str):
        for tr in lex.terms_by_field[field]:
            if not tr.needle:
                continue
            if tr.needle not in norm_text:
                continue
            cat = lex.categories_by_id.get(tr.category_id)
            if not cat:
                continue
            if _has_negative(cat, field, norm_text):
                continue
            score = tr.weight * (len(tr.needle) / 12.0)
            is_learned = tr.weight < lex.learned_weight_threshold
            prev = best.get(tr.category_id)
            if prev is None or score > prev[0] or (score == prev[0] and len(tr.needle) > len(prev[2])):
                best[tr.category_id] = (score, field, tr.needle, is_learned)

    consider("n0", n0)
    consider("n1", n1)

    if not best:
        return MatchResult(
            category_id=None,
            category_key=None,
            category_label=None,
            quality="none",
            matched_field=None,
            matched_needle=None,
            score=0.0,
            second_score=0.0,
            is_learned_signal=False,
        )

    # Sort categories by score, then needle length, then precision_hint
    ranked = []
    for cat_id, (score, field, needle, is_learned) in best.items():
        cat = lex.categories_by_id[cat_id]
        ranked.append((score, len(needle), cat.precision_hint, cat_id, field, needle, is_learned))
    ranked.sort(reverse=True)

    top = ranked[0]
    top_score, _, _, top_cat_id, top_field, top_needle, top_is_learned = top
    second_score = ranked[1][0] if len(ranked) >= 2 else 0.0

    quality = "clear"
    if second_score > 0:
        ratio = top_score / second_score if second_score else 999.0
        if ratio <= 1.05:
            quality = "ambiguous"

    cat = lex.categories_by_id[top_cat_id]
    return MatchResult(
        category_id=top_cat_id,
        category_key=cat.key,
        category_label=cat.label,
        quality=quality,
        matched_field=top_field,
        matched_needle=top_needle,
        score=float(top_score),
        second_score=float(second_score),
        is_learned_signal=bool(top_is_learned),
    )
