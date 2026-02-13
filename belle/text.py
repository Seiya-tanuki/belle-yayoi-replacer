# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import unicodedata
from typing import List, Optional

_T_NUMBER_RE = re.compile(r"T\d{13}")

_SPLITTERS_IN_PRIORITY: List[str] = [" / ", "/", " | ", "|", " : ", "："]

# Legal entity forms (会社種別) - aligned with legacy v1.11 ruleset
_LEGAL_FORMS: List[str] = [
    "株式会社","有限会社","合同会社","合名会社","合資会社",
    "一般財団法人","公益財団法人","一般社団法人","公益社団法人",
    "特定非営利活動法人","NPO法人","学校法人","宗教法人","医療法人","社会福祉法人",
    "独立行政法人","国立大学法人","地方独立行政法人",
    "弁護士法人","税理士法人","行政書士法人","司法書士法人","社会保険労務士法人","監査法人",
]
_ABBR_FORMS: List[str] = ["（株）","(株)","（有）","(有)"]

_MAX_STRIP_ITER = 4


def nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def normalize_n0(s: str) -> str:
    """
    Aggressive normalization (matches lexicon n0):
    - NFKC
    - uppercase
    - drop unicode categories: Z*, P*, S*, C*
    """
    s = nfkc(s).upper()
    out = []
    for ch in s:
        cat = unicodedata.category(ch)
        if cat and cat[0] in ("Z","P","S","C"):
            continue
        out.append(ch)
    return "".join(out)


def normalize_n1(s: str) -> str:
    """
    Conservative normalization (matches lexicon n1):
    - NFKC
    - uppercase
    - collapse whitespace
    - drop control chars
    - trim
    """
    s = nfkc(s).upper()
    # drop control chars
    s = "".join(ch for ch in s if unicodedata.category(ch)[0] != "C")
    # collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_t_number(summary: str) -> Optional[str]:
    m = _T_NUMBER_RE.search(summary)
    return m.group(0) if m else None


def _split_vendor_candidate(summary: str) -> str:
    s = summary
    for sp in _SPLITTERS_IN_PRIORITY:
        if sp in s:
            return s.split(sp, 1)[0]
    return s


def _strip_legal_forms_once(s: str) -> str:
    # prefix
    for p in _ABBR_FORMS + _LEGAL_FORMS:
        if s.startswith(p) and len(s) > len(p):
            return s[len(p):].strip()
    # suffix
    for p in _ABBR_FORMS + _LEGAL_FORMS:
        if s.endswith(p) and len(s) > len(p):
            return s[:-len(p)].strip()
    return s


def strip_legal_forms(name: str) -> str:
    original = name
    s = nfkc(name).strip()
    if not s:
        return original
    for _ in range(_MAX_STRIP_ITER):
        s2 = _strip_legal_forms_once(s)
        if s2 == s:
            break
        s = s2
        if not s:
            return original
    return s


def vendor_key_from_summary(summary: str) -> str:
    """
    Extract a deterministic vendor key from summary (摘要, 17th col).
    - split by conservative delimiters
    - strip legal forms at start/end iteratively
    - normalize using n0
    """
    cand = _split_vendor_candidate(summary)
    cand = strip_legal_forms(cand)
    key = normalize_n0(cand)
    if key:
        return key
    # fallback to summary itself
    return normalize_n0(summary)
