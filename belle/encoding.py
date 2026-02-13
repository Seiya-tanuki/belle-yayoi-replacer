# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple, Optional

import codecs

_CANDIDATE_ENCODINGS: List[str] = [
    "cp932",        # Yayoi default (Shift-JIS family)
    "shift_jis",
    "utf-8-sig",
    "utf-8",
]

def detect_encoding(data: bytes, candidates: Optional[List[str]] = None) -> str:
    """
    Best-effort encoding detection without external deps.
    Chooses the encoding that produces the fewest replacement characters.
    """
    cands = candidates or _CANDIDATE_ENCODINGS
    best = None
    best_repl = None
    for enc in cands:
        try:
            decoded = data.decode(enc, errors="replace")
        except Exception:
            continue
        repl = decoded.count("\ufffd")
        if best is None or repl < best_repl:
            best = enc
            best_repl = repl
            if repl == 0:
                break
    return best or "cp932"

def detect_line_ending(data: bytes) -> bytes:
    # Prefer CRLF if present.
    if b"\r\n" in data:
        return b"\r\n"
    if b"\n" in data:
        return b"\n"
    if b"\r" in data:
        return b"\r"
    # default
    return b"\r\n"
