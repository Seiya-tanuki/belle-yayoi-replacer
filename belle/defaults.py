# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Any

import json


@dataclass(frozen=True)
class DefaultRule:
    debit_account: str
    confidence: float
    priority: str
    reason_code: str


@dataclass
class CategoryDefaults:
    schema: str
    version: str
    defaults: Dict[str, DefaultRule]          # category_key -> rule
    global_fallback: DefaultRule


def load_category_defaults(path: Path) -> CategoryDefaults:
    obj = json.loads(path.read_text(encoding="utf-8"))
    defs: Dict[str, DefaultRule] = {}
    for k, v in (obj.get("defaults") or {}).items():
        defs[str(k)] = DefaultRule(
            debit_account=str(v["debit_account"]),
            confidence=float(v.get("confidence") or 0.5),
            priority=str(v.get("priority") or "MED"),
            reason_code=str(v.get("reason_code") or "category_default"),
        )
    gf = obj.get("global_fallback") or {}
    global_rule = DefaultRule(
        debit_account=str(gf.get("debit_account") or "雑費"),
        confidence=float(gf.get("confidence") or 0.35),
        priority=str(gf.get("priority") or "HIGH"),
        reason_code=str(gf.get("reason_code") or "global_fallback"),
    )
    return CategoryDefaults(
        schema=str(obj.get("schema") or "belle.category_defaults.v1"),
        version=str(obj.get("version") or ""),
        defaults=defs,
        global_fallback=global_rule,
    )
