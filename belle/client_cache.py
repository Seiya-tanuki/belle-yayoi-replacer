# -*- coding: utf-8 -*-
from __future__ import annotations

"""
ClientCache is a per-client **append-only cache** derived from finalized historical ledgers (ledger_ref).

Design goals:
- Deterministic, tool-friendly JSON (no external lookups).
- Append-only growth: previously observed evidence MUST NOT disappear during updates.
- Supports strong evidence routes:
  - T-number (T\d{13}) from 摘要 (17th column)
  - T-number × category (category inferred from lexicon)
  - vendor_key from 摘要
  - category from 摘要 via lexicon

IMPORTANT INVARIANTS
- Only 摘要 (17th column) and 借方勘定科目 (5th column) are used.
- 仕訳メモ (22th column) MUST NOT be used.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import json

from .io_atomic import atomic_write_text

# ----------------------------
# StatsEntry (distribution)
# ----------------------------

@dataclass
class StatsEntry:
    sample_total: int
    top_account: Optional[str]
    top_count: int
    p_majority: float
    debit_account_counts: Dict[str, int]

    @staticmethod
    def empty() -> "StatsEntry":
        return StatsEntry(sample_total=0, top_account=None, top_count=0, p_majority=0.0, debit_account_counts={})

    @staticmethod
    def from_counts(counts: Dict[str, int]) -> "StatsEntry":
        total = int(sum(counts.values()))
        if total <= 0:
            return StatsEntry.empty()
        top_account = max(counts.items(), key=lambda kv: kv[1])[0]
        top_count = int(counts[top_account])
        return StatsEntry(
            sample_total=total,
            top_account=str(top_account),
            top_count=top_count,
            p_majority=float(top_count / total),
            debit_account_counts={str(k): int(v) for k, v in counts.items()},
        )

    @staticmethod
    def from_obj(obj: Dict[str, Any]) -> "StatsEntry":
        return StatsEntry(
            sample_total=int(obj.get("sample_total") or obj.get("total") or 0),
            top_account=obj.get("top_account") or obj.get("topAccount"),
            top_count=int(obj.get("top_count") or obj.get("topCount") or 0),
            p_majority=float(obj.get("p_majority") or obj.get("p_top") or obj.get("pTop") or 0.0),
            debit_account_counts={str(k): int(v) for k, v in (obj.get("debit_account_counts") or obj.get("accounts") or {}).items()},
        )

    def to_obj(self) -> Dict[str, Any]:
        return {
            "sample_total": self.sample_total,
            "top_account": self.top_account,
            "top_count": self.top_count,
            "p_majority": self.p_majority,
            "debit_account_counts": self.debit_account_counts,
        }

    def add_account(self, debit_account: str, n: int = 1) -> None:
        if n <= 0:
            return
        k = str(debit_account)
        self.debit_account_counts[k] = int(self.debit_account_counts.get(k, 0)) + int(n)
        self.sample_total = int(self.sample_total) + int(n)

        # Update top (append-only => no need to handle subtract)
        if self.top_account is None:
            self.top_account = k
            self.top_count = int(self.debit_account_counts[k])
        else:
            c = int(self.debit_account_counts[k])
            if c > int(self.top_count):
                self.top_account = k
                self.top_count = c
            else:
                # keep top_count in sync
                self.top_count = int(self.debit_account_counts.get(str(self.top_account), self.top_count))

        if self.sample_total > 0 and self.top_account is not None:
            self.p_majority = float(int(self.debit_account_counts.get(str(self.top_account), 0)) / self.sample_total)
        else:
            self.p_majority = 0.0


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ----------------------------
# ClientCache (append-only cache)
# ----------------------------

@dataclass
class ClientCache:
    schema: str
    version: str
    client_id: str
    created_at: str
    updated_at: str

    # Cache metadata
    append_only: bool
    applied_ledger_ref_sha256: Dict[str, Dict[str, Any]]  # sha256 -> {applied_at, stored_name, rows_total, rows_used}

    # Build/use parameters (auditable)
    decision_thresholds: Dict[str, Any]

    # Stats (distributions)
    t_numbers: Dict[str, StatsEntry]                       # T -> distribution
    t_numbers_by_category: Dict[str, Dict[str, StatsEntry]]# T -> category -> distribution
    vendor_keys: Dict[str, StatsEntry]                     # vendor_key -> distribution
    categories: Dict[str, StatsEntry]                      # category_key -> distribution
    global_stats: StatsEntry                               # global distribution

    @staticmethod
    def empty(client_id: str, *, created_at: Optional[str] = None, thresholds: Optional[Dict[str, Any]] = None) -> "ClientCache":
        now = created_at or _now_utc_iso()
        return ClientCache(
            schema="belle.client_cache.v1",
            version="1.15",
            client_id=str(client_id),
            created_at=now,
            updated_at=now,
            append_only=True,
            applied_ledger_ref_sha256={},
            decision_thresholds=thresholds or {},
            t_numbers={},
            t_numbers_by_category={},
            vendor_keys={},
            categories={},
            global_stats=StatsEntry.empty(),
        )

    @staticmethod
    def load(path: Path) -> "ClientCache":
        obj = json.loads(path.read_text(encoding="utf-8"))
        stats = obj.get("stats") or {}
        tnums = {k: StatsEntry.from_obj(v) for k, v in (stats.get("t_numbers") or {}).items()}
        vkeys = {k: StatsEntry.from_obj(v) for k, v in (stats.get("vendor_keys") or {}).items()}
        cats = {k: StatsEntry.from_obj(v) for k, v in (stats.get("categories") or {}).items()}
        t_by_cat_obj = stats.get("t_numbers_by_category") or {}
        t_by_cat: Dict[str, Dict[str, StatsEntry]] = {}
        for t, inner in t_by_cat_obj.items():
            if not isinstance(inner, dict):
                continue
            t_by_cat[str(t)] = {ck: StatsEntry.from_obj(se) for ck, se in inner.items()}

        glob = StatsEntry.from_obj(stats.get("global") or {})

        return ClientCache(
            schema=str(obj.get("schema") or "belle.client_cache.v1"),
            version=str(obj.get("version") or ""),
            client_id=str(obj.get("client_id") or ""),
            created_at=str(obj.get("created_at") or ""),
            updated_at=str(obj.get("updated_at") or obj.get("created_at") or ""),
            append_only=bool(obj.get("append_only", True)),
            applied_ledger_ref_sha256=obj.get("applied_ledger_ref_sha256") or {},
            decision_thresholds=obj.get("decision_thresholds") or {},
            t_numbers=tnums,
            t_numbers_by_category=t_by_cat,
            vendor_keys=vkeys,
            categories=cats,
            global_stats=glob,
        )

    def save(self, path: Path) -> None:
        obj = {
            "schema": self.schema,
            "version": self.version,
            "client_id": self.client_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "append_only": self.append_only,
            "applied_ledger_ref_sha256": self.applied_ledger_ref_sha256,
            "decision_thresholds": self.decision_thresholds,
            "stats": {
                "t_numbers": {k: v.to_obj() for k, v in self.t_numbers.items()},
                "t_numbers_by_category": {
                    t: {ck: se.to_obj() for ck, se in inner.items()}
                    for t, inner in self.t_numbers_by_category.items()
                },
                "vendor_keys": {k: v.to_obj() for k, v in self.vendor_keys.items()},
                "categories": {k: v.to_obj() for k, v in self.categories.items()},
                "global": self.global_stats.to_obj(),
            },
        }
        atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


