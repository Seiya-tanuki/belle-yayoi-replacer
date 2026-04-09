# -*- coding: utf-8 -*-
from __future__ import annotations

"""
ClientCache is a per-client append-only cache derived from finalized historical ledgers (ledger_ref).

Design goals:
- Deterministic, tool-friendly JSON (no external lookups).
- Append-only growth: previously observed evidence MUST NOT disappear during updates.
- Supports receipt debit-account evidence routes and receipt debit-side tax-division evidence
  conditioned on the chosen debit account.

IMPORTANT INVARIANTS
- Only 摘要 (17th column), 借方勘定科目 (5th column), and 借方税区分 (8th column) are used.
- 仕訳メモ (22th column) MUST NOT be used.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import json

from .io_atomic import atomic_write_text

CLIENT_CACHE_SCHEMA_V2 = "belle.client_cache.v2"
CLIENT_CACHE_VERSION_V2 = "1.16"


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
    def from_obj(obj: Dict[str, Any]) -> "StatsEntry":
        return StatsEntry(
            sample_total=int(obj.get("sample_total") or obj.get("total") or 0),
            top_account=obj.get("top_account") or obj.get("topAccount"),
            top_count=int(obj.get("top_count") or obj.get("topCount") or 0),
            p_majority=float(obj.get("p_majority") or obj.get("p_top") or obj.get("pTop") or 0.0),
            debit_account_counts={
                str(k): int(v)
                for k, v in (obj.get("debit_account_counts") or obj.get("accounts") or {}).items()
            },
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
        key = str(debit_account)
        self.debit_account_counts[key] = int(self.debit_account_counts.get(key, 0)) + int(n)
        self.sample_total = int(self.sample_total) + int(n)

        if self.top_account is None:
            self.top_account = key
            self.top_count = int(self.debit_account_counts[key])
        else:
            count = int(self.debit_account_counts[key])
            if count > int(self.top_count):
                self.top_account = key
                self.top_count = count
            else:
                self.top_count = int(self.debit_account_counts.get(str(self.top_account), self.top_count))

        if self.sample_total > 0 and self.top_account is not None:
            self.p_majority = float(int(self.debit_account_counts.get(str(self.top_account), 0)) / self.sample_total)
        else:
            self.p_majority = 0.0


@dataclass
class TaxStatsEntry:
    sample_total: int
    top_tax_division: Optional[str]
    top_count: int
    p_majority: float
    tax_division_counts: Dict[str, int]

    @staticmethod
    def empty() -> "TaxStatsEntry":
        return TaxStatsEntry(
            sample_total=0,
            top_tax_division=None,
            top_count=0,
            p_majority=0.0,
            tax_division_counts={},
        )

    @staticmethod
    def from_obj(obj: Dict[str, Any]) -> "TaxStatsEntry":
        return TaxStatsEntry(
            sample_total=int(obj.get("sample_total") or obj.get("total") or 0),
            top_tax_division=obj.get("top_tax_division") or obj.get("top_value"),
            top_count=int(obj.get("top_count") or obj.get("topCount") or 0),
            p_majority=float(obj.get("p_majority") or obj.get("p_top") or obj.get("pTop") or 0.0),
            tax_division_counts={
                str(k): int(v)
                for k, v in (obj.get("tax_division_counts") or obj.get("value_counts") or {}).items()
            },
        )

    def to_obj(self) -> Dict[str, Any]:
        return {
            "sample_total": self.sample_total,
            "top_tax_division": self.top_tax_division,
            "top_count": self.top_count,
            "p_majority": self.p_majority,
            "tax_division_counts": self.tax_division_counts,
        }

    def add_tax_division(self, tax_division: str, n: int = 1) -> None:
        if n <= 0:
            return
        key = str(tax_division)
        self.tax_division_counts[key] = int(self.tax_division_counts.get(key, 0)) + int(n)
        self.sample_total = int(self.sample_total) + int(n)

        if self.top_tax_division is None:
            self.top_tax_division = key
            self.top_count = int(self.tax_division_counts[key])
        else:
            count = int(self.tax_division_counts[key])
            if count > int(self.top_count):
                self.top_tax_division = key
                self.top_count = count
            else:
                self.top_count = int(self.tax_division_counts.get(str(self.top_tax_division), self.top_count))

        if self.sample_total > 0 and self.top_tax_division is not None:
            self.p_majority = float(int(self.tax_division_counts.get(str(self.top_tax_division), 0)) / self.sample_total)
        else:
            self.p_majority = 0.0


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_stats_map(obj: Any) -> Dict[str, StatsEntry]:
    src = obj if isinstance(obj, dict) else {}
    return {str(k): StatsEntry.from_obj(v) for k, v in src.items()}


def _load_nested_stats_map(obj: Any) -> Dict[str, Dict[str, StatsEntry]]:
    src = obj if isinstance(obj, dict) else {}
    outer: Dict[str, Dict[str, StatsEntry]] = {}
    for key, inner in src.items():
        if not isinstance(inner, dict):
            continue
        outer[str(key)] = {str(inner_key): StatsEntry.from_obj(value) for inner_key, value in inner.items()}
    return outer


def _load_tax_stats_map(obj: Any) -> Dict[str, TaxStatsEntry]:
    src = obj if isinstance(obj, dict) else {}
    return {str(k): TaxStatsEntry.from_obj(v) for k, v in src.items()}


def _load_nested_tax_stats_map(obj: Any) -> Dict[str, Dict[str, TaxStatsEntry]]:
    src = obj if isinstance(obj, dict) else {}
    outer: Dict[str, Dict[str, TaxStatsEntry]] = {}
    for key, inner in src.items():
        if not isinstance(inner, dict):
            continue
        outer[str(key)] = {str(inner_key): TaxStatsEntry.from_obj(value) for inner_key, value in inner.items()}
    return outer


def _load_t_category_account_tax_stats_map(obj: Any) -> Dict[str, Dict[str, Dict[str, TaxStatsEntry]]]:
    src = obj if isinstance(obj, dict) else {}
    outer: Dict[str, Dict[str, Dict[str, TaxStatsEntry]]] = {}
    for t_number, category_map in src.items():
        if not isinstance(category_map, dict):
            continue
        outer[str(t_number)] = {}
        for category_key, account_map in category_map.items():
            if not isinstance(account_map, dict):
                continue
            outer[str(t_number)][str(category_key)] = {
                str(account): TaxStatsEntry.from_obj(value)
                for account, value in account_map.items()
            }
    return outer


@dataclass
class ClientCache:
    schema: str
    version: str
    client_id: str
    created_at: str
    updated_at: str
    append_only: bool
    applied_ledger_ref_sha256: Dict[str, Dict[str, Any]]
    decision_thresholds: Dict[str, Any]
    t_numbers: Dict[str, StatsEntry]
    t_numbers_by_category: Dict[str, Dict[str, StatsEntry]]
    vendor_keys: Dict[str, StatsEntry]
    categories: Dict[str, StatsEntry]
    global_stats: StatsEntry
    tax_t_numbers_by_category_and_account: Dict[str, Dict[str, Dict[str, TaxStatsEntry]]]
    tax_t_numbers_by_account: Dict[str, Dict[str, TaxStatsEntry]]
    tax_vendor_keys_by_account: Dict[str, Dict[str, TaxStatsEntry]]
    tax_categories_by_account: Dict[str, Dict[str, TaxStatsEntry]]
    tax_global_by_account: Dict[str, TaxStatsEntry]

    @staticmethod
    def empty(client_id: str, *, created_at: Optional[str] = None, thresholds: Optional[Dict[str, Any]] = None) -> "ClientCache":
        now = created_at or _now_utc_iso()
        return ClientCache(
            schema=CLIENT_CACHE_SCHEMA_V2,
            version=CLIENT_CACHE_VERSION_V2,
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
            tax_t_numbers_by_category_and_account={},
            tax_t_numbers_by_account={},
            tax_vendor_keys_by_account={},
            tax_categories_by_account={},
            tax_global_by_account={},
        )

    @staticmethod
    def load(path: Path) -> "ClientCache":
        obj = json.loads(path.read_text(encoding="utf-8"))
        stats = obj.get("stats") or {}
        tax_stats = obj.get("tax_stats") or {}

        return ClientCache(
            schema=str(obj.get("schema") or CLIENT_CACHE_SCHEMA_V2),
            version=str(obj.get("version") or ""),
            client_id=str(obj.get("client_id") or ""),
            created_at=str(obj.get("created_at") or ""),
            updated_at=str(obj.get("updated_at") or obj.get("created_at") or ""),
            append_only=bool(obj.get("append_only", True)),
            applied_ledger_ref_sha256=obj.get("applied_ledger_ref_sha256") or {},
            decision_thresholds=obj.get("decision_thresholds") or {},
            t_numbers=_load_stats_map(stats.get("t_numbers")),
            t_numbers_by_category=_load_nested_stats_map(stats.get("t_numbers_by_category")),
            vendor_keys=_load_stats_map(stats.get("vendor_keys")),
            categories=_load_stats_map(stats.get("categories")),
            global_stats=StatsEntry.from_obj(stats.get("global") or {}),
            tax_t_numbers_by_category_and_account=_load_t_category_account_tax_stats_map(
                tax_stats.get("t_numbers_by_category_and_account")
            ),
            tax_t_numbers_by_account=_load_nested_tax_stats_map(tax_stats.get("t_numbers_by_account")),
            tax_vendor_keys_by_account=_load_nested_tax_stats_map(tax_stats.get("vendor_keys_by_account")),
            tax_categories_by_account=_load_nested_tax_stats_map(tax_stats.get("categories_by_account")),
            tax_global_by_account=_load_tax_stats_map(tax_stats.get("global_by_account")),
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
                    t_number: {category_key: value.to_obj() for category_key, value in inner.items()}
                    for t_number, inner in self.t_numbers_by_category.items()
                },
                "vendor_keys": {k: v.to_obj() for k, v in self.vendor_keys.items()},
                "categories": {k: v.to_obj() for k, v in self.categories.items()},
                "global": self.global_stats.to_obj(),
            },
            "tax_stats": {
                "t_numbers_by_category_and_account": {
                    t_number: {
                        category_key: {account: value.to_obj() for account, value in account_map.items()}
                        for category_key, account_map in category_map.items()
                    }
                    for t_number, category_map in self.tax_t_numbers_by_category_and_account.items()
                },
                "t_numbers_by_account": {
                    key: {account: value.to_obj() for account, value in inner.items()}
                    for key, inner in self.tax_t_numbers_by_account.items()
                },
                "vendor_keys_by_account": {
                    key: {account: value.to_obj() for account, value in inner.items()}
                    for key, inner in self.tax_vendor_keys_by_account.items()
                },
                "categories_by_account": {
                    key: {account: value.to_obj() for account, value in inner.items()}
                    for key, inner in self.tax_categories_by_account.items()
                },
                "global_by_account": {account: value.to_obj() for account, value in self.tax_global_by_account.items()},
            },
        }
        atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
