# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import json

from .client_cache import StatsEntry
from .io_atomic import atomic_write_text

SCHEMA_CC_CLIENT_CACHE_V1 = "belle.cc_client_cache.v1"
CC_CLIENT_CACHE_VERSION_V1 = "0.2"
LINE_ID_CC = "credit_card_statement"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


@dataclass
class ValueStatsEntry:
    sample_total: int
    top_value: Optional[str]
    top_count: int
    p_majority: float
    value_counts: Dict[str, int]

    @staticmethod
    def empty() -> "ValueStatsEntry":
        return ValueStatsEntry(
            sample_total=0,
            top_value=None,
            top_count=0,
            p_majority=0.0,
            value_counts={},
        )

    @staticmethod
    def from_obj(obj: Dict[str, Any]) -> "ValueStatsEntry":
        raw_counts = obj.get("value_counts")
        if not isinstance(raw_counts, dict):
            raw_counts = obj.get("values") or {}
        counts = {str(k): _as_int(v) for k, v in (raw_counts or {}).items()}

        sample_total = _as_int(obj.get("sample_total"), default=sum(counts.values()))
        top_value_raw = obj.get("top_value")
        if top_value_raw is None:
            top_value_raw = obj.get("top")
        top_value = str(top_value_raw) if top_value_raw not in (None, "") else None
        top_count = _as_int(obj.get("top_count"), default=counts.get(str(top_value), 0))
        p_majority = _as_float(obj.get("p_majority"), default=0.0)
        return ValueStatsEntry(
            sample_total=sample_total,
            top_value=top_value,
            top_count=top_count,
            p_majority=p_majority,
            value_counts=counts,
        )

    def to_obj(self) -> Dict[str, Any]:
        return {
            "sample_total": int(self.sample_total),
            "top_value": self.top_value,
            "top_count": int(self.top_count),
            "p_majority": float(self.p_majority),
            "value_counts": {str(k): int(v) for k, v in self.value_counts.items()},
        }

    def update(self, value: str, n: int = 1) -> None:
        if n <= 0:
            return
        v = str(value)
        self.value_counts[v] = int(self.value_counts.get(v, 0)) + int(n)
        self.sample_total = int(self.sample_total) + int(n)

        top_value, top_count = self._compute_top()
        self.top_value = top_value
        self.top_count = top_count
        if self.sample_total > 0 and self.top_count > 0:
            self.p_majority = float(self.top_count / self.sample_total)
        else:
            self.p_majority = 0.0

    def _compute_top(self) -> Tuple[Optional[str], int]:
        if not self.value_counts:
            return None, 0
        # Deterministic tie-break for cache serialization.
        top_value, top_count = min(
            ((value, int(cnt)) for value, cnt in self.value_counts.items()),
            key=lambda kv: (-kv[1], kv[0]),
        )
        return str(top_value), int(top_count)


@dataclass
class CCClientCache:
    schema: str
    version: str
    client_id: str
    line_id: str
    created_at: str
    updated_at: str
    append_only: bool
    decision_thresholds: Dict[str, Any]
    applied_ledger_ref_sha256: Dict[str, Dict[str, Any]]
    card_subaccount_candidates: Dict[str, Dict[str, Any]]
    merchant_key_account_stats: Dict[str, StatsEntry]
    merchant_key_payable_sub_stats: Dict[str, ValueStatsEntry]
    merchant_key_target_account_tax_stats: Dict[str, Dict[str, ValueStatsEntry]]
    payable_sub_global_stats: ValueStatsEntry

    @staticmethod
    def empty(
        client_id: str,
        thresholds: Optional[Dict[str, Any]] = None,
        *,
        created_at: Optional[str] = None,
    ) -> "CCClientCache":
        now = created_at or _now_utc_iso()
        return CCClientCache(
            schema=SCHEMA_CC_CLIENT_CACHE_V1,
            version=CC_CLIENT_CACHE_VERSION_V1,
            client_id=str(client_id),
            line_id=LINE_ID_CC,
            created_at=now,
            updated_at=now,
            append_only=True,
            decision_thresholds=thresholds or {},
            applied_ledger_ref_sha256={},
            card_subaccount_candidates={},
            merchant_key_account_stats={},
            merchant_key_payable_sub_stats={},
            merchant_key_target_account_tax_stats={},
            payable_sub_global_stats=ValueStatsEntry.empty(),
        )

    @staticmethod
    def from_obj(obj: Dict[str, Any]) -> "CCClientCache":
        thresholds = obj.get("decision_thresholds")
        if not isinstance(thresholds, dict):
            thresholds = {}

        applied_raw = obj.get("applied_ledger_ref_sha256")
        applied: Dict[str, Dict[str, Any]] = {}
        if isinstance(applied_raw, dict):
            applied = {
                str(k): (v if isinstance(v, dict) else {})
                for k, v in applied_raw.items()
            }
        elif isinstance(applied_raw, list):
            for v in applied_raw:
                sha = str(v or "").strip()
                if sha:
                    applied[sha] = {}

        candidates_raw = obj.get("card_subaccount_candidates")
        if not isinstance(candidates_raw, dict):
            candidates_raw = {}
        candidates = {
            str(k): (v if isinstance(v, dict) else {})
            for k, v in candidates_raw.items()
        }

        merchant_account_raw = obj.get("merchant_key_account_stats")
        if not isinstance(merchant_account_raw, dict):
            merchant_account_raw = {}
        merchant_key_account_stats = {
            str(k): StatsEntry.from_obj(v if isinstance(v, dict) else {})
            for k, v in merchant_account_raw.items()
        }

        payable_sub_raw = obj.get("merchant_key_payable_sub_stats")
        if not isinstance(payable_sub_raw, dict):
            payable_sub_raw = {}
        merchant_key_payable_sub_stats = {
            str(k): ValueStatsEntry.from_obj(v if isinstance(v, dict) else {})
            for k, v in payable_sub_raw.items()
        }

        tax_stats_raw = obj.get("merchant_key_target_account_tax_stats")
        if not isinstance(tax_stats_raw, dict):
            tax_stats_raw = {}
        merchant_key_target_account_tax_stats: Dict[str, Dict[str, ValueStatsEntry]] = {}
        for merchant_key, account_map in tax_stats_raw.items():
            if not isinstance(account_map, dict):
                continue
            merchant_key_target_account_tax_stats[str(merchant_key)] = {
                str(account): ValueStatsEntry.from_obj(value if isinstance(value, dict) else {})
                for account, value in account_map.items()
            }

        global_stats_obj = obj.get("payable_sub_global_stats")
        if not isinstance(global_stats_obj, dict):
            global_stats_obj = {}

        created_at = str(obj.get("created_at") or "")
        updated_at = str(obj.get("updated_at") or created_at)

        return CCClientCache(
            schema=str(obj.get("schema") or SCHEMA_CC_CLIENT_CACHE_V1),
            version=str(obj.get("version") or CC_CLIENT_CACHE_VERSION_V1),
            client_id=str(obj.get("client_id") or ""),
            line_id=str(obj.get("line_id") or LINE_ID_CC),
            created_at=created_at,
            updated_at=updated_at,
            append_only=bool(obj.get("append_only", True)),
            decision_thresholds=thresholds,
            applied_ledger_ref_sha256=applied,
            card_subaccount_candidates=candidates,
            merchant_key_account_stats=merchant_key_account_stats,
            merchant_key_payable_sub_stats=merchant_key_payable_sub_stats,
            merchant_key_target_account_tax_stats=merchant_key_target_account_tax_stats,
            payable_sub_global_stats=ValueStatsEntry.from_obj(global_stats_obj),
        )

    def to_obj(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "version": self.version,
            "client_id": self.client_id,
            "line_id": self.line_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "append_only": bool(self.append_only),
            "decision_thresholds": self.decision_thresholds,
            "applied_ledger_ref_sha256": self.applied_ledger_ref_sha256,
            "card_subaccount_candidates": self.card_subaccount_candidates,
            "merchant_key_account_stats": {
                k: v.to_obj() for k, v in self.merchant_key_account_stats.items()
            },
            "merchant_key_payable_sub_stats": {
                k: v.to_obj() for k, v in self.merchant_key_payable_sub_stats.items()
            },
            "merchant_key_target_account_tax_stats": {
                merchant_key: {
                    account: stats.to_obj()
                    for account, stats in account_map.items()
                }
                for merchant_key, account_map in self.merchant_key_target_account_tax_stats.items()
            },
            "payable_sub_global_stats": self.payable_sub_global_stats.to_obj(),
        }

    @staticmethod
    def load(path: Path) -> "CCClientCache":
        if not path.exists():
            return CCClientCache.empty(client_id=_infer_client_id_from_cache_path(path))
        obj = json.loads(path.read_text(encoding="utf-8"))
        return CCClientCache.from_obj(obj if isinstance(obj, dict) else {})

    def save(self, path: Path) -> None:
        atomic_write_text(
            path,
            json.dumps(self.to_obj(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _infer_client_id_from_cache_path(path: Path) -> str:
    try:
        parents = path.parents
        if len(parents) >= 5 and parents[3].name == "lines" and parents[2].name == LINE_ID_CC:
            return str(parents[4].name or "")
    except Exception:
        pass
    return ""
