# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json

from .io_atomic import atomic_write_text

SCHEMA_BANK_CLIENT_CACHE_V0 = "belle.bank_client_cache.v0"
LINE_ID_BANK_STATEMENT = "bank_statement"
ROUTE_KANA_SIGN_AMOUNT = "kana_sign_amount"
ROUTE_KANA_SIGN = "kana_sign"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_bank_label_id(
    corrected_summary: str,
    counter_account: str,
    counter_subaccount: str,
    counter_tax_division: str,
) -> str:
    payload = (
        f"{corrected_summary}\u241E{counter_account}\u241E"
        f"{counter_subaccount}\u241E{counter_tax_division}"
    )
    return "L" + sha256(payload.encode("utf-8")).hexdigest()[:12]


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
class LabelStatsEntry:
    sample_total: int
    top_label_id: Optional[str]
    top_count: int
    p_majority: float
    label_counts: Dict[str, int]

    @staticmethod
    def empty() -> "LabelStatsEntry":
        return LabelStatsEntry(
            sample_total=0,
            top_label_id=None,
            top_count=0,
            p_majority=0.0,
            label_counts={},
        )

    @staticmethod
    def from_obj(obj: Dict[str, Any]) -> "LabelStatsEntry":
        raw_counts = obj.get("label_counts")
        if not isinstance(raw_counts, dict):
            raw_counts = obj.get("labels") or {}
        counts = {str(k): _as_int(v) for k, v in (raw_counts or {}).items()}
        sample_total = _as_int(obj.get("sample_total"), default=sum(counts.values()))
        top_label_id = obj.get("top_label_id") or obj.get("top_label")
        top_count = _as_int(obj.get("top_count"), default=counts.get(str(top_label_id), 0))
        p_majority = _as_float(obj.get("p_majority"), default=0.0)
        return LabelStatsEntry(
            sample_total=sample_total,
            top_label_id=str(top_label_id) if top_label_id else None,
            top_count=top_count,
            p_majority=p_majority,
            label_counts=counts,
        )

    def to_obj(self) -> Dict[str, Any]:
        return {
            "sample_total": int(self.sample_total),
            "top_label_id": self.top_label_id,
            "top_count": int(self.top_count),
            "p_majority": float(self.p_majority),
            "label_counts": {str(k): int(v) for k, v in self.label_counts.items()},
        }

    def add_label(self, label_id: str, n: int = 1) -> None:
        if n <= 0:
            return
        lid = str(label_id)
        self.label_counts[lid] = int(self.label_counts.get(lid, 0)) + int(n)
        self.sample_total = int(self.sample_total) + int(n)

        top_id, top_count = self._compute_top()
        self.top_label_id = top_id
        self.top_count = top_count
        if self.sample_total > 0 and self.top_count > 0:
            self.p_majority = float(self.top_count / self.sample_total)
        else:
            self.p_majority = 0.0

    def _compute_top(self) -> Tuple[Optional[str], int]:
        if not self.label_counts:
            return None, 0
        # Deterministic tie-break for cache serialization.
        top_id, top_count = min(
            ((lid, int(cnt)) for lid, cnt in self.label_counts.items()),
            key=lambda kv: (-kv[1], kv[0]),
        )
        return str(top_id), int(top_count)


@dataclass
class BankLabel:
    corrected_summary: str
    counter_account: str
    counter_subaccount: str
    counter_tax_division: str
    first_seen_at: str
    last_seen_at: str
    count_total: int = 0
    examples: List[Dict[str, Any]] = field(default_factory=list)

    @staticmethod
    def from_obj(obj: Dict[str, Any]) -> "BankLabel":
        first_seen_at = str(
            obj.get("first_seen_at")
            or obj.get("created_at")
            or ""
        )
        last_seen_at = str(
            obj.get("last_seen_at")
            or obj.get("updated_at")
            or first_seen_at
        )
        raw_examples = obj.get("examples")
        examples = raw_examples if isinstance(raw_examples, list) else []
        return BankLabel(
            corrected_summary=str(obj.get("corrected_summary") or ""),
            counter_account=str(obj.get("counter_account") or ""),
            counter_subaccount=str(obj.get("counter_subaccount") or ""),
            counter_tax_division=str(obj.get("counter_tax_division") or ""),
            first_seen_at=first_seen_at,
            last_seen_at=last_seen_at,
            count_total=_as_int(obj.get("count_total"), default=0),
            examples=[e for e in examples if isinstance(e, dict)],
        )

    def to_obj(self) -> Dict[str, Any]:
        obj: Dict[str, Any] = {
            "corrected_summary": self.corrected_summary,
            "counter_account": self.counter_account,
            "counter_subaccount": self.counter_subaccount,
            "counter_tax_division": self.counter_tax_division,
            "first_seen_at": self.first_seen_at,
            "last_seen_at": self.last_seen_at,
            "count_total": int(self.count_total),
        }
        if self.examples:
            obj["examples"] = self.examples
        return obj


@dataclass
class BankClientCache:
    schema: str
    version: str
    client_id: str
    line_id: str
    created_at: str
    updated_at: str
    append_only: bool
    decision_thresholds: Dict[str, Any]
    applied_training_sets: Dict[str, Dict[str, Any]]
    labels: Dict[str, BankLabel]
    stats: Dict[str, Dict[str, LabelStatsEntry]]

    @staticmethod
    def empty(
        client_id: str,
        *,
        line_id: str = LINE_ID_BANK_STATEMENT,
        created_at: Optional[str] = None,
        thresholds: Optional[Dict[str, Any]] = None,
    ) -> "BankClientCache":
        now = created_at or _now_utc_iso()
        return BankClientCache(
            schema=SCHEMA_BANK_CLIENT_CACHE_V0,
            version="0.1",
            client_id=str(client_id),
            line_id=str(line_id or LINE_ID_BANK_STATEMENT),
            created_at=now,
            updated_at=now,
            append_only=True,
            decision_thresholds=thresholds or {},
            applied_training_sets={},
            labels={},
            stats={
                ROUTE_KANA_SIGN_AMOUNT: {},
                ROUTE_KANA_SIGN: {},
            },
        )

    @staticmethod
    def from_obj(obj: Dict[str, Any]) -> "BankClientCache":
        labels_obj = obj.get("labels")
        if not isinstance(labels_obj, dict):
            labels_obj = obj.get("label_dictionary") or {}

        raw_stats = obj.get("stats") if isinstance(obj.get("stats"), dict) else {}
        strong_obj = raw_stats.get(ROUTE_KANA_SIGN_AMOUNT)
        if not isinstance(strong_obj, dict):
            strong_obj = raw_stats.get("strong_by_kana_sign_amount") or {}
        weak_obj = raw_stats.get(ROUTE_KANA_SIGN)
        if not isinstance(weak_obj, dict):
            weak_obj = raw_stats.get("weak_by_kana_sign") or {}

        stats = {
            ROUTE_KANA_SIGN_AMOUNT: {
                str(k): LabelStatsEntry.from_obj(v if isinstance(v, dict) else {})
                for k, v in strong_obj.items()
            },
            ROUTE_KANA_SIGN: {
                str(k): LabelStatsEntry.from_obj(v if isinstance(v, dict) else {})
                for k, v in weak_obj.items()
            },
        }

        applied = obj.get("applied_training_sets")
        if not isinstance(applied, dict):
            applied = {}

        thresholds = obj.get("decision_thresholds")
        if not isinstance(thresholds, dict):
            thresholds = {}

        created_at = str(obj.get("created_at") or "")
        updated_at = str(obj.get("updated_at") or created_at)

        return BankClientCache(
            schema=str(obj.get("schema") or SCHEMA_BANK_CLIENT_CACHE_V0),
            version=str(obj.get("version") or "0.1"),
            client_id=str(obj.get("client_id") or ""),
            line_id=str(obj.get("line_id") or LINE_ID_BANK_STATEMENT),
            created_at=created_at,
            updated_at=updated_at,
            append_only=bool(obj.get("append_only", True)),
            decision_thresholds=thresholds,
            applied_training_sets={
                str(k): (v if isinstance(v, dict) else {})
                for k, v in applied.items()
            },
            labels={
                str(label_id): BankLabel.from_obj(item if isinstance(item, dict) else {})
                for label_id, item in labels_obj.items()
            },
            stats=stats,
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
            "applied_training_sets": self.applied_training_sets,
            "labels": {k: v.to_obj() for k, v in self.labels.items()},
            "stats": {
                ROUTE_KANA_SIGN_AMOUNT: {
                    k: v.to_obj() for k, v in self.stats.get(ROUTE_KANA_SIGN_AMOUNT, {}).items()
                },
                ROUTE_KANA_SIGN: {
                    k: v.to_obj() for k, v in self.stats.get(ROUTE_KANA_SIGN, {}).items()
                },
            },
        }


def _infer_client_line_from_cache_path(path: Path) -> Tuple[str, str]:
    client_id = ""
    line_id = LINE_ID_BANK_STATEMENT
    try:
        parents = path.parents
        if len(parents) >= 5 and parents[3].name == "lines":
            line_id = parents[2].name or line_id
            client_id = parents[4].name or client_id
    except Exception:
        pass
    return client_id, line_id


def load_bank_cache(path: Path) -> BankClientCache:
    if not path.exists():
        client_id, line_id = _infer_client_line_from_cache_path(path)
        return BankClientCache.empty(client_id=client_id, line_id=line_id)
    obj = json.loads(path.read_text(encoding="utf-8"))
    return BankClientCache.from_obj(obj if isinstance(obj, dict) else {})


def save_bank_cache(path: Path, cache: BankClientCache) -> None:
    atomic_write_text(
        path,
        json.dumps(cache.to_obj(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
