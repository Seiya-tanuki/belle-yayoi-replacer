# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Lexicon pending workflow (global, offline):

Two-phase operation is recommended:
1) $lexicon-extract
   - Scans per-client inputs/ledger_train/*.csv (append-only batches)
   - Extracts unknown vendor-like terms (not covered by current lexicon)
   - Updates lexicon/pending/label_queue.csv (append-only, cumulative counts)
   - Requires NO user decisions

2) $lexicon-apply
   - Reads lexicon/pending/label_queue.csv
   - Applies ONLY rows with action=ADD and user_category_key set
   - Updates lexicon/lexicon.json (appends learned term_rows; rebuilds buckets)
   - Removes applied rows from label_queue.csv
   - Appends an audit line to lexicon/pending/applied_log.jsonl

This design keeps lexicon stable and avoids accidental "learning pollution" while still enabling growth.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import csv
import json
import re

from .yayoi_csv import read_yayoi_csv, token_to_text
from .text import nfkc, strip_legal_forms, normalize_n0
from .lexicon import Lexicon, match_summary


# Same splitters as text.vendor_key_from_summary, but we want the raw candidate string.
_SPLITTERS_IN_PRIORITY: List[str] = [" / ", "/", " | ", "|", " : ", "："]

_T_NUMBER_RE = re.compile(r"T\d{13}")

_STOPWORDS = {
    "カード", "ｶｰﾄﾞ", "振込", "ﾌﾘｺﾐ", "振替", "ｽｲﾀｲ", "決済", "ｹｯｻｲ",
    "VISA", "MASTER", "JCB", "AMEX", "DINERS",
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def vendor_candidate_from_summary(summary: str) -> str:
    s = summary
    for sp in _SPLITTERS_IN_PRIORITY:
        if sp in s:
            return s.split(sp, 1)[0]
    return s


def is_noise_term(raw: str) -> bool:
    s = nfkc(raw).strip()
    if not s:
        return True
    if _T_NUMBER_RE.search(s):
        return True
    # digits only
    if re.fullmatch(r"[0-9]+", s):
        return True
    # very short
    if len(s) < 2:
        return True
    if s in _STOPWORDS:
        return True
    return False


LABEL_QUEUE_COLUMNS: List[str] = [
    "norm_key",
    "raw_example",
    "example_summary",
    "count_total",
    "clients_seen",
    "first_seen_at",
    "last_seen_at",
    "suggested_category_key",
    "user_category_key",
    "action",
    "notes",
]


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_label_queue(path: Path) -> Dict[str, Dict[str, str]]:
    """
    Returns mapping norm_key -> row dict (string values).
    """
    if not path.exists():
        return {}
    rows: Dict[str, Dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if not r:
                continue
            nk = (r.get("norm_key") or "").strip()
            if not nk:
                continue
            rows[nk] = {k: (r.get(k) or "") for k in LABEL_QUEUE_COLUMNS}
    return rows


def write_label_queue(path: Path, rows: Dict[str, Dict[str, str]]) -> None:
    _ensure_dir(path.parent)
    tmp = path.with_suffix(".csv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=LABEL_QUEUE_COLUMNS)
        writer.writeheader()
        # sort by count desc, then norm_key asc
        def keyfn(item):
            nk, r = item
            try:
                c = int(float(r.get("count_total") or 0))
            except Exception:
                c = 0
            return (-c, nk)
        for nk, r in sorted(rows.items(), key=keyfn):
            out = {k: (r.get(k) or "") for k in LABEL_QUEUE_COLUMNS}
            out["norm_key"] = nk
            writer.writerow(out)
    tmp.replace(path)


def load_label_queue_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": "1.0", "clients_by_norm_key": {}}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(obj, dict) and "clients_by_norm_key" in obj:
            return obj
    except Exception:
        pass
    return {"version": "1.0", "clients_by_norm_key": {}}


def save_label_queue_state(path: Path, obj: Dict[str, Any]) -> None:
    _ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


@dataclass
class ExtractRunSummary:
    client_id: str
    processed_files: int
    new_norm_keys: int
    updated_norm_keys: int
    rows_scanned: int
    terms_observed: int
    warnings: List[str]


def extract_unknown_terms_update_queue(
    *,
    client_id: str,
    ledger_train_files: List[Path],
    lex: Lexicon,
    queue_csv_path: Path,
    queue_state_path: Path,
    dummy_summary_exact: str = "##DUMMY_OCR_UNREADABLE##",
    min_count_per_run: int = 1,
) -> ExtractRunSummary:
    """
    Extract unknown terms from given ledger_train_files and merge into the global label_queue.csv.

    The queue is cumulative (append-only in meaning): counts grow over time and across clients.
    """
    warnings: List[str] = []
    queue = load_label_queue(queue_csv_path)
    state = load_label_queue_state(queue_state_path)
    clients_by_key: Dict[str, List[str]] = state.setdefault("clients_by_norm_key", {})

    rows_scanned = 0
    terms_observed = 0
    new_keys = 0
    updated_keys = 0

    counter: Counter = Counter()
    example_summary_by_raw: Dict[str, str] = {}

    for f in ledger_train_files:
        csv_obj = read_yayoi_csv(f)
        for row in csv_obj.rows:
            rows_scanned += 1
            summary = token_to_text(row.tokens[16], csv_obj.encoding)
            if not summary or summary == dummy_summary_exact:
                continue
            cand = vendor_candidate_from_summary(summary)
            cand = strip_legal_forms(cand)
            cand = nfkc(cand).strip()
            if is_noise_term(cand):
                continue

            # If the candidate already matches some category via lexicon, consider it "known enough".
            m_cand = match_summary(lex, cand)
            if m_cand.quality != "none":
                continue

            counter[cand] += 1
            if cand not in example_summary_by_raw:
                example_summary_by_raw[cand] = summary

    for raw, cnt in counter.items():
        if int(cnt) < int(min_count_per_run):
            continue
        norm_key = normalize_n0(raw)
        if not norm_key:
            continue
        terms_observed += int(cnt)

        ex_summary = example_summary_by_raw.get(raw, "")
        suggested = ""
        if ex_summary:
            m_ex = match_summary(lex, ex_summary)
            if m_ex.quality == "clear" and m_ex.category_key:
                suggested = m_ex.category_key

        now = now_utc_iso()

        if norm_key not in queue:
            queue[norm_key] = {k: "" for k in LABEL_QUEUE_COLUMNS}
            queue[norm_key]["norm_key"] = norm_key
            queue[norm_key]["raw_example"] = raw
            queue[norm_key]["example_summary"] = ex_summary
            queue[norm_key]["count_total"] = str(int(cnt))
            queue[norm_key]["clients_seen"] = "0"
            queue[norm_key]["first_seen_at"] = now
            queue[norm_key]["last_seen_at"] = now
            queue[norm_key]["suggested_category_key"] = suggested
            queue[norm_key]["user_category_key"] = ""
            queue[norm_key]["action"] = "HOLD"
            queue[norm_key]["notes"] = ""
            new_keys += 1
        else:
            # update counts and last_seen, but do not overwrite user edits
            r = queue[norm_key]
            try:
                prev = int(float(r.get("count_total") or 0))
            except Exception:
                prev = 0
            r["count_total"] = str(prev + int(cnt))
            r["last_seen_at"] = now
            # keep existing raw_example unless empty
            if not (r.get("raw_example") or "").strip():
                r["raw_example"] = raw
            if not (r.get("example_summary") or "").strip():
                r["example_summary"] = ex_summary
            # fill suggestion only if empty
            if not (r.get("suggested_category_key") or "").strip() and suggested:
                r["suggested_category_key"] = suggested
            updated_keys += 1

        # clients_seen tracking (set semantics)
        tlist = clients_by_key.get(norm_key) or []
        if client_id not in tlist:
            tlist.append(client_id)
            clients_by_key[norm_key] = tlist
        queue[norm_key]["clients_seen"] = str(len(tlist))

    # Persist only when at least one training file was processed.
    if ledger_train_files:
        write_label_queue(queue_csv_path, queue)
        save_label_queue_state(queue_state_path, state)

    return ExtractRunSummary(
        client_id=client_id,
        processed_files=len(ledger_train_files),
        new_norm_keys=new_keys,
        updated_norm_keys=updated_keys,
        rows_scanned=rows_scanned,
        terms_observed=terms_observed,
        warnings=warnings,
    )


@dataclass
class ApplyRunSummary:
    added: int
    skipped: int
    removed_from_queue: int
    errors: List[str]


def _rebuild_prefix2_buckets(term_rows: List[List[Any]]) -> Dict[str, List[int]]:
    buckets: Dict[str, List[int]] = {}
    for idx, row in enumerate(term_rows):
        try:
            field, needle, cat_id, weight, typ = row
        except Exception:
            continue
        if not isinstance(needle, str):
            continue
        p2 = needle[:2] if len(needle) >= 2 else needle
        buckets.setdefault(p2, []).append(idx)
    return buckets


def apply_label_queue_adds(
    *,
    lexicon_path: Path,
    queue_csv_path: Path,
    queue_state_path: Path,
    applied_log_path: Path,
    learned_weight: float = 0.85,
) -> ApplyRunSummary:
    """
    Apply action=ADD rows in label_queue.csv into lexicon.json (append-only term_rows),
    then remove successfully applied rows from label_queue.csv and state.

    This function is deterministic and offline.
    """
    errors: List[str] = []
    added = 0
    skipped = 0
    removed = 0

    if not queue_csv_path.exists():
        return ApplyRunSummary(added=0, skipped=0, removed_from_queue=0, errors=["label_queue.csv not found"])

    lex_obj = json.loads(lexicon_path.read_text(encoding="utf-8"))
    cat_key_to_id = {c["key"]: int(c["id"]) for c in lex_obj.get("categories", [])}

    # Build existing mapping for conflict detection
    existing_by_needle: Dict[Tuple[str, str], List[int]] = {}
    for r in lex_obj.get("term_rows", []):
        try:
            field, needle, cat_id, weight, typ = r
            key = (str(field), str(needle))
            existing_by_needle.setdefault(key, []).append(int(cat_id))
        except Exception:
            continue

    queue = load_label_queue(queue_csv_path)
    state = load_label_queue_state(queue_state_path)
    clients_by_key: Dict[str, List[str]] = state.setdefault("clients_by_norm_key", {})

    # Apply decisions
    to_delete: List[str] = []
    applied_records: List[Dict[str, Any]] = []

    for norm_key, r in queue.items():
        action = (r.get("action") or "").strip().upper()
        if action != "ADD":
            continue
        user_cat = (r.get("user_category_key") or "").strip()
        if not user_cat:
            errors.append(f"missing_user_category_key: norm_key={norm_key}")
            continue
        if user_cat not in cat_key_to_id:
            errors.append(f"unknown_category_key: {user_cat} norm_key={norm_key}")
            continue

        cat_id = cat_key_to_id[user_cat]
        key = ("n0", norm_key)
        existing_ids = existing_by_needle.get(key) or []
        if existing_ids and cat_id in existing_ids:
            # already exists with same category -> ok to remove
            skipped += 1
            to_delete.append(norm_key)
            applied_records.append({
                "status": "already_exists_same_category",
                "norm_key": norm_key,
                "category_key": user_cat,
                "category_id": cat_id,
                "applied_at": now_utc_iso(),
            })
            continue
        if existing_ids and cat_id not in existing_ids:
            # conflict: same norm_key already mapped to another category
            errors.append(f"conflict_existing_category: norm_key={norm_key} existing={existing_ids} requested={cat_id}")
            continue

        # append learned row
        lex_obj.setdefault("term_rows", []).append(["n0", norm_key, int(cat_id), float(learned_weight), "S"])
        existing_by_needle.setdefault(key, []).append(cat_id)
        added += 1
        to_delete.append(norm_key)

        reg = {
            "raw_example": r.get("raw_example") or "",
            "norm_key": norm_key,
            "category_key": user_cat,
            "category_id": int(cat_id),
            "added_at": now_utc_iso(),
            "weight": float(learned_weight),
        }
        lex_obj.setdefault("learned", {}).setdefault("provenance_registry", []).append(reg)

        applied_records.append({"status": "added", **reg})

    # Rebuild buckets (speed index)
    lex_obj["term_buckets_prefix2"] = _rebuild_prefix2_buckets(lex_obj.get("term_rows", []))

    # Atomic write lexicon
    tmp = lexicon_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(lex_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(lexicon_path)

    # Remove applied rows from queue + state
    for nk in to_delete:
        if nk in queue:
            del queue[nk]
            removed += 1
        if nk in clients_by_key:
            del clients_by_key[nk]

    write_label_queue(queue_csv_path, queue)
    save_label_queue_state(queue_state_path, state)

    # Append log
    _ensure_dir(applied_log_path.parent)
    with applied_log_path.open("a", encoding="utf-8") as f:
        for rec in applied_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return ApplyRunSummary(added=added, skipped=skipped, removed_from_queue=removed, errors=errors)

