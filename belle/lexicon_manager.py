# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Lexicon pending workflow (global, offline).

Current mode:
- `ledger_ref` is the only active source for candidate extraction.
- Candidate queue growth is append-only and guarded by a global lock.
- Queue apply is guarded by the same global lock to avoid lost updates.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Iterable
import copy
import csv
import io
import json
import os
import random
import re
import socket
import time
from contextlib import contextmanager
from uuid import uuid4

from .ingest import ingest_csv_dir, load_manifest_strict, mark_ingested_entries_processed
from .io_atomic import atomic_write_bytes, atomic_write_text
from .lexicon import Lexicon, match_summary
from .paths import (
    get_artifacts_telemetry_dir,
    get_client_root,
    get_label_queue_lock_path,
    get_ledger_ref_ingested_path,
)
from .text import nfkc, normalize_n0, strip_legal_forms
from .yayoi_csv import read_yayoi_csv, token_to_text


_SPLITTERS_IN_PRIORITY: List[str] = [" / ", "/", " | ", "|", " : ", "："]
_T_NUMBER_RE = re.compile(r"T\d{13}")
_DATE_LIKE_RE = re.compile(
    r"^(?:\d{4}[/-年]\d{1,2}(?:[/-月]\d{1,2}(?:日)?)?|\d{1,2}[/-]\d{1,2}(?:[/-]\d{1,2})?)$"
)
_PHONE_LIKE_RE = re.compile(r"^(?:\+?\d{1,4}[- ]?)?(?:\d{2,4}[- ]?\d{2,4}[- ]?\d{3,4})$")
_LONG_NUMERIC_RE = re.compile(r"^\d{6,}$")
_STRICT_BROAD_VERBS = {"支払", "入金", "返済", "振込"}

_STOPWORDS = {
    "カード",
    "ｶｰﾄﾞ",
    "振込",
    "ﾌﾘｺﾐ",
    "振替",
    "ｽｲﾀｲ",
    "決済",
    "ｹｯｻｲ",
    "VISA",
    "MASTER",
    "JCB",
    "AMEX",
    "DINERS",
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _strip_t_numbers(text: str) -> str:
    return _normalize_spaces(_T_NUMBER_RE.sub(" ", text))


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
    if re.fullmatch(r"[0-9]+", s):
        return True
    if len(s) < 2:
        return True
    if s in _STOPWORDS:
        return True
    return False


def _strict_candidate_skip_reason(raw: str) -> Optional[str]:
    s = _normalize_spaces(nfkc(raw))
    if not s:
        return "empty_candidate"
    if _T_NUMBER_RE.search(s):
        return "contains_t_number"
    if _LONG_NUMERIC_RE.fullmatch(s):
        return "long_numeric_id"
    if _DATE_LIKE_RE.fullmatch(s):
        return "date_like"
    if _PHONE_LIKE_RE.fullmatch(s):
        return "phone_like"
    if len(normalize_n0(s)) < 3:
        return "too_short"
    if s in _STRICT_BROAD_VERBS:
        return "broad_verb"
    if s in _STOPWORDS:
        return "stopword"
    return None


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

LABEL_QUEUE_HEARTBEAT_INTERVAL_SEC = 30


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_label_queue(path: Path) -> Dict[str, Dict[str, str]]:
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
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=LABEL_QUEUE_COLUMNS)
    writer.writeheader()

    def keyfn(item: Tuple[str, Dict[str, str]]) -> Tuple[int, str]:
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

    atomic_write_text(path, buf.getvalue(), encoding="utf-8", newline="")


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
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_pending_workspace(
    *,
    pending_dir: Path,
    queue_csv_path: Path,
    queue_state_path: Path,
    lock_path: Path,
) -> None:
    pending_dir.mkdir(parents=True, exist_ok=True)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if not queue_csv_path.exists():
        write_label_queue(queue_csv_path, {})
    if not queue_state_path.exists():
        save_label_queue_state(queue_state_path, {"version": "1.0", "clients_by_norm_key": {}})


def _read_lock_owner_id(lock_path: Path) -> str:
    try:
        obj = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    return str(obj.get("owner_id") or "")


@dataclass
class LabelQueueLockToken:
    lock_path: Path
    owner_id: str
    heartbeat_interval_sec: int = LABEL_QUEUE_HEARTBEAT_INTERVAL_SEC
    last_heartbeat_mono: float = 0.0

    def heartbeat(self, *, now_mono: Optional[float] = None) -> bool:
        if now_mono is None:
            now_mono = time.monotonic()
        if _read_lock_owner_id(self.lock_path) != self.owner_id:
            return False
        try:
            os.utime(self.lock_path, None)
        except FileNotFoundError:
            return False
        self.last_heartbeat_mono = float(now_mono)
        return True

    def maybe_heartbeat(self, *, now_mono: Optional[float] = None) -> bool:
        if now_mono is None:
            now_mono = time.monotonic()
        if (float(now_mono) - float(self.last_heartbeat_mono)) < float(self.heartbeat_interval_sec):
            return False
        return self.heartbeat(now_mono=now_mono)


def _lock_metadata(*, owner_id: str, client_id: str, timeout_sec: int, stale_after_sec: int) -> Dict[str, Any]:
    return {
        "owner_id": owner_id,
        "client_id": client_id,
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "acquired_at": now_utc_iso(),
        "timeout_sec": int(timeout_sec),
        "stale_after_sec": int(stale_after_sec),
    }


def _is_stale_lock(lock_path: Path, stale_after_sec: float) -> bool:
    try:
        mtime = lock_path.stat().st_mtime
    except FileNotFoundError:
        return False
    return (time.time() - float(mtime)) > float(stale_after_sec)


def _break_stale_lock(lock_path: Path) -> None:
    stale_name = lock_path.with_name(
        f"{lock_path.name}.stale.{int(time.time())}.{os.getpid()}.{uuid4().hex[:8]}"
    )
    try:
        lock_path = lock_path.rename(stale_name)
    except FileNotFoundError:
        return
    except OSError:
        return
    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass


def acquire_label_queue_lock(
    *,
    lock_path: Path,
    client_id: str,
    timeout_sec: int = 120,
    stale_after_sec: int = 120,
    heartbeat_interval_sec: int = LABEL_QUEUE_HEARTBEAT_INTERVAL_SEC,
) -> LabelQueueLockToken:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    owner_id = f"{client_id}:{os.getpid()}:{uuid4().hex}"
    deadline = time.time() + float(timeout_sec)
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if _is_stale_lock(lock_path, stale_after_sec):
                _break_stale_lock(lock_path)
                continue
            if time.time() >= deadline:
                raise TimeoutError(f"label queue lock timeout: {lock_path}")
            time.sleep(random.uniform(0.08, 0.35))
            continue
        try:
            metadata = _lock_metadata(
                owner_id=owner_id,
                client_id=client_id,
                timeout_sec=timeout_sec,
                stale_after_sec=stale_after_sec,
            )
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(json.dumps(metadata, ensure_ascii=False))
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                lock_path.unlink()
            except OSError:
                pass
            raise
        now_mono = time.monotonic()
        token = LabelQueueLockToken(
            lock_path=lock_path,
            owner_id=owner_id,
            heartbeat_interval_sec=max(1, int(heartbeat_interval_sec)),
            last_heartbeat_mono=float(now_mono),
        )
        token.heartbeat(now_mono=now_mono)
        return token


def release_label_queue_lock(token: LabelQueueLockToken) -> None:
    try:
        if not token.lock_path.exists():
            return
        owner_id = _read_lock_owner_id(token.lock_path)
        if owner_id and owner_id != token.owner_id:
            return
        token.lock_path.unlink()
    except FileNotFoundError:
        pass


@contextmanager
def label_queue_lock(
    *,
    lock_path: Path,
    client_id: str,
    timeout_sec: int = 120,
    stale_after_sec: int = 120,
    heartbeat_interval_sec: int = LABEL_QUEUE_HEARTBEAT_INTERVAL_SEC,
) -> Iterable[LabelQueueLockToken]:
    token = acquire_label_queue_lock(
        lock_path=lock_path,
        client_id=client_id,
        timeout_sec=timeout_sec,
        stale_after_sec=stale_after_sec,
        heartbeat_interval_sec=heartbeat_interval_sec,
    )
    try:
        yield token
    finally:
        release_label_queue_lock(token)


def _merge_terms_into_queue(
    *,
    queue: Dict[str, Dict[str, str]],
    state: Dict[str, Any],
    client_id: str,
    terms_by_norm_key: Dict[str, Dict[str, Any]],
    lock_token: Optional[LabelQueueLockToken] = None,
) -> Tuple[int, int]:
    clients_by_key: Dict[str, List[str]] = state.setdefault("clients_by_norm_key", {})
    new_keys = 0
    updated_keys = 0
    now = now_utc_iso()

    for norm_key, item in terms_by_norm_key.items():
        if lock_token is not None:
            lock_token.maybe_heartbeat()
        raw = str(item.get("raw_example") or "")
        ex_summary = str(item.get("example_summary") or "")
        suggested = str(item.get("suggested_category_key") or "")
        try:
            cnt = int(item.get("count") or 0)
        except Exception:
            cnt = 0
        if cnt <= 0:
            continue
        if not norm_key:
            continue

        if norm_key not in queue:
            queue[norm_key] = {k: "" for k in LABEL_QUEUE_COLUMNS}
            queue[norm_key]["norm_key"] = norm_key
            queue[norm_key]["raw_example"] = raw
            queue[norm_key]["example_summary"] = ex_summary
            queue[norm_key]["count_total"] = str(cnt)
            queue[norm_key]["clients_seen"] = "0"
            queue[norm_key]["first_seen_at"] = now
            queue[norm_key]["last_seen_at"] = now
            queue[norm_key]["suggested_category_key"] = suggested
            queue[norm_key]["user_category_key"] = ""
            queue[norm_key]["action"] = "HOLD"
            queue[norm_key]["notes"] = ""
            new_keys += 1
        else:
            row = queue[norm_key]
            try:
                prev = int(float(row.get("count_total") or 0))
            except Exception:
                prev = 0
            row["count_total"] = str(prev + cnt)
            row["last_seen_at"] = now
            if not (row.get("raw_example") or "").strip():
                row["raw_example"] = raw
            if not (row.get("example_summary") or "").strip():
                row["example_summary"] = ex_summary
            if not (row.get("suggested_category_key") or "").strip() and suggested:
                row["suggested_category_key"] = suggested
            updated_keys += 1

        seen_list = clients_by_key.get(norm_key) or []
        if client_id not in seen_list:
            seen_list.append(client_id)
            clients_by_key[norm_key] = seen_list
        queue[norm_key]["clients_seen"] = str(len(seen_list))

    return new_keys, updated_keys


@dataclass
class _ShaScan:
    sha256: str
    rows_scanned: int
    unclassified_rows_seen: int
    skipped_by_reason: Dict[str, int]
    terms_by_norm_key: Dict[str, Dict[str, Any]]


def _scan_ledger_ref_file_for_autogrow(
    *,
    sha256: str,
    csv_path: Path,
    lex: Lexicon,
    dummy_summary_exact: str,
) -> _ShaScan:
    csv_obj = read_yayoi_csv(csv_path)
    skipped: Counter = Counter()
    rows_scanned = 0
    unclassified_rows_seen = 0
    terms_by_norm_key: Dict[str, Dict[str, Any]] = {}

    for row in csv_obj.rows:
        rows_scanned += 1
        summary = token_to_text(row.tokens[16], csv_obj.encoding)
        if not summary:
            skipped["empty_summary"] += 1
            continue
        if summary == dummy_summary_exact:
            skipped["dummy_summary"] += 1
            continue

        summary_for_classification = _strip_t_numbers(summary)
        if not summary_for_classification:
            skipped["summary_empty_after_t_strip"] += 1
            continue

        m_summary = match_summary(lex, summary_for_classification)
        if m_summary.category_key:
            skipped["summary_already_classified"] += 1
            continue
        unclassified_rows_seen += 1

        cand = vendor_candidate_from_summary(summary_for_classification)
        cand = strip_legal_forms(cand)
        cand = _normalize_spaces(nfkc(cand))

        reason = _strict_candidate_skip_reason(cand)
        if reason:
            skipped[reason] += 1
            continue

        if match_summary(lex, cand).quality != "none":
            skipped["known_term"] += 1
            continue

        norm_key = normalize_n0(cand)
        if not norm_key:
            skipped["empty_norm_key"] += 1
            continue

        item = terms_by_norm_key.get(norm_key)
        if item is None:
            terms_by_norm_key[norm_key] = {
                "count": 1,
                "raw_example": cand,
                "example_summary": summary,
                "suggested_category_key": "",
            }
        else:
            item["count"] = int(item.get("count") or 0) + 1

    return _ShaScan(
        sha256=sha256,
        rows_scanned=rows_scanned,
        unclassified_rows_seen=unclassified_rows_seen,
        skipped_by_reason=dict(skipped),
        terms_by_norm_key=terms_by_norm_key,
    )


@dataclass
class LexiconAutogrowSummary:
    client_id: str
    processed_files: int
    processed_rows: int
    unclassified_rows_seen: int
    new_keys: int
    updated_keys: int
    skipped_by_reason: Dict[str, int]
    scanned_pending_sha_count: int
    applied_pending_sha_count: int
    warnings: List[str]


def _sum_skipped(scans: List[_ShaScan], shas: List[str]) -> Dict[str, int]:
    counter: Counter = Counter()
    by_sha = {s.sha256: s for s in scans}
    for sha in shas:
        scan = by_sha.get(sha)
        if not scan:
            continue
        counter.update(scan.skipped_by_reason)
    return dict(counter)


def _merge_scan_terms(scans: List[_ShaScan], shas: List[str]) -> Dict[str, Dict[str, Any]]:
    by_sha = {s.sha256: s for s in scans}
    merged: Dict[str, Dict[str, Any]] = {}
    for sha in shas:
        scan = by_sha.get(sha)
        if not scan:
            continue
        for norm_key, src in scan.terms_by_norm_key.items():
            if norm_key not in merged:
                merged[norm_key] = {
                    "count": int(src.get("count") or 0),
                    "raw_example": src.get("raw_example") or "",
                    "example_summary": src.get("example_summary") or "",
                    "suggested_category_key": src.get("suggested_category_key") or "",
                }
                continue
            merged[norm_key]["count"] = int(merged[norm_key].get("count") or 0) + int(src.get("count") or 0)
            if not (merged[norm_key].get("raw_example") or "").strip():
                merged[norm_key]["raw_example"] = src.get("raw_example") or ""
            if not (merged[norm_key].get("example_summary") or "").strip():
                merged[norm_key]["example_summary"] = src.get("example_summary") or ""
    return merged


def ensure_lexicon_candidates_updated_from_ledger_ref(
    *,
    repo_root: Path,
    client_id: str,
    lex: Lexicon,
    config: Dict[str, Any],
    ingest_inputs: bool = False,
    processed_run_id: Optional[str] = None,
    processed_version: Optional[str] = None,
    lock_timeout_sec: int = 120,
    lock_stale_sec: int = 120,
) -> LexiconAutogrowSummary:
    """
    Strict autogrow path used by yayoi-replacer and lexicon-extract.

    Order:
      1) scan unprocessed ledger_ref shas
      2) acquire global label_queue lock
      3) re-check manifest markers under lock
      4) queue/state write
      5) manifest marker write (same lock scope)
    """
    client_dir = get_client_root(repo_root, client_id)
    ledger_ref_dir = client_dir / "inputs" / "ledger_ref"
    manifest_path = get_ledger_ref_ingested_path(repo_root, client_id)
    pending_dir = repo_root / "lexicon" / "pending"
    queue_csv_path = pending_dir / "label_queue.csv"
    queue_state_path = pending_dir / "label_queue_state.json"
    lock_path = get_label_queue_lock_path(repo_root)
    telemetry_dir = get_artifacts_telemetry_dir(repo_root, client_id)
    telemetry_path = telemetry_dir / "lexicon_autogrow_latest.json"

    ensure_pending_workspace(
        pending_dir=pending_dir,
        queue_csv_path=queue_csv_path,
        queue_state_path=queue_state_path,
        lock_path=lock_path,
    )

    if ingest_inputs:
        ingest_csv_dir(
            dir_path=ledger_ref_dir,
            manifest_path=manifest_path,
            client_id=client_id,
            kind="ledger_ref",
            allow_rename=True,
            include_glob="*.csv",
        )
        ingest_csv_dir(
            dir_path=ledger_ref_dir,
            manifest_path=manifest_path,
            client_id=client_id,
            kind="ledger_ref",
            allow_rename=True,
            include_glob="*.txt",
        )

    manifest = load_manifest_strict(manifest_path)
    ingested = manifest.get("ingested") or {}
    ingested_order = manifest.get("ingested_order") or list(ingested.keys())
    warnings: List[str] = []
    to_scan: List[Tuple[str, Path]] = []
    for sha in ingested_order:
        ent = ingested.get(sha)
        if not isinstance(ent, dict):
            warnings.append(f"invalid_manifest_entry: sha={sha}")
            continue
        if ent.get("processed_to_label_queue_at"):
            continue
        stored_name = ent.get("stored_name")
        if not stored_name:
            warnings.append(f"missing_stored_name: sha={sha}")
            continue
        csv_path = ledger_ref_dir / str(stored_name)
        if not csv_path.exists():
            warnings.append(f"missing_ingested_file: sha={sha} expected={csv_path}")
            continue
        to_scan.append((str(sha), csv_path))

    dummy_summary = (config.get("csv_contract") or {}).get("dummy_summary_exact") or "##DUMMY_OCR_UNREADABLE##"
    scans: List[_ShaScan] = []
    for sha, csv_path in to_scan:
        scans.append(
            _scan_ledger_ref_file_for_autogrow(
                sha256=sha,
                csv_path=csv_path,
                lex=lex,
                dummy_summary_exact=dummy_summary,
            )
        )

    applied_shas: List[str] = []
    new_keys = 0
    updated_keys = 0
    with label_queue_lock(
        lock_path=lock_path,
        client_id=client_id,
        timeout_sec=int(lock_timeout_sec),
        stale_after_sec=int(lock_stale_sec),
    ) as lock_token:
        manifest_locked = load_manifest_strict(manifest_path)
        ingested_locked = manifest_locked.get("ingested") or {}

        to_apply: List[str] = []
        scan_order = [sha for sha, _ in to_scan]
        for sha in scan_order:
            lock_token.maybe_heartbeat()
            ent = ingested_locked.get(sha)
            if not isinstance(ent, dict):
                continue
            if ent.get("processed_to_label_queue_at"):
                continue
            to_apply.append(sha)

        terms_to_apply = _merge_scan_terms(scans, to_apply)
        if to_apply and terms_to_apply:
            queue = load_label_queue(queue_csv_path)
            state = load_label_queue_state(queue_state_path)
            queue_before = copy.deepcopy(queue)
            state_before = copy.deepcopy(state)

            new_keys, updated_keys = _merge_terms_into_queue(
                queue=queue,
                state=state,
                client_id=client_id,
                terms_by_norm_key=terms_to_apply,
                lock_token=lock_token,
            )
            lock_token.maybe_heartbeat()
            write_label_queue(queue_csv_path, queue)
            lock_token.maybe_heartbeat()
            save_label_queue_state(queue_state_path, state)
            try:
                lock_token.maybe_heartbeat()
                mark_ingested_entries_processed(
                    manifest_path=manifest_path,
                    sha256_list=to_apply,
                    processed_at=now_utc_iso(),
                    processed_run_id=processed_run_id,
                    processed_version=processed_version,
                )
            except Exception:
                write_label_queue(queue_csv_path, queue_before)
                save_label_queue_state(queue_state_path, state_before)
                raise
            applied_shas = to_apply
        elif to_apply:
            lock_token.maybe_heartbeat()
            mark_ingested_entries_processed(
                manifest_path=manifest_path,
                sha256_list=to_apply,
                processed_at=now_utc_iso(),
                processed_run_id=processed_run_id,
                processed_version=processed_version,
            )
            applied_shas = to_apply

    scan_by_sha = {s.sha256: s for s in scans}
    processed_rows = sum(scan_by_sha[sha].rows_scanned for sha in applied_shas if sha in scan_by_sha)
    unclassified_rows_seen = sum(
        scan_by_sha[sha].unclassified_rows_seen for sha in applied_shas if sha in scan_by_sha
    )
    skipped = _sum_skipped(scans, applied_shas)

    telemetry_dir.mkdir(parents=True, exist_ok=True)
    telemetry = {
        "schema": "belle.lexicon_autogrow.v1",
        "version": "1.0",
        "created_at": now_utc_iso(),
        "client_id": client_id,
        "processed_files": len(applied_shas),
        "processed_rows": int(processed_rows),
        "unclassified_rows_seen": int(unclassified_rows_seen),
        "new_keys": int(new_keys),
        "updated_keys": int(updated_keys),
        "skipped_by_reason": skipped,
        "scanned_pending_sha_count": len(to_scan),
        "applied_pending_sha_count": len(applied_shas),
        "warnings": warnings,
        "paths": {
            "ledger_ref_ingest_manifest": str(manifest_path),
            "label_queue_csv": str(queue_csv_path),
            "label_queue_state": str(queue_state_path),
            "label_queue_lock": str(lock_path),
        },
    }
    telemetry_path.write_text(json.dumps(telemetry, ensure_ascii=False, indent=2), encoding="utf-8")

    return LexiconAutogrowSummary(
        client_id=client_id,
        processed_files=len(applied_shas),
        processed_rows=int(processed_rows),
        unclassified_rows_seen=int(unclassified_rows_seen),
        new_keys=int(new_keys),
        updated_keys=int(updated_keys),
        skipped_by_reason=skipped,
        scanned_pending_sha_count=len(to_scan),
        applied_pending_sha_count=len(applied_shas),
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
    """
    errors: List[str] = []
    added = 0
    skipped = 0
    removed = 0

    if not queue_csv_path.exists():
        return ApplyRunSummary(added=0, skipped=0, removed_from_queue=0, errors=["label_queue.csv not found"])

    lock_path = queue_csv_path.parent / "locks" / "label_queue.lock"
    with label_queue_lock(lock_path=lock_path, client_id="lexicon-apply") as lock_token:
        lex_obj = json.loads(lexicon_path.read_text(encoding="utf-8"))
        cat_key_to_id = {c["key"]: int(c["id"]) for c in lex_obj.get("categories", [])}

        existing_by_needle: Dict[Tuple[str, str], List[int]] = {}
        for r in lex_obj.get("term_rows", []):
            lock_token.maybe_heartbeat()
            try:
                field, needle, cat_id, weight, typ = r
                key = (str(field), str(needle))
                existing_by_needle.setdefault(key, []).append(int(cat_id))
            except Exception:
                continue

        queue = load_label_queue(queue_csv_path)
        state = load_label_queue_state(queue_state_path)
        clients_by_key: Dict[str, List[str]] = state.setdefault("clients_by_norm_key", {})

        to_delete: List[str] = []
        applied_records: List[Dict[str, Any]] = []

        for norm_key, r in queue.items():
            lock_token.maybe_heartbeat()
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
                skipped += 1
                to_delete.append(norm_key)
                applied_records.append(
                    {
                        "status": "already_exists_same_category",
                        "norm_key": norm_key,
                        "category_key": user_cat,
                        "category_id": cat_id,
                        "applied_at": now_utc_iso(),
                    }
                )
                continue
            if existing_ids and cat_id not in existing_ids:
                errors.append(
                    f"conflict_existing_category: norm_key={norm_key} existing={existing_ids} requested={cat_id}"
                )
                continue

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

        lex_obj["term_buckets_prefix2"] = _rebuild_prefix2_buckets(lex_obj.get("term_rows", []))
        lock_token.maybe_heartbeat()

        lexicon_payload = json.dumps(lex_obj, ensure_ascii=False, indent=2).encode("utf-8")
        atomic_write_bytes(lexicon_path, lexicon_payload)

        for nk in to_delete:
            lock_token.maybe_heartbeat()
            if nk in queue:
                del queue[nk]
                removed += 1
            if nk in clients_by_key:
                del clients_by_key[nk]

        lock_token.maybe_heartbeat()
        write_label_queue(queue_csv_path, queue)
        lock_token.maybe_heartbeat()
        save_label_queue_state(queue_state_path, state)

        _ensure_dir(applied_log_path.parent)
        with applied_log_path.open("a", encoding="utf-8") as f:
            for rec in applied_records:
                lock_token.maybe_heartbeat()
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return ApplyRunSummary(added=added, skipped=skipped, removed_from_queue=removed, errors=errors)
