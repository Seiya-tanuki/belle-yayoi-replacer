# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Ingest utilities for append-only ledgers.

Goals:
- Treat user-provided CSVs as append-only batches.
- Ensure stable filenames (do not trust manual naming).
- Avoid copies: use in-place rename only.
- Deduplicate by sha256.
- Record ingestion in a single manifest JSON.

This module is intentionally simple and deterministic.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import json
import hashlib

from .io_atomic import atomic_write_text


def now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def load_manifest(path: Path, *, client_id: str, kind: str) -> Dict[str, Any]:
    """
    kind: metadata-only logical source name (example: 'ledger_ref')
    """
    if path.exists():
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(obj, dict) and obj.get("policy"):
                return obj
        except Exception:
            pass
    obj = {
        "version": "1.0",
        "client_id": str(client_id),
        "kind": str(kind),
        "policy": {
            "mode": "append_only",
            "dedupe_key": "sha256",
            "rename_on_ingest": True,
            "rename_format": "INGESTED_{UTC_TS}_{SHA8}.csv",
            "duplicate_handling": "rename_and_ignore",
            "clock": "utc",
        },
        "ingested_order": [],
        "ingested": {},
        "ignored_duplicates": {},
    }
    return obj


def load_manifest_strict(path: Path) -> Dict[str, Any]:
    """
    Strict loader used by fail-closed workflows.

    Raises:
      - FileNotFoundError: manifest does not exist
      - ValueError: malformed JSON or invalid shape
    """
    if not path.exists():
        raise FileNotFoundError(f"ingest manifest not found: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to parse ingest manifest JSON: {path}") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"ingest manifest root must be object: {path}")
    if not isinstance(obj.get("ingested"), dict):
        raise ValueError(f"ingest manifest missing/invalid 'ingested': {path}")
    if not isinstance(obj.get("ingested_order"), list):
        raise ValueError(f"ingest manifest missing/invalid 'ingested_order': {path}")
    return obj




def save_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    _atomic_write_json(path, manifest)


def _unique_name(dir_path: Path, base_name: str) -> str:
    """
    If base_name exists, append _{n} before extension.
    """
    cand = dir_path / base_name
    if not cand.exists():
        return base_name
    stem = cand.stem
    suf = cand.suffix
    for i in range(1, 10_000):
        nn = f"{stem}_{i}{suf}"
        if not (dir_path / nn).exists():
            return nn
    raise RuntimeError(f"Could not allocate unique filename for {base_name}")


def ingest_csv_dir(
    *,
    dir_path: Path,
    manifest_path: Path,
    client_id: str,
    kind: str,
    allow_rename: bool = True,
    include_glob: str = "*.csv",
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    """
    Ingest CSV files in dir_path:
    - Computes sha256 for each *.csv
    - If new: rename -> INGESTED_{TS}_{SHA8}.csv (unless already prefixed), record in manifest.
    - If duplicate: rename -> IGNORED_DUPLICATE_{TS}_{SHA8}.csv, record in manifest, do NOT return as new.

    Returns (manifest_obj, newly_ingested_sha256_list, duplicate_sha256_list)
    """
    dir_path.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(manifest_path, client_id=client_id, kind=kind)

    ingested: Dict[str, Any] = manifest.setdefault("ingested", {})
    ignored: Dict[str, Any] = manifest.setdefault("ignored_duplicates", {})
    order: List[str] = manifest.setdefault("ingested_order", [])

    new_shas: List[str] = []
    dup_shas: List[str] = []

    for p in sorted(dir_path.glob(include_glob)):
        if not p.is_file():
            continue
        if p.name.endswith(".tmp"):
            continue

        original_name_before = p.name
        sha = sha256_file(p)
        sha8 = sha[:8].upper()
        ts = now_utc_compact()

        if sha in ingested:
            # duplicate file (same content). We ignore but can normalize name.
            dup_shas.append(sha)
            stored = p.name
            if allow_rename:
                base = f"IGNORED_DUPLICATE_{ts}_{sha8}.csv"
                base = _unique_name(dir_path, base)
                if not p.name.startswith("IGNORED_DUPLICATE_") and not p.name.startswith("INGESTED_"):
                    try:
                        p = p.rename(dir_path / base)
                        stored = p.name
                    except Exception:
                        stored = p.name

            ignored.setdefault(sha, []).append(
                {
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "original_name": original_name_before,
                    "stored_name": stored,
                    "byte_size": int(p.stat().st_size),
                    "status": "ignored_duplicate",
                }
            )
            continue

        # new batch
        stored_name = p.name
        if allow_rename and manifest.get("policy", {}).get("rename_on_ingest", True):
            # If file already starts with INGESTED_, keep it; else rename.
            if p.name.startswith("INGESTED_"):
                stored_name = p.name
            else:
                base = f"INGESTED_{ts}_{sha8}.csv"
                base = _unique_name(dir_path, base)
                try:
                    p = p.rename(dir_path / base)
                    stored_name = p.name
                except Exception:
                    stored_name = p.name

        ingested[sha] = {
            "sha256": sha,
            "sha8": sha8,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "original_name": original_name_before,
            "stored_name": stored_name,
            "byte_size": int(p.stat().st_size),
            "status": "ingested",
        }
        order.append(sha)
        new_shas.append(sha)

    _atomic_write_json(manifest_path, manifest)
    return manifest, new_shas, dup_shas


def mark_ingested_entries_processed(
    *,
    manifest_path: Path,
    sha256_list: List[str],
    processed_at: str,
    processed_run_id: Optional[str] = None,
    processed_version: Optional[str] = None,
) -> int:
    """
    Set processed markers on manifest.ingested entries.
    This write is atomic and idempotent.
    """
    if not sha256_list:
        return 0
    manifest = load_manifest_strict(manifest_path)
    ingested = manifest.get("ingested") or {}
    marked = 0
    for sha in sha256_list:
        ent = ingested.get(str(sha))
        if not isinstance(ent, dict):
            continue
        if ent.get("processed_to_label_queue_at"):
            continue
        ent["processed_to_label_queue_at"] = str(processed_at)
        if processed_run_id:
            ent["processed_to_label_queue_run_id"] = str(processed_run_id)
        if processed_version:
            ent["processed_to_label_queue_version"] = str(processed_version)
        marked += 1
    save_manifest(manifest_path, manifest)
    return marked


