# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Ingest utilities for append-only ledgers.

Goals:
- Treat user-provided CSVs as append-only batches.
- Ensure stable filenames (do not trust manual naming).
- Avoid copies: use move semantics only.
- Deduplicate by sha256.
- Record ingestion in a single manifest JSON.

This module is intentionally simple and deterministic.
"""

from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple, Optional
import json
import shutil

from belle.fs_utils import sha256_file_chunked

from .io_atomic import atomic_write_text


def now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path) -> str:
    return sha256_file_chunked(path)


def _normalize_allowed_extensions(allowed_extensions: Iterable[str]) -> set[str]:
    normalized: set[str] = set()
    for ext in allowed_extensions:
        text = str(ext or "").strip().lower()
        if not text:
            continue
        if not text.startswith("."):
            text = f".{text}"
        normalized.add(text)
    if not normalized:
        raise ValueError("allowed_extensions must not be empty")
    return normalized


def list_discoverable_files(
    dir_path: Path,
    *,
    allowed_extensions: Optional[Iterable[str]] = None,
) -> List[Path]:
    if not dir_path.exists():
        return []

    normalized_exts: Optional[set[str]] = None
    if allowed_extensions is not None:
        normalized_exts = _normalize_allowed_extensions(allowed_extensions)

    files: List[Path] = []
    for p in sorted(dir_path.iterdir(), key=lambda v: v.name):
        if not p.is_file():
            continue
        if p.name == ".gitkeep":
            continue
        if p.name.endswith(".tmp"):
            continue
        if normalized_exts is not None and p.suffix.lower() not in normalized_exts:
            continue
        files.append(p)
    return files


def _allowed_extensions_from_include_glob(include_glob: str) -> tuple[str, ...]:
    normalized = str(include_glob or "").strip().lower()
    if normalized == "*.csv":
        return (".csv",)
    if normalized == "*.txt":
        return (".txt",)
    raise ValueError(f"unsupported include_glob for ingest_csv_dir: {include_glob}")


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


def _move_with_fallback(src: Path, dst: Path) -> Path:
    """
    Move file with rename semantics.
    - First try atomic/fast replace (same volume).
    - Fall back to shutil.move for cross-volume moves.
    """
    try:
        src.replace(dst)
        return dst
    except OSError:
        moved = shutil.move(str(src), str(dst))
        return Path(moved)


def _count_rows_observed(path: Path) -> int:
    """
    Prefer Yayoi CSV row counting; fall back to non-empty physical lines.
    """
    try:
        from .yayoi_csv import read_yayoi_csv

        return int(len(read_yayoi_csv(path).rows))
    except Exception:
        rows = 0
        with path.open("rb") as f:
            for raw in f:
                if raw.strip():
                    rows += 1
        return rows


def _to_relpath(stored_path: Path, *, relpath_base_dir: Optional[Path]) -> str:
    if relpath_base_dir is not None:
        try:
            return stored_path.relative_to(relpath_base_dir).as_posix()
        except ValueError:
            pass
    return stored_path.name


@dataclass
class SingleFileIngestResult:
    sha256: str
    sha8: str
    original_name: str
    stored_name: str
    ingested_at: str
    byte_size: int
    rows_observed: int
    stored_path: Path
    status: str


def ingest_single_file(
    *,
    source_path: Path,
    store_dir: Path,
    manifest_path: Path,
    client_id: str,
    kind: str,
    manifest_schema: Optional[str] = None,
) -> Tuple[Dict[str, Any], SingleFileIngestResult]:
    """
    Ingest exactly one file by sha256 into store_dir.

    Behavior:
    - New sha: move+rename to INGESTED_{UTC_TS}_{SHA8}.csv and append manifest.ingested.
    - Existing sha: move+rename to IGNORED_DUPLICATE_{UTC_TS}_{SHA8}.csv and append
      manifest.ignored_duplicates[sha], while returning the canonical stored entry.
    """
    if not source_path.exists() or not source_path.is_file():
        raise FileNotFoundError(f"ingest source file not found: {source_path}")

    store_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(manifest_path, client_id=client_id, kind=kind)
    if manifest_schema:
        manifest["schema"] = str(manifest_schema)

    ingested: Dict[str, Any] = manifest.setdefault("ingested", {})
    ignored: Dict[str, Any] = manifest.setdefault("ignored_duplicates", {})
    order: List[str] = manifest.setdefault("ingested_order", [])

    original_name = source_path.name
    sha = sha256_file(source_path)
    sha8 = sha[:8].upper()
    ts = now_utc_compact()
    now_iso = datetime.now(timezone.utc).isoformat()
    byte_size = int(source_path.stat().st_size)
    rows_observed = int(_count_rows_observed(source_path))

    if sha in ingested:
        duplicate_name = _unique_name(store_dir, f"IGNORED_DUPLICATE_{ts}_{sha8}.csv")
        duplicate_path = _move_with_fallback(source_path, store_dir / duplicate_name)
        ignored.setdefault(sha, []).append(
            {
                "ingested_at": now_iso,
                "original_name": original_name,
                "stored_name": duplicate_path.name,
                "byte_size": byte_size,
                "rows_observed": rows_observed,
                "status": "ignored_duplicate",
            }
        )
        existing = ingested.get(sha) or {}
        existing_stored_name = str(existing.get("stored_name") or duplicate_path.name)
        existing_stored_path = store_dir / existing_stored_name
        if not existing_stored_path.exists():
            existing_stored_name = duplicate_path.name
            existing_stored_path = duplicate_path
        save_manifest(manifest_path, manifest)
        return manifest, SingleFileIngestResult(
            sha256=sha,
            sha8=sha8,
            original_name=original_name,
            stored_name=existing_stored_name,
            ingested_at=str(existing.get("ingested_at") or now_iso),
            byte_size=byte_size,
            rows_observed=rows_observed,
            stored_path=existing_stored_path,
            status="duplicate_existing",
        )

    stored_name = _unique_name(store_dir, f"INGESTED_{ts}_{sha8}.csv")
    stored_path = _move_with_fallback(source_path, store_dir / stored_name)
    ingested[sha] = {
        "sha256": sha,
        "sha8": sha8,
        "ingested_at": now_iso,
        "original_name": original_name,
        "stored_name": stored_path.name,
        "byte_size": byte_size,
        "rows_observed": rows_observed,
        "status": "ingested",
    }
    order.append(sha)
    save_manifest(manifest_path, manifest)
    return manifest, SingleFileIngestResult(
        sha256=sha,
        sha8=sha8,
        original_name=original_name,
        stored_name=stored_path.name,
        ingested_at=now_iso,
        byte_size=byte_size,
        rows_observed=rows_observed,
        stored_path=stored_path,
        status="ingested",
    )


def ingest_csv_dir(
    *,
    dir_path: Path,
    store_dir: Optional[Path] = None,
    manifest_path: Path,
    client_id: str,
    kind: str,
    allow_rename: bool = True,
    include_glob: str = "*.csv",
    relpath_base_dir: Optional[Path] = None,
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    """
    Ingest CSV/TXT files from dir_path into store_dir:
    - Computes sha256 for each *.csv
    - If new: move+rename -> INGESTED_{TS}_{SHA8}.csv, record in manifest.
    - If duplicate: move+rename -> IGNORED_DUPLICATE_{TS}_{SHA8}.csv, record in manifest, do NOT return as new.

    Returns (manifest_obj, newly_ingested_sha256_list, duplicate_sha256_list)
    """
    source_dir = dir_path
    target_dir = store_dir or dir_path
    source_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(manifest_path, client_id=client_id, kind=kind)

    ingested: Dict[str, Any] = manifest.setdefault("ingested", {})
    ignored: Dict[str, Any] = manifest.setdefault("ignored_duplicates", {})
    order: List[str] = manifest.setdefault("ingested_order", [])

    new_shas: List[str] = []
    dup_shas: List[str] = []
    allowed_extensions = _allowed_extensions_from_include_glob(include_glob)

    for p in list_discoverable_files(source_dir, allowed_extensions=allowed_extensions):
        original_name_before = p.name
        sha = sha256_file(p)
        sha8 = sha[:8].upper()
        ts = now_utc_compact()
        now_iso = datetime.now(timezone.utc).isoformat()
        byte_size = int(p.stat().st_size)
        rows_observed = int(_count_rows_observed(p))

        if sha in ingested:
            # Duplicate file (same content): keep canonical existing entry, move incoming duplicate away.
            dup_shas.append(sha)
            stored_path = p
            if allow_rename:
                duplicate_name = _unique_name(target_dir, f"IGNORED_DUPLICATE_{ts}_{sha8}.csv")
                stored_path = _move_with_fallback(p, target_dir / duplicate_name)
            elif p.parent != target_dir:
                stored_path = _move_with_fallback(p, target_dir / p.name)
            stored_relpath = _to_relpath(stored_path, relpath_base_dir=relpath_base_dir)

            ignored.setdefault(sha, []).append(
                {
                    "ingested_at": now_iso,
                    "original_name": original_name_before,
                    "stored_name": stored_path.name,
                    "stored_relpath": stored_relpath,
                    "byte_size": byte_size,
                    "rows_observed": rows_observed,
                    "status": "ignored_duplicate",
                }
            )
            continue

        # new batch
        stored_path = p
        if allow_rename and manifest.get("policy", {}).get("rename_on_ingest", True):
            stored_name = _unique_name(target_dir, f"INGESTED_{ts}_{sha8}.csv")
            stored_path = _move_with_fallback(p, target_dir / stored_name)
        elif p.parent != target_dir:
            stored_path = _move_with_fallback(p, target_dir / p.name)

        stored_relpath = _to_relpath(stored_path, relpath_base_dir=relpath_base_dir)

        ingested[sha] = {
            "sha256": sha,
            "sha8": sha8,
            "ingested_at": now_iso,
            "original_name": original_name_before,
            "stored_name": stored_path.name,
            "stored_relpath": stored_relpath,
            "byte_size": byte_size,
            "rows_observed": rows_observed,
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


