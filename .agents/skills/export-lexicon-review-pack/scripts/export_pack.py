#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import io
import json
import hashlib
import platform
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path as _Path
from pathlib import Path
from typing import Dict, List, Tuple

_REPO_ROOT = _Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

from belle.lexicon_manager import label_queue_lock
from belle.lines import validate_line_id
from belle.paths import get_label_queue_lock_path


def _fixed_paths(line_id: str) -> List[Tuple[str, bool]]:
    return [
        (f"lexicon/{line_id}/lexicon.json", True),
        (f"lexicon/{line_id}/pending/label_queue.csv", True),
        (f"lexicon/{line_id}/pending/label_queue_state.json", False),
        (f"defaults/{line_id}/category_defaults.json", True),
        ("spec/LEXICON_PENDING_SPEC.md", False),
        ("spec/REPLACER_SPEC.md", False),
        ("spec/CATEGORY_OVERRIDES_SPEC.md", False),
        ("spec/CLIENT_CACHE_SPEC.md", False),
        ("spec/FILE_LAYOUT.md", False),
        ("AGENTS.md", True),
    ]


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _now_utc_iso(now: datetime) -> str:
    return now.isoformat().replace("+00:00", "Z")


def _now_utc_ts(now: datetime) -> str:
    return now.strftime("%Y%m%dT%H%M%SZ")


def _read_git_head(repo_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        value = proc.stdout.strip()
        return value or "unknown"
    except Exception:
        return "unknown"


def _detect_repo_version(repo_root: Path, line_id: str) -> str | None:
    config_path = repo_root / "rulesets" / line_id / "replacer_config_v1_15.json"
    if not config_path.exists():
        return None
    try:
        obj = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    version = str(obj.get("version") or "").strip()
    return version or None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--line", default="receipt", help="Document processing line_id")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2
    if line_id != "receipt":
        print(f"[ERROR] This skill is receipt-only. {line_id} is not supported.", file=sys.stderr)
        return 2

    lock_path = get_label_queue_lock_path(repo_root, line_id)
    included_files: List[Tuple[str, bytes]] = []
    file_hashes: Dict[str, str] = {}
    missing_required: List[str] = []
    fixed_paths = _fixed_paths(line_id)

    try:
        with label_queue_lock(
            lock_path=lock_path,
            client_id="export-lexicon-review-pack",
            timeout_sec=60,
            stale_after_sec=120,
        ):
            for rel_path, required in fixed_paths:
                abs_path = repo_root / rel_path
                if not abs_path.exists():
                    if required:
                        missing_required.append(rel_path)
                    continue
                file_hashes[rel_path] = _sha256_file(abs_path)
                included_files.append((rel_path, abs_path.read_bytes()))
    except TimeoutError:
        print(f"[ERROR] label_queue lock timeout: {lock_path}", file=sys.stderr)
        return 1

    if missing_required:
        print("[ERROR] Required file missing. Export aborted.", file=sys.stderr)
        for rel_path in missing_required:
            print(f"  - {rel_path}", file=sys.stderr)
        return 1

    now = _now_utc()
    tool_versions: Dict[str, str] = {"python": platform.python_version()}
    repo_version = _detect_repo_version(repo_root, line_id)
    if repo_version:
        tool_versions["repo"] = repo_version

    manifest_obj = {
        "exported_at_utc": _now_utc_iso(now),
        "git_commit": _read_git_head(repo_root),
        "line_id": line_id,
        "file_hashes": dict(sorted(file_hashes.items(), key=lambda x: x[0])),
        "tool_versions": tool_versions,
        "note_ja": "This pack is a read-only snapshot for Lexicon review.",
    }
    manifest_json = json.dumps(manifest_obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    manifest_sha8 = _sha256_bytes(manifest_json.encode("utf-8"))[:8]
    zip_name = f"lexicon_review_pack_{_now_utc_ts(now)}_{manifest_sha8}.zip"

    export_dir = repo_root / "exports" / "gpts_lexicon_review"
    export_dir.mkdir(parents=True, exist_ok=True)
    zip_path = export_dir / zip_name

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel_path, data in included_files:
            zf.writestr(rel_path, data)
        zf.writestr("MANIFEST.json", manifest_json.encode("utf-8"))

    tmp_zip_path = zip_path.with_suffix(zip_path.suffix + ".tmp")
    tmp_zip_path.write_bytes(zip_buffer.getvalue())
    tmp_zip_path.replace(zip_path)

    latest_path = export_dir / "LATEST.txt"
    latest_tmp_path = export_dir / "LATEST.txt.tmp"
    latest_tmp_path.write_text(f"{zip_name}\n", encoding="utf-8", newline="\n")
    latest_tmp_path.replace(latest_path)

    print(f"[OK] Created ZIP: {zip_path}")
    print("[OK] Included files:")
    for rel_path, _ in included_files:
        print(f"  - {rel_path}")
    print("  - MANIFEST.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
