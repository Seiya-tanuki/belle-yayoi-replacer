#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

from belle.lexicon_manager import label_queue_lock
from belle.paths import get_label_queue_lock_path


MANIFEST_SCHEMA = "belle.assets_backup_manifest.v1"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_iso(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _utc_compact(ts: datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M%SZ")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def _list_dirs(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*") if path.is_dir())


def _list_files(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*") if path.is_file())


def _repo_rel(repo_root: Path, path: Path) -> str:
    return path.relative_to(repo_root).as_posix()


def _client_count(clients_dir: Path) -> int:
    if not clients_dir.exists():
        return 0
    return sum(1 for path in clients_dir.iterdir() if path.is_dir())


def _write_assets_zip(tmp_zip_path: Path, repo_root: Path, exported_at: datetime) -> Tuple[str, Dict[str, int]]:
    clients_dir = repo_root / "clients"
    pending_dir = repo_root / "lexicon" / "pending"
    lock_path = get_label_queue_lock_path(repo_root)

    files_manifest: List[Dict[str, object]] = []
    total_bytes = 0
    count_clients = _client_count(clients_dir)

    with zipfile.ZipFile(tmp_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        written_dirs: set[str] = set()
        for root in [clients_dir, pending_dir]:
            root_rel = _repo_rel(repo_root, root) + "/"
            if root_rel not in written_dirs:
                zf.writestr(root_rel, b"")
                written_dirs.add(root_rel)
            for directory in _list_dirs(root):
                rel_dir = _repo_rel(repo_root, directory) + "/"
                if rel_dir not in written_dirs:
                    zf.writestr(rel_dir, b"")
                    written_dirs.add(rel_dir)

        for src_path in _list_files(clients_dir):
            rel_path = _repo_rel(repo_root, src_path)
            data = src_path.read_bytes()
            file_sha = _sha256_bytes(data)
            file_size = len(data)
            zf.writestr(rel_path, data)
            files_manifest.append({"path": rel_path, "sha256": file_sha, "size_bytes": file_size})
            total_bytes += file_size

        with label_queue_lock(
            lock_path=lock_path,
            client_id="backup-assets",
            timeout_sec=60,
            stale_after_sec=120,
        ) as lock_token:
            for src_path in _list_files(pending_dir):
                rel_path = _repo_rel(repo_root, src_path)
                data = src_path.read_bytes()
                file_sha = _sha256_bytes(data)
                file_size = len(data)
                zf.writestr(rel_path, data)
                files_manifest.append({"path": rel_path, "sha256": file_sha, "size_bytes": file_size})
                total_bytes += file_size
                lock_token.maybe_heartbeat()

        files_manifest.sort(key=lambda row: str(row["path"]))
        manifest_obj = {
            "schema": MANIFEST_SCHEMA,
            "exported_at_utc": _utc_iso(exported_at),
            "git_head": _read_git_head(repo_root),
            "files": files_manifest,
            "counts": {
                "files": len(files_manifest),
                "clients": count_clients,
                "total_bytes": total_bytes,
            },
            "notes_ja": "clients と lexicon/pending の現場アセットを固定スコープで退避したバックアップです。",
        }
        manifest_json = json.dumps(manifest_obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        zf.writestr("MANIFEST.json", manifest_json.encode("utf-8"))

    return manifest_json, manifest_obj["counts"]


def main() -> int:
    repo_root = Path(__file__).resolve().parents[4]
    backup_dir = repo_root / "exports" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    exported_at = _utc_now()
    ts = _utc_compact(exported_at)
    tmp_zip_path = backup_dir / f".assets_{ts}_{os.getpid()}.zip.tmp"

    try:
        manifest_json, counts = _write_assets_zip(tmp_zip_path, repo_root, exported_at)
    except TimeoutError:
        print("[ERROR] label_queue のロック取得に失敗しました（60秒タイムアウト）。", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"[ERROR] バックアップ作成に失敗しました: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    sha8 = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()[:8]
    zip_name = f"assets_{ts}_{sha8}.zip"
    final_zip_path = backup_dir / zip_name
    if final_zip_path.exists():
        final_zip_path.unlink()
    tmp_zip_path.replace(final_zip_path)

    latest_path = backup_dir / "LATEST.txt"
    latest_tmp = backup_dir / "LATEST.txt.tmp"
    latest_tmp.write_text(f"{zip_name}\n", encoding="utf-8", newline="\n")
    latest_tmp.replace(latest_path)

    print("[OK] アセットバックアップを作成しました。")
    print(f"[OK] ZIP: {final_zip_path}")
    print(f"[OK] LATEST: {latest_path}")
    print(
        "[OK] 件数: files={files}, clients={clients}, total_bytes={total_bytes}".format(
            files=counts["files"],
            clients=counts["clients"],
            total_bytes=counts["total_bytes"],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
