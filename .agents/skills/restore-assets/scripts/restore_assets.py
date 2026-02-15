#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, List, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

from belle.lexicon_manager import label_queue_lock
from belle.paths import get_label_queue_lock_path


MANIFEST_SCHEMA = "belle.assets_backup_manifest.v1"
ALLOWED_PREFIXES = ("clients/", "lexicon/pending/")


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


def _normalize_member_name(name: str, *, is_dir: bool) -> str:
    candidate = name.replace("\\", "/")
    while candidate.startswith("./"):
        candidate = candidate[2:]
    if not candidate:
        raise ValueError(f"empty zip member name: {name!r}")
    pure = PurePosixPath(candidate)
    if pure.is_absolute() or ".." in pure.parts:
        raise ValueError(f"unsafe zip member path: {name!r}")
    normalized = pure.as_posix()
    if normalized in ("", "."):
        raise ValueError(f"invalid zip member path: {name!r}")
    if is_dir and not normalized.endswith("/"):
        normalized += "/"
    return normalized


def _is_allowed_asset_path(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in ALLOWED_PREFIXES)


def _dir_has_content(path: Path) -> bool:
    return path.exists() and any(path.iterdir())


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
            client_id="restore-assets-snapshot",
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


def _create_pre_restore_snapshot(repo_root: Path, backup_dir: Path) -> Path:
    exported_at = _utc_now()
    ts = _utc_compact(exported_at)
    tmp_zip_path = backup_dir / f".pre_restore_{ts}_{os.getpid()}.zip.tmp"
    manifest_json, _ = _write_assets_zip(tmp_zip_path, repo_root, exported_at)
    sha8 = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()[:8]
    zip_name = f"pre_restore_{ts}_{sha8}.zip"
    final_zip_path = backup_dir / zip_name
    if final_zip_path.exists():
        final_zip_path.unlink()
    tmp_zip_path.replace(final_zip_path)
    return final_zip_path


def _validate_backup_zip(zip_path: Path) -> Dict[str, int]:
    with zipfile.ZipFile(zip_path, mode="r") as zf:
        manifest_info = None
        zip_dirs: set[str] = set()
        zip_files: Dict[str, zipfile.ZipInfo] = {}

        for info in zf.infolist():
            normalized = _normalize_member_name(info.filename, is_dir=info.is_dir())
            if normalized == "MANIFEST.json":
                if info.is_dir():
                    raise ValueError("MANIFEST.json がディレクトリです。")
                manifest_info = info
                continue
            if not _is_allowed_asset_path(normalized):
                raise ValueError(f"許可されていないパスが含まれています: {normalized}")
            if info.is_dir():
                zip_dirs.add(normalized)
                continue
            if normalized in zip_files:
                raise ValueError(f"ZIP 内に重複ファイルがあります: {normalized}")
            zip_files[normalized] = info

        if manifest_info is None:
            raise ValueError("MANIFEST.json が見つかりません。")

        has_clients_root = "clients/" in zip_dirs or any(p.startswith("clients/") for p in zip_files)
        has_pending_root = "lexicon/pending/" in zip_dirs or any(
            p.startswith("lexicon/pending/") for p in zip_files
        )
        if not has_clients_root:
            raise ValueError("ZIP に clients/ ルートがありません。")
        if not has_pending_root:
            raise ValueError("ZIP に lexicon/pending/ ルートがありません。")

        try:
            manifest_obj = json.loads(zf.read(manifest_info).decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"MANIFEST.json の JSON 解析に失敗しました: {exc}") from exc

        if manifest_obj.get("schema") != MANIFEST_SCHEMA:
            raise ValueError(
                f"MANIFEST schema が不正です: {manifest_obj.get('schema')!r} != {MANIFEST_SCHEMA!r}"
            )

        manifest_files = manifest_obj.get("files")
        if not isinstance(manifest_files, list):
            raise ValueError("MANIFEST.json の files が配列ではありません。")

        expected: Dict[str, Tuple[str, int]] = {}
        hex64 = re.compile(r"^[0-9a-f]{64}$")
        for idx, item in enumerate(manifest_files, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"MANIFEST files[{idx}] がオブジェクトではありません。")
            raw_path = item.get("path")
            raw_sha = item.get("sha256")
            raw_size = item.get("size_bytes")
            if not isinstance(raw_path, str):
                raise ValueError(f"MANIFEST files[{idx}].path が文字列ではありません。")
            normalized_path = _normalize_member_name(raw_path, is_dir=False)
            if not _is_allowed_asset_path(normalized_path):
                raise ValueError(f"MANIFEST files[{idx}] の path が許可範囲外です: {normalized_path}")
            if not isinstance(raw_sha, str) or not hex64.match(raw_sha):
                raise ValueError(f"MANIFEST files[{idx}].sha256 が不正です。")
            try:
                declared_size = int(raw_size)
            except Exception as exc:
                raise ValueError(f"MANIFEST files[{idx}].size_bytes が不正です。") from exc
            if declared_size < 0:
                raise ValueError(f"MANIFEST files[{idx}].size_bytes が負数です。")
            if normalized_path in expected:
                raise ValueError(f"MANIFEST に重複 path があります: {normalized_path}")
            expected[normalized_path] = (raw_sha, declared_size)

        zip_file_paths = set(zip_files.keys())
        expected_paths = set(expected.keys())
        missing = sorted(expected_paths - zip_file_paths)
        extra = sorted(zip_file_paths - expected_paths)
        if missing:
            raise ValueError(f"MANIFEST 記載ファイルが ZIP に存在しません: {missing[0]}")
        if extra:
            raise ValueError(f"MANIFEST 未記載ファイルが ZIP に存在します: {extra[0]}")

        total_bytes = 0
        for path, (declared_sha, declared_size) in expected.items():
            data = zf.read(zip_files[path])
            actual_size = len(data)
            if actual_size != declared_size:
                raise ValueError(f"サイズ不一致: {path} manifest={declared_size} actual={actual_size}")
            actual_sha = _sha256_bytes(data)
            if actual_sha != declared_sha:
                raise ValueError(f"sha256 不一致: {path}")
            total_bytes += actual_size

        counts = manifest_obj.get("counts")
        if isinstance(counts, dict):
            if "files" in counts and int(counts["files"]) != len(expected):
                raise ValueError("MANIFEST counts.files が files 件数と一致しません。")
            if "total_bytes" in counts and int(counts["total_bytes"]) != total_bytes:
                raise ValueError("MANIFEST counts.total_bytes が実サイズ合計と一致しません。")

        clients_from_manifest = 0
        if isinstance(counts, dict) and "clients" in counts:
            try:
                clients_from_manifest = int(counts["clients"])
            except Exception:
                clients_from_manifest = 0
        if clients_from_manifest < 0:
            clients_from_manifest = 0

        return {
            "files": len(expected),
            "clients": clients_from_manifest,
            "total_bytes": total_bytes,
        }


def _extract_to_staging(zip_path: Path, staging_dir: Path) -> None:
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=False)

    with zipfile.ZipFile(zip_path, mode="r") as zf:
        normalized_infos: List[Tuple[str, zipfile.ZipInfo]] = []
        for info in zf.infolist():
            normalized = _normalize_member_name(info.filename, is_dir=info.is_dir())
            normalized_infos.append((normalized, info))

        for normalized, info in sorted(normalized_infos, key=lambda row: row[0]):
            if normalized == "MANIFEST.json":
                continue
            if not _is_allowed_asset_path(normalized):
                continue

            target = staging_dir / Path(normalized.rstrip("/"))
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, mode="r") as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)

    (staging_dir / "clients").mkdir(parents=True, exist_ok=True)
    (staging_dir / "lexicon" / "pending").mkdir(parents=True, exist_ok=True)


def _apply_restore(repo_root: Path, staging_dir: Path) -> None:
    stage_clients = staging_dir / "clients"
    stage_pending = staging_dir / "lexicon" / "pending"
    if not stage_clients.exists():
        raise RuntimeError("ステージングに clients/ が存在しません。")
    if not stage_pending.exists():
        raise RuntimeError("ステージングに lexicon/pending/ が存在しません。")

    dest_clients = repo_root / "clients"
    dest_pending = repo_root / "lexicon" / "pending"

    if dest_clients.exists():
        shutil.rmtree(dest_clients)
    if dest_pending.exists():
        shutil.rmtree(dest_pending)

    (repo_root / "lexicon").mkdir(parents=True, exist_ok=True)
    shutil.copytree(stage_clients, dest_clients)
    shutil.copytree(stage_pending, dest_pending)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restore field assets from a fixed-scope backup ZIP.")
    parser.add_argument("--zip", required=True, dest="zip_path", help="バックアップ ZIP のパス")
    parser.add_argument(
        "--force",
        action="store_true",
        help="既存アセットを上書きする場合に必須",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parents[4]
    backup_dir = repo_root / "exports" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    zip_path = Path(args.zip_path).expanduser()
    if not zip_path.is_absolute():
        zip_path = (Path.cwd() / zip_path).resolve()

    if not zip_path.exists() or not zip_path.is_file():
        print(f"[ERROR] ZIP が見つかりません: {zip_path}", file=sys.stderr)
        return 1
    if not zipfile.is_zipfile(zip_path):
        print(f"[ERROR] ZIP 形式ではありません: {zip_path}", file=sys.stderr)
        return 1

    try:
        restored_counts = _validate_backup_zip(zip_path)
    except Exception as exc:
        print(f"[ERROR] バックアップ検証に失敗しました: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    clients_dir = repo_root / "clients"
    pending_dir = repo_root / "lexicon" / "pending"
    overwrite_needed = _dir_has_content(clients_dir) or _dir_has_content(pending_dir)

    if overwrite_needed and not args.force:
        print("[SAFE-EXIT] 既存アセットが存在するため復元を中止しました。")
        print("上書きする場合は `--force` を指定して再実行してください。")
        print("検証は完了済みで、現行データには一切変更を加えていません。")
        return 2

    pre_restore_snapshot: Path | None = None
    if overwrite_needed:
        try:
            pre_restore_snapshot = _create_pre_restore_snapshot(repo_root, backup_dir)
        except TimeoutError:
            print("[ERROR] pre-restore スナップショット用 lock 取得に失敗しました。", file=sys.stderr)
            return 1
        except Exception as exc:
            print(
                f"[ERROR] pre-restore スナップショット作成に失敗しました: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            return 1

    stage_ts = _utc_compact(_utc_now())
    staging_dir = backup_dir / f"restore_staging_{stage_ts}_{os.getpid()}"
    try:
        _extract_to_staging(zip_path, staging_dir)
        _apply_restore(repo_root, staging_dir)
    except Exception as exc:
        print(f"[ERROR] 復元処理に失敗しました: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)

    print("[OK] アセット復元が完了しました。")
    print(f"[OK] 復元元ZIP: {zip_path.name}")
    if pre_restore_snapshot is not None:
        print(f"[OK] pre-restore スナップショット: {pre_restore_snapshot}")
    else:
        print("[OK] pre-restore スナップショット: 既存アセットなしのため未作成")
    print(
        "[OK] 復元件数: files={files}, clients={clients}, total_bytes={total_bytes}".format(
            files=restored_counts["files"],
            clients=restored_counts["clients"],
            total_bytes=restored_counts["total_bytes"],
        )
    )
    print("[INFO] belle/ spec/ .agents/ defaults/ lexicon/lexicon.json tools/ などのコード領域は変更していません。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
