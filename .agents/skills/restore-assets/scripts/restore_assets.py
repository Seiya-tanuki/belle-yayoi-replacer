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
from typing import Dict, List, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

from belle.lexicon_manager import label_queue_lock
from belle.lines import is_line_implemented, line_asset_paths, validate_line_id
from belle.paths import get_label_queue_lock_path as _line_label_queue_lock_path

MANIFEST_SCHEMA = "belle.assets_backup_manifest.v1"
TEMPLATE_CLIENT_NAME = "TEMPLATE"
LEGACY_PENDING_PREFIX = "lexicon/pending/"
BANK_FORBIDDEN_CLIENT_SUBPATHS: tuple[tuple[str, ...], ...] = (
    ("clients", "*", "lines", "bank_statement", "inputs", "ledger_ref"),
    ("clients", "*", "lines", "bank_statement", "artifacts", "ingest", "ledger_ref"),
)
BANK_FORBIDDEN_ERROR = (
    "Zip contains receipt-only bank forbidden paths (ledger_ref). "
    "Recreate backup using updated tool or remove those paths."
)


def get_label_queue_lock_path(repo_root: Path, line_id: str = "receipt") -> Path:
    return _line_label_queue_lock_path(repo_root, line_id)


def _line_uses_pending(line_id: str) -> bool:
    return line_id == "receipt"


def _is_bank_like_line(line_id: str) -> bool:
    return line_id in {"bank_statement", "credit_card_statement"}


def _line_pending_prefix(line_id: str) -> str:
    return f"lexicon/{line_id}/pending/"


def _allowed_prefixes(line_id: str) -> tuple[str, ...]:
    if _line_uses_pending(line_id):
        return ("clients/", _line_pending_prefix(line_id), LEGACY_PENDING_PREFIX)
    return ("clients/",)


def _is_path_under_pattern(path: str, pattern: tuple[str, ...]) -> bool:
    parts = tuple(part for part in path.strip("/").split("/") if part)
    if len(parts) < len(pattern):
        return False
    for actual, expected in zip(parts, pattern):
        if expected == "*":
            continue
        if actual != expected:
            return False
    return True


def _is_forbidden_bank_client_path(path: str, *, line_id: str) -> bool:
    if not _is_bank_like_line(line_id):
        return False
    return any(_is_path_under_pattern(path, pattern) for pattern in BANK_FORBIDDEN_CLIENT_SUBPATHS)


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


def _is_allowed_asset_path(path: str, *, line_id: str) -> bool:
    return any(path.startswith(prefix) for prefix in _allowed_prefixes(line_id))


def _dir_has_content(path: Path) -> bool:
    return path.exists() and any(path.iterdir())


def _map_pending_prefix_from_zip(rel_path: str, *, line_id: str) -> str:
    if _line_uses_pending(line_id) and rel_path.startswith(LEGACY_PENDING_PREFIX):
        tail = rel_path[len(LEGACY_PENDING_PREFIX):]
        return _line_pending_prefix(line_id) + tail
    return rel_path


def _write_assets_zip(
    tmp_zip_path: Path,
    repo_root: Path,
    exported_at: datetime,
    *,
    line_id: str,
) -> Tuple[str, Dict[str, int]]:
    clients_dir = repo_root / "clients"
    uses_pending = _line_uses_pending(line_id)
    pending_dir: Path | None = None
    lock_path: Path | None = None
    if uses_pending:
        pending_dir = line_asset_paths(repo_root, line_id)["pending_dir"]
        lock_path = get_label_queue_lock_path(repo_root, line_id)

    files_manifest: List[Dict[str, object]] = []
    total_bytes = 0
    count_clients = _client_count(clients_dir)

    with zipfile.ZipFile(tmp_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        written_dirs: set[str] = set()
        roots: List[Path] = [clients_dir]
        if pending_dir is not None:
            roots.append(pending_dir)

        for root in roots:
            root_rel = _repo_rel(repo_root, root) + "/"
            if root == clients_dir and _is_forbidden_bank_client_path(root_rel, line_id=line_id):
                continue
            if root_rel not in written_dirs:
                zf.writestr(root_rel, b"")
                written_dirs.add(root_rel)
            for directory in _list_dirs(root):
                rel_dir = _repo_rel(repo_root, directory) + "/"
                if root == clients_dir and _is_forbidden_bank_client_path(rel_dir, line_id=line_id):
                    continue
                if rel_dir not in written_dirs:
                    zf.writestr(rel_dir, b"")
                    written_dirs.add(rel_dir)

        for src_path in _list_files(clients_dir):
            rel_path = _repo_rel(repo_root, src_path)
            if _is_forbidden_bank_client_path(rel_path, line_id=line_id):
                continue
            data = src_path.read_bytes()
            file_sha = _sha256_bytes(data)
            file_size = len(data)
            zf.writestr(rel_path, data)
            files_manifest.append({"path": rel_path, "sha256": file_sha, "size_bytes": file_size})
            total_bytes += file_size

        if uses_pending:
            assert pending_dir is not None
            assert lock_path is not None
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
        notes_ja = (
            f"clients と lexicon/{line_id}/pending の現場アセットを固定スコープで退避したバックアップです。"
            if uses_pending
            else "clients の現場アセットを固定スコープで退避したバックアップです。"
        )
        manifest_obj = {
            "schema": MANIFEST_SCHEMA,
            "exported_at_utc": _utc_iso(exported_at),
            "git_head": _read_git_head(repo_root),
            "line_id": line_id,
            "files": files_manifest,
            "counts": {
                "files": len(files_manifest),
                "clients": count_clients,
                "total_bytes": total_bytes,
            },
            "notes_ja": notes_ja,
        }
        manifest_json = json.dumps(manifest_obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        zf.writestr("MANIFEST.json", manifest_json.encode("utf-8"))

    return manifest_json, manifest_obj["counts"]


def _create_pre_restore_snapshot(repo_root: Path, backup_dir: Path, *, line_id: str) -> Path:
    exported_at = _utc_now()
    ts = _utc_compact(exported_at)
    tmp_zip_path = backup_dir / f".pre_restore_{ts}_{os.getpid()}.zip.tmp"
    manifest_json, _ = _write_assets_zip(tmp_zip_path, repo_root, exported_at, line_id=line_id)
    sha8 = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()[:8]
    zip_name = f"pre_restore_{ts}_{sha8}.zip"
    final_zip_path = backup_dir / zip_name
    if final_zip_path.exists():
        final_zip_path.unlink()
    tmp_zip_path.replace(final_zip_path)
    return final_zip_path


def _validate_backup_zip(zip_path: Path, *, line_id: str) -> Dict[str, int]:
    uses_pending = _line_uses_pending(line_id)
    pending_prefix = _line_pending_prefix(line_id)
    with zipfile.ZipFile(zip_path, mode="r") as zf:
        manifest_info: zipfile.ZipInfo | None = None
        zip_dirs: set[str] = set()
        zip_files: Dict[str, zipfile.ZipInfo] = {}

        for info in zf.infolist():
            normalized = _normalize_member_name(info.filename, is_dir=info.is_dir())
            if normalized == "MANIFEST.json":
                if info.is_dir():
                    raise ValueError("MANIFEST.json must be a file")
                manifest_info = info
                continue
            if _is_forbidden_bank_client_path(normalized, line_id=line_id):
                raise ValueError(f"{BANK_FORBIDDEN_ERROR} path={normalized}")
            if not _is_allowed_asset_path(normalized, line_id=line_id):
                raise ValueError(f"unexpected path in zip: {normalized}")
            if info.is_dir():
                zip_dirs.add(normalized)
                continue
            if normalized in zip_files:
                raise ValueError(f"duplicate file in zip: {normalized}")
            zip_files[normalized] = info

        if manifest_info is None:
            raise ValueError("MANIFEST.json not found")

        has_clients_root = "clients/" in zip_dirs or any(p.startswith("clients/") for p in zip_files)
        has_pending_root = pending_prefix in zip_dirs or LEGACY_PENDING_PREFIX in zip_dirs or any(
            p.startswith(pending_prefix) or p.startswith(LEGACY_PENDING_PREFIX) for p in zip_files
        )
        if not has_clients_root:
            raise ValueError("clients/ root missing in zip")
        if uses_pending and not has_pending_root:
            raise ValueError(f"{pending_prefix} (or legacy lexicon/pending/) root missing in zip")

        try:
            manifest_obj = json.loads(zf.read(manifest_info).decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid MANIFEST.json: {exc}") from exc

        if manifest_obj.get("schema") != MANIFEST_SCHEMA:
            raise ValueError(
                f"manifest schema mismatch: {manifest_obj.get('schema')!r} != {MANIFEST_SCHEMA!r}"
            )

        manifest_files = manifest_obj.get("files")
        if not isinstance(manifest_files, list):
            raise ValueError("manifest files must be a list")

        expected: Dict[str, Tuple[str, int]] = {}
        hex64 = re.compile(r"^[0-9a-f]{64}$")
        for idx, item in enumerate(manifest_files, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"manifest files[{idx}] must be an object")
            raw_path = item.get("path")
            raw_sha = item.get("sha256")
            raw_size = item.get("size_bytes")
            if not isinstance(raw_path, str):
                raise ValueError(f"manifest files[{idx}].path must be a string")
            normalized_path = _normalize_member_name(raw_path, is_dir=False)
            if _is_forbidden_bank_client_path(normalized_path, line_id=line_id):
                raise ValueError(f"{BANK_FORBIDDEN_ERROR} path={normalized_path}")
            if not _is_allowed_asset_path(normalized_path, line_id=line_id):
                raise ValueError(f"manifest files[{idx}] path out of scope: {normalized_path}")
            if not isinstance(raw_sha, str) or not hex64.match(raw_sha):
                raise ValueError(f"manifest files[{idx}].sha256 must be hex-64")
            try:
                declared_size = int(raw_size)
            except Exception as exc:
                raise ValueError(f"manifest files[{idx}].size_bytes must be an integer") from exc
            if declared_size < 0:
                raise ValueError(f"manifest files[{idx}].size_bytes must be >= 0")
            if normalized_path in expected:
                raise ValueError(f"duplicate path in manifest: {normalized_path}")
            expected[normalized_path] = (raw_sha, declared_size)

        zip_file_paths = set(zip_files.keys())
        expected_paths = set(expected.keys())
        missing = sorted(expected_paths - zip_file_paths)
        extra = sorted(zip_file_paths - expected_paths)
        if missing:
            raise ValueError(f"manifest entry missing in zip: {missing[0]}")
        if extra:
            raise ValueError(f"zip file missing in manifest: {extra[0]}")

        total_bytes = 0
        for path, (declared_sha, declared_size) in expected.items():
            data = zf.read(zip_files[path])
            actual_size = len(data)
            if actual_size != declared_size:
                raise ValueError(f"size mismatch for {path}: manifest={declared_size} actual={actual_size}")
            actual_sha = _sha256_bytes(data)
            if actual_sha != declared_sha:
                raise ValueError(f"sha256 mismatch for {path}")
            total_bytes += actual_size

        counts = manifest_obj.get("counts")
        if isinstance(counts, dict):
            if "files" in counts and int(counts["files"]) != len(expected):
                raise ValueError("manifest counts.files mismatch")
            if "total_bytes" in counts and int(counts["total_bytes"]) != total_bytes:
                raise ValueError("manifest counts.total_bytes mismatch")

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


def _extract_to_staging(zip_path: Path, staging_dir: Path, *, line_id: str) -> None:
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
            if not _is_allowed_asset_path(normalized, line_id=line_id):
                continue

            mapped_rel = _map_pending_prefix_from_zip(normalized, line_id=line_id)
            target = staging_dir / Path(mapped_rel.rstrip("/"))
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, mode="r") as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)

    (staging_dir / "clients").mkdir(parents=True, exist_ok=True)
    if _line_uses_pending(line_id):
        (staging_dir / "lexicon" / line_id / "pending").mkdir(parents=True, exist_ok=True)


def _move_path(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))


def _remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
        return
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def _restore_clients_with_swap(*, repo_root: Path, stage_clients: Path, restore_old_dir: Path) -> None:
    dest_clients = repo_root / "clients"
    dest_clients.mkdir(parents=True, exist_ok=True)
    restore_old_dir.mkdir(parents=True, exist_ok=True)

    existing_clients = sorted(
        path for path in dest_clients.iterdir() if path.is_dir() and path.name != TEMPLATE_CLIENT_NAME
    )
    staged_clients = sorted(
        path for path in stage_clients.iterdir() if path.is_dir() and path.name != TEMPLATE_CLIENT_NAME
    )

    moved_old: List[Tuple[Path, Path]] = []
    moved_new: List[Path] = []

    try:
        for old_dir in existing_clients:
            rollback_path = restore_old_dir / old_dir.name
            if rollback_path.exists():
                raise RuntimeError(f"rollback path already exists: {rollback_path}")
            _move_path(old_dir, rollback_path)
            moved_old.append((rollback_path, old_dir))

        for staged_dir in staged_clients:
            dest_dir = dest_clients / staged_dir.name
            if dest_dir.exists():
                raise RuntimeError(f"destination client already exists before restore: {dest_dir}")
            _move_path(staged_dir, dest_dir)
            moved_new.append(dest_dir)
    except Exception:
        for path in reversed(moved_new):
            _remove_path(path)
        for rollback_path, dest_path in reversed(moved_old):
            if rollback_path.exists():
                _move_path(rollback_path, dest_path)
        raise


def _restore_pending_with_swap(
    *,
    repo_root: Path,
    stage_pending: Path,
    restore_old_dir: Path,
    line_id: str = "receipt",
) -> None:
    dest_pending = line_asset_paths(repo_root, line_id)["pending_dir"]
    dest_pending.mkdir(parents=True, exist_ok=True)
    (dest_pending / "locks").mkdir(parents=True, exist_ok=True)
    restore_old_dir.mkdir(parents=True, exist_ok=True)

    lock_path = get_label_queue_lock_path(repo_root, line_id)
    moved_old: List[Tuple[Path, Path]] = []
    moved_new: List[Path] = []

    with label_queue_lock(
        lock_path=lock_path,
        client_id="restore-assets",
        timeout_sec=60,
        stale_after_sec=120,
    ) as lock_token:
        try:
            for current in sorted(dest_pending.iterdir(), key=lambda p: p.name):
                if current.name == ".gitkeep":
                    continue
                if current.name == "locks" and current.is_dir():
                    for lock_item in sorted(current.iterdir(), key=lambda p: p.name):
                        if lock_item.name == ".gitkeep":
                            continue
                        if lock_item.resolve() == lock_path.resolve():
                            continue
                        rollback_path = restore_old_dir / "locks" / lock_item.name
                        _move_path(lock_item, rollback_path)
                        moved_old.append((rollback_path, lock_item))
                        lock_token.maybe_heartbeat()
                    continue

                rollback_path = restore_old_dir / current.name
                _move_path(current, rollback_path)
                moved_old.append((rollback_path, current))
                lock_token.maybe_heartbeat()

            for staged in sorted(stage_pending.iterdir(), key=lambda p: p.name):
                if staged.name == ".gitkeep":
                    continue
                if staged.name == "locks" and staged.is_dir():
                    for staged_lock_item in sorted(staged.iterdir(), key=lambda p: p.name):
                        if staged_lock_item.name == ".gitkeep":
                            continue
                        if staged_lock_item.name.endswith(".lock"):
                            continue
                        dest_lock_item = dest_pending / "locks" / staged_lock_item.name
                        if dest_lock_item.exists():
                            raise RuntimeError(f"destination pending lock item already exists: {dest_lock_item}")
                        _move_path(staged_lock_item, dest_lock_item)
                        moved_new.append(dest_lock_item)
                        lock_token.maybe_heartbeat()
                    continue

                dest_item = dest_pending / staged.name
                if dest_item.exists():
                    raise RuntimeError(f"destination pending item already exists: {dest_item}")
                _move_path(staged, dest_item)
                moved_new.append(dest_item)
                lock_token.maybe_heartbeat()
        except Exception:
            for path in reversed(moved_new):
                _remove_path(path)
            for rollback_path, dest_path in reversed(moved_old):
                if rollback_path.exists():
                    _move_path(rollback_path, dest_path)
            raise


def _apply_restore(repo_root: Path, staging_dir: Path, restore_old_root: Path, *, line_id: str = "receipt") -> None:
    stage_clients = staging_dir / "clients"
    uses_pending = _line_uses_pending(line_id)
    stage_pending: Path | None = staging_dir / "lexicon" / line_id / "pending" if uses_pending else None
    if not stage_clients.exists():
        raise RuntimeError("staging missing clients/")
    if uses_pending and (stage_pending is None or not stage_pending.exists()):
        raise RuntimeError(f"staging missing lexicon/{line_id}/pending/")

    restore_old_root.mkdir(parents=True, exist_ok=True)
    _restore_clients_with_swap(
        repo_root=repo_root,
        stage_clients=stage_clients,
        restore_old_dir=restore_old_root / "clients",
    )
    if stage_pending is not None:
        _restore_pending_with_swap(
            repo_root=repo_root,
            stage_pending=stage_pending,
            restore_old_dir=restore_old_root / f"lexicon_{line_id}_pending",
            line_id=line_id,
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restore field assets from a fixed-scope backup ZIP.")
    parser.add_argument("--zip", required=True, dest="zip_path", help="Path to backup ZIP")
    parser.add_argument("--line", default="receipt", help="Document processing line_id")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Required when destination assets already contain data.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parents[4]
    backup_dir = repo_root / "exports" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2
    if not is_line_implemented(line_id):
        print("[ERROR] line is unimplemented in Phase 1", file=sys.stderr)
        return 2

    zip_path = Path(args.zip_path).expanduser()
    if not zip_path.is_absolute():
        zip_path = (Path.cwd() / zip_path).resolve()

    if not zip_path.exists() or not zip_path.is_file():
        print(f"[ERROR] zip not found: {zip_path}", file=sys.stderr)
        return 1
    if not zipfile.is_zipfile(zip_path):
        print(f"[ERROR] not a valid zip file: {zip_path}", file=sys.stderr)
        return 1

    try:
        restored_counts = _validate_backup_zip(zip_path, line_id=line_id)
    except Exception as exc:
        print(f"[ERROR] backup validation failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    clients_dir = repo_root / "clients"
    pending_dir: Path | None = None
    if _line_uses_pending(line_id):
        pending_dir = line_asset_paths(repo_root, line_id)["pending_dir"]
    overwrite_needed = _dir_has_content(clients_dir) or (
        pending_dir is not None and _dir_has_content(pending_dir)
    )

    if overwrite_needed and not args.force:
        print("[SAFE-EXIT] restore would overwrite existing assets")
        print("Use `--force` to continue.")
        print("Validation passed and no data was changed.")
        return 2

    pre_restore_snapshot: Path | None = None
    if overwrite_needed:
        try:
            pre_restore_snapshot = _create_pre_restore_snapshot(repo_root, backup_dir, line_id=line_id)
        except TimeoutError:
            print("[ERROR] failed to acquire lock for pre-restore snapshot", file=sys.stderr)
            return 1
        except Exception as exc:
            print(f"[ERROR] pre-restore snapshot failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1

    stage_ts = _utc_compact(_utc_now())
    staging_dir = backup_dir / f"restore_staging_{stage_ts}_{os.getpid()}"
    restore_old_dir = backup_dir / f"restore_old_{stage_ts}_{os.getpid()}"
    try:
        _extract_to_staging(zip_path, staging_dir, line_id=line_id)
        _apply_restore(repo_root, staging_dir, restore_old_dir, line_id=line_id)
    except Exception as exc:
        print(f"[ERROR] restore failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)

    print("[OK] assets restore completed")
    print(f"[OK] restored zip: {zip_path.name}")
    if pre_restore_snapshot is not None:
        print(f"[OK] pre-restore snapshot: {pre_restore_snapshot}")
    else:
        print("[OK] pre-restore snapshot: skipped (destination empty)")
    print(f"[OK] rollback assets: {restore_old_dir}")
    print(
        "[OK] restored counts: files={files}, clients={clients}, total_bytes={total_bytes}".format(
            files=restored_counts["files"],
            clients=restored_counts["clients"],
            total_bytes=restored_counts["total_bytes"],
        )
    )
    print("[INFO] tracked code paths were not modified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
