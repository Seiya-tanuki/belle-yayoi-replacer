# -*- coding: utf-8 -*-
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Literal

LEGACY_CLIENT_DIR_NAMES = ("config", "inputs", "outputs", "artifacts")
PENDING_QUEUE_FILES = ("label_queue.csv", "label_queue_state.json")
PENDING_MARKER_PREFIX = "ledger_ref_processed_markers"
PENDING_MARKER_SUFFIX = ".json"
PENDING_LOCK_FILE = "label_queue.lock"


class MigrationError(RuntimeError):
    """Base class for migration failures."""


class MigrationSafetyError(MigrationError):
    """Raised when fail-closed safety checks block migration."""


def _validate_options(mode: str, apply: bool, dry_run: bool) -> None:
    if mode not in {"copy", "move"}:
        raise ValueError(f"mode must be 'copy' or 'move', got {mode!r}")
    if apply and dry_run:
        raise ValueError("apply=True requires dry_run=False")


def _repo_relpath(repo_root: Path, path: Path) -> str:
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return str(path)


def _is_non_empty_dir(path: Path) -> bool:
    return path.exists() and path.is_dir() and any(path.iterdir())


def _prune_empty_parents(path: Path, *, stop_at: Path) -> None:
    current = path
    while True:
        if current == stop_at:
            return
        if not current.exists() or not current.is_dir():
            return
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def _remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
        return
    path.unlink(missing_ok=True)


def _assert_dirs_exist(paths: list[Path]) -> None:
    missing = [path for path in paths if not path.exists() or not path.is_dir()]
    if missing:
        missing_text = ", ".join(str(path) for path in missing)
        raise MigrationError(f"destination directories missing after migration: {missing_text}")


def _assert_files_exist(paths: list[Path]) -> None:
    missing = [path for path in paths if not path.exists() or not path.is_file()]
    if missing:
        missing_text = ", ".join(str(path) for path in missing)
        raise MigrationError(f"destination files missing after migration: {missing_text}")


def _apply_copy_directories(operations: list[tuple[Path, Path]], *, stop_at: Path) -> None:
    copied: list[Path] = []
    try:
        for src, dst in operations:
            shutil.copytree(src, dst)
            copied.append(dst)
        _assert_dirs_exist([dst for _, dst in operations])
    except Exception as exc:
        for dst in reversed(copied):
            _remove_path(dst)
            _prune_empty_parents(dst.parent, stop_at=stop_at)
        raise MigrationError(f"copy migration failed: {type(exc).__name__}: {exc}") from exc


def _apply_move_directories_with_rollback(
    operations: list[tuple[Path, Path]],
    *,
    stop_at: Path,
) -> None:
    moved: list[tuple[Path, Path]] = []
    try:
        for src, dst in operations:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
            moved.append((src, dst))
        _assert_dirs_exist([dst for _, dst in operations])
    except Exception as exc:
        rollback_errors: list[str] = []
        for src, dst in reversed(moved):
            if not dst.exists():
                continue
            src.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(dst), str(src))
            except Exception as rollback_exc:  # pragma: no cover - defensive path
                rollback_errors.append(f"{dst} -> {src}: {type(rollback_exc).__name__}: {rollback_exc}")

        for _, dst in reversed(moved):
            _prune_empty_parents(dst.parent, stop_at=stop_at)

        if rollback_errors:
            joined = "; ".join(rollback_errors)
            raise MigrationError(
                f"move migration failed and rollback was incomplete: {joined}"
            ) from exc
        raise MigrationError(f"move migration failed and rollback completed: {type(exc).__name__}: {exc}") from exc


def _apply_copy_files(operations: list[tuple[Path, Path]], *, stop_at: Path) -> None:
    copied: list[Path] = []
    try:
        for src, dst in operations:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            copied.append(dst)
        _assert_files_exist([dst for _, dst in operations])
    except Exception as exc:
        for dst in reversed(copied):
            _remove_path(dst)
            _prune_empty_parents(dst.parent, stop_at=stop_at)
        raise MigrationError(f"copy migration failed: {type(exc).__name__}: {exc}") from exc


def _apply_move_files_with_rollback(operations: list[tuple[Path, Path]], *, stop_at: Path) -> None:
    moved: list[tuple[Path, Path]] = []
    try:
        for src, dst in operations:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
            moved.append((src, dst))
        _assert_files_exist([dst for _, dst in operations])
    except Exception as exc:
        rollback_errors: list[str] = []
        for src, dst in reversed(moved):
            if not dst.exists():
                continue
            src.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(dst), str(src))
            except Exception as rollback_exc:  # pragma: no cover - defensive path
                rollback_errors.append(f"{dst} -> {src}: {type(rollback_exc).__name__}: {rollback_exc}")

        for _, dst in reversed(moved):
            _prune_empty_parents(dst.parent, stop_at=stop_at)

        if rollback_errors:
            joined = "; ".join(rollback_errors)
            raise MigrationError(
                f"move migration failed and rollback was incomplete: {joined}"
            ) from exc
        raise MigrationError(f"move migration failed and rollback completed: {type(exc).__name__}: {exc}") from exc


def _is_pending_marker_file(name: str) -> bool:
    return name.startswith(PENDING_MARKER_PREFIX) and name.endswith(PENDING_MARKER_SUFFIX)


def migrate_receipt_client_layout(
    repo_root: Path,
    client_id: str,
    mode: Literal["copy", "move"] = "copy",
    apply: bool = False,
    dry_run: bool = True,
) -> dict:
    repo = Path(repo_root)
    _validate_options(mode, apply, dry_run)

    cid = str(client_id or "").strip()
    if not cid:
        raise ValueError("client_id must be non-empty")
    if cid == "TEMPLATE":
        raise MigrationSafetyError("clients/TEMPLATE is never a migration target")

    clients_root = repo / "clients"
    client_root = clients_root / cid
    line_root = client_root / "lines" / "receipt"

    result = {
        "kind": "receipt_client_layout",
        "client_id": cid,
        "mode": mode,
        "apply": bool(apply),
        "dry_run": bool(dry_run),
        "applied": False,
        "source_root": _repo_relpath(repo, client_root),
        "destination_root": _repo_relpath(repo, line_root),
        "legacy_dirs": [],
        "operations": [],
        "status": "noop",
        "reason": "",
    }

    if not client_root.exists():
        result["reason"] = "client_not_found"
        return result

    if _is_non_empty_dir(line_root):
        raise MigrationSafetyError(
            f"destination already exists and is non-empty: {_repo_relpath(repo, line_root)}"
        )

    legacy_dirs: list[str] = []
    operations: list[tuple[Path, Path]] = []
    for name in LEGACY_CLIENT_DIR_NAMES:
        src = client_root / name
        if not src.exists():
            continue
        if not src.is_dir():
            raise MigrationSafetyError(f"legacy path is not a directory: {_repo_relpath(repo, src)}")
        dst = line_root / name
        if dst.exists():
            raise MigrationSafetyError(
                f"destination path already exists: {_repo_relpath(repo, dst)}"
            )
        legacy_dirs.append(name)
        operations.append((src, dst))

    result["legacy_dirs"] = legacy_dirs
    result["operations"] = [
        {
            "source": _repo_relpath(repo, src),
            "destination": _repo_relpath(repo, dst),
        }
        for src, dst in operations
    ]

    if not operations:
        result["reason"] = "no_legacy_dirs_found"
        return result

    result["status"] = "planned"
    if dry_run or not apply:
        result["reason"] = "dry_run"
        return result

    if mode == "copy":
        _apply_copy_directories(operations, stop_at=client_root)
    else:
        _apply_move_directories_with_rollback(operations, stop_at=client_root)

    result["applied"] = True
    result["status"] = "applied"
    result["reason"] = "applied"
    return result


def migrate_legacy_pending_to_receipt(
    repo_root: Path,
    mode: Literal["copy", "move"] = "copy",
    apply: bool = False,
    dry_run: bool = True,
) -> dict:
    repo = Path(repo_root)
    _validate_options(mode, apply, dry_run)

    legacy_pending_dir = repo / "lexicon" / "pending"
    receipt_pending_dir = repo / "lexicon" / "receipt" / "pending"

    result = {
        "kind": "legacy_pending_to_receipt",
        "mode": mode,
        "apply": bool(apply),
        "dry_run": bool(dry_run),
        "applied": False,
        "source_root": _repo_relpath(repo, legacy_pending_dir),
        "destination_root": _repo_relpath(repo, receipt_pending_dir),
        "operations": [],
        "skipped_entries": [],
        "skipped_locks": [],
        "status": "noop",
        "reason": "",
    }

    if not legacy_pending_dir.exists():
        result["reason"] = "legacy_pending_not_found"
        return result

    files_to_migrate: list[Path] = []
    skipped_entries: list[str] = []
    skipped_locks: list[str] = []

    for entry in sorted(legacy_pending_dir.iterdir(), key=lambda p: p.name):
        if entry.is_dir():
            if entry.name == "locks":
                for nested in sorted(entry.rglob("*"), key=lambda p: p.as_posix()):
                    if not nested.is_file():
                        continue
                    if nested.name == PENDING_LOCK_FILE:
                        skipped_locks.append(_repo_relpath(repo, nested))
                    else:
                        skipped_entries.append(_repo_relpath(repo, nested))
                continue
            skipped_entries.append(_repo_relpath(repo, entry))
            continue

        if entry.name == PENDING_LOCK_FILE:
            skipped_locks.append(_repo_relpath(repo, entry))
            continue
        if entry.name in PENDING_QUEUE_FILES or _is_pending_marker_file(entry.name):
            files_to_migrate.append(entry)
            continue
        skipped_entries.append(_repo_relpath(repo, entry))

    operations = [(src, receipt_pending_dir / src.name) for src in files_to_migrate]
    result["operations"] = [
        {
            "source": _repo_relpath(repo, src),
            "destination": _repo_relpath(repo, dst),
        }
        for src, dst in operations
    ]
    result["skipped_entries"] = skipped_entries
    result["skipped_locks"] = skipped_locks

    if not operations:
        result["reason"] = "no_migratable_files_found"
        return result

    existing_dest_queue: list[str] = []
    for name in PENDING_QUEUE_FILES:
        path = receipt_pending_dir / name
        if path.exists():
            existing_dest_queue.append(_repo_relpath(repo, path))
    if receipt_pending_dir.exists():
        for marker_path in sorted(receipt_pending_dir.glob(f"{PENDING_MARKER_PREFIX}*{PENDING_MARKER_SUFFIX}")):
            if marker_path.is_file():
                existing_dest_queue.append(_repo_relpath(repo, marker_path))
    if existing_dest_queue:
        joined = ", ".join(existing_dest_queue)
        raise MigrationSafetyError(f"destination queue already exists: {joined}")

    result["status"] = "planned"
    if dry_run or not apply:
        result["reason"] = "dry_run"
        return result

    if mode == "copy":
        _apply_copy_files(operations, stop_at=repo / "lexicon")
    else:
        _apply_move_files_with_rollback(operations, stop_at=repo / "lexicon")

    result["applied"] = True
    result["status"] = "applied"
    result["reason"] = "applied"
    return result
