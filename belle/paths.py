# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from secrets import token_hex
from typing import Optional, Tuple


def get_client_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id


def get_client_config_dir(repo_root: Path, client_id: str) -> Path:
    return get_client_root(repo_root, client_id) / "config"


def get_category_overrides_path(repo_root: Path, client_id: str) -> Path:
    return get_client_config_dir(repo_root, client_id) / "category_overrides.json"


def get_outputs_dir(repo_root: Path, client_id: str) -> Path:
    return get_client_root(repo_root, client_id) / "outputs"


def get_outputs_runs_dir(repo_root: Path, client_id: str) -> Path:
    return get_outputs_dir(repo_root, client_id) / "runs"


def get_latest_path(repo_root: Path, client_id: str) -> Path:
    return get_outputs_dir(repo_root, client_id) / "LATEST.txt"


def get_artifacts_root(repo_root: Path, client_id: str) -> Path:
    return get_client_root(repo_root, client_id) / "artifacts"


def get_artifacts_cache_dir(repo_root: Path, client_id: str) -> Path:
    return get_artifacts_root(repo_root, client_id) / "cache"


def get_artifacts_ingest_dir(repo_root: Path, client_id: str) -> Path:
    return get_artifacts_root(repo_root, client_id) / "ingest"


def get_artifacts_telemetry_dir(repo_root: Path, client_id: str) -> Path:
    return get_artifacts_root(repo_root, client_id) / "telemetry"


def get_client_cache_path(repo_root: Path, client_id: str) -> Path:
    return get_artifacts_cache_dir(repo_root, client_id) / "client_cache.json"


def get_ledger_ref_ingested_path(repo_root: Path, client_id: str) -> Path:
    return get_artifacts_ingest_dir(repo_root, client_id) / "ledger_ref_ingested.json"


def get_ledger_train_ingested_path(repo_root: Path, client_id: str) -> Path:
    return get_artifacts_ingest_dir(repo_root, client_id) / "ledger_train_ingested.json"


def get_lexicon_pending_dir(repo_root: Path) -> Path:
    return repo_root / "lexicon" / "pending"


def get_lexicon_pending_locks_dir(repo_root: Path) -> Path:
    return get_lexicon_pending_dir(repo_root) / "locks"


def get_label_queue_lock_path(repo_root: Path) -> Path:
    return get_lexicon_pending_locks_dir(repo_root) / "label_queue.lock"


def generate_run_id(*, now: Optional[datetime] = None) -> str:
    ts = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    suffix = token_hex(2).upper()
    return f"{ts}_{suffix}"


def make_run_dir(
    repo_root: Path,
    client_id: str,
    run_id: Optional[str] = None,
) -> Tuple[str, Path]:
    runs_dir = get_outputs_runs_dir(repo_root, client_id)
    runs_dir.mkdir(parents=True, exist_ok=True)

    if run_id:
        run_dir = runs_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=False)
        return run_id, run_dir

    for _ in range(16):
        candidate = generate_run_id()
        run_dir = runs_dir / candidate
        try:
            run_dir.mkdir(parents=True, exist_ok=False)
            return candidate, run_dir
        except FileExistsError:
            continue
    raise RuntimeError("Could not allocate unique RUN_ID after multiple attempts.")


def ensure_client_system_dirs(repo_root: Path, client_id: str) -> None:
    get_outputs_runs_dir(repo_root, client_id).mkdir(parents=True, exist_ok=True)
    get_artifacts_cache_dir(repo_root, client_id).mkdir(parents=True, exist_ok=True)
    get_artifacts_ingest_dir(repo_root, client_id).mkdir(parents=True, exist_ok=True)
    get_artifacts_telemetry_dir(repo_root, client_id).mkdir(parents=True, exist_ok=True)
