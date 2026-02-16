# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from secrets import token_hex
from typing import Any, Dict, Optional, Tuple


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


def get_ledger_ref_ingest_dir(repo_root: Path, client_id: str) -> Path:
    return get_artifacts_ingest_dir(repo_root, client_id) / "ledger_ref"


def get_kari_shiwake_ingest_dir(repo_root: Path, client_id: str) -> Path:
    return get_artifacts_ingest_dir(repo_root, client_id) / "kari_shiwake"


def get_kari_shiwake_ingested_path(repo_root: Path, client_id: str) -> Path:
    return get_artifacts_ingest_dir(repo_root, client_id) / "kari_shiwake_ingested.json"


def get_lexicon_pending_dir(repo_root: Path) -> Path:
    return repo_root / "lexicon" / "pending"


def get_lexicon_pending_locks_dir(repo_root: Path) -> Path:
    return get_lexicon_pending_dir(repo_root) / "locks"


def get_label_queue_lock_path(repo_root: Path) -> Path:
    return get_lexicon_pending_locks_dir(repo_root) / "label_queue.lock"


def build_input_artifact_prefix(*, in_path: Path, input_index: int, run_id: str) -> str:
    idx = int(input_index)
    if idx < 1:
        raise ValueError(f"input_index must be >= 1, got {input_index}")
    rid = str(run_id).strip()
    if not rid:
        raise ValueError("run_id must be non-empty")
    return f"{in_path.stem}_{idx:02d}_{rid}"


def get_review_report_path(run_dir: Path, artifact_prefix: str) -> Path:
    return run_dir / f"{artifact_prefix}_review_report.csv"


def get_input_manifest_path(run_dir: Path, artifact_prefix: str) -> Path:
    return run_dir / f"{artifact_prefix}_manifest.json"


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
    get_ledger_ref_ingest_dir(repo_root, client_id).mkdir(parents=True, exist_ok=True)
    get_kari_shiwake_ingest_dir(repo_root, client_id).mkdir(parents=True, exist_ok=True)
    get_artifacts_telemetry_dir(repo_root, client_id).mkdir(parents=True, exist_ok=True)


def resolve_ledger_ref_stored_path(repo_root: Path, client_id: str, entry: Dict[str, Any]) -> Optional[Path]:
    client_root = get_client_root(repo_root, client_id)
    stored_relpath = str(entry.get("stored_relpath") or "").strip()
    if stored_relpath:
        return client_root / Path(stored_relpath)

    stored_name = str(entry.get("stored_name") or "").strip()
    if not stored_name:
        return None

    ingest_candidate = get_ledger_ref_ingest_dir(repo_root, client_id) / stored_name
    if ingest_candidate.exists():
        return ingest_candidate

    legacy_candidate = client_root / "inputs" / "ledger_ref" / stored_name
    if legacy_candidate.exists():
        return legacy_candidate

    return ingest_candidate
