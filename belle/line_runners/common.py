# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import NoReturn

from belle.application.errors import LineRunnerFailure
from belle.application.models import LinePlan
from belle.ingest import list_discoverable_files
from belle.paths import get_client_root
from belle.ui_reason_codes import RUN_FAIL_UNKNOWN

def build_line_plan(
    *,
    line_id: str,
    status: str,
    reason: str,
    reason_key: str,
    ui_reason_code: str,
    target_files: list[str],
    run_failure_ui_reason_code: str = RUN_FAIL_UNKNOWN,
    details: dict[str, object] | None = None,
) -> LinePlan:
    return LinePlan(
        line_id=line_id,
        status=status,
        reason=reason,
        reason_key=reason_key,
        target_files=tuple(target_files),
        ui_reason_code=ui_reason_code,
        ui_reason_detail={"phase": "plan", "status": status, "reason": reason, "reason_key": reason_key},
        run_failure_ui_reason_code=run_failure_ui_reason_code,
        details=dict(details or {}),
    )


def list_input_files(
    dir_path: Path,
    *,
    allowed_extensions: set[str] | None = None,
) -> list[Path]:
    return list_discoverable_files(dir_path, allowed_extensions=allowed_extensions)


def resolve_client_layout(
    repo_root: Path,
    client_id: str,
    line_id: str,
) -> tuple[str, Path]:
    line_dir = get_client_root(repo_root, client_id, line_id=line_id)
    if line_dir.exists():
        return line_id, line_dir
    raise FileNotFoundError(f"client dir not found: {line_dir}")


def compute_target_file_status(client_dir: Path) -> tuple[str, str, str, list[str]]:
    input_files = list_input_files(
        client_dir / "inputs" / "kari_shiwake",
        allowed_extensions={".csv"},
    )
    target_names = [p.name for p in input_files]
    if not input_files:
        return "SKIP", "no_target_input", "no target input", target_names
    if len(input_files) >= 2:
        return "FAIL", "multiple_target_inputs", "multiple target inputs", target_names
    return "OK", "single_target_input", "single target input", target_names


def raise_line_runner_failure(
    *,
    line_id: str,
    message: str,
    failure_key: str,
    ui_reason_code: str,
    detail: dict[str, object] | None = None,
) -> NoReturn:
    ui_reason_detail: dict[str, object] = {
        "phase": "run",
        "status": "failure",
        "failure_key": failure_key,
    }
    if detail:
        ui_reason_detail.update(dict(detail))
    raise LineRunnerFailure(
        line_id=line_id,
        message=message,
        failure_key=failure_key,
        ui_reason_code=ui_reason_code,
        ui_reason_detail=ui_reason_detail,
    )
