# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

from belle.application.models import LinePlan
from belle.ingest import list_discoverable_files
from belle.paths import get_client_root
from belle.ui_reason_codes import precheck_reason_code_for

def build_line_plan(
    *,
    line_id: str,
    status: str,
    reason: str,
    target_files: list[str],
    details: dict[str, object] | None = None,
) -> LinePlan:
    return LinePlan(
        line_id=line_id,
        status=status,
        reason=reason,
        target_files=tuple(target_files),
        ui_reason_code=precheck_reason_code_for(line_id, status, reason),
        ui_reason_detail={"phase": "plan", "status": status, "reason": reason},
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


def compute_target_file_status(client_dir: Path) -> tuple[str, str, list[str]]:
    input_files = list_input_files(
        client_dir / "inputs" / "kari_shiwake",
        allowed_extensions={".csv"},
    )
    target_names = [p.name for p in input_files]
    if not input_files:
        return "SKIP", "no target input", target_names
    if len(input_files) >= 2:
        return "FAIL", "multiple target inputs", target_names
    return "OK", "single target input", target_names
