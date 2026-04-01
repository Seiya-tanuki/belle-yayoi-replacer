from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from belle.local_ui.state import LINE_ORDER, normalize_selected_lines
from belle.ui_reason_codes import (
    RUN_OK,
    parse_ui_reason_from_text,
    precheck_reason_code_for,
    run_failure_reason_code_for,
    run_needs_review_reason_code_for,
)

STATUS_LABELS = {
    "RUN": "準備OK",
    "SKIP": "今回は対象ファイルがありません",
    "FAIL": "このままでは進めません",
}

EXECUTION_LABELS = {
    "success": "処理が完了しました",
    "needs_review": "処理は完了しましたが、確認が必要です",
    "failure": "処理を完了できませんでした",
}

PLAN_LINE_RE = re.compile(
    r"^- (?P<line_id>[a-z_]+): (?P<status>RUN|SKIP|FAIL) \((?P<reason>.+)\) target=\[(?P<targets>.*)\]$"
)


@dataclass(frozen=True)
class PrecheckResult:
    line_id: str
    status: str
    status_label: str
    ui_reason_code: str
    ui_reason_detail: dict[str, object]
    reason: str
    target_files: list[str]
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class RunResult:
    line_id: str
    status: str
    status_label: str
    ui_reason_code: str
    ui_reason_detail: dict[str, object]
    returncode: int
    stdout: str
    stderr: str
    run_id: str
    run_dir: str
    run_manifest: str
    changed_ratio: str


def source_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def replacer_script_path(root: Path | None = None) -> Path:
    current_root = root or source_repo_root()
    return current_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"


def build_replacer_command(
    client_id: str,
    line_id: str,
    *,
    root: Path | None = None,
    dry_run: bool = False,
    confirm_yes: bool = False,
) -> list[str]:
    command = [
        sys.executable,
        str(replacer_script_path(root)),
        "--client",
        client_id,
        "--line",
        line_id,
    ]
    if dry_run:
        command.append("--dry-run")
    if confirm_yes:
        command.append("--yes")
    return command


def normalized_line_order(selected_lines: list[str]) -> list[str]:
    return normalize_selected_lines(selected_lines)


def _command_env() -> dict[str, str]:
    env = os.environ.copy()
    source_root = str(source_repo_root())
    current_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = source_root if not current_pythonpath else f"{source_root}{os.pathsep}{current_pythonpath}"
    return env


def _run_command(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=_command_env(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
    )


def parse_plan_output(stdout: str, *, returncode: int, stderr: str = "") -> list[PrecheckResult]:
    results: list[PrecheckResult] = []
    for line in stdout.splitlines():
        match = PLAN_LINE_RE.match(line.strip())
        if not match:
            continue
        raw_status = match.group("status")
        targets = match.group("targets").strip()
        target_files = [] if targets in {"", "-"} else [part.strip() for part in targets.split(",") if part.strip()]
        results.append(
            PrecheckResult(
                line_id=match.group("line_id"),
                status=raw_status,
                status_label=STATUS_LABELS[raw_status],
                ui_reason_code=precheck_reason_code_for(match.group("line_id"), raw_status, match.group("reason")),
                ui_reason_detail={"phase": "plan", "status": raw_status, "reason": match.group("reason")},
                reason=match.group("reason"),
                target_files=target_files,
                returncode=returncode,
                stdout=stdout,
                stderr=stderr,
            )
        )
    return results


def parse_run_output(stdout: str, *, line_id: str, returncode: int, stderr: str = "") -> RunResult:
    run_id = ""
    run_dir = ""
    run_manifest = ""
    changed_ratio = ""

    for line in stdout.splitlines():
        stripped = line.strip()
        if "[OK] client=" in stripped and " run_id=" in stripped:
            run_id = stripped.split(" run_id=", 1)[1].split()[0]
        elif stripped.startswith("[OK] run_dir="):
            run_dir = stripped.split("=", 1)[1]
        elif stripped.startswith("[OK] run_manifest="):
            run_manifest = stripped.split("=", 1)[1]
        elif "changed_ratio=" in stripped:
            changed_ratio = stripped.split("changed_ratio=", 1)[1].split()[0]

    parsed_reason = parse_ui_reason_from_text(stdout, line_id=line_id) or parse_ui_reason_from_text(stderr, line_id=line_id)
    if returncode == 0:
        status = "success"
        fallback_code = RUN_OK
    elif returncode == 2:
        status = "needs_review"
        fallback_code = run_needs_review_reason_code_for(line_id)
    else:
        status = "failure"
        fallback_code = run_failure_reason_code_for(line_id, f"{stdout}\n{stderr}")

    if parsed_reason is not None:
        ui_reason_code, ui_reason_detail = parsed_reason
    else:
        ui_reason_code, ui_reason_detail = fallback_code, {}

    return RunResult(
        line_id=line_id,
        status=status,
        status_label=EXECUTION_LABELS[status],
        ui_reason_code=ui_reason_code,
        ui_reason_detail=ui_reason_detail,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        run_id=run_id,
        run_dir=run_dir,
        run_manifest=run_manifest,
        changed_ratio=changed_ratio,
    )


def run_precheck_for_lines(client_id: str, selected_lines: list[str], *, root: Path | None = None) -> list[PrecheckResult]:
    current_root = root or source_repo_root()
    results: list[PrecheckResult] = []
    for line_id in normalized_line_order(selected_lines):
        proc = _run_command(
            build_replacer_command(client_id, line_id, root=current_root, dry_run=True),
            cwd=current_root,
        )
        results.extend(parse_plan_output(proc.stdout, returncode=proc.returncode, stderr=proc.stderr))
    return results


def run_selected_lines(client_id: str, selected_lines: list[str], *, root: Path | None = None) -> list[RunResult]:
    current_root = root or source_repo_root()
    results: list[RunResult] = []
    for line_id in normalized_line_order(selected_lines):
        proc = _run_command(
            build_replacer_command(client_id, line_id, root=current_root, confirm_yes=True),
            cwd=current_root,
        )
        results.append(parse_run_output(proc.stdout, line_id=line_id, returncode=proc.returncode, stderr=proc.stderr))
    return results


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def serialize_precheck_results(results: list[PrecheckResult]) -> list[dict[str, object]]:
    return [asdict(result) for result in results]


def serialize_run_results(results: list[RunResult]) -> list[dict[str, object]]:
    return [asdict(result) for result in results]
