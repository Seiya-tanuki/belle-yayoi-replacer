from __future__ import annotations

import locale
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
    SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID,
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

SESSION_FATAL_REASON = (
    "想定外の問題が発生したため、このまま進めません。"
    "システムを再起動するため、まずコマンドプロンプト"
    "（システム起動時に表示されるテキストだけの黒い画面）を右上のバツボタンを押して終了してください。"
    "さらにこのブラウザも終了し、その後改めてデスクトップからシステムを起動してください。"
)
SESSION_FATAL_DETAIL_TEXT = (
    "想定外の問題が発生したため、今回の処理は完了できませんでした。\n"
    "システムを再起動するため、まずコマンドプロンプト"
    "（システム起動時に表示されるテキストだけの黒い画面）を右上のバツボタンを押して終了してください。\n"
    "さらにこのブラウザも終了し、その後改めてデスクトップからシステムを起動してください。\n"
    "再起動後も同じエラーコードが表示される場合は、管理者に連絡してください。"
)

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


class SessionFatalError(RuntimeError):
    def __init__(
        self,
        *,
        phase: str,
        line_id: str,
        command: list[str],
        returncode: int | None,
        stdout: str | None,
        stderr: str | None,
        raw_error: str,
    ) -> None:
        normalized_stdout = "" if stdout is None else str(stdout)
        normalized_stderr = "" if stderr is None else str(stderr)
        message = str(raw_error or "subprocess output invalid")
        super().__init__(message)
        self.phase = str(phase)
        self.line_id = str(line_id)
        self.command = [str(part) for part in command]
        self.returncode = returncode
        self.stdout = normalized_stdout
        self.stderr = normalized_stderr
        self.raw_error = message
        self.ui_reason_code = SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID
        self.detail: dict[str, object] = {
            "phase": self.phase,
            "origin_line_id": self.line_id,
            "command": list(self.command),
            "returncode": self.returncode,
            "stdout_was_none": stdout is None,
            "stderr_was_none": stderr is None,
            "raw_error": self.raw_error,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }


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
        encoding=locale.getpreferredencoding(False),
        errors="replace",
        timeout=120,
    )


def parse_plan_output(stdout: str, *, returncode: int, stderr: str = "") -> list[PrecheckResult]:
    stdout = str(stdout or "")
    stderr = str(stderr or "")
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
    stdout = str(stdout or "")
    stderr = str(stderr or "")
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
        command = build_replacer_command(client_id, line_id, root=current_root, dry_run=True)
        try:
            proc = _run_command(
                command,
                cwd=current_root,
            )
        except Exception as exc:
            raise SessionFatalError(
                phase="precheck",
                line_id=line_id,
                command=command,
                returncode=None,
                stdout=None,
                stderr=None,
                raw_error=f"precheck subprocess failed before output parsing: {exc}",
            ) from exc
        parsed = parse_plan_output(proc.stdout, returncode=proc.returncode, stderr=proc.stderr)
        if proc.stdout is None or not parsed:
            raise SessionFatalError(
                phase="precheck",
                line_id=line_id,
                command=command,
                returncode=proc.returncode,
                stdout=proc.stdout,
                stderr=proc.stderr,
                raw_error="precheck output did not contain any parseable PLAN lines",
            )
        results.extend(parsed)
    return results


def run_selected_lines(client_id: str, selected_lines: list[str], *, root: Path | None = None) -> list[RunResult]:
    current_root = root or source_repo_root()
    results: list[RunResult] = []
    for line_id in normalized_line_order(selected_lines):
        command = build_replacer_command(client_id, line_id, root=current_root, confirm_yes=True)
        try:
            proc = _run_command(
                command,
                cwd=current_root,
            )
        except Exception as exc:
            raise SessionFatalError(
                phase="run",
                line_id=line_id,
                command=command,
                returncode=None,
                stdout=None,
                stderr=None,
                raw_error=f"run subprocess failed before output parsing: {exc}",
            ) from exc
        result = parse_run_output(proc.stdout, line_id=line_id, returncode=proc.returncode, stderr=proc.stderr)
        missing_success_markers = proc.returncode in {0, 2} and (
            not result.run_id or not result.run_dir or not result.run_manifest
        )
        if proc.stdout is None or missing_success_markers:
            raise SessionFatalError(
                phase="run",
                line_id=line_id,
                command=command,
                returncode=proc.returncode,
                stdout=proc.stdout,
                stderr=proc.stderr,
                raw_error="run output did not contain required success markers",
            )
        results.append(result)
    return results


def session_fatal_payload(error: SessionFatalError) -> dict[str, object]:
    return {
        "ui_reason_code": error.ui_reason_code,
        "detail": dict(error.detail),
        "user_message": SESSION_FATAL_DETAIL_TEXT,
    }


def build_session_fatal_precheck_results(
    selected_lines: list[str],
    *,
    error: SessionFatalError,
) -> list[PrecheckResult]:
    log_text = f"{SESSION_FATAL_DETAIL_TEXT}\n\nエラーコード: {error.ui_reason_code}"
    return [
        PrecheckResult(
            line_id=line_id,
            status="FAIL",
            status_label=STATUS_LABELS["FAIL"],
            ui_reason_code=error.ui_reason_code,
            ui_reason_detail=dict(error.detail),
            reason=SESSION_FATAL_REASON,
            target_files=[],
            returncode=int(error.returncode or 1),
            stdout=log_text,
            stderr="",
        )
        for line_id in normalized_line_order(selected_lines)
    ]


def build_session_fatal_run_results(
    selected_lines: list[str],
    *,
    error: SessionFatalError,
) -> list[RunResult]:
    return [
        RunResult(
            line_id=line_id,
            status="failure",
            status_label=EXECUTION_LABELS["failure"],
            ui_reason_code=error.ui_reason_code,
            ui_reason_detail=dict(error.detail),
            returncode=int(error.returncode or 1),
            stdout="",
            stderr="",
            run_id="",
            run_dir="",
            run_manifest="",
            changed_ratio="",
        )
        for line_id in normalized_line_order(selected_lines)
    ]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def serialize_precheck_results(results: list[PrecheckResult]) -> list[dict[str, object]]:
    return [asdict(result) for result in results]


def serialize_run_results(results: list[RunResult]) -> list[dict[str, object]]:
    return [asdict(result) for result in results]
