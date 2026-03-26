from __future__ import annotations

import json
import locale
import os
import re
import subprocess
import sys
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

JST = timezone(timedelta(hours=9))
RUN_ID_PREFIX_RE = re.compile(r"^(?P<stamp>\d{8}T\d{6}Z)")


@dataclass(frozen=True)
class CollectResult:
    ok: bool
    status: str
    message: str
    zip_path: str
    latest_path: str
    stdout: str
    stderr: str
    exact_match: bool
    included_run_refs: list[str]
    session_run_refs: list[str]
    extra_run_refs: list[str]
    missing_run_refs: list[str]


def source_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def collect_script_path(root: Path | None = None) -> Path:
    current_root = root or source_repo_root()
    return current_root / ".agents" / "skills" / "collect-outputs" / "scripts" / "collect_outputs.py"


def _command_env() -> dict[str, str]:
    env = os.environ.copy()
    source_root = str(source_repo_root())
    current_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = source_root if not current_pythonpath else f"{source_root}{os.pathsep}{current_pythonpath}"
    return env


def _parse_run_id_utc(run_id: str) -> datetime | None:
    match = RUN_ID_PREFIX_RE.match(run_id)
    if not match:
        return None
    return datetime.strptime(match.group("stamp"), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def _parse_session_timestamp(raw: str) -> datetime | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _derive_date_and_time(run_results: list[dict[str, object]], session_started_at_utc: str, session_finished_at_utc: str) -> tuple[str, str]:
    run_times = []
    for result in run_results:
        run_id = str(result.get("run_id") or "").strip()
        parsed = _parse_run_id_utc(run_id)
        if parsed is not None:
            run_times.append(parsed.astimezone(JST))

    if not run_times:
        started = _parse_session_timestamp(session_started_at_utc)
        finished = _parse_session_timestamp(session_finished_at_utc) or started
        if started is None:
            now_jst = datetime.now(JST).replace(second=0, microsecond=0)
            run_times = [now_jst]
        else:
            run_times = [started.astimezone(JST), (finished or started).astimezone(JST)]

    date_text = run_times[0].date().isoformat()
    start = min(run_times)
    end = max(run_times)
    time_text = f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}"
    return date_text, time_text


def _session_run_refs(client_id: str, run_results: list[dict[str, object]]) -> list[str]:
    refs = []
    for result in run_results:
        run_id = str(result.get("run_id") or "").strip()
        if run_id:
            refs.append(f"{client_id}:{run_id}")
    return sorted(set(refs))


def _manifest_included_run_refs(manifest_obj: dict[str, object]) -> list[str]:
    summary = manifest_obj.get("summary")
    if not isinstance(summary, dict):
        return []

    line_id = manifest_obj.get("line_id")
    if line_id == "all":
        lines = summary.get("lines")
        refs: list[str] = []
        if isinstance(lines, dict):
            for line_summary in lines.values():
                if isinstance(line_summary, dict):
                    included = line_summary.get("included_run_ids")
                    if isinstance(included, list):
                        refs.extend(str(item) for item in included)
        return sorted(set(refs))

    included = summary.get("included_run_ids")
    if isinstance(included, list):
        return sorted(set(str(item) for item in included))
    return []


def _load_manifest_from_zip(zip_path: Path) -> dict[str, object]:
    with zipfile.ZipFile(zip_path, mode="r") as zf:
        return json.loads(zf.read("MANIFEST.json").decode("utf-8"))


def _zip_message(base_message: str, zip_path: str) -> str:
    zip_name = Path(str(zip_path)).name if zip_path else ""
    if not zip_name:
        return base_message
    return f"{base_message} ファイル名: {zip_name}"


def build_collect_command(
    *,
    client_id: str,
    run_results: list[dict[str, object]],
    session_started_at_utc: str,
    session_finished_at_utc: str,
    root: Path | None = None,
) -> list[str]:
    current_root = root or source_repo_root()
    line_ids = sorted({str(result.get("line_id") or "").strip() for result in run_results if result.get("line_id")})
    line_arg = line_ids[0] if len(line_ids) == 1 else "all"
    date_text, time_text = _derive_date_and_time(run_results, session_started_at_utc, session_finished_at_utc)
    return [
        sys.executable,
        str(collect_script_path(current_root)),
        "--client",
        client_id,
        "--line",
        line_arg,
        "--date",
        date_text,
        "--time",
        time_text,
        "--yes",
    ]


def overall_result_title(run_results: list[dict[str, object]]) -> str:
    statuses = {str(result.get("status") or "") for result in run_results}
    if "failure" in statuses:
        return "処理を完了できませんでした"
    if "needs_review" in statuses:
        return "処理は完了しましたが、確認が必要です"
    return "処理が完了しました"


def run_collect(
    *,
    client_id: str,
    run_results: list[dict[str, object]],
    session_started_at_utc: str,
    session_finished_at_utc: str,
    root: Path | None = None,
) -> CollectResult:
    current_root = root or source_repo_root()
    command = build_collect_command(
        client_id=client_id,
        run_results=run_results,
        session_started_at_utc=session_started_at_utc,
        session_finished_at_utc=session_finished_at_utc,
        root=current_root,
    )
    proc = subprocess.run(
        command,
        cwd=current_root,
        env=_command_env(),
        capture_output=True,
        text=True,
        encoding=locale.getpreferredencoding(False),
        errors="replace",
        timeout=120,
    )
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    zip_path = ""
    latest_path = ""
    for line in stdout.splitlines():
        if line.startswith("[OK] ZIP: "):
            zip_path = line.split(": ", 1)[1].strip()
        elif line.startswith("[OK] LATEST: "):
            latest_path = line.split(": ", 1)[1].strip()

    if proc.returncode != 0 or not zip_path:
        return CollectResult(
            ok=False,
            status="error",
            message="成果物ZIPを作成できませんでした。",
            zip_path=zip_path,
            latest_path=latest_path,
            stdout=stdout,
            stderr=stderr,
            exact_match=False,
            included_run_refs=[],
            session_run_refs=_session_run_refs(client_id, run_results),
            extra_run_refs=[],
            missing_run_refs=[],
        )

    manifest_obj = _load_manifest_from_zip(Path(zip_path))
    included_run_refs = _manifest_included_run_refs(manifest_obj)
    session_run_refs = _session_run_refs(client_id, run_results)
    included_set = set(included_run_refs)
    session_set = set(session_run_refs)
    extra_run_refs = sorted(included_set - session_set)
    missing_run_refs = sorted(session_set - included_set)
    exact_match = included_set == session_set

    if missing_run_refs:
        status = "error"
        message = "成果物ZIPを作成できませんでした。"
        ok = False
    elif exact_match:
        status = "success"
        message = _zip_message("成果物ZIPを作成しました。", zip_path)
        ok = True
    else:
        status = "warning"
        message = _zip_message("ZIPに今回以外の成果物が含まれている可能性があります。", zip_path)
        ok = True

    return CollectResult(
        ok=ok,
        status=status,
        message=message,
        zip_path=zip_path,
        latest_path=latest_path,
        stdout=stdout,
        stderr=stderr,
        exact_match=exact_match,
        included_run_refs=included_run_refs,
        session_run_refs=session_run_refs,
        extra_run_refs=extra_run_refs,
        missing_run_refs=missing_run_refs,
    )


def serialize_collect_result(result: CollectResult) -> dict[str, object]:
    return asdict(result)
