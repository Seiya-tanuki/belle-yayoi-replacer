from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Sequence

from belle.lines import validate_line_id
from belle.ui_reason_codes import (
    COLLECT_FAIL_MISSING_RUN_REFS,
    COLLECT_FAIL_NO_RUNS_FOUND,
    COLLECT_OK_EXACT,
    COLLECT_WARN_EXTRA_RUNS_INCLUDED,
)

JST = timezone(timedelta(hours=9))
MANIFEST_SCHEMA = "belle.collect_outputs_manifest.v1"
RUN_ID_PREFIX_FORMAT = "%Y%m%dT%H%M%SZ"
ALL_LINE_ID = "all"
ALL_MODE_LINE_ORDER = ("receipt", "bank_statement", "credit_card_statement")
LINE_ARG_CHOICES = (*ALL_MODE_LINE_ORDER, ALL_LINE_ID)
CollectPlanStatus = Literal["ready", "skip", "error"]
CollectResultStatus = Literal["success", "error"]


def _copy_dict(detail: dict[str, Any] | None) -> dict[str, Any]:
    if not detail:
        return {}
    return dict(detail)


@dataclass(frozen=True, slots=True)
class CollectRequest:
    line_id: str = ALL_LINE_ID
    target_jst_date: str = ""
    client_ids: tuple[str, ...] = ()
    time_range: str = ""
    requested_run_refs: tuple[str, ...] = ()
    expected_run_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "line_id", str(self.line_id or "").strip().lower() or ALL_LINE_ID)
        object.__setattr__(self, "target_jst_date", str(self.target_jst_date or "").strip())
        object.__setattr__(
            self,
            "client_ids",
            tuple(str(client_id or "").strip() for client_id in self.client_ids if str(client_id or "").strip()),
        )
        object.__setattr__(self, "time_range", str(self.time_range or "").strip())
        object.__setattr__(
            self,
            "requested_run_refs",
            tuple(str(run_ref or "").strip() for run_ref in self.requested_run_refs if str(run_ref or "").strip()),
        )
        object.__setattr__(
            self,
            "expected_run_refs",
            tuple(str(run_ref or "").strip() for run_ref in self.expected_run_refs if str(run_ref or "").strip()),
        )


@dataclass(frozen=True, slots=True)
class CollectRunPreview:
    client_id: str
    run_id: str
    layout: str
    replaced_count: int
    report_count: int
    manifest_count: int


@dataclass(frozen=True, slots=True)
class CollectLinePlan:
    line_id: str
    matched_count: int
    collected_count: int
    skipped_incomplete_count: int
    skipped_invalid_run_id_count: int
    collected_run_refs: tuple[str, ...] = ()
    collected_runs: tuple[CollectRunPreview, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "collected_run_refs", tuple(self.collected_run_refs))
        object.__setattr__(self, "collected_runs", tuple(self.collected_runs))


@dataclass(frozen=True, slots=True)
class CollectPlan:
    status: CollectPlanStatus
    ui_reason_code: str
    message: str
    line_id: str
    target_jst_date: str
    selected_client_ids: tuple[str, ...]
    filter_client_ids: tuple[str, ...]
    filter_time_range: str
    filter_mode: str
    requested_run_refs: tuple[str, ...] = ()
    expected_run_refs: tuple[str, ...] = ()
    missing_run_refs: tuple[str, ...] = ()
    lines: tuple[CollectLinePlan, ...] = ()
    _line_results: tuple["_LineCollectionResult", ...] = field(default_factory=tuple, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "selected_client_ids", tuple(self.selected_client_ids))
        object.__setattr__(self, "filter_client_ids", tuple(self.filter_client_ids))
        object.__setattr__(self, "requested_run_refs", tuple(self.requested_run_refs))
        object.__setattr__(self, "expected_run_refs", tuple(self.expected_run_refs))
        object.__setattr__(self, "missing_run_refs", tuple(self.missing_run_refs))
        object.__setattr__(self, "lines", tuple(self.lines))
        object.__setattr__(self, "_line_results", tuple(self._line_results))

    @property
    def can_collect(self) -> bool:
        return self.status == "ready"


@dataclass(frozen=True, slots=True)
class CollectResult:
    ok: bool
    status: CollectResultStatus
    ui_reason_code: str
    ui_reason_detail: dict[str, Any] = field(default_factory=dict)
    message: str = ""
    line_id: str = ""
    filter_mode: str = ""
    zip_path: str = ""
    latest_path: str = ""
    requested_run_refs: tuple[str, ...] = ()
    expected_run_refs: tuple[str, ...] = ()
    included_run_refs: tuple[str, ...] = ()
    missing_run_refs: tuple[str, ...] = ()
    extra_run_refs: tuple[str, ...] = ()
    exact_match: bool = False
    manifest: dict[str, Any] = field(default_factory=dict)
    manifest_summary: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ui_reason_detail", _copy_dict(self.ui_reason_detail))
        object.__setattr__(self, "requested_run_refs", tuple(self.requested_run_refs))
        object.__setattr__(self, "expected_run_refs", tuple(self.expected_run_refs))
        object.__setattr__(self, "included_run_refs", tuple(self.included_run_refs))
        object.__setattr__(self, "missing_run_refs", tuple(self.missing_run_refs))
        object.__setattr__(self, "extra_run_refs", tuple(self.extra_run_refs))
        object.__setattr__(self, "manifest", _copy_dict(self.manifest))
        object.__setattr__(self, "manifest_summary", _copy_dict(self.manifest_summary))


@dataclass(frozen=True, slots=True)
class _RunFiles:
    line_id: str
    client_id: str
    run_id: str
    run_dir: Path
    run_utc: datetime
    run_jst: datetime
    replaced_files: tuple[Path, ...]
    report_files: tuple[Path, ...]
    manifest_files: tuple[Path, ...]
    layout: str


@dataclass(frozen=True, slots=True)
class _LineCollectionResult:
    line_id: str
    matched_runs: tuple[_RunFiles, ...]
    collected_runs: tuple[_RunFiles, ...]
    skipped_incomplete_runs: tuple[_RunFiles, ...]
    skipped_invalid_run_id_count: int


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_iso(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _utc_compact(ts: datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M%SZ")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _repo_relpath(repo_root: Path, src: Path) -> str:
    return src.relative_to(repo_root).as_posix()


def normalize_client_ids(raw: str | Sequence[str] | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    tokens: Sequence[str] = raw.split(",") if isinstance(raw, str) else raw
    values: list[str] = []
    seen = set()
    for raw_value in tokens:
        for token in str(raw_value or "").split(","):
            client_id = token.strip()
            if not client_id or client_id in seen:
                continue
            values.append(client_id)
            seen.add(client_id)
    return tuple(values)


def split_run_ref(run_ref: str) -> tuple[str, str]:
    value = str(run_ref or "").strip()
    if ":" not in value:
        raise ValueError(f"invalid run_ref format: {value} (CLIENT_ID:RUN_ID)")
    client_id, run_id = value.split(":", 1)
    client_id = client_id.strip()
    run_id = run_id.strip()
    if not client_id or not run_id:
        raise ValueError(f"invalid run_ref format: {value} (CLIENT_ID:RUN_ID)")
    return client_id, run_id


def normalize_run_refs(raw_values: str | Sequence[str] | None) -> tuple[str, ...]:
    if raw_values is None:
        return ()
    tokens: Sequence[str] = [raw_values] if isinstance(raw_values, str) else raw_values
    values: list[str] = []
    seen = set()
    for raw in tokens:
        for token in str(raw or "").split(","):
            run_ref = token.strip()
            if not run_ref:
                continue
            client_id, run_id = split_run_ref(run_ref)
            normalized = f"{client_id}:{run_id}"
            if normalized in seen:
                continue
            values.append(normalized)
            seen.add(normalized)
    return tuple(values)


def parse_jst_date(raw: str | None, *, default_date: date) -> date:
    value = str(raw or "").strip()
    if not value:
        return default_date
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"invalid date format: {value} (YYYY-MM-DD)") from exc


def parse_time_range(raw: str | None) -> tuple[int, int, str] | None:
    value = str(raw or "").strip()
    if not value:
        return None
    parts = value.split("-")
    if len(parts) != 2:
        raise ValueError(f"invalid time range format: {value} (HH:MM-HH:MM)")
    start_hhmm = parts[0].strip()
    end_hhmm = parts[1].strip()
    start_min = _parse_hhmm(start_hhmm)
    end_min = _parse_hhmm(end_hhmm)
    if start_min > end_min:
        raise ValueError(f"invalid time range (start must be <= end): {value}")
    return start_min, end_min, f"{start_hhmm}-{end_hhmm}"


def _parse_hhmm(value: str) -> int:
    parts = value.split(":")
    if len(parts) != 2:
        raise ValueError(f"invalid time format: {value}")
    try:
        hh = int(parts[0])
        mm = int(parts[1])
    except ValueError as exc:
        raise ValueError(f"invalid time format: {value}") from exc
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise ValueError(f"time out of range: {value}")
    return hh * 60 + mm


def _parse_run_utc_from_run_id(run_id: str) -> datetime | None:
    prefix = run_id.split("_", 1)[0]
    try:
        parsed = datetime.strptime(prefix, RUN_ID_PREFIX_FORMAT)
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def _discover_clients(repo_root: Path) -> list[str]:
    clients_dir = repo_root / "clients"
    if not clients_dir.exists():
        return []
    clients: list[str] = []
    for path in sorted(clients_dir.iterdir(), key=lambda candidate: candidate.name):
        if not path.is_dir() or path.name == "TEMPLATE":
            continue
        clients.append(path.name)
    return clients


def _discover_run_roots_for_client(repo_root: Path, client_id: str, line_id: str) -> tuple[list[tuple[str, Path]], bool]:
    roots: list[tuple[str, Path]] = []
    line_root = repo_root / "clients" / client_id / "lines" / line_id / "outputs" / "runs"
    line_exists = line_root.exists()
    if line_exists:
        roots.append(("line", line_root))
    return roots, line_exists


def _build_run_files(client_id: str, line_id: str, run_dir: Path, layout: str) -> _RunFiles | None:
    run_id = run_dir.name
    run_utc = _parse_run_utc_from_run_id(run_id)
    if run_utc is None:
        return None

    manifest_paths: list[Path] = []
    run_manifest = run_dir / "run_manifest.json"
    if run_manifest.exists() and run_manifest.is_file():
        manifest_paths.append(run_manifest)
    for path in sorted(run_dir.glob("*_manifest.json")):
        if path == run_manifest or not path.is_file():
            continue
        manifest_paths.append(path)

    return _RunFiles(
        line_id=line_id,
        client_id=client_id,
        run_id=run_id,
        run_dir=run_dir,
        run_utc=run_utc,
        run_jst=run_utc.astimezone(JST),
        replaced_files=tuple(path for path in sorted(run_dir.glob("*_replaced_*.csv")) if path.is_file()),
        report_files=tuple(path for path in sorted(run_dir.glob("*_review_report.csv")) if path.is_file()),
        manifest_files=tuple(manifest_paths),
        layout=layout,
    )


def _discover_runs_for_client(
    repo_root: Path,
    client_id: str,
    *,
    line_id: str,
    target_date_jst: date,
    time_range: tuple[int, int, str] | None,
) -> tuple[list[_RunFiles], int, bool]:
    run_roots, line_exists = _discover_run_roots_for_client(repo_root, client_id, line_id)
    if not run_roots:
        return [], 0, line_exists

    matched: list[_RunFiles] = []
    invalid_run_id_count = 0
    for layout, runs_dir in run_roots:
        for run_dir in sorted(runs_dir.iterdir(), key=lambda candidate: candidate.name):
            if not run_dir.is_dir():
                continue
            run_files = _build_run_files(client_id, line_id, run_dir, layout)
            if run_files is None:
                invalid_run_id_count += 1
                continue

            run_jst = run_files.run_jst
            if run_jst.date() != target_date_jst:
                continue
            if time_range is not None:
                run_minutes = run_jst.hour * 60 + run_jst.minute
                start_min, end_min, _time_label = time_range
                if run_minutes < start_min or run_minutes > end_min:
                    continue
            matched.append(run_files)
    return matched, invalid_run_id_count, line_exists


def _collect_runs_for_line(
    *,
    repo_root: Path,
    line_id: str,
    selected_client_ids: Sequence[str],
    target_jst_date: date,
    time_range: tuple[int, int, str] | None,
) -> _LineCollectionResult:
    matched_runs: list[_RunFiles] = []
    invalid_run_id_count = 0
    for client_id in selected_client_ids:
        runs, invalid_count, _line_exists = _discover_runs_for_client(
            repo_root,
            client_id,
            line_id=line_id,
            target_date_jst=target_jst_date,
            time_range=time_range,
        )
        matched_runs.extend(runs)
        invalid_run_id_count += invalid_count

    matched_runs.sort(key=lambda row: (row.client_id, row.run_id, row.layout))
    skipped_incomplete_runs = [run for run in matched_runs if len(run.replaced_files) == 0]
    collected_runs = [run for run in matched_runs if len(run.replaced_files) > 0]
    return _LineCollectionResult(
        line_id=line_id,
        matched_runs=tuple(matched_runs),
        collected_runs=tuple(collected_runs),
        skipped_incomplete_runs=tuple(skipped_incomplete_runs),
        skipped_invalid_run_id_count=invalid_run_id_count,
    )


def _resolve_run_ref(repo_root: Path, run_ref: str, *, allowed_line_ids: Sequence[str]) -> _RunFiles | None:
    client_id, run_id = split_run_ref(run_ref)
    matches: list[_RunFiles] = []
    for line_id in allowed_line_ids:
        run_roots, _line_exists = _discover_run_roots_for_client(repo_root, client_id, line_id)
        for layout, runs_dir in run_roots:
            run_dir = runs_dir / run_id
            if not run_dir.is_dir():
                continue
            run_files = _build_run_files(client_id, line_id, run_dir, layout)
            if run_files is not None:
                matches.append(run_files)
    if len(matches) > 1:
        raise ValueError(f"ambiguous run_ref matched multiple runs: {run_ref}")
    if not matches:
        return None
    return matches[0]


def _collect_runs_for_run_refs(
    *,
    repo_root: Path,
    requested_run_refs: Sequence[str],
    allowed_line_ids: Sequence[str],
) -> tuple[list[_LineCollectionResult], list[str]]:
    grouped: dict[str, list[_RunFiles]] = {line_id: [] for line_id in allowed_line_ids}
    missing_run_refs: list[str] = []
    for run_ref in requested_run_refs:
        run_files = _resolve_run_ref(repo_root, run_ref, allowed_line_ids=allowed_line_ids)
        if run_files is None:
            missing_run_refs.append(run_ref)
            continue
        grouped.setdefault(run_files.line_id, []).append(run_files)

    line_results: list[_LineCollectionResult] = []
    for line_id in allowed_line_ids:
        matched_runs = sorted(grouped.get(line_id, []), key=lambda row: (row.client_id, row.run_id, row.layout))
        skipped_incomplete_runs = [run for run in matched_runs if len(run.replaced_files) == 0]
        collected_runs = [run for run in matched_runs if len(run.replaced_files) > 0]
        line_results.append(
            _LineCollectionResult(
                line_id=line_id,
                matched_runs=tuple(matched_runs),
                collected_runs=tuple(collected_runs),
                skipped_incomplete_runs=tuple(skipped_incomplete_runs),
                skipped_invalid_run_id_count=0,
            )
        )
    return line_results, missing_run_refs


def _collect_run_refs(runs: Sequence[_RunFiles]) -> list[str]:
    return [f"{run.client_id}:{run.run_id}" for run in runs]


def _to_line_plan(line_result: _LineCollectionResult) -> CollectLinePlan:
    collected_runs = tuple(
        CollectRunPreview(
            client_id=run.client_id,
            run_id=run.run_id,
            layout=run.layout,
            replaced_count=len(run.replaced_files),
            report_count=len(run.report_files),
            manifest_count=len(run.manifest_files),
        )
        for run in line_result.collected_runs
    )
    return CollectLinePlan(
        line_id=line_result.line_id,
        matched_count=len(line_result.matched_runs),
        collected_count=len(line_result.collected_runs),
        skipped_incomplete_count=len(line_result.skipped_incomplete_runs),
        skipped_invalid_run_id_count=line_result.skipped_invalid_run_id_count,
        collected_run_refs=tuple(_collect_run_refs(line_result.collected_runs)),
        collected_runs=collected_runs,
    )


def prepare_collect_plan(repo_root: Path, request: CollectRequest) -> CollectPlan:
    normalized_line_id = request.line_id or ALL_LINE_ID
    if normalized_line_id == ALL_LINE_ID:
        line_ids = list(ALL_MODE_LINE_ORDER)
    else:
        line_ids = [validate_line_id(normalized_line_id)]

    now_utc = _now_utc()
    default_jst_date = now_utc.astimezone(JST).date()
    requested_run_refs = normalize_run_refs(request.requested_run_refs)
    expected_run_refs = normalize_run_refs(request.expected_run_refs)

    if requested_run_refs:
        line_results, missing_run_refs = _collect_runs_for_run_refs(
            repo_root=repo_root,
            requested_run_refs=requested_run_refs,
            allowed_line_ids=line_ids,
        )
        target_jst_date = default_jst_date
        run_dates = [run.run_jst.date() for result in line_results for run in result.matched_runs]
        if run_dates:
            target_jst_date = min(run_dates)
        selected_client_ids = tuple(sorted({split_run_ref(run_ref)[0] for run_ref in requested_run_refs}))
        if missing_run_refs:
            return CollectPlan(
                status="error",
                ui_reason_code=COLLECT_FAIL_MISSING_RUN_REFS,
                message="requested run_refs not found: " + ", ".join(missing_run_refs),
                line_id=normalized_line_id,
                target_jst_date=target_jst_date.isoformat(),
                selected_client_ids=selected_client_ids,
                filter_client_ids=selected_client_ids,
                filter_time_range="",
                filter_mode="run_refs",
                requested_run_refs=requested_run_refs,
                expected_run_refs=expected_run_refs or requested_run_refs,
                missing_run_refs=tuple(missing_run_refs),
                lines=tuple(_to_line_plan(result) for result in line_results),
                _line_results=tuple(line_results),
            )
        return CollectPlan(
            status="ready",
            ui_reason_code="",
            message="ready",
            line_id=normalized_line_id,
            target_jst_date=target_jst_date.isoformat(),
            selected_client_ids=selected_client_ids,
            filter_client_ids=selected_client_ids,
            filter_time_range="",
            filter_mode="run_refs",
            requested_run_refs=requested_run_refs,
            expected_run_refs=expected_run_refs or requested_run_refs,
            missing_run_refs=(),
            lines=tuple(_to_line_plan(result) for result in line_results),
            _line_results=tuple(line_results),
        )

    target_jst_date = parse_jst_date(request.target_jst_date or None, default_date=default_jst_date)
    parsed_time_range = parse_time_range(request.time_range or None)
    all_client_ids = _discover_clients(repo_root)
    requested_client_ids = normalize_client_ids(request.client_ids)
    if requested_client_ids:
        selected_client_ids = tuple(client_id for client_id in requested_client_ids if client_id in all_client_ids)
    else:
        selected_client_ids = tuple(all_client_ids)

    if not selected_client_ids:
        if normalized_line_id == ALL_LINE_ID:
            return CollectPlan(
                status="error",
                ui_reason_code=COLLECT_FAIL_NO_RUNS_FOUND,
                message="no runs found",
                line_id=normalized_line_id,
                target_jst_date=target_jst_date.isoformat(),
                selected_client_ids=(),
                filter_client_ids=requested_client_ids,
                filter_time_range=parsed_time_range[2] if parsed_time_range is not None else "",
                filter_mode="date_time",
                expected_run_refs=expected_run_refs,
            )
        return CollectPlan(
            status="skip",
            ui_reason_code=COLLECT_FAIL_NO_RUNS_FOUND,
            message="no clients selected. skip.",
            line_id=normalized_line_id,
            target_jst_date=target_jst_date.isoformat(),
            selected_client_ids=(),
            filter_client_ids=requested_client_ids,
            filter_time_range=parsed_time_range[2] if parsed_time_range is not None else "",
            filter_mode="date_time",
            expected_run_refs=expected_run_refs,
        )

    line_results = tuple(
        _collect_runs_for_line(
            repo_root=repo_root,
            line_id=line_id,
            selected_client_ids=selected_client_ids,
            target_jst_date=target_jst_date,
            time_range=parsed_time_range,
        )
        for line_id in line_ids
    )
    filter_time_range = parsed_time_range[2] if parsed_time_range is not None else ""
    line_plans = tuple(_to_line_plan(result) for result in line_results)

    if normalized_line_id == ALL_LINE_ID:
        total_collected_runs = sum(line.collected_count for line in line_plans)
        if total_collected_runs == 0:
            return CollectPlan(
                status="error",
                ui_reason_code=COLLECT_FAIL_NO_RUNS_FOUND,
                message="no runs found",
                line_id=normalized_line_id,
                target_jst_date=target_jst_date.isoformat(),
                selected_client_ids=selected_client_ids,
                filter_client_ids=requested_client_ids,
                filter_time_range=filter_time_range,
                filter_mode="date_time",
                expected_run_refs=expected_run_refs,
                lines=line_plans,
                _line_results=line_results,
            )
    elif not line_plans[0].collected_runs:
        return CollectPlan(
            status="skip",
            ui_reason_code=COLLECT_FAIL_NO_RUNS_FOUND,
            message="no eligible runs found. skip.",
            line_id=normalized_line_id,
            target_jst_date=target_jst_date.isoformat(),
            selected_client_ids=selected_client_ids,
            filter_client_ids=requested_client_ids,
            filter_time_range=filter_time_range,
            filter_mode="date_time",
            expected_run_refs=expected_run_refs,
            lines=line_plans,
            _line_results=line_results,
        )

    return CollectPlan(
        status="ready",
        ui_reason_code="",
        message="ready",
        line_id=normalized_line_id,
        target_jst_date=target_jst_date.isoformat(),
        selected_client_ids=selected_client_ids,
        filter_client_ids=requested_client_ids,
        filter_time_range=filter_time_range,
        filter_mode="date_time",
        expected_run_refs=expected_run_refs,
        lines=line_plans,
        _line_results=line_results,
    )


def _append_payload_items(
    *,
    repo_root: Path,
    line_id: str,
    runs: Sequence[_RunFiles],
    payload_by_zip_relpath: dict[str, bytes],
    items: list[dict[str, object]],
    zip_prefix: str,
) -> dict[str, int]:
    csv_count = 0
    report_count = 0
    manifest_count = 0
    total_bytes = 0

    for run in runs:
        for kind, rel_dir, src_files in [
            ("csv", "csv", run.replaced_files),
            ("report", "reports", run.report_files),
            ("manifest", "manifests", run.manifest_files),
        ]:
            for src_path in src_files:
                zip_relpath = f"{zip_prefix}{rel_dir}/{run.client_id}__{run.run_id}__{src_path.name}"
                if zip_relpath in payload_by_zip_relpath:
                    zip_relpath = f"{zip_prefix}{rel_dir}/{run.client_id}__{run.run_id}__{run.layout}__{src_path.name}"
                if zip_relpath in payload_by_zip_relpath:
                    raise RuntimeError(f"ZIP path collision: {zip_relpath}")

                data = src_path.read_bytes()
                payload_by_zip_relpath[zip_relpath] = data
                total_bytes += len(data)
                if kind == "csv":
                    csv_count += 1
                elif kind == "report":
                    report_count += 1
                else:
                    manifest_count += 1
                items.append(
                    {
                        "line_id": line_id,
                        "client_id": run.client_id,
                        "run_id": run.run_id,
                        "layout": run.layout,
                        "source_relpath": _repo_relpath(repo_root, src_path),
                        "zip_relpath": zip_relpath,
                        "sha256": _sha256_bytes(data),
                        "size_bytes": len(data),
                    }
                )

    return {
        "csv_files": csv_count,
        "report_files": report_count,
        "manifest_files": manifest_count,
        "items": csv_count + report_count + manifest_count,
        "total_bytes": total_bytes,
    }


def _build_manifest_and_payload(
    *,
    repo_root: Path,
    line_id: str,
    runs: Sequence[_RunFiles],
    exported_at_utc: datetime,
    jst_date: date,
    filter_client_ids: Sequence[str],
    filter_time_range: str,
    filter_mode: str,
    requested_run_refs: Sequence[str],
    skipped_incomplete_runs: Sequence[_RunFiles],
    skipped_invalid_run_id_count: int,
) -> tuple[bytes, dict[str, bytes]]:
    payload_by_zip_relpath: dict[str, bytes] = {}
    items: list[dict[str, object]] = []
    line_counts = _append_payload_items(
        repo_root=repo_root,
        line_id=line_id,
        runs=runs,
        payload_by_zip_relpath=payload_by_zip_relpath,
        items=items,
        zip_prefix="",
    )
    items.sort(
        key=lambda row: (
            str(row.get("line_id", "")),
            str(row["client_id"]),
            str(row["run_id"]),
            str(row["zip_relpath"]),
        )
    )
    manifest_obj: dict[str, object] = {
        "schema": MANIFEST_SCHEMA,
        "exported_at_utc": _utc_iso(exported_at_utc),
        "line_id": line_id,
        "jst_date": jst_date.isoformat(),
        "filters": {
            "mode": filter_mode,
            "client_ids": list(filter_client_ids),
            "time_range": filter_time_range or None,
            "run_refs": list(requested_run_refs),
        },
        "summary": {
            "matched_runs": len(runs) + len(skipped_incomplete_runs),
            "collected_runs": len(runs),
            "skipped_incomplete_runs": len(skipped_incomplete_runs),
            "skipped_invalid_run_id_count": skipped_invalid_run_id_count,
            "csv_files": line_counts["csv_files"],
            "report_files": line_counts["report_files"],
            "manifest_files": line_counts["manifest_files"],
            "items": line_counts["items"],
            "total_bytes": line_counts["total_bytes"],
            "included_run_ids": _collect_run_refs(runs),
            "skipped_lines": [],
        },
        "items": items,
    }
    manifest_bytes = (json.dumps(manifest_obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return manifest_bytes, payload_by_zip_relpath


def _build_manifest_and_payload_all(
    *,
    repo_root: Path,
    line_results: Sequence[_LineCollectionResult],
    exported_at_utc: datetime,
    jst_date: date,
    filter_client_ids: Sequence[str],
    filter_time_range: str,
    filter_mode: str,
    requested_run_refs: Sequence[str],
) -> tuple[bytes, dict[str, bytes]]:
    payload_by_zip_relpath: dict[str, bytes] = {}
    items: list[dict[str, object]] = []
    lines_summary: dict[str, dict[str, object]] = {}
    skipped_lines: list[str] = []

    matched_total = 0
    collected_total = 0
    skipped_incomplete_total = 0
    invalid_run_total = 0
    csv_total = 0
    report_total = 0
    manifest_total = 0
    total_bytes = 0

    for result in line_results:
        line_counts = {"csv_files": 0, "report_files": 0, "manifest_files": 0, "items": 0, "total_bytes": 0}
        if result.collected_runs:
            line_counts = _append_payload_items(
                repo_root=repo_root,
                line_id=result.line_id,
                runs=result.collected_runs,
                payload_by_zip_relpath=payload_by_zip_relpath,
                items=items,
                zip_prefix=f"{result.line_id}/",
            )
        else:
            skipped_lines.append(result.line_id)

        matched_total += len(result.matched_runs)
        collected_total += len(result.collected_runs)
        skipped_incomplete_total += len(result.skipped_incomplete_runs)
        invalid_run_total += result.skipped_invalid_run_id_count
        csv_total += int(line_counts["csv_files"])
        report_total += int(line_counts["report_files"])
        manifest_total += int(line_counts["manifest_files"])
        total_bytes += int(line_counts["total_bytes"])
        lines_summary[result.line_id] = {
            "matched_runs": len(result.matched_runs),
            "collected_runs": len(result.collected_runs),
            "skipped_incomplete_runs": len(result.skipped_incomplete_runs),
            "skipped_invalid_run_id_count": result.skipped_invalid_run_id_count,
            "csv_files": line_counts["csv_files"],
            "report_files": line_counts["report_files"],
            "manifest_files": line_counts["manifest_files"],
            "items": line_counts["items"],
            "total_bytes": line_counts["total_bytes"],
            "included_run_ids": _collect_run_refs(result.collected_runs),
        }

    items.sort(
        key=lambda row: (
            str(row.get("line_id", "")),
            str(row["client_id"]),
            str(row["run_id"]),
            str(row["zip_relpath"]),
        )
    )
    manifest_obj: dict[str, object] = {
        "schema": MANIFEST_SCHEMA,
        "exported_at_utc": _utc_iso(exported_at_utc),
        "line_id": ALL_LINE_ID,
        "jst_date": jst_date.isoformat(),
        "filters": {
            "mode": filter_mode,
            "client_ids": list(filter_client_ids),
            "time_range": filter_time_range or None,
            "run_refs": list(requested_run_refs),
        },
        "summary": {
            "matched_runs": matched_total,
            "collected_runs": collected_total,
            "skipped_incomplete_runs": skipped_incomplete_total,
            "skipped_invalid_run_id_count": invalid_run_total,
            "csv_files": csv_total,
            "report_files": report_total,
            "manifest_files": manifest_total,
            "items": len(items),
            "total_bytes": total_bytes,
            "lines": lines_summary,
            "skipped_lines": skipped_lines,
        },
        "items": items,
    }
    manifest_bytes = (json.dumps(manifest_obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return manifest_bytes, payload_by_zip_relpath


def _write_zip(
    *,
    repo_root: Path,
    zip_name: str,
    manifest_bytes: bytes,
    payload_by_zip_relpath: dict[str, bytes],
) -> tuple[Path, Path]:
    collect_dir = repo_root / "exports" / "collect"
    collect_dir.mkdir(parents=True, exist_ok=True)
    final_zip_path = collect_dir / zip_name
    tmp_zip_path = collect_dir / f".{zip_name}.tmp"
    if tmp_zip_path.exists():
        tmp_zip_path.unlink()

    with zipfile.ZipFile(tmp_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for zip_relpath in sorted(payload_by_zip_relpath.keys()):
            zf.writestr(zip_relpath, payload_by_zip_relpath[zip_relpath])
        zf.writestr("MANIFEST.json", manifest_bytes)

    if final_zip_path.exists():
        final_zip_path.unlink()
    tmp_zip_path.replace(final_zip_path)

    latest_tmp = collect_dir / "LATEST.txt.tmp"
    latest_path = collect_dir / "LATEST.txt"
    latest_tmp.write_text(f"{zip_name}\n", encoding="utf-8", newline="\n")
    latest_tmp.replace(latest_path)
    return final_zip_path, latest_path


def _zip_message(base_message: str, zip_path: Path | str) -> str:
    zip_name = Path(str(zip_path)).name if str(zip_path) else ""
    if not zip_name:
        return base_message
    return f"{base_message} ファイル名: {zip_name}"


def manifest_included_run_refs(manifest_obj: dict[str, object]) -> list[str]:
    summary = manifest_obj.get("summary")
    if not isinstance(summary, dict):
        return []
    line_id = manifest_obj.get("line_id")
    if line_id == ALL_LINE_ID:
        lines = summary.get("lines")
        refs: list[str] = []
        if isinstance(lines, dict):
            for line_summary in lines.values():
                if not isinstance(line_summary, dict):
                    continue
                included = line_summary.get("included_run_ids")
                if isinstance(included, list):
                    refs.extend(str(item) for item in included)
        return sorted(set(refs))
    included = summary.get("included_run_ids")
    if isinstance(included, list):
        return sorted(set(str(item) for item in included))
    return []


def _plan_failure_result(plan: CollectPlan) -> CollectResult:
    expected_run_refs = plan.expected_run_refs or plan.requested_run_refs
    return CollectResult(
        ok=False,
        status="error",
        ui_reason_code=plan.ui_reason_code,
        ui_reason_detail={
            "plan_status": plan.status,
            "requested_run_refs": list(plan.requested_run_refs),
            "expected_run_refs": list(expected_run_refs),
            "missing_run_refs": list(plan.missing_run_refs),
        },
        message="成果物ZIPを作成できませんでした。",
        line_id=plan.line_id,
        filter_mode=plan.filter_mode,
        requested_run_refs=plan.requested_run_refs,
        expected_run_refs=expected_run_refs,
        missing_run_refs=plan.missing_run_refs,
    )


def execute_collect_plan(repo_root: Path, plan: CollectPlan) -> CollectResult:
    if not plan.can_collect:
        return _plan_failure_result(plan)

    exported_at_utc = _now_utc()
    target_jst_date = datetime.strptime(plan.target_jst_date, "%Y-%m-%d").date()
    if plan.line_id == ALL_LINE_ID:
        manifest_bytes, payload_by_zip_relpath = _build_manifest_and_payload_all(
            repo_root=repo_root,
            line_results=plan._line_results,
            exported_at_utc=exported_at_utc,
            jst_date=target_jst_date,
            filter_client_ids=plan.filter_client_ids,
            filter_time_range=plan.filter_time_range,
            filter_mode=plan.filter_mode,
            requested_run_refs=plan.requested_run_refs,
        )
    else:
        line_result = plan._line_results[0]
        manifest_bytes, payload_by_zip_relpath = _build_manifest_and_payload(
            repo_root=repo_root,
            line_id=line_result.line_id,
            runs=line_result.collected_runs,
            exported_at_utc=exported_at_utc,
            jst_date=target_jst_date,
            filter_client_ids=plan.filter_client_ids,
            filter_time_range=plan.filter_time_range,
            filter_mode=plan.filter_mode,
            requested_run_refs=plan.requested_run_refs,
            skipped_incomplete_runs=line_result.skipped_incomplete_runs,
            skipped_invalid_run_id_count=line_result.skipped_invalid_run_id_count,
        )

    manifest_obj = json.loads(manifest_bytes.decode("utf-8"))
    summary = manifest_obj.get("summary")
    manifest_summary = dict(summary) if isinstance(summary, dict) else {}
    sha8 = _sha256_bytes(manifest_bytes)[:8]
    zip_name = f"collect_{plan.target_jst_date}_{_utc_compact(exported_at_utc)}_{sha8}.zip"
    zip_path, latest_path = _write_zip(
        repo_root=repo_root,
        zip_name=zip_name,
        manifest_bytes=manifest_bytes,
        payload_by_zip_relpath=payload_by_zip_relpath,
    )

    expected_run_refs = plan.expected_run_refs or plan.requested_run_refs
    included_run_refs = tuple(manifest_included_run_refs(manifest_obj))
    included_set = set(included_run_refs)
    expected_set = set(expected_run_refs)
    exact_match = True
    extra_run_refs: tuple[str, ...] = ()
    missing_run_refs: tuple[str, ...] = ()
    if expected_set:
        extra_run_refs = tuple(sorted(included_set - expected_set))
        missing_run_refs = tuple(sorted(expected_set - included_set))
        exact_match = included_set == expected_set

    if missing_run_refs:
        return CollectResult(
            ok=False,
            status="error",
            ui_reason_code=COLLECT_FAIL_MISSING_RUN_REFS,
            ui_reason_detail={
                "exact_match": exact_match,
                "requested_run_refs": list(plan.requested_run_refs),
                "expected_run_refs": list(expected_run_refs),
                "included_run_refs": list(included_run_refs),
                "extra_run_refs": list(extra_run_refs),
                "missing_run_refs": list(missing_run_refs),
            },
            message="成果物ZIPを作成できませんでした。",
            line_id=plan.line_id,
            filter_mode=plan.filter_mode,
            zip_path=str(zip_path),
            latest_path=str(latest_path),
            requested_run_refs=plan.requested_run_refs,
            expected_run_refs=expected_run_refs,
            included_run_refs=included_run_refs,
            missing_run_refs=missing_run_refs,
            extra_run_refs=extra_run_refs,
            exact_match=exact_match,
            manifest=manifest_obj,
            manifest_summary=manifest_summary,
        )

    return CollectResult(
        ok=True,
        status="success",
        ui_reason_code=COLLECT_OK_EXACT if exact_match else COLLECT_WARN_EXTRA_RUNS_INCLUDED,
        ui_reason_detail={
            "exact_match": exact_match,
            "requested_run_refs": list(plan.requested_run_refs),
            "expected_run_refs": list(expected_run_refs),
            "included_run_refs": list(included_run_refs),
            "extra_run_refs": list(extra_run_refs),
            "missing_run_refs": list(missing_run_refs),
        },
        message=_zip_message("成果物ZIPを作成しました。", zip_path),
        line_id=plan.line_id,
        filter_mode=plan.filter_mode,
        zip_path=str(zip_path),
        latest_path=str(latest_path),
        requested_run_refs=plan.requested_run_refs,
        expected_run_refs=expected_run_refs,
        included_run_refs=included_run_refs,
        missing_run_refs=missing_run_refs,
        extra_run_refs=extra_run_refs,
        exact_match=exact_match,
        manifest=manifest_obj,
        manifest_summary=manifest_summary,
    )


def run_collect(repo_root: Path, request: CollectRequest) -> CollectResult:
    plan = prepare_collect_plan(repo_root, request)
    return execute_collect_plan(repo_root, plan)


__all__ = [
    "ALL_LINE_ID",
    "ALL_MODE_LINE_ORDER",
    "CollectLinePlan",
    "CollectPlan",
    "CollectRequest",
    "CollectResult",
    "CollectRunPreview",
    "JST",
    "LINE_ARG_CHOICES",
    "MANIFEST_SCHEMA",
    "execute_collect_plan",
    "manifest_included_run_refs",
    "normalize_client_ids",
    "normalize_run_refs",
    "parse_jst_date",
    "parse_time_range",
    "prepare_collect_plan",
    "run_collect",
    "split_run_ref",
]
