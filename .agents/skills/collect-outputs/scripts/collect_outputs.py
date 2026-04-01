#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from belle.lines import validate_line_id

JST = timezone(timedelta(hours=9))
MANIFEST_SCHEMA = "belle.collect_outputs_manifest.v1"
RUN_ID_PREFIX_FORMAT = "%Y%m%dT%H%M%SZ"
PREVIEW_LIMIT = 200
ALL_LINE_ID = "all"
ALL_MODE_LINE_ORDER = ["receipt", "bank_statement", "credit_card_statement"]
LINE_ARG_CHOICES = [*ALL_MODE_LINE_ORDER, ALL_LINE_ID]


class RunFiles:
    def __init__(
        self,
        *,
        line_id: str,
        client_id: str,
        run_id: str,
        run_dir: Path,
        run_utc: datetime,
        run_jst: datetime,
        replaced_files: List[Path],
        report_files: List[Path],
        manifest_files: List[Path],
        layout: str,
    ) -> None:
        self.client_id = client_id
        self.run_id = run_id
        self.run_dir = run_dir
        self.run_utc = run_utc
        self.run_jst = run_jst
        self.replaced_files = replaced_files
        self.report_files = report_files
        self.manifest_files = manifest_files
        self.layout = layout
        self.line_id = line_id


class LineCollectionResult:
    def __init__(
        self,
        *,
        line_id: str,
        matched_runs: List[RunFiles],
        collected_runs: List[RunFiles],
        skipped_incomplete_runs: List[RunFiles],
        skipped_invalid_run_id_count: int,
    ) -> None:
        self.line_id = line_id
        self.matched_runs = matched_runs
        self.collected_runs = collected_runs
        self.skipped_incomplete_runs = skipped_incomplete_runs
        self.skipped_invalid_run_id_count = skipped_invalid_run_id_count


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


def _normalize_client_ids(raw: Optional[str]) -> List[str]:
    if raw is None:
        return []
    values = []
    seen = set()
    for token in raw.split(","):
        cid = token.strip()
        if not cid:
            continue
        if cid in seen:
            continue
        values.append(cid)
        seen.add(cid)
    return values


def _normalize_run_refs(raw_values: Optional[Sequence[str]]) -> List[str]:
    if not raw_values:
        return []
    values: List[str] = []
    seen = set()
    for raw in raw_values:
        for token in str(raw or "").split(","):
            run_ref = token.strip()
            if not run_ref:
                continue
            client_id, run_id = _split_run_ref(run_ref)
            normalized = f"{client_id}:{run_id}"
            if normalized in seen:
                continue
            seen.add(normalized)
            values.append(normalized)
    return values


def _split_run_ref(run_ref: str) -> Tuple[str, str]:
    value = str(run_ref or "").strip()
    if ":" not in value:
        raise ValueError(f"invalid run_ref format: {value} (CLIENT_ID:RUN_ID)")
    client_id, run_id = value.split(":", 1)
    client_id = client_id.strip()
    run_id = run_id.strip()
    if not client_id or not run_id:
        raise ValueError(f"invalid run_ref format: {value} (CLIENT_ID:RUN_ID)")
    return client_id, run_id


def _parse_jst_date(raw: Optional[str], *, default_date: date) -> date:
    value = (raw or "").strip()
    if not value:
        return default_date
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"invalid date format: {value} (YYYY-MM-DD)") from exc


def _parse_time_range(raw: Optional[str]) -> Optional[Tuple[int, int, str]]:
    value = (raw or "").strip()
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


def _parse_run_utc_from_run_id(run_id: str) -> Optional[datetime]:
    prefix = run_id.split("_", 1)[0]
    try:
        parsed = datetime.strptime(prefix, RUN_ID_PREFIX_FORMAT)
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def _discover_clients(repo_root: Path) -> List[str]:
    clients_dir = repo_root / "clients"
    if not clients_dir.exists():
        return []

    clients: List[str] = []
    for path in sorted(clients_dir.iterdir(), key=lambda p: p.name):
        if not path.is_dir():
            continue
        if path.name == "TEMPLATE":
            continue
        clients.append(path.name)
    return clients


def _discover_run_roots_for_client(repo_root: Path, client_id: str, line_id: str) -> Tuple[List[Tuple[str, Path]], bool, bool]:
    roots: List[Tuple[str, Path]] = []
    line_root = repo_root / "clients" / client_id / "lines" / line_id / "outputs" / "runs"
    legacy_root = repo_root / "clients" / client_id / "outputs" / "runs"
    line_exists = line_root.exists()
    legacy_exists = legacy_root.exists()
    if line_exists:
        roots.append(("line", line_root))
    if line_id == "receipt" and legacy_exists:
        roots.append(("legacy", legacy_root))
    return roots, line_exists, legacy_exists


def _build_run_files(client_id: str, line_id: str, run_dir: Path, layout: str) -> Optional[RunFiles]:
    run_id = run_dir.name
    run_utc = _parse_run_utc_from_run_id(run_id)
    if run_utc is None:
        return None

    run_manifest = run_dir / "run_manifest.json"
    manifest_paths: List[Path] = []
    if run_manifest.exists() and run_manifest.is_file():
        manifest_paths.append(run_manifest)

    for path in sorted(run_dir.glob("*_manifest.json")):
        if path == run_manifest:
            continue
        if path.is_file():
            manifest_paths.append(path)

    return RunFiles(
        line_id=line_id,
        client_id=client_id,
        run_id=run_id,
        run_dir=run_dir,
        run_utc=run_utc,
        run_jst=run_utc.astimezone(JST),
        replaced_files=[p for p in sorted(run_dir.glob("*_replaced_*.csv")) if p.is_file()],
        report_files=[p for p in sorted(run_dir.glob("*_review_report.csv")) if p.is_file()],
        manifest_files=manifest_paths,
        layout=layout,
    )


def _discover_runs_for_client(
    repo_root: Path,
    client_id: str,
    *,
    line_id: str,
    target_date_jst: date,
    time_range: Optional[Tuple[int, int, str]],
) -> Tuple[List[RunFiles], int, bool, bool]:
    run_roots, line_exists, legacy_exists = _discover_run_roots_for_client(repo_root, client_id, line_id)
    if not run_roots:
        return [], 0, line_exists, legacy_exists

    matched: List[RunFiles] = []
    invalid_run_id_count = 0
    for layout, runs_dir in run_roots:
        for run_dir in sorted(runs_dir.iterdir(), key=lambda p: p.name):
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
                start_min, end_min, _ = time_range
                if run_minutes < start_min or run_minutes > end_min:
                    continue
            matched.append(run_files)
    return matched, invalid_run_id_count, line_exists, legacy_exists


def _collect_runs_for_line(
    *,
    repo_root: Path,
    line_id: str,
    selected_client_ids: Sequence[str],
    target_jst_date: date,
    time_range: Optional[Tuple[int, int, str]],
) -> LineCollectionResult:
    matched_runs: List[RunFiles] = []
    invalid_run_id_count = 0
    for client_id in selected_client_ids:
        runs, invalid_count, line_exists, legacy_exists = _discover_runs_for_client(
            repo_root,
            client_id,
            line_id=line_id,
            target_date_jst=target_jst_date,
            time_range=time_range,
        )
        if line_id == "receipt" and (not line_exists) and legacy_exists:
            print(f"[WARN] legacy client layout detected (no lines/{line_id}/). Using legacy paths for this run.")
        matched_runs.extend(runs)
        invalid_run_id_count += invalid_count

    matched_runs.sort(key=lambda row: (row.client_id, row.run_id, row.layout))
    skipped_incomplete_runs = [run for run in matched_runs if len(run.replaced_files) == 0]
    collected_runs = [run for run in matched_runs if len(run.replaced_files) > 0]
    return LineCollectionResult(
        line_id=line_id,
        matched_runs=matched_runs,
        collected_runs=collected_runs,
        skipped_incomplete_runs=skipped_incomplete_runs,
        skipped_invalid_run_id_count=invalid_run_id_count,
    )


def _resolve_run_ref(repo_root: Path, run_ref: str, *, allowed_line_ids: Sequence[str]) -> Optional[RunFiles]:
    client_id, run_id = _split_run_ref(run_ref)
    matches: List[RunFiles] = []
    for line_id in allowed_line_ids:
        run_roots, _line_exists, _legacy_exists = _discover_run_roots_for_client(repo_root, client_id, line_id)
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
) -> tuple[list[LineCollectionResult], list[str]]:
    grouped: Dict[str, List[RunFiles]] = {line_id: [] for line_id in allowed_line_ids}
    missing_run_refs: List[str] = []
    for run_ref in requested_run_refs:
        run_files = _resolve_run_ref(repo_root, run_ref, allowed_line_ids=allowed_line_ids)
        if run_files is None:
            missing_run_refs.append(run_ref)
            continue
        grouped.setdefault(run_files.line_id, []).append(run_files)

    line_results: List[LineCollectionResult] = []
    for line_id in allowed_line_ids:
        matched_runs = sorted(grouped.get(line_id, []), key=lambda row: (row.client_id, row.run_id, row.layout))
        skipped_incomplete_runs = [run for run in matched_runs if len(run.replaced_files) == 0]
        collected_runs = [run for run in matched_runs if len(run.replaced_files) > 0]
        line_results.append(
            LineCollectionResult(
                line_id=line_id,
                matched_runs=matched_runs,
                collected_runs=collected_runs,
                skipped_incomplete_runs=skipped_incomplete_runs,
                skipped_invalid_run_id_count=0,
            )
        )
    return line_results, missing_run_refs


def _append_payload_items(
    *,
    repo_root: Path,
    line_id: str,
    runs: Sequence[RunFiles],
    payload_by_zip_relpath: Dict[str, bytes],
    items: List[Dict[str, object]],
    zip_prefix: str,
) -> Dict[str, int]:
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


def _collect_run_refs(runs: Sequence[RunFiles]) -> List[str]:
    return [f"{run.client_id}:{run.run_id}" for run in runs]


def _build_manifest_and_payload(
    *,
    repo_root: Path,
    line_id: str,
    runs: Sequence[RunFiles],
    exported_at_utc: datetime,
    jst_date: date,
    filter_client_ids: Sequence[str],
    filter_time_range: Optional[str],
    filter_mode: str,
    requested_run_refs: Sequence[str],
    skipped_incomplete_runs: Sequence[RunFiles],
    skipped_invalid_run_id_count: int,
) -> Tuple[bytes, Dict[str, bytes], Dict[str, object]]:
    payload_by_zip_relpath: Dict[str, bytes] = {}
    items: List[Dict[str, object]] = []
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
    manifest_obj: Dict[str, object] = {
        "schema": MANIFEST_SCHEMA,
        "exported_at_utc": _utc_iso(exported_at_utc),
        "line_id": line_id,
        "jst_date": jst_date.isoformat(),
        "filters": {
            "mode": filter_mode,
            "client_ids": list(filter_client_ids),
            "time_range": filter_time_range,
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
    return manifest_bytes, payload_by_zip_relpath, manifest_obj["summary"]  # type: ignore[return-value]


def _build_manifest_and_payload_all(
    *,
    repo_root: Path,
    line_results: Sequence[LineCollectionResult],
    exported_at_utc: datetime,
    jst_date: date,
    filter_client_ids: Sequence[str],
    filter_time_range: Optional[str],
    filter_mode: str,
    requested_run_refs: Sequence[str],
) -> Tuple[bytes, Dict[str, bytes], Dict[str, object]]:
    payload_by_zip_relpath: Dict[str, bytes] = {}
    items: List[Dict[str, object]] = []
    lines_summary: Dict[str, Dict[str, object]] = {}
    skipped_lines: List[str] = []

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

    manifest_obj: Dict[str, object] = {
        "schema": MANIFEST_SCHEMA,
        "exported_at_utc": _utc_iso(exported_at_utc),
        "line_id": ALL_LINE_ID,
        "jst_date": jst_date.isoformat(),
        "filters": {
            "mode": filter_mode,
            "client_ids": list(filter_client_ids),
            "time_range": filter_time_range,
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
    return manifest_bytes, payload_by_zip_relpath, manifest_obj["summary"]  # type: ignore[return-value]


def _write_zip(
    *,
    repo_root: Path,
    zip_name: str,
    manifest_bytes: bytes,
    payload_by_zip_relpath: Dict[str, bytes],
) -> Path:
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
    return final_zip_path


def _print_preview(
    *,
    line_id: str,
    jst_date: date,
    selected_client_ids: Sequence[str],
    filter_client_ids: Sequence[str],
    filter_time_range: Optional[str],
    matched_runs: Sequence[RunFiles],
    collected_runs: Sequence[RunFiles],
    skipped_incomplete_runs: Sequence[RunFiles],
    skipped_invalid_run_id_count: int,
) -> None:
    client_label = ",".join(filter_client_ids) if filter_client_ids else "ALL"
    time_label = filter_time_range if filter_time_range else "full-day"
    print("[INFO] 収集条件")
    print(f"  - line: {line_id}")
    print(f"  - date(JST): {jst_date.isoformat()}")
    print(f"  - clients: {client_label}")
    print(f"  - time(JST): {time_label}")
    print(f"  - selected_clients: {len(selected_client_ids)}")
    print(f"  - matched_runs: {len(matched_runs)}")
    print(f"  - collected_runs: {len(collected_runs)}")
    print(f"  - skipped_incomplete_runs: {len(skipped_incomplete_runs)}")
    if skipped_invalid_run_id_count > 0:
        print(f"  - skipped_invalid_run_id_count: {skipped_invalid_run_id_count}")

    print("[INFO] Preview (client_id | run_id | layout | replaced_count | report_count | manifest_count)")
    for idx, run in enumerate(collected_runs):
        if idx >= PREVIEW_LIMIT:
            break
        print(
            f"{run.client_id} | {run.run_id} | {run.layout} | "
            f"{len(run.replaced_files)} | {len(run.report_files)} | {len(run.manifest_files)}"
        )
    omitted = len(collected_runs) - PREVIEW_LIMIT
    if omitted > 0:
        print(f"[INFO] preview truncated: +{omitted}")


def _print_preview_all(
    *,
    jst_date: date,
    selected_client_ids: Sequence[str],
    filter_client_ids: Sequence[str],
    filter_time_range: Optional[str],
    line_results: Sequence[LineCollectionResult],
) -> None:
    client_label = ",".join(filter_client_ids) if filter_client_ids else "ALL"
    time_label = filter_time_range if filter_time_range else "full-day"
    print("[INFO] 収集条件")
    print(f"  - line: {ALL_LINE_ID}")
    print(f"  - date(JST): {jst_date.isoformat()}")
    print(f"  - clients: {client_label}")
    print(f"  - time(JST): {time_label}")
    print(f"  - selected_clients: {len(selected_client_ids)}")
    print(f"  - target_lines: {','.join(ALL_MODE_LINE_ORDER)}")
    print("[INFO] line summary")
    for result in line_results:
        run_refs = _collect_run_refs(result.collected_runs)
        if len(run_refs) > 12:
            run_ref_text = ", ".join(run_refs[:12]) + f", ... (+{len(run_refs) - 12})"
        else:
            run_ref_text = ", ".join(run_refs) if run_refs else "-"
        status = "included" if result.collected_runs else "skipped(no eligible runs)"
        print(
            f"  - {result.line_id}: status={status}, matched={len(result.matched_runs)}, "
            f"collected={len(result.collected_runs)}, skipped_incomplete={len(result.skipped_incomplete_runs)}, "
            f"invalid_run_id={result.skipped_invalid_run_id_count}"
        )
        print(f"    run_ids: {run_ref_text}")


def _confirm() -> bool:
    answer = input("この条件で収集ZIPを作成しますか? (y/N) ").strip().lower()
    return answer in {"y", "yes"}


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Collect per-run deliverables across clients and export a single zip under exports/collect/."
    )
    ap.add_argument(
        "--line",
        choices=LINE_ARG_CHOICES,
        default=ALL_LINE_ID,
        help="Document processing line_id. Default: all",
    )
    ap.add_argument("--date", help="JST date filter (YYYY-MM-DD). Default: today JST.", default=None)
    ap.add_argument(
        "--client",
        help="Comma-separated client IDs filter. Default: all clients except TEMPLATE.",
        default=None,
    )
    ap.add_argument(
        "--time",
        dest="time_range",
        help="JST time range filter (HH:MM-HH:MM). Default: full day.",
        default=None,
    )
    ap.add_argument(
        "--run-ref",
        action="append",
        dest="run_refs",
        help="Exact run reference(s) in CLIENT_ID:RUN_ID format. Repeatable or comma-separated.",
        default=None,
    )
    ap.add_argument("--yes", action="store_true", help="Skip confirmation prompt.")
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None, *, repo_root: Optional[Path] = None) -> int:
    args = _parse_args(argv)
    repo = repo_root or Path(__file__).resolve().parents[4]

    line_arg = str(args.line).strip().lower()
    if line_arg == ALL_LINE_ID:
        line_ids = list(ALL_MODE_LINE_ORDER)
    else:
        try:
            line_ids = [validate_line_id(line_arg)]
        except ValueError as exc:
            print(f"[ERROR] {exc}", file=sys.stderr)
            return 2

    now_utc = _now_utc()
    default_jst_date = now_utc.astimezone(JST).date()

    client_raw = args.client
    date_raw = args.date
    time_raw = args.time_range
    run_ref_raw = args.run_refs

    if not args.yes:
        if run_ref_raw:
            pass
        elif not sys.stdin.isatty():
            print("[ERROR] interactive input unavailable. Use --yes.", file=sys.stderr)
            return 2
        if run_ref_raw:
            pass
        elif client_raw is None:
            client_raw = input("対象クライアントID (カンマ区切り。空で全件): ").strip()
        if run_ref_raw:
            pass
        elif date_raw is None:
            date_raw = input(f"対象日付(JST, YYYY-MM-DD。空で{default_jst_date.isoformat()}): ").strip()
        if run_ref_raw:
            pass
        elif time_raw is None:
            time_raw = input("対象時間帯(JST, HH:MM-HH:MM。空で終日): ").strip()

    try:
        requested_run_refs = _normalize_run_refs(run_ref_raw)
        if requested_run_refs:
            target_jst_date = default_jst_date
            requested_client_ids = []
            parsed_time_range = None
        else:
            target_jst_date = _parse_jst_date(date_raw, default_date=default_jst_date)
            requested_client_ids = _normalize_client_ids(client_raw)
            parsed_time_range = _parse_time_range(time_raw)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    filter_mode = "run_refs" if requested_run_refs else "date_time"
    if requested_run_refs:
        line_results, missing_run_refs = _collect_runs_for_run_refs(
            repo_root=repo,
            requested_run_refs=requested_run_refs,
            allowed_line_ids=line_ids,
        )
        filter_time_text = None
        selected_client_ids = sorted({_split_run_ref(run_ref)[0] for run_ref in requested_run_refs})
        if missing_run_refs:
            print("[ERROR] requested run_refs not found: " + ", ".join(missing_run_refs), file=sys.stderr)
            return 2
        if not selected_client_ids:
            print("[ERROR] no runs found", file=sys.stderr)
            return 1
        run_dates = [run.run_jst.date() for result in line_results for run in result.matched_runs]
        if run_dates:
            target_jst_date = min(run_dates)
    else:
        all_client_ids = _discover_clients(repo)
        if requested_client_ids:
            selected_client_ids = [cid for cid in requested_client_ids if cid in all_client_ids]
            missing = [cid for cid in requested_client_ids if cid not in all_client_ids]
            if missing:
                print(f"[WARN] unknown client IDs were ignored: {', '.join(missing)}")
        else:
            selected_client_ids = all_client_ids

        if not selected_client_ids:
            if line_arg == ALL_LINE_ID:
                print("[ERROR] no runs found", file=sys.stderr)
                return 1
            print("[INFO] no clients selected. skip.")
            return 0

        line_results = [
            _collect_runs_for_line(
                repo_root=repo,
                line_id=line_id,
                selected_client_ids=selected_client_ids,
                target_jst_date=target_jst_date,
                time_range=parsed_time_range,
            )
            for line_id in line_ids
        ]
        filter_time_text = parsed_time_range[2] if parsed_time_range is not None else None

    filter_client_ids = list(selected_client_ids) if requested_run_refs else requested_client_ids

    if line_arg == ALL_LINE_ID:
        _print_preview_all(
            jst_date=target_jst_date,
            selected_client_ids=selected_client_ids,
            filter_client_ids=filter_client_ids,
            filter_time_range=filter_time_text,
            line_results=line_results,
        )
        total_collected_runs = sum(len(result.collected_runs) for result in line_results)
        if total_collected_runs == 0:
            print("[ERROR] no runs found", file=sys.stderr)
            return 1
    else:
        result = line_results[0]
        _print_preview(
            line_id=result.line_id,
            jst_date=target_jst_date,
            selected_client_ids=selected_client_ids,
            filter_client_ids=filter_client_ids,
            filter_time_range=filter_time_text,
            matched_runs=result.matched_runs,
            collected_runs=result.collected_runs,
            skipped_incomplete_runs=result.skipped_incomplete_runs,
            skipped_invalid_run_id_count=result.skipped_invalid_run_id_count,
        )
        if not result.collected_runs:
            print("[INFO] no eligible runs found. skip.")
            return 0

    if not args.yes and not _confirm():
        print("[INFO] user canceled. skip.")
        return 0

    exported_at_utc = _now_utc()
    if line_arg == ALL_LINE_ID:
        manifest_bytes, payload_by_zip_relpath, summary = _build_manifest_and_payload_all(
            repo_root=repo,
            line_results=line_results,
            exported_at_utc=exported_at_utc,
            jst_date=target_jst_date,
            filter_client_ids=filter_client_ids,
            filter_time_range=filter_time_text,
            filter_mode=filter_mode,
            requested_run_refs=requested_run_refs,
        )
    else:
        result = line_results[0]
        manifest_bytes, payload_by_zip_relpath, summary = _build_manifest_and_payload(
            repo_root=repo,
            line_id=result.line_id,
            runs=result.collected_runs,
            exported_at_utc=exported_at_utc,
            jst_date=target_jst_date,
            filter_client_ids=filter_client_ids,
            filter_time_range=filter_time_text,
            filter_mode=filter_mode,
            requested_run_refs=requested_run_refs,
            skipped_incomplete_runs=result.skipped_incomplete_runs,
            skipped_invalid_run_id_count=result.skipped_invalid_run_id_count,
        )

    sha8 = _sha256_bytes(manifest_bytes)[:8]
    zip_name = f"collect_{target_jst_date.isoformat()}_{_utc_compact(exported_at_utc)}_{sha8}.zip"
    zip_path = _write_zip(
        repo_root=repo,
        zip_name=zip_name,
        manifest_bytes=manifest_bytes,
        payload_by_zip_relpath=payload_by_zip_relpath,
    )

    print("[OK] 収集ZIPを作成しました。")
    print(f"[OK] ZIP: {zip_path}")
    print(f"[OK] LATEST: {zip_path.parent / 'LATEST.txt'}")
    print(
        "[OK] 件数: runs={runs}, csv={csv}, reports={reports}, manifests={manifests}, items={items}".format(
            runs=summary["collected_runs"],
            csv=summary["csv_files"],
            reports=summary["report_files"],
            manifests=summary["manifest_files"],
            items=summary["items"],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
