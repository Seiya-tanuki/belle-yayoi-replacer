from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from belle.application.collect import (
    ALL_LINE_ID,
    CollectRequest,
    CollectResult,
    manifest_included_run_refs,
    run_collect as run_collect_application,
)
from belle.ui_reason_codes import (
    COLLECT_FAIL_UNKNOWN,
    RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED,
    RUN_NEEDS_REVIEW_CARD_CANONICAL_PAYABLE_FAILED,
    RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED,
)

JST = timezone(timedelta(hours=9))
RUN_ID_PREFIX_FORMAT = "%Y%m%dT%H%M%SZ"


def source_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _parse_run_id_utc(run_id: str) -> datetime | None:
    prefix = str(run_id or "").split("_", 1)[0]
    try:
        return datetime.strptime(prefix, RUN_ID_PREFIX_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_session_timestamp(raw: str) -> datetime | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _derive_date_and_time(
    run_results: list[dict[str, object]],
    session_started_at_utc: str,
    session_finished_at_utc: str,
) -> tuple[str, str]:
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


def _session_run_refs(client_id: str, run_results: list[dict[str, object]]) -> tuple[str, ...]:
    refs = []
    seen = set()
    for result in run_results:
        run_id = str(result.get("run_id") or "").strip()
        if not run_id:
            continue
        run_ref = f"{client_id}:{run_id}"
        if run_ref in seen:
            continue
        seen.add(run_ref)
        refs.append(run_ref)
    return tuple(refs)


def _collect_line_id(
    run_results: list[dict[str, object]],
    *,
    collect_today_all: bool,
    collect_today_all_clients: bool,
) -> str:
    if collect_today_all or collect_today_all_clients:
        return ALL_LINE_ID
    line_ids = sorted({str(result.get("line_id") or "").strip() for result in run_results if result.get("line_id")})
    if len(line_ids) == 1:
        return line_ids[0]
    return ALL_LINE_ID


def build_collect_request(
    *,
    client_id: str,
    run_results: list[dict[str, object]],
    session_started_at_utc: str,
    session_finished_at_utc: str,
    requested_run_refs: list[str] | None = None,
    collect_today_all: bool = False,
    collect_today_all_clients: bool = False,
) -> CollectRequest:
    normalized_run_refs = tuple(str(run_ref or "").strip() for run_ref in (requested_run_refs or []) if str(run_ref or "").strip())
    session_run_refs = _session_run_refs(client_id, run_results)
    line_id = _collect_line_id(
        run_results,
        collect_today_all=collect_today_all,
        collect_today_all_clients=collect_today_all_clients,
    )
    if normalized_run_refs:
        return CollectRequest(
            line_id=line_id,
            requested_run_refs=normalized_run_refs,
            expected_run_refs=normalized_run_refs,
        )

    date_text, time_text = _derive_date_and_time(run_results, session_started_at_utc, session_finished_at_utc)
    return CollectRequest(
        line_id=line_id,
        target_jst_date=date_text,
        client_ids=() if collect_today_all_clients else (client_id,),
        time_range="" if (collect_today_all or collect_today_all_clients) else time_text,
        expected_run_refs=session_run_refs,
    )


def overall_result_title(run_results: list[dict[str, object]]) -> str:
    reason_codes = {str(result.get("ui_reason_code") or "").strip() for result in run_results if result.get("ui_reason_code")}
    if any(code.startswith("RUN_FAIL_") for code in reason_codes):
        return "処理に失敗しました"
    if reason_codes & {
        RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED,
        RUN_NEEDS_REVIEW_CARD_CANONICAL_PAYABLE_FAILED,
        RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED,
    }:
        return "処理は完了しましたが確認が必要です"

    statuses = {str(result.get("status") or "") for result in run_results}
    if "failure" in statuses:
        return "処理に失敗しました"
    if "needs_review" in statuses:
        return "処理は完了しましたが確認が必要です"
    return "処理が完了しました"


def _unexpected_collect_failure(exc: Exception) -> CollectResult:
    return CollectResult(
        ok=False,
        status="error",
        ui_reason_code=COLLECT_FAIL_UNKNOWN,
        ui_reason_detail={"exception_type": type(exc).__name__, "exception_message": str(exc)},
        message="成果物ZIPを作成できませんでした。",
    )


def run_collect(
    *,
    client_id: str,
    run_results: list[dict[str, object]],
    session_started_at_utc: str,
    session_finished_at_utc: str,
    requested_run_refs: list[str] | None = None,
    collect_today_all: bool = False,
    collect_today_all_clients: bool = False,
    root: Path | None = None,
) -> CollectResult:
    current_root = root or source_repo_root()
    request = build_collect_request(
        client_id=client_id,
        run_results=run_results,
        session_started_at_utc=session_started_at_utc,
        session_finished_at_utc=session_finished_at_utc,
        requested_run_refs=requested_run_refs,
        collect_today_all=collect_today_all,
        collect_today_all_clients=collect_today_all_clients,
    )
    try:
        return run_collect_application(current_root, request)
    except Exception as exc:
        return _unexpected_collect_failure(exc)


def serialize_collect_result(result: CollectResult) -> dict[str, object]:
    payload = asdict(result)
    payload["session_run_refs"] = list(result.expected_run_refs)
    return payload


__all__ = [
    "CollectRequest",
    "CollectResult",
    "build_collect_request",
    "manifest_included_run_refs",
    "overall_result_title",
    "run_collect",
    "serialize_collect_result",
    "source_repo_root",
]
