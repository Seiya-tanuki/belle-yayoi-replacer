from __future__ import annotations

import json
from typing import Any

UI_REASON_PREFIX = "[UI_REASON] "

RUN_OK = "RUN_OK"
RUN_FAIL_TARGET_INGEST = "RUN_FAIL_TARGET_INGEST"
RUN_FAIL_RECEIPT_CLIENT_CACHE_UPDATE = "RUN_FAIL_RECEIPT_CLIENT_CACHE_UPDATE"
RUN_FAIL_RECEIPT_LEXICON_AUTOGROW = "RUN_FAIL_RECEIPT_LEXICON_AUTOGROW"
RUN_FAIL_BANK_CACHE_UPDATE = "RUN_FAIL_BANK_CACHE_UPDATE"
RUN_FAIL_BANK_CONFIG_LOAD = "RUN_FAIL_BANK_CONFIG_LOAD"
RUN_FAIL_CARD_CONFIG_MISSING = "RUN_FAIL_CARD_CONFIG_MISSING"
RUN_FAIL_CARD_CACHE_UPDATE = "RUN_FAIL_CARD_CACHE_UPDATE"
RUN_FAIL_MULTIPLE_TARGET_INPUTS = "RUN_FAIL_MULTIPLE_TARGET_INPUTS"
RUN_FAIL_UNKNOWN = "RUN_FAIL_UNKNOWN"
RUN_NEEDS_REVIEW_CARD_CANONICAL_PAYABLE_FAILED = "RUN_NEEDS_REVIEW_CARD_CANONICAL_PAYABLE_FAILED"
RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED = "RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED"
RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED = "RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED"
RUN_NEEDS_REVIEW_UNKNOWN = "RUN_NEEDS_REVIEW_UNKNOWN"

PRECHECK_READY = "PRECHECK_READY"
PRECHECK_SKIP_NO_TARGET = "PRECHECK_SKIP_NO_TARGET"
PRECHECK_FAIL_MULTIPLE_TARGET_INPUTS = "PRECHECK_FAIL_MULTIPLE_TARGET_INPUTS"
PRECHECK_FAIL_CARD_CONFIG_MISSING = "PRECHECK_FAIL_CARD_CONFIG_MISSING"
PRECHECK_FAIL_RECEIPT_CONFIG_MISSING = "PRECHECK_FAIL_RECEIPT_CONFIG_MISSING"
PRECHECK_FAIL_BANK_CONFIG_MISSING = "PRECHECK_FAIL_BANK_CONFIG_MISSING"
PRECHECK_FAIL_BANK_TRAINING_OCR_TOO_MANY = "PRECHECK_FAIL_BANK_TRAINING_OCR_TOO_MANY"
PRECHECK_FAIL_BANK_TRAINING_REFERENCE_TOO_MANY = "PRECHECK_FAIL_BANK_TRAINING_REFERENCE_TOO_MANY"
PRECHECK_FAIL_BANK_TRAINING_PAIR_INCOMPLETE = "PRECHECK_FAIL_BANK_TRAINING_PAIR_INCOMPLETE"
PRECHECK_FAIL_CLIENT_DIR_NOT_FOUND = "PRECHECK_FAIL_CLIENT_DIR_NOT_FOUND"
PRECHECK_FAIL_LEGACY_LAYOUT_UNSUPPORTED = "PRECHECK_FAIL_LEGACY_LAYOUT_UNSUPPORTED"
PRECHECK_FAIL_UNKNOWN = "PRECHECK_FAIL_UNKNOWN"

COLLECT_OK_EXACT = "COLLECT_OK_EXACT"
COLLECT_WARN_EXTRA_RUNS_INCLUDED = "COLLECT_WARN_EXTRA_RUNS_INCLUDED"
COLLECT_FAIL_NO_RUNS_FOUND = "COLLECT_FAIL_NO_RUNS_FOUND"
COLLECT_FAIL_MISSING_RUN_REFS = "COLLECT_FAIL_MISSING_RUN_REFS"
COLLECT_FAIL_UNKNOWN = "COLLECT_FAIL_UNKNOWN"

SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID = "SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID"


def build_ui_reason_event(
    code: str,
    *,
    line_id: str = "",
    detail: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {"code": str(code)}
    if line_id:
        payload["line_id"] = str(line_id)
    if detail:
        payload["detail"] = detail
    return UI_REASON_PREFIX + json.dumps(payload, ensure_ascii=False, sort_keys=True)


def parse_ui_reason_event(
    line: str,
    *,
    line_id: str = "",
) -> tuple[str, dict[str, Any]] | None:
    stripped = str(line or "").strip()
    if not stripped.startswith(UI_REASON_PREFIX):
        return None
    try:
        payload = json.loads(stripped[len(UI_REASON_PREFIX) :])
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    payload_line_id = str(payload.get("line_id") or "").strip()
    if line_id and payload_line_id and payload_line_id != line_id:
        return None
    code = str(payload.get("code") or "").strip()
    if not code:
        return None
    detail = payload.get("detail")
    if not isinstance(detail, dict):
        detail = {}
    return code, detail


def parse_ui_reason_from_text(
    text: str,
    *,
    line_id: str = "",
) -> tuple[str, dict[str, Any]] | None:
    for raw_line in reversed((text or "").splitlines()):
        parsed = parse_ui_reason_event(raw_line, line_id=line_id)
        if parsed is not None:
            return parsed
    return None


def precheck_reason_code_for(line_id: str, status: str, reason: str) -> str:
    normalized_reason = str(reason or "")
    if status == "RUN":
        return PRECHECK_READY
    if status == "SKIP":
        if normalized_reason == "no target input":
            return PRECHECK_SKIP_NO_TARGET
        return PRECHECK_FAIL_UNKNOWN
    if "multiple target inputs" in normalized_reason:
        return PRECHECK_FAIL_MULTIPLE_TARGET_INPUTS
    if "missing_cc_config" in normalized_reason:
        return PRECHECK_FAIL_CARD_CONFIG_MISSING
    if "config not found:" in normalized_reason:
        return PRECHECK_FAIL_RECEIPT_CONFIG_MISSING
    if "bank_line_config.json not found:" in normalized_reason:
        return PRECHECK_FAIL_BANK_CONFIG_MISSING
    if "training OCR count must be <=1" in normalized_reason:
        return PRECHECK_FAIL_BANK_TRAINING_OCR_TOO_MANY
    if "training reference count must be <=1" in normalized_reason:
        return PRECHECK_FAIL_BANK_TRAINING_REFERENCE_TOO_MANY
    if "training pair is incomplete:" in normalized_reason:
        return PRECHECK_FAIL_BANK_TRAINING_PAIR_INCOMPLETE
    if "client dir not found:" in normalized_reason:
        return PRECHECK_FAIL_CLIENT_DIR_NOT_FOUND
    if "does not support legacy client layout" in normalized_reason:
        return PRECHECK_FAIL_LEGACY_LAYOUT_UNSUPPORTED
    return PRECHECK_FAIL_UNKNOWN


def run_failure_reason_code_for(line_id: str, text: str) -> str:
    normalized = str(text or "")
    if "multiple target inputs" in normalized:
        return RUN_FAIL_MULTIPLE_TARGET_INPUTS
    if "仮仕訳CSVの取り込みに失敗しました" in normalized:
        return RUN_FAIL_TARGET_INGEST
    if line_id == "receipt":
        if "client_cache 更新に失敗しました" in normalized:
            return RUN_FAIL_RECEIPT_CLIENT_CACHE_UPDATE
        if "label_queue 自動更新に失敗しました" in normalized:
            return RUN_FAIL_RECEIPT_LEXICON_AUTOGROW
    if line_id == "bank_statement":
        if "bank client_cache 更新に失敗しました" in normalized:
            return RUN_FAIL_BANK_CACHE_UPDATE
        if "bank_line_config 読み込みに失敗しました" in normalized:
            return RUN_FAIL_BANK_CONFIG_LOAD
        if "bank_line_config.json not found:" in normalized:
            return RUN_FAIL_BANK_CONFIG_LOAD
    if line_id == "credit_card_statement":
        if "missing_cc_config:" in normalized:
            return RUN_FAIL_CARD_CONFIG_MISSING
        if "credit card client_cache 更新に失敗しました" in normalized:
            return RUN_FAIL_CARD_CACHE_UPDATE
    return RUN_FAIL_UNKNOWN


def run_needs_review_reason_code_for(line_id: str) -> str:
    if line_id == "credit_card_statement":
        return RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED
    if line_id == "bank_statement":
        return RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED
    return RUN_NEEDS_REVIEW_UNKNOWN
