# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Sequence

from .yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_SUMMARY,
)
from .yayoi_csv import read_yayoi_csv, token_to_text

LINE_ID_CC = "credit_card_statement"
SCHEMA_CC_TEACHER_EXTRACTION_RULES_V1 = "belle.cc_teacher_extraction_rules.v1"
SCHEMA_CC_TEACHER_EXTRACTION_MANIFEST_V1 = "belle.cc_teacher_extract_manifest.v1"
DEFAULT_CC_TEACHER_RULESET_RELPATH = "rulesets/credit_card_statement/teacher_extraction_rules_v1.json"


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_str_list(value: Any, *, default: list[str]) -> list[str]:
    if not isinstance(value, list):
        return list(default)
    out: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            out.append(text)
    if not out:
        return list(default)
    return sorted(set(out))


def _normalize_match_text(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).strip().upper()


def _normalized_set(values: Sequence[str]) -> set[str]:
    return {normalized for normalized in (_normalize_match_text(v) for v in values) if normalized}


def _normalize_terms(values: Sequence[str]) -> list[str]:
    return sorted({term for term in (_normalize_match_text(v) for v in values) if term})


def _row_cell(row: Sequence[Any], index: int) -> str:
    if index < 0 or index >= len(row):
        return ""
    return str(row[index] or "").strip()


def _row_copy(row: Sequence[Any]) -> list[str]:
    return [str(value or "") for value in row]


def load_credit_card_teacher_extraction_config(repo_root: Path, client_id: str) -> Dict[str, Any]:
    cfg_path = repo_root / "clients" / client_id / "lines" / LINE_ID_CC / "config" / "credit_card_line_config.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"missing_cc_config: expected={cfg_path}")
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to parse credit_card_line_config.json (fail-closed): {cfg_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"credit_card_line_config.json must be a JSON object (fail-closed): {cfg_path}")
    return normalize_credit_card_teacher_extraction_config(raw)


def normalize_credit_card_teacher_extraction_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    teacher_raw = raw.get("teacher_extraction") if isinstance(raw.get("teacher_extraction"), dict) else {}
    soft_raw = (
        teacher_raw.get("soft_match_thresholds")
        if isinstance(teacher_raw.get("soft_match_thresholds"), dict)
        else {}
    )
    payable_account_name = str(raw.get("payable_account_name") or "未払金").strip() or "未払金"
    return {
        "schema": str(raw.get("schema") or "belle.credit_card_line_config.v1"),
        "version": str(raw.get("version") or ""),
        "payable_account_name": payable_account_name,
        "target_payable_placeholder_names": _normalize_str_list(
            raw.get("target_payable_placeholder_names"),
            default=[payable_account_name],
        ),
        "teacher_extraction": {
            "enabled": bool(teacher_raw.get("enabled", True)),
            "ruleset_relpath": str(teacher_raw.get("ruleset_relpath") or DEFAULT_CC_TEACHER_RULESET_RELPATH),
            "manual_include_subaccounts": _normalize_str_list(
                teacher_raw.get("manual_include_subaccounts"),
                default=[],
            ),
            "manual_exclude_subaccounts": _normalize_str_list(
                teacher_raw.get("manual_exclude_subaccounts"),
                default=[],
            ),
            "soft_match_thresholds": {
                "min_total_count": _as_int(soft_raw.get("min_total_count"), 2),
                "min_unique_counter_accounts": _as_int(soft_raw.get("min_unique_counter_accounts"), 2),
                "min_unique_summaries": _as_int(soft_raw.get("min_unique_summaries"), 2),
            },
        },
    }


def resolve_cc_teacher_ruleset_path(repo_root: Path, config: Dict[str, Any]) -> Path:
    teacher = config.get("teacher_extraction") if isinstance(config.get("teacher_extraction"), dict) else {}
    relpath = str(teacher.get("ruleset_relpath") or DEFAULT_CC_TEACHER_RULESET_RELPATH).strip()
    if not relpath:
        relpath = DEFAULT_CC_TEACHER_RULESET_RELPATH
    return repo_root / Path(relpath)


def load_cc_teacher_extraction_ruleset(path: Path) -> Dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to parse teacher extraction ruleset: {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"teacher extraction ruleset must be a JSON object: {path}")
    return normalize_cc_teacher_extraction_ruleset(raw)


def normalize_cc_teacher_extraction_ruleset(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema": str(raw.get("schema") or SCHEMA_CC_TEACHER_EXTRACTION_RULES_V1),
        "version": str(raw.get("version") or "1"),
        "teacher_payable_candidate_accounts": _normalize_str_list(
            raw.get("teacher_payable_candidate_accounts"),
            default=["未払金"],
        ),
        "hard_include_terms": _normalize_terms(_normalize_str_list(raw.get("hard_include_terms"), default=[])),
        "soft_include_terms": _normalize_terms(_normalize_str_list(raw.get("soft_include_terms"), default=[])),
        "exclude_terms": _normalize_terms(_normalize_str_list(raw.get("exclude_terms"), default=[])),
    }


def read_yayoi_csv_rows(path: Path) -> list[list[str]]:
    csv_obj = read_yayoi_csv(path)
    return [[token_to_text(token, csv_obj.encoding) for token in row.tokens] for row in csv_obj.rows]


def extract_cc_teacher_rows_from_yayoi_csv(
    path: Path,
    *,
    source_identity: Dict[str, Any] | None,
    config: Dict[str, Any],
    ruleset: Dict[str, Any],
) -> Dict[str, Any]:
    source = dict(source_identity or {})
    source.setdefault("csv_path", str(path))
    return extract_cc_teacher_rows(
        read_yayoi_csv_rows(path),
        source_identity=source,
        config=config,
        ruleset=ruleset,
    )


def _find_matched_terms(text: str, terms: Sequence[str]) -> list[str]:
    normalized_text = _normalize_match_text(text)
    if not normalized_text:
        return []
    return sorted({term for term in terms if term and term in normalized_text})


def extract_cc_teacher_rows(
    rows: Sequence[Sequence[Any]],
    *,
    source_identity: Dict[str, Any] | None,
    config: Dict[str, Any],
    ruleset: Dict[str, Any],
) -> Dict[str, Any]:
    normalized_config = normalize_credit_card_teacher_extraction_config(config)
    normalized_ruleset = normalize_cc_teacher_extraction_ruleset(ruleset)
    teacher_config = normalized_config["teacher_extraction"]
    thresholds = teacher_config["soft_match_thresholds"]

    manual_include = _normalized_set(teacher_config["manual_include_subaccounts"])
    manual_exclude = _normalized_set(teacher_config["manual_exclude_subaccounts"])
    candidate_accounts = set(normalized_ruleset["teacher_payable_candidate_accounts"])
    hard_terms = list(normalized_ruleset["hard_include_terms"])
    soft_terms = list(normalized_ruleset["soft_include_terms"])
    exclude_terms = list(normalized_ruleset["exclude_terms"])

    candidate_row_entries: list[dict[str, Any]] = []
    row_reason_counts: dict[str, int] = {}

    for index, row in enumerate(rows):
        debit_account = _row_cell(row, COL_DEBIT_ACCOUNT)
        debit_subaccount = _row_cell(row, COL_DEBIT_SUBACCOUNT)
        credit_account = _row_cell(row, COL_CREDIT_ACCOUNT)
        credit_subaccount = _row_cell(row, COL_CREDIT_SUBACCOUNT)
        summary = _row_cell(row, COL_SUMMARY)

        payable_account = ""
        payable_subaccount = ""
        counter_account = ""
        if debit_account in candidate_accounts and credit_account not in candidate_accounts:
            payable_account = debit_account
            payable_subaccount = debit_subaccount
            counter_account = credit_account
        elif credit_account in candidate_accounts and debit_account not in candidate_accounts:
            payable_account = credit_account
            payable_subaccount = credit_subaccount
            counter_account = debit_account
        else:
            row_reason_counts["payable_account_not_candidate"] = row_reason_counts.get("payable_account_not_candidate", 0) + 1
            continue

        candidate_row_entries.append(
            {
                "row_index": int(index),
                "row": _row_copy(row),
                "summary": summary,
                "payable_account": payable_account,
                "payable_subaccount": payable_subaccount,
                "counter_account": counter_account,
            }
        )

    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in candidate_row_entries:
        grouped.setdefault(str(entry["payable_subaccount"]), []).append(entry)

    selected_subaccounts: list[dict[str, Any]] = []
    excluded_subaccounts: list[dict[str, Any]] = []
    group_reason_counts: dict[str, int] = {}
    selected_subaccount_names: set[str] = set()

    for subaccount in sorted(grouped.keys()):
        group_rows = grouped[subaccount]
        matched_exclude_terms = _find_matched_terms(subaccount, exclude_terms)
        matched_hard_terms = _find_matched_terms(subaccount, hard_terms)
        matched_soft_terms = _find_matched_terms(subaccount, soft_terms)

        total_count = len(group_rows)
        unique_counter_accounts = len({str(entry["counter_account"]) for entry in group_rows if str(entry["counter_account"])})
        unique_summaries = len({str(entry["summary"]) for entry in group_rows if str(entry["summary"])})
        normalized_subaccount = _normalize_match_text(subaccount)
        meets_soft_thresholds = (
            total_count >= int(thresholds["min_total_count"])
            and unique_counter_accounts >= int(thresholds["min_unique_counter_accounts"])
            and unique_summaries >= int(thresholds["min_unique_summaries"])
        )

        if normalized_subaccount in manual_exclude:
            selected = False
            reason = "manual_exclude"
        elif normalized_subaccount in manual_include:
            selected = True
            reason = "manual_include"
        elif matched_exclude_terms:
            selected = False
            reason = "exclude_term"
        elif matched_hard_terms:
            selected = True
            reason = "hard_include_term"
        elif matched_soft_terms and meets_soft_thresholds:
            selected = True
            reason = "soft_include_term"
        elif matched_soft_terms:
            selected = False
            reason = "soft_include_threshold_failed"
        elif not subaccount.strip():
            selected = False
            reason = "blank_payable_subaccount"
        else:
            selected = False
            reason = "no_include_match"

        group_reason_counts[reason] = group_reason_counts.get(reason, 0) + 1
        row_reason_counts[reason] = row_reason_counts.get(reason, 0) + total_count

        detail = {
            "payable_subaccount": subaccount,
            "reason": reason,
            "total_count": int(total_count),
            "unique_counter_accounts": int(unique_counter_accounts),
            "unique_summaries": int(unique_summaries),
            "matched_terms": {
                "exclude_terms": matched_exclude_terms,
                "hard_include_terms": matched_hard_terms,
                "soft_include_terms": matched_soft_terms,
            },
            "payable_accounts_seen": sorted({str(entry["payable_account"]) for entry in group_rows if str(entry["payable_account"])}),
        }
        if selected:
            selected_subaccounts.append(detail)
            selected_subaccount_names.add(subaccount)
        else:
            excluded_subaccounts.append(detail)

    selected_rows: list[list[str]] = []
    selected_row_indexes: list[int] = []
    for entry in candidate_row_entries:
        if str(entry["payable_subaccount"]) not in selected_subaccount_names:
            continue
        selected_row_indexes.append(int(entry["row_index"]))
        selected_rows.append(list(entry["row"]))

    manifest = {
        "schema": SCHEMA_CC_TEACHER_EXTRACTION_MANIFEST_V1,
        "line_id": LINE_ID_CC,
        "source_identity": dict(source_identity or {}),
        "selection_thresholds": {
            "enabled": bool(teacher_config["enabled"]),
            "manual_include_subaccounts": list(teacher_config["manual_include_subaccounts"]),
            "manual_exclude_subaccounts": list(teacher_config["manual_exclude_subaccounts"]),
            "soft_match_thresholds": dict(thresholds),
            "teacher_payable_candidate_accounts": list(normalized_ruleset["teacher_payable_candidate_accounts"]),
            "ruleset_schema": str(normalized_ruleset["schema"]),
            "ruleset_version": str(normalized_ruleset["version"]),
        },
        "selected_subaccounts": selected_subaccounts,
        "excluded_subaccounts": excluded_subaccounts,
        "row_counts": {
            "source_rows_total": int(len(rows)),
            "payable_candidate_rows": int(len(candidate_row_entries)),
            "non_candidate_rows": int(len(rows) - len(candidate_row_entries)),
            "selected_rows": int(len(selected_rows)),
            "rejected_rows": int(len(candidate_row_entries) - len(selected_rows)),
        },
        "reasons": {
            "group_reason_counts": dict(sorted(group_reason_counts.items())),
            "row_reason_counts": dict(sorted(row_reason_counts.items())),
        },
    }

    return {
        "selected_rows": selected_rows,
        "selected_row_indexes": selected_row_indexes,
        "manifest": manifest,
    }
