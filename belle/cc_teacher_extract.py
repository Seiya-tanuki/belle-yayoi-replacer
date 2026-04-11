# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import hashlib
import io
import json
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Sequence

from .fs_utils import sha256_file_chunked
from .io_atomic import atomic_write_bytes
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
SCHEMA_CC_TEACHER_MANIFEST_INDEX_V1 = "belle.cc_teacher_manifest_index.v1"
SCHEMA_CC_TEACHER_EXTRACTOR_V1 = "belle.cc_teacher_extract.v1"
CC_TEACHER_EXTRACTOR_VERSION_V1 = "0.3"
DEFAULT_CC_TEACHER_RULESET_RELPATH = "rulesets/credit_card_statement/teacher_extraction_rules_v1.json"


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


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


def _normalize_required_str_list(value: Any, *, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} is required and must be a list of non-blank strings")
    out: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            out.append(text)
    normalized = sorted(set(out))
    if not normalized:
        raise ValueError(f"{field_name} must contain at least one non-blank value")
    return normalized


def _require_dict(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} is required and must be an object")
    return value


def _require_int(value: Any, *, field_name: str, minimum: int) -> int:
    try:
        parsed = int(value)
    except Exception as exc:
        raise ValueError(f"{field_name} must be an integer >= {minimum}") from exc
    if parsed < minimum:
        raise ValueError(f"{field_name} must be an integer >= {minimum}")
    return parsed


def _require_float(value: Any, *, field_name: str, minimum_exclusive: float, maximum_inclusive: float) -> float:
    try:
        parsed = float(value)
    except Exception as exc:
        raise ValueError(
            f"{field_name} must be > {minimum_exclusive:g} and <= {maximum_inclusive:g}"
        ) from exc
    if parsed <= minimum_exclusive or parsed > maximum_inclusive:
        raise ValueError(f"{field_name} must be > {minimum_exclusive:g} and <= {maximum_inclusive:g}")
    return parsed


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
    try:
        return normalize_credit_card_teacher_extraction_config(raw)
    except ValueError as exc:
        raise ValueError(f"invalid credit_card_line_config.json (fail-closed): {cfg_path}: {exc}") from exc


def normalize_credit_card_teacher_extraction_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    teacher_raw = _require_dict(raw.get("teacher_extraction"), field_name="teacher_extraction")
    soft_raw = (
        teacher_raw.get("soft_match_thresholds")
        if isinstance(teacher_raw.get("soft_match_thresholds"), dict)
        else {}
    )
    canonical_payable_raw = _require_dict(
        teacher_raw.get("canonical_payable_thresholds"),
        field_name="teacher_extraction.canonical_payable_thresholds",
    )
    return {
        "schema": str(raw.get("schema") or "belle.credit_card_line_config.v1"),
        "version": str(raw.get("version") or ""),
        "target_payable_placeholder_names": _normalize_required_str_list(
            raw.get("target_payable_placeholder_names"),
            field_name="target_payable_placeholder_names",
        ),
        "teacher_extraction": {
            "enabled": bool(teacher_raw.get("enabled", True)),
            "ruleset_relpath": str(teacher_raw.get("ruleset_relpath") or DEFAULT_CC_TEACHER_RULESET_RELPATH),
            "payable_candidate_accounts": _normalize_str_list(
                teacher_raw.get("payable_candidate_accounts"),
                default=[],
            ),
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
            "canonical_payable_thresholds": {
                "min_count": _require_int(
                    canonical_payable_raw.get("min_count"),
                    field_name="teacher_extraction.canonical_payable_thresholds.min_count",
                    minimum=1,
                ),
                "min_p_majority": _require_float(
                    canonical_payable_raw.get("min_p_majority"),
                    field_name="teacher_extraction.canonical_payable_thresholds.min_p_majority",
                    minimum_exclusive=0.0,
                    maximum_inclusive=1.0,
                ),
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
            default=["未払費用", "未払金"],
        ),
        "hard_include_terms": _normalize_terms(_normalize_str_list(raw.get("hard_include_terms"), default=[])),
        "soft_include_terms": _normalize_terms(_normalize_str_list(raw.get("soft_include_terms"), default=[])),
        "exclude_terms": _normalize_terms(_normalize_str_list(raw.get("exclude_terms"), default=[])),
        "soft_negative_terms": _normalize_terms(_normalize_str_list(raw.get("soft_negative_terms"), default=[])),
    }


def effective_cc_teacher_payable_candidate_accounts(
    config: Dict[str, Any],
    ruleset: Dict[str, Any],
) -> list[str]:
    normalized_config = normalize_credit_card_teacher_extraction_config(config)
    normalized_ruleset = normalize_cc_teacher_extraction_ruleset(ruleset)
    teacher_config = normalized_config["teacher_extraction"]
    configured = _normalize_str_list(teacher_config.get("payable_candidate_accounts"), default=[])
    if configured:
        return configured
    return list(normalized_ruleset["teacher_payable_candidate_accounts"])


def cc_teacher_ruleset_identity(*, ruleset_path: Path, ruleset: Dict[str, Any]) -> Dict[str, Any]:
    normalized_ruleset = normalize_cc_teacher_extraction_ruleset(ruleset)
    payload = json.dumps(normalized_ruleset, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "path": str(ruleset_path.as_posix()),
        "schema": str(normalized_ruleset["schema"]),
        "version": str(normalized_ruleset["version"]),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def write_cc_teacher_csv(path: Path, rows: Sequence[Sequence[Any]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, dialect="excel", lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        writer.writerow([str(value or "") for value in row])
    atomic_write_bytes(path, buffer.getvalue().encode("cp932", errors="strict"))
    return sha256_file_chunked(path)


def derive_cc_teacher_csv_from_raw_source(
    *,
    line_root: Path,
    raw_sha256: str,
    raw_source_entry: Dict[str, Any],
    raw_stored_path: Path,
    derived_dir: Path,
    config: Dict[str, Any],
    ruleset: Dict[str, Any],
    ruleset_path: Path,
    applied_to_cache_learning: bool,
    applied_to_cache_learning_at: str = "",
) -> Dict[str, Any]:
    source_identity = {
        "raw_sha256": str(raw_sha256),
        "original_name": str(raw_source_entry.get("original_name") or raw_stored_path.name),
        "stored_name": str(raw_source_entry.get("stored_name") or raw_stored_path.name),
        "stored_relpath": str(raw_source_entry.get("stored_relpath") or ""),
        "csv_path": str(raw_stored_path),
    }
    extracted = extract_cc_teacher_rows_from_yayoi_csv(
        raw_stored_path,
        source_identity=source_identity,
        config=config,
        ruleset=ruleset,
    )

    derived_dir.mkdir(parents=True, exist_ok=True)
    derived_name = f"{raw_sha256}__cc_teacher.csv"
    derived_path = derived_dir / derived_name
    derived_sha256 = write_cc_teacher_csv(derived_path, extracted["selected_rows"])
    try:
        derived_relpath = derived_path.relative_to(line_root).as_posix()
    except ValueError:
        derived_relpath = derived_name

    manifest = extracted["manifest"] if isinstance(extracted.get("manifest"), dict) else {}
    row_counts = manifest.get("row_counts") if isinstance(manifest.get("row_counts"), dict) else {}
    reasons = manifest.get("reasons") if isinstance(manifest.get("reasons"), dict) else {}
    selected_subaccounts = manifest.get("selected_subaccounts") if isinstance(manifest.get("selected_subaccounts"), list) else []
    excluded_subaccounts = manifest.get("excluded_subaccounts") if isinstance(manifest.get("excluded_subaccounts"), list) else []
    threshold_snapshot = (
        manifest.get("selection_thresholds") if isinstance(manifest.get("selection_thresholds"), dict) else {}
    )
    if isinstance(config.get("teacher_extraction"), dict):
        threshold_snapshot = dict(threshold_snapshot)
        threshold_snapshot["canonical_payable_thresholds"] = dict(
            (config.get("teacher_extraction") or {}).get("canonical_payable_thresholds") or {}
        )

    entry = {
        "raw_sha256": str(raw_sha256),
        "source_identity": {
            "original_name": str(source_identity["original_name"]),
            "stored_name": str(source_identity["stored_name"]),
            "stored_relpath": str(source_identity["stored_relpath"]),
        },
        "derived_csv_relpath": str(derived_relpath),
        "derived_csv_sha256": str(derived_sha256),
        "extractor": {
            "schema": SCHEMA_CC_TEACHER_EXTRACTOR_V1,
            "version": CC_TEACHER_EXTRACTOR_VERSION_V1,
            "manifest_schema": SCHEMA_CC_TEACHER_EXTRACTION_MANIFEST_V1,
        },
        "ruleset": cc_teacher_ruleset_identity(ruleset_path=ruleset_path, ruleset=ruleset),
        "effective_thresholds_snapshot": threshold_snapshot,
        "row_counts": {
            "source_rows_total": int(row_counts.get("source_rows_total") or 0),
            "payable_candidate_rows": int(row_counts.get("payable_candidate_rows") or 0),
            "selected_rows": int(row_counts.get("selected_rows") or 0),
            "rejected_rows": int(row_counts.get("rejected_rows") or 0),
        },
        "selected_subaccounts": selected_subaccounts,
        "excluded_subaccounts": excluded_subaccounts,
        "reason_counts": {
            "group_reason_counts": dict(reasons.get("group_reason_counts") or {}),
            "row_reason_counts": dict(reasons.get("row_reason_counts") or {}),
        },
        "applied_to_cache_learning": bool(applied_to_cache_learning),
    }
    if applied_to_cache_learning_at:
        entry["applied_to_cache_learning_at"] = str(applied_to_cache_learning_at)
    return entry


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
    candidate_accounts = set(effective_cc_teacher_payable_candidate_accounts(normalized_config, normalized_ruleset))
    hard_terms = list(normalized_ruleset["hard_include_terms"])
    soft_terms = list(normalized_ruleset["soft_include_terms"])
    exclude_terms = list(normalized_ruleset["exclude_terms"])
    soft_negative_terms = list(normalized_ruleset["soft_negative_terms"])

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
        matched_soft_negative_terms = _find_matched_terms(subaccount, soft_negative_terms)

        total_count = len(group_rows)
        unique_counter_accounts = len({str(entry["counter_account"]) for entry in group_rows if str(entry["counter_account"])})
        unique_summaries = len({str(entry["summary"]) for entry in group_rows if str(entry["summary"])})
        normalized_subaccount = _normalize_match_text(subaccount)
        meets_soft_thresholds = (
            total_count >= int(thresholds["min_total_count"])
            and unique_counter_accounts >= int(thresholds["min_unique_counter_accounts"])
            and unique_summaries >= int(thresholds["min_unique_summaries"])
        )
        positive_reason = ""
        positive_strength = ""
        if matched_hard_terms:
            positive_reason = "hard_include_term"
            positive_strength = "hard"
        elif matched_soft_terms and meets_soft_thresholds:
            positive_reason = "soft_include_term"
            positive_strength = "soft"
        elif matched_soft_terms:
            positive_reason = "soft_include_threshold_failed"
            positive_strength = "weak_soft"

        if normalized_subaccount in manual_exclude:
            selected = False
            reason = "manual_exclude"
        elif normalized_subaccount in manual_include:
            selected = True
            reason = "manual_include"
        elif matched_exclude_terms:
            selected = False
            reason = "exclude_term"
        elif positive_strength == "hard" and matched_soft_negative_terms:
            selected = True
            reason = "soft_negative_overridden_by_hard_include"
        elif positive_strength == "soft" and matched_soft_negative_terms:
            selected = True
            reason = "soft_negative_overridden_by_soft_include"
        elif positive_strength == "hard":
            selected = True
            reason = "hard_include_term"
        elif positive_strength == "soft":
            selected = True
            reason = "soft_include_term"
        elif positive_strength == "weak_soft" and matched_soft_negative_terms:
            selected = False
            reason = "soft_negative_insufficient_positive"
        elif positive_strength == "weak_soft":
            selected = False
            reason = "soft_include_threshold_failed"
        elif matched_soft_negative_terms:
            selected = False
            reason = "soft_negative_only"
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
                "soft_negative_terms": matched_soft_negative_terms,
            },
            "evaluation": {
                "positive_reason": positive_reason,
                "positive_strength": positive_strength,
                "meets_soft_thresholds": bool(meets_soft_thresholds),
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
            "canonical_payable_thresholds": dict(teacher_config["canonical_payable_thresholds"]),
            "teacher_payable_candidate_accounts": list(
                effective_cc_teacher_payable_candidate_accounts(normalized_config, normalized_ruleset)
            ),
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
