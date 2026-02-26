# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Iterable

import json

CATEGORY_OVERRIDES_SCHEMA_V1 = "belle.category_overrides.v1"
UTF8_BOM = b"\xEF\xBB\xBF"


@dataclass(frozen=True)
class DefaultRule:
    debit_account: str
    confidence: float
    priority: str
    reason_code: str


@dataclass
class CategoryDefaults:
    schema: str
    version: str
    defaults: Dict[str, DefaultRule]          # category_key -> rule
    global_fallback: DefaultRule


def _format_key_summary(keys: Iterable[str], *, limit: int = 20) -> str:
    normalized = sorted({str(k) for k in keys})
    sample = normalized[:limit]
    return f"count={len(normalized)} sample={json.dumps(sample, ensure_ascii=False)}"


def load_category_defaults(path: Path) -> CategoryDefaults:
    obj = json.loads(path.read_text(encoding="utf-8"))
    defs: Dict[str, DefaultRule] = {}
    for k, v in (obj.get("defaults") or {}).items():
        defs[str(k)] = DefaultRule(
            debit_account=str(v["debit_account"]),
            confidence=float(v.get("confidence") or 0.5),
            priority=str(v.get("priority") or "MED"),
            reason_code=str(v.get("reason_code") or "category_default"),
        )
    gf = obj.get("global_fallback") or {}
    global_rule = DefaultRule(
        debit_account=str(gf.get("debit_account") or "仮払金"),
        confidence=float(gf.get("confidence") or 0.35),
        priority=str(gf.get("priority") or "HIGH"),
        reason_code=str(gf.get("reason_code") or "global_fallback"),
    )
    return CategoryDefaults(
        schema=str(obj.get("schema") or "belle.category_defaults.v1"),
        version=str(obj.get("version") or ""),
        defaults=defs,
        global_fallback=global_rule,
    )


def load_category_overrides(path: Path, lexicon_category_keys: Iterable[str]) -> Dict[str, str]:
    raw_bytes = path.read_bytes()
    has_utf8_bom = raw_bytes.startswith(UTF8_BOM)
    parse_bytes = raw_bytes[len(UTF8_BOM):] if has_utf8_bom else raw_bytes

    try:
        obj = json.loads(parse_bytes.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ValueError(f"Invalid UTF-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc

    if not isinstance(obj, dict):
        raise ValueError("Top-level object must be a JSON object.")

    schema = str(obj.get("schema") or "")
    if schema != CATEGORY_OVERRIDES_SCHEMA_V1:
        raise ValueError(
            f"schema must be '{CATEGORY_OVERRIDES_SCHEMA_V1}', got '{schema or '(empty)'}'."
        )

    overrides = obj.get("overrides")
    if not isinstance(overrides, dict):
        raise ValueError("'overrides' must be a JSON object.")

    expected_keys = {str(k) for k in lexicon_category_keys}
    actual_keys = {str(k) for k in overrides.keys()}
    missing = sorted(expected_keys - actual_keys)
    extra = sorted(actual_keys - expected_keys)
    if missing or extra:
        parts = []
        if missing:
            parts.append(f"missing={missing}")
        if extra:
            parts.append(f"extra={extra}")
        raise ValueError("overrides keys mismatch: " + ", ".join(parts))

    resolved: Dict[str, str] = {}
    for key in sorted(expected_keys):
        row = overrides.get(key)
        if not isinstance(row, dict):
            raise ValueError(f"overrides['{key}'] must be an object.")
        debit = row.get("debit_account")
        if not isinstance(debit, str) or not debit.strip():
            raise ValueError(f"overrides['{key}'].debit_account must be a non-empty string.")
        resolved[key] = debit

    if has_utf8_bom:
        # Tiny unit-style check: BOM + valid JSON should parse, validate, then write back bytes[3:].
        path.write_bytes(parse_bytes)
        print(f"[WARN] UTF-8 BOM detected and removed: {path}")

    return resolved


def try_load_category_overrides(
    path: Path,
    lexicon_category_keys: Iterable[str],
) -> tuple[dict[str, str], list[str]]:
    warnings: list[str] = []
    if not path.exists():
        return {}, [f"category_overrides_missing_file: path={path}"]

    raw_bytes = path.read_bytes()
    has_utf8_bom = raw_bytes.startswith(UTF8_BOM)
    parse_bytes = raw_bytes[len(UTF8_BOM):] if has_utf8_bom else raw_bytes

    if has_utf8_bom:
        path.write_bytes(parse_bytes)
        warnings.append(f"category_overrides_bom_removed: path={path}")

    try:
        decoded = parse_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        warnings.append(
            f"category_overrides_invalid_utf8: path={path} start={exc.start} end={exc.end}"
        )
        return {}, warnings

    try:
        obj = json.loads(decoded)
    except json.JSONDecodeError as exc:
        warnings.append(
            f"category_overrides_invalid_json: path={path} line={exc.lineno} col={exc.colno}"
        )
        return {}, warnings

    if not isinstance(obj, dict):
        warnings.append(
            f"category_overrides_top_level_invalid: path={path} type={type(obj).__name__}"
        )
        return {}, warnings

    schema = str(obj.get("schema") or "")
    if schema != CATEGORY_OVERRIDES_SCHEMA_V1:
        warnings.append(
            "category_overrides_schema_invalid: "
            f"path={path} expected={CATEGORY_OVERRIDES_SCHEMA_V1} actual={schema or '(empty)'}"
        )
        return {}, warnings

    overrides = obj.get("overrides")
    if not isinstance(overrides, dict):
        warnings.append(
            f"category_overrides_overrides_invalid: path={path} type={type(overrides).__name__}"
        )
        return {}, warnings

    expected_keys = {str(k) for k in lexicon_category_keys}
    actual_keys = {str(k) for k in overrides.keys()}

    missing_keys = sorted(expected_keys - actual_keys)
    extra_keys = sorted(actual_keys - expected_keys)
    if extra_keys:
        warnings.append(
            "category_overrides_extra_keys: "
            f"path={path} {_format_key_summary(extra_keys)}"
        )
    if missing_keys:
        warnings.append(
            "category_overrides_missing_keys: "
            f"path={path} {_format_key_summary(missing_keys)}"
        )

    resolved: dict[str, str] = {}
    invalid_rows: list[str] = []
    invalid_values: list[str] = []

    for key in sorted(expected_keys):
        row = overrides.get(key)
        if not isinstance(row, dict):
            invalid_rows.append(key)
            continue
        debit = row.get("debit_account")
        if not isinstance(debit, str) or not debit.strip():
            invalid_values.append(key)
            continue
        resolved[key] = debit

    if invalid_rows:
        warnings.append(
            "category_overrides_row_invalid: "
            f"path={path} {_format_key_summary(invalid_rows)}"
        )
    if invalid_values:
        warnings.append(
            "category_overrides_value_invalid: "
            f"path={path} {_format_key_summary(invalid_values)}"
        )

    return resolved, warnings


def generate_full_category_overrides(
    path: Path,
    client_id: str,
    global_defaults: CategoryDefaults,
    lexicon_category_keys: Iterable[str],
) -> None:
    keys = sorted({str(k) for k in lexicon_category_keys})
    missing_defaults = [k for k in keys if k not in global_defaults.defaults]
    if missing_defaults:
        print(
            "[WARN] category_overrides_generate_missing_defaults: "
            f"{_format_key_summary(missing_defaults)} "
            f"fallback={global_defaults.global_fallback.debit_account}"
        )

    overrides = {}
    for key in keys:
        rule = global_defaults.defaults.get(key)
        debit_account = (
            rule.debit_account
            if rule is not None
            else global_defaults.global_fallback.debit_account
        )
        overrides[key] = {"debit_account": debit_account}

    payload = {
        "schema": CATEGORY_OVERRIDES_SCHEMA_V1,
        "client_id": str(client_id),
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "note_ja": "Edit ONLY debit_account string values. Do not change keys/structure.",
        "overrides": overrides,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def merge_effective_defaults(
    global_defaults: CategoryDefaults,
    override_debit_accounts: Dict[str, str],
) -> CategoryDefaults:
    merged_defaults: Dict[str, DefaultRule] = {}
    for key, rule in global_defaults.defaults.items():
        debit = override_debit_accounts.get(key, rule.debit_account)
        if not isinstance(debit, str) or not debit.strip():
            raise ValueError(f"Invalid override debit_account for category '{key}'.")
        merged_defaults[key] = DefaultRule(
            debit_account=debit,
            confidence=rule.confidence,
            priority=rule.priority,
            reason_code=rule.reason_code,
        )

    return CategoryDefaults(
        schema=global_defaults.schema,
        version=global_defaults.version,
        defaults=merged_defaults,
        global_fallback=global_defaults.global_fallback,
    )
