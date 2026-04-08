# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable

import json

CATEGORY_DEFAULTS_SCHEMA_V2 = "belle.category_defaults.v2"
CATEGORY_OVERRIDES_SCHEMA_V2 = "belle.category_overrides.v2"
UTF8_BOM = b"\xEF\xBB\xBF"
_VALID_PRIORITIES = {"HIGH", "MED", "LOW"}
_OVERRIDE_ROW_KEYS = {"target_account", "target_tax_division"}


@dataclass(frozen=True)
class DefaultRule:
    target_account: str
    target_tax_division: str
    confidence: float
    priority: str
    reason_code: str


@dataclass(frozen=True)
class CategoryOverride:
    target_account: str
    target_tax_division: str


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


def _require_object(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object.")
    return value


def _require_string(
    value: Any,
    *,
    label: str,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string.")
    if not allow_empty and not value.strip():
        raise ValueError(f"{label} must be a non-empty string.")
    return value


def _require_float(value: Any, *, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a float.") from exc
    if number < 0.0 or number > 1.0:
        raise ValueError(f"{label} must be in range 0..1.")
    return number


def _require_priority(value: Any, *, label: str) -> str:
    priority = _require_string(value, label=label)
    if priority not in _VALID_PRIORITIES:
        raise ValueError(
            f"{label} must be one of {sorted(_VALID_PRIORITIES)}, got '{priority}'."
        )
    return priority


def _parse_default_rule(value: Any, *, label: str) -> DefaultRule:
    obj = _require_object(value, label=label)
    return DefaultRule(
        target_account=_require_string(obj.get("target_account"), label=f"{label}.target_account"),
        target_tax_division=_require_string(
            obj.get("target_tax_division"),
            label=f"{label}.target_tax_division",
            allow_empty=True,
        ),
        confidence=_require_float(obj.get("confidence"), label=f"{label}.confidence"),
        priority=_require_priority(obj.get("priority"), label=f"{label}.priority"),
        reason_code=_require_string(obj.get("reason_code"), label=f"{label}.reason_code"),
    )


def _default_global_fallback_rule() -> DefaultRule:
    return DefaultRule(
        target_account="仮払金",
        target_tax_division="",
        confidence=0.35,
        priority="HIGH",
        reason_code="global_fallback",
    )


def load_category_defaults(path: Path) -> CategoryDefaults:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("category_defaults top-level object must be a JSON object.")

    schema = _require_string(obj.get("schema"), label="schema")
    if schema != CATEGORY_DEFAULTS_SCHEMA_V2:
        raise ValueError(
            f"schema must be '{CATEGORY_DEFAULTS_SCHEMA_V2}', got '{schema}'."
        )

    version = _require_string(obj.get("version"), label="version")
    defaults_obj = _require_object(obj.get("defaults"), label="defaults")
    defs: Dict[str, DefaultRule] = {}
    for key, value in defaults_obj.items():
        defs[str(key)] = _parse_default_rule(value, label=f"defaults['{key}']")

    global_fallback_obj = obj.get("global_fallback")
    global_rule = (
        _default_global_fallback_rule()
        if global_fallback_obj is None
        else _parse_default_rule(global_fallback_obj, label="global_fallback")
    )

    return CategoryDefaults(
        schema=schema,
        version=version,
        defaults=defs,
        global_fallback=global_rule,
    )


def _read_json_without_bom(path: Path) -> tuple[dict[str, Any], bool]:
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

    return obj, has_utf8_bom


def _parse_override_row(value: Any, *, label: str) -> CategoryOverride:
    row = _require_object(value, label=label)
    actual_keys = {str(k) for k in row.keys()}
    missing_keys = sorted(_OVERRIDE_ROW_KEYS - actual_keys)
    extra_keys = sorted(actual_keys - _OVERRIDE_ROW_KEYS)
    if missing_keys or extra_keys:
        parts = []
        if missing_keys:
            parts.append(f"missing={missing_keys}")
        if extra_keys:
            parts.append(f"extra={extra_keys}")
        raise ValueError(f"{label} keys mismatch: " + ", ".join(parts))

    return CategoryOverride(
        target_account=_require_string(row.get("target_account"), label=f"{label}.target_account"),
        target_tax_division=_require_string(
            row.get("target_tax_division"),
            label=f"{label}.target_tax_division",
            allow_empty=True,
        ),
    )


def load_category_overrides(
    path: Path,
    lexicon_category_keys: Iterable[str],
) -> Dict[str, CategoryOverride]:
    obj, has_utf8_bom = _read_json_without_bom(path)

    schema = str(obj.get("schema") or "")
    if schema != CATEGORY_OVERRIDES_SCHEMA_V2:
        raise ValueError(
            f"schema must be '{CATEGORY_OVERRIDES_SCHEMA_V2}', got '{schema or '(empty)'}'."
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

    resolved: Dict[str, CategoryOverride] = {}
    for key in sorted(expected_keys):
        resolved[key] = _parse_override_row(overrides.get(key), label=f"overrides['{key}']")

    if has_utf8_bom:
        path.write_bytes(path.read_bytes()[len(UTF8_BOM):])
        print(f"[WARN] UTF-8 BOM detected and removed: {path}")

    return resolved


def try_load_category_overrides(
    path: Path,
    lexicon_category_keys: Iterable[str],
) -> tuple[dict[str, CategoryOverride], list[str]]:
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
    if schema != CATEGORY_OVERRIDES_SCHEMA_V2:
        warnings.append(
            "category_overrides_schema_invalid: "
            f"path={path} expected={CATEGORY_OVERRIDES_SCHEMA_V2} actual={schema or '(empty)'}"
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

    resolved: dict[str, CategoryOverride] = {}
    invalid_rows: list[str] = []
    invalid_values: list[str] = []
    missing_row_fields: list[str] = []
    extra_row_fields: list[str] = []

    for key in sorted(expected_keys):
        row = overrides.get(key)
        if not isinstance(row, dict):
            invalid_rows.append(key)
            continue

        row_keys = {str(k) for k in row.keys()}
        missing_required = sorted(_OVERRIDE_ROW_KEYS - row_keys)
        extra = sorted(row_keys - _OVERRIDE_ROW_KEYS)
        if missing_required:
            missing_row_fields.append(key)
        if extra:
            extra_row_fields.append(key)

        target_account = row.get("target_account")
        target_tax_division = row.get("target_tax_division")
        if not isinstance(target_account, str) or not target_account.strip():
            invalid_values.append(key)
            continue
        if not isinstance(target_tax_division, str):
            invalid_values.append(key)
            continue
        if missing_required:
            invalid_values.append(key)
            continue

        resolved[key] = CategoryOverride(
            target_account=target_account,
            target_tax_division=target_tax_division,
        )

    if missing_row_fields:
        warnings.append(
            "category_overrides_row_missing_keys: "
            f"path={path} {_format_key_summary(missing_row_fields)}"
        )
    if extra_row_fields:
        warnings.append(
            "category_overrides_row_extra_keys: "
            f"path={path} {_format_key_summary(extra_row_fields)}"
        )
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
            f"fallback={global_defaults.global_fallback.target_account}"
        )

    overrides = {}
    for key in keys:
        rule = global_defaults.defaults.get(key)
        effective_rule = rule if rule is not None else global_defaults.global_fallback
        overrides[key] = {
            "target_account": effective_rule.target_account,
            "target_tax_division": effective_rule.target_tax_division,
        }

    payload = {
        "schema": CATEGORY_OVERRIDES_SCHEMA_V2,
        "client_id": str(client_id),
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "note_ja": "target_account と target_tax_division の文字列値のみ編集してください。キーや構造は変更しないでください。",
        "overrides": overrides,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def merge_effective_defaults(
    global_defaults: CategoryDefaults,
    overrides_by_category: Dict[str, CategoryOverride],
) -> CategoryDefaults:
    merged_defaults: Dict[str, DefaultRule] = {}
    for key, rule in global_defaults.defaults.items():
        override = overrides_by_category.get(key)
        target_account = rule.target_account
        target_tax_division = rule.target_tax_division
        if override is not None:
            target_account = override.target_account
            target_tax_division = override.target_tax_division
        if not isinstance(target_account, str) or not target_account.strip():
            raise ValueError(f"Invalid override target_account for category '{key}'.")
        if not isinstance(target_tax_division, str):
            raise ValueError(f"Invalid override target_tax_division for category '{key}'.")
        merged_defaults[key] = DefaultRule(
            target_account=target_account,
            target_tax_division=target_tax_division,
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
