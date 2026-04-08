# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

from .paths import get_client_config_dir
from .yayoi_columns import (
    COL_CREDIT_AMOUNT,
    COL_CREDIT_TAX_AMOUNT,
    COL_CREDIT_TAX_DIVISION,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_TAX_AMOUNT,
    COL_DEBIT_TAX_DIVISION,
)
from .yayoi_csv import YayoiCSV
from .yayoi_text import safe_cell_text, set_cell_text

YAYOI_TAX_CONFIG_FILENAME = "yayoi_tax_config.json"
YAYOI_TAX_CONFIG_SCHEMA = "belle.yayoi_tax_config.v1"
YAYOI_TAX_CONFIG_VERSION = "1.0"

BOOKKEEPING_MODE_TAX_EXCLUDED = "tax_excluded"
BOOKKEEPING_MODE_TAX_INCLUDED = "tax_included"
SUPPORTED_BOOKKEEPING_MODES = {
    BOOKKEEPING_MODE_TAX_EXCLUDED,
    BOOKKEEPING_MODE_TAX_INCLUDED,
}

ROUNDING_MODE_FLOOR = "floor"
SUPPORTED_ROUNDING_MODES = {ROUNDING_MODE_FLOOR}

STATUS_APPLIED_INNER_FLOOR = "applied_inner_floor"
STATUS_TAX_AMOUNT_ALREADY_PRESENT = "tax_amount_already_present"
STATUS_NON_TARGET_TAX_DIVISION = "non_target_tax_division"
STATUS_UNSUPPORTED_CALC_MODE = "unsupported_calc_mode"
STATUS_TAX_RATE_PARSE_FAILED = "tax_rate_parse_failed"
STATUS_AMOUNT_BLANK = "amount_blank"
STATUS_AMOUNT_PARSE_FAILED = "amount_parse_failed"
STATUS_DISABLED = "disabled"
STATUS_BOOKKEEPING_MODE_NOT_SUPPORTED = "bookkeeping_mode_not_supported"

STATUS_VOCABULARY = (
    STATUS_APPLIED_INNER_FLOOR,
    STATUS_TAX_AMOUNT_ALREADY_PRESENT,
    STATUS_NON_TARGET_TAX_DIVISION,
    STATUS_UNSUPPORTED_CALC_MODE,
    STATUS_TAX_RATE_PARSE_FAILED,
    STATUS_AMOUNT_BLANK,
    STATUS_AMOUNT_PARSE_FAILED,
    STATUS_DISABLED,
    STATUS_BOOKKEEPING_MODE_NOT_SUPPORTED,
)

CALC_MODE_INNER = "inner"
CALC_MODE_OUTER = "outer"
CALC_MODE_SEPARATE = "separate"
CALC_MODE_INCLUSIVE = "inclusive"
CALC_MODE_OTHER = "other"

TRAILING_SUFFIX_MARKERS = (
    "区分100%",
    "区分80%",
    "区分50%",
    "適格",
    "控不",
)

_RATE_RE = re.compile(r"(\d+)%")


@dataclass(frozen=True)
class YayoiTaxPostprocessConfig:
    schema: str = YAYOI_TAX_CONFIG_SCHEMA
    version: str = YAYOI_TAX_CONFIG_VERSION
    enabled: bool = False
    bookkeeping_mode: str = BOOKKEEPING_MODE_TAX_EXCLUDED
    rounding_mode: str = ROUNDING_MODE_FLOOR


@dataclass(frozen=True)
class TaxDivisionParseResult:
    original_text: str
    normalized_text: str
    stripped_text: str
    calc_mode: str
    rate_percent: Optional[int]


@dataclass(frozen=True)
class TaxPostprocessSideResult:
    row_index_1b: int
    side: str
    status: str
    tax_division_text: str
    normalized_tax_division_text: str
    stripped_tax_division_text: str
    calc_mode: str
    rate_percent: Optional[int]
    amount_text: str
    existing_tax_amount_text: str
    filled_tax_amount_text: str


@dataclass
class TaxPostprocessSummary:
    enabled: bool
    bookkeeping_mode: str
    rounding_mode: str
    total_rows_changed: int = 0
    debit_filled_count: int = 0
    credit_filled_count: int = 0
    debit_status_counts: Dict[str, int] = field(default_factory=dict)
    credit_status_counts: Dict[str, int] = field(default_factory=dict)
    side_results: list[TaxPostprocessSideResult] = field(default_factory=list)


def default_yayoi_tax_postprocess_config() -> YayoiTaxPostprocessConfig:
    return YayoiTaxPostprocessConfig()


def get_yayoi_tax_config_path(repo_root: Path, client_id: str) -> Path:
    return get_client_config_dir(repo_root, client_id) / YAYOI_TAX_CONFIG_FILENAME


def load_yayoi_tax_postprocess_config(repo_root: Path, client_id: str) -> YayoiTaxPostprocessConfig:
    cfg_path = get_yayoi_tax_config_path(repo_root, client_id)
    if not cfg_path.exists():
        return default_yayoi_tax_postprocess_config()

    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to parse {YAYOI_TAX_CONFIG_FILENAME}: {cfg_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"{YAYOI_TAX_CONFIG_FILENAME} must be a JSON object: {cfg_path}")

    allowed_keys = {"schema", "version", "enabled", "bookkeeping_mode", "rounding_mode"}
    actual_keys = set(raw.keys())
    missing_keys = sorted(allowed_keys - actual_keys)
    extra_keys = sorted(actual_keys - allowed_keys)
    if missing_keys:
        raise ValueError(
            f"{YAYOI_TAX_CONFIG_FILENAME} missing required keys: {', '.join(missing_keys)}: {cfg_path}"
        )
    if extra_keys:
        raise ValueError(
            f"{YAYOI_TAX_CONFIG_FILENAME} contains unsupported keys: {', '.join(extra_keys)}: {cfg_path}"
        )

    schema = str(raw.get("schema") or "").strip()
    version = str(raw.get("version") or "").strip()
    if schema != YAYOI_TAX_CONFIG_SCHEMA:
        raise ValueError(
            f"{YAYOI_TAX_CONFIG_FILENAME} schema must be {YAYOI_TAX_CONFIG_SCHEMA!r}: {cfg_path}"
        )
    if version != YAYOI_TAX_CONFIG_VERSION:
        raise ValueError(
            f"{YAYOI_TAX_CONFIG_FILENAME} version must be {YAYOI_TAX_CONFIG_VERSION!r}: {cfg_path}"
        )

    enabled = raw.get("enabled")
    if not isinstance(enabled, bool):
        raise ValueError(f"{YAYOI_TAX_CONFIG_FILENAME} enabled must be a boolean: {cfg_path}")

    bookkeeping_mode = str(raw.get("bookkeeping_mode") or "").strip()
    if bookkeeping_mode not in SUPPORTED_BOOKKEEPING_MODES:
        supported = ", ".join(sorted(SUPPORTED_BOOKKEEPING_MODES))
        raise ValueError(
            f"{YAYOI_TAX_CONFIG_FILENAME} bookkeeping_mode must be one of [{supported}]: {cfg_path}"
        )

    rounding_mode = str(raw.get("rounding_mode") or "").strip()
    if rounding_mode not in SUPPORTED_ROUNDING_MODES:
        supported = ", ".join(sorted(SUPPORTED_ROUNDING_MODES))
        raise ValueError(
            f"{YAYOI_TAX_CONFIG_FILENAME} rounding_mode must be one of [{supported}]: {cfg_path}"
        )

    return YayoiTaxPostprocessConfig(
        schema=schema,
        version=version,
        enabled=enabled,
        bookkeeping_mode=bookkeeping_mode,
        rounding_mode=rounding_mode,
    )


def normalize_tax_division_text(text: str) -> str:
    return unicodedata.normalize("NFKC", text).strip()


def strip_tax_division_suffix_markers(text: str) -> str:
    stripped = normalize_tax_division_text(text)
    while True:
        updated = stripped
        for marker in TRAILING_SUFFIX_MARKERS:
            if updated.endswith(marker):
                updated = updated[: -len(marker)].rstrip()
                break
        if updated == stripped:
            return stripped
        stripped = updated


def parse_tax_division(text: str) -> TaxDivisionParseResult:
    normalized = normalize_tax_division_text(text)
    stripped = strip_tax_division_suffix_markers(normalized)
    rate_matches = list(_RATE_RE.finditer(stripped))
    rate_match = rate_matches[-1] if rate_matches else None
    calc_mode = _classify_calc_mode(stripped, rate_match)
    rate_percent = int(rate_match.group(1)) if rate_match else None
    return TaxDivisionParseResult(
        original_text=text,
        normalized_text=normalized,
        stripped_text=stripped,
        calc_mode=calc_mode,
        rate_percent=rate_percent,
    )


def compute_inner_tax_floor(amount: int, rate_percent: int) -> int:
    return (amount * rate_percent) // (100 + rate_percent)


def apply_yayoi_tax_postprocess(
    csv_obj: YayoiCSV,
    config: YayoiTaxPostprocessConfig,
) -> TaxPostprocessSummary:
    summary = TaxPostprocessSummary(
        enabled=config.enabled,
        bookkeeping_mode=config.bookkeeping_mode,
        rounding_mode=config.rounding_mode,
        debit_status_counts=_empty_status_counts(),
        credit_status_counts=_empty_status_counts(),
    )
    changed_rows: set[int] = set()

    for row_index_1b, row in enumerate(csv_obj.rows, start=1):
        for side, tax_division_idx, amount_idx, tax_amount_idx in (
            ("debit", COL_DEBIT_TAX_DIVISION, COL_DEBIT_AMOUNT, COL_DEBIT_TAX_AMOUNT),
            ("credit", COL_CREDIT_TAX_DIVISION, COL_CREDIT_AMOUNT, COL_CREDIT_TAX_AMOUNT),
        ):
            result = _apply_side(
                row_index_1b=row_index_1b,
                csv_obj=csv_obj,
                tokens=row.tokens,
                side=side,
                tax_division_idx=tax_division_idx,
                amount_idx=amount_idx,
                tax_amount_idx=tax_amount_idx,
                config=config,
            )
            summary.side_results.append(result)
            side_counts = summary.debit_status_counts if side == "debit" else summary.credit_status_counts
            side_counts[result.status] = side_counts.get(result.status, 0) + 1
            if result.status == STATUS_APPLIED_INNER_FLOOR:
                changed_rows.add(row_index_1b)
                if side == "debit":
                    summary.debit_filled_count += 1
                else:
                    summary.credit_filled_count += 1

    summary.total_rows_changed = len(changed_rows)
    return summary


def _apply_side(
    *,
    row_index_1b: int,
    csv_obj: YayoiCSV,
    tokens: list[bytes],
    side: str,
    tax_division_idx: int,
    amount_idx: int,
    tax_amount_idx: int,
    config: YayoiTaxPostprocessConfig,
) -> TaxPostprocessSideResult:
    tax_division_text = safe_cell_text(tokens, tax_division_idx, csv_obj.encoding)
    amount_text = safe_cell_text(tokens, amount_idx, csv_obj.encoding)
    existing_tax_amount_text = safe_cell_text(tokens, tax_amount_idx, csv_obj.encoding)
    parsed = parse_tax_division(tax_division_text)

    if not config.enabled:
        return _build_side_result(
            row_index_1b=row_index_1b,
            side=side,
            status=STATUS_DISABLED,
            parsed=parsed,
            amount_text=amount_text,
            existing_tax_amount_text=existing_tax_amount_text,
            filled_tax_amount_text="",
        )

    if config.bookkeeping_mode != BOOKKEEPING_MODE_TAX_EXCLUDED:
        return _build_side_result(
            row_index_1b=row_index_1b,
            side=side,
            status=STATUS_BOOKKEEPING_MODE_NOT_SUPPORTED,
            parsed=parsed,
            amount_text=amount_text,
            existing_tax_amount_text=existing_tax_amount_text,
            filled_tax_amount_text="",
        )

    if existing_tax_amount_text.strip():
        return _build_side_result(
            row_index_1b=row_index_1b,
            side=side,
            status=STATUS_TAX_AMOUNT_ALREADY_PRESENT,
            parsed=parsed,
            amount_text=amount_text,
            existing_tax_amount_text=existing_tax_amount_text,
            filled_tax_amount_text="",
        )

    if not parsed.normalized_text:
        return _build_side_result(
            row_index_1b=row_index_1b,
            side=side,
            status=STATUS_NON_TARGET_TAX_DIVISION,
            parsed=parsed,
            amount_text=amount_text,
            existing_tax_amount_text=existing_tax_amount_text,
            filled_tax_amount_text="",
        )

    if parsed.calc_mode in (CALC_MODE_OUTER, CALC_MODE_SEPARATE, CALC_MODE_INCLUSIVE):
        return _build_side_result(
            row_index_1b=row_index_1b,
            side=side,
            status=STATUS_UNSUPPORTED_CALC_MODE,
            parsed=parsed,
            amount_text=amount_text,
            existing_tax_amount_text=existing_tax_amount_text,
            filled_tax_amount_text="",
        )

    if parsed.calc_mode != CALC_MODE_INNER:
        return _build_side_result(
            row_index_1b=row_index_1b,
            side=side,
            status=STATUS_NON_TARGET_TAX_DIVISION,
            parsed=parsed,
            amount_text=amount_text,
            existing_tax_amount_text=existing_tax_amount_text,
            filled_tax_amount_text="",
        )

    if parsed.rate_percent is None:
        return _build_side_result(
            row_index_1b=row_index_1b,
            side=side,
            status=STATUS_TAX_RATE_PARSE_FAILED,
            parsed=parsed,
            amount_text=amount_text,
            existing_tax_amount_text=existing_tax_amount_text,
            filled_tax_amount_text="",
        )

    normalized_amount = unicodedata.normalize("NFKC", amount_text).strip().replace(",", "")
    if not normalized_amount:
        return _build_side_result(
            row_index_1b=row_index_1b,
            side=side,
            status=STATUS_AMOUNT_BLANK,
            parsed=parsed,
            amount_text=amount_text,
            existing_tax_amount_text=existing_tax_amount_text,
            filled_tax_amount_text="",
        )

    try:
        amount_value = int(normalized_amount)
    except ValueError:
        return _build_side_result(
            row_index_1b=row_index_1b,
            side=side,
            status=STATUS_AMOUNT_PARSE_FAILED,
            parsed=parsed,
            amount_text=amount_text,
            existing_tax_amount_text=existing_tax_amount_text,
            filled_tax_amount_text="",
        )

    filled_tax_amount_text = str(compute_inner_tax_floor(amount_value, parsed.rate_percent))
    set_cell_text(tokens, tax_amount_idx, csv_obj.encoding, filled_tax_amount_text)
    return _build_side_result(
        row_index_1b=row_index_1b,
        side=side,
        status=STATUS_APPLIED_INNER_FLOOR,
        parsed=parsed,
        amount_text=amount_text,
        existing_tax_amount_text=existing_tax_amount_text,
        filled_tax_amount_text=filled_tax_amount_text,
    )


def _build_side_result(
    *,
    row_index_1b: int,
    side: str,
    status: str,
    parsed: TaxDivisionParseResult,
    amount_text: str,
    existing_tax_amount_text: str,
    filled_tax_amount_text: str,
) -> TaxPostprocessSideResult:
    return TaxPostprocessSideResult(
        row_index_1b=row_index_1b,
        side=side,
        status=status,
        tax_division_text=parsed.original_text,
        normalized_tax_division_text=parsed.normalized_text,
        stripped_tax_division_text=parsed.stripped_text,
        calc_mode=parsed.calc_mode,
        rate_percent=parsed.rate_percent,
        amount_text=amount_text,
        existing_tax_amount_text=existing_tax_amount_text,
        filled_tax_amount_text=filled_tax_amount_text,
    )


def _classify_calc_mode(normalized_text: str, rate_match: Optional[re.Match[str]]) -> str:
    if not normalized_text:
        return CALC_MODE_OTHER

    inspection_text = normalized_text if rate_match is None else normalized_text[: rate_match.start()]
    if any(marker in normalized_text for marker in ("対象外", "非課税", "不課税", "免税")):
        return CALC_MODE_OTHER
    if any(marker in inspection_text for marker in ("税込", "内税込", "含")) or inspection_text.endswith("込"):
        return CALC_MODE_INCLUSIVE
    if any(marker in inspection_text for marker in ("税別",)) or inspection_text.endswith("別") or "別" in inspection_text:
        return CALC_MODE_SEPARATE
    if any(marker in inspection_text for marker in ("外税",)) or inspection_text.endswith("外") or "外" in inspection_text:
        return CALC_MODE_OUTER
    if any(marker in inspection_text for marker in ("内税",)) or inspection_text.endswith("内") or "内" in inspection_text:
        return CALC_MODE_INNER
    return CALC_MODE_OTHER


def _empty_status_counts() -> Dict[str, int]:
    return {status: 0 for status in STATUS_VOCABULARY}
