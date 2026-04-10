# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

CANONICAL_LINE_IDS = ["receipt", "bank_statement", "credit_card_statement"]
SUPPORTED_BOOKKEEPING_MODES = ["tax_excluded", "tax_included"]
_BOOKKEEPING_MODE_AWARE_DEFAULT_LINES = {"receipt", "credit_card_statement"}
_TRACKED_DEFAULTS_FILENAMES_BY_MODE = {
    "tax_excluded": "category_defaults_tax_excluded.json",
    "tax_included": "category_defaults_tax_included.json",
}


def validate_line_id(line_id: str) -> str:
    value = str(line_id or "").strip().lower()
    if value in CANONICAL_LINE_IDS:
        return value
    raise ValueError(
        f"invalid line_id: {line_id!r}. allowed={','.join(CANONICAL_LINE_IDS)}"
    )


def is_line_implemented(line_id: str) -> bool:
    return validate_line_id(line_id) in {"receipt", "bank_statement", "credit_card_statement"}


def validate_bookkeeping_mode(bookkeeping_mode: str) -> str:
    value = str(bookkeeping_mode or "").strip()
    if value in SUPPORTED_BOOKKEEPING_MODES:
        return value
    raise ValueError(
        f"invalid bookkeeping_mode: {bookkeeping_mode!r}. allowed={','.join(SUPPORTED_BOOKKEEPING_MODES)}"
    )


def tracked_category_defaults_relpaths(line_id: str) -> list[Path]:
    line = validate_line_id(line_id)
    if line in _BOOKKEEPING_MODE_AWARE_DEFAULT_LINES:
        return [
            Path("defaults") / line / _TRACKED_DEFAULTS_FILENAMES_BY_MODE[mode]
            for mode in SUPPORTED_BOOKKEEPING_MODES
        ]
    return [Path("defaults") / line / "category_defaults.json"]


def tracked_category_defaults_paths(repo_root: Path, line_id: str) -> list[Path]:
    return [repo_root / rel_path for rel_path in tracked_category_defaults_relpaths(line_id)]


def resolve_tracked_category_defaults_path(
    repo_root: Path,
    line_id: str,
    *,
    bookkeeping_mode: str,
) -> Path:
    line = validate_line_id(line_id)
    if line not in _BOOKKEEPING_MODE_AWARE_DEFAULT_LINES:
        raise ValueError(f"bookkeeping-mode-aware defaults are unsupported for line_id={line!r}")
    mode = validate_bookkeeping_mode(bookkeeping_mode)
    return repo_root / "defaults" / line / _TRACKED_DEFAULTS_FILENAMES_BY_MODE[mode]


def line_mode_independent_asset_paths(
    repo_root: Path,
    line_id: str,
) -> dict[str, Path]:
    line = validate_line_id(line_id)
    return {
        "lexicon_path": repo_root / "lexicon" / "lexicon.json",
        "ruleset_default_path": repo_root / "rulesets" / line / "replacer_config_v1_15.json",
        "pending_dir": repo_root / "lexicon" / line / "pending",
    }


def line_asset_paths(
    repo_root: Path,
    line_id: str,
    *,
    bookkeeping_mode: str | None = None,
) -> dict[str, Path]:
    line = validate_line_id(line_id)
    assets = line_mode_independent_asset_paths(repo_root, line)
    if line in _BOOKKEEPING_MODE_AWARE_DEFAULT_LINES:
        if bookkeeping_mode is None:
            raise ValueError(f"bookkeeping_mode is required for line_id={line!r}")
        defaults_path = resolve_tracked_category_defaults_path(
            repo_root,
            line,
            bookkeeping_mode=bookkeeping_mode,
        )
    else:
        defaults_path = repo_root / "defaults" / line / "category_defaults.json"
    assets["defaults_path"] = defaults_path
    return assets
