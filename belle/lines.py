# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

CANONICAL_LINE_IDS = ["receipt", "bank_statement", "credit_card_statement"]


def validate_line_id(line_id: str) -> str:
    value = str(line_id or "").strip().lower()
    if value in CANONICAL_LINE_IDS:
        return value
    raise ValueError(
        f"invalid line_id: {line_id!r}. allowed={','.join(CANONICAL_LINE_IDS)}"
    )


def is_line_implemented(line_id: str) -> bool:
    return validate_line_id(line_id) in {"receipt", "bank_statement", "credit_card_statement"}


def line_asset_paths(repo_root: Path, line_id: str) -> dict[str, Path]:
    line = validate_line_id(line_id)
    return {
        "lexicon_path": repo_root / "lexicon" / line / "lexicon.json",
        "defaults_path": repo_root / "defaults" / line / "category_defaults.json",
        "ruleset_default_path": repo_root / "rulesets" / line / "replacer_config_v1_15.json",
        "pending_dir": repo_root / "lexicon" / line / "pending",
    }
