# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Iterable

from .category_override_bootstrap import (
    CategoryOverrideBootstrapAnalysis,
    CategoryOverrideBootstrapChange,
    apply_category_override_bootstrap_payload,
)
from .defaults import CATEGORY_OVERRIDES_SCHEMA_V2, load_category_defaults
from .lexicon import load_lexicon
from .lines import line_asset_paths, validate_bookkeeping_mode, validate_line_id

CATEGORY_OVERRIDE_REGISTRATION_LINES = ("receipt", "credit_card_statement")
_EDIT_NOTE_JA = "target_account と target_tax_division の文字列値のみ編集してください。キーや構造は変更しないでください。"


@dataclass(frozen=True)
class PreparedRegistrationCategoryOverrides:
    line_id: str
    payload: dict[str, object]
    changes: tuple[CategoryOverrideBootstrapChange, ...] = ()


def _format_key_summary(keys: Iterable[str], *, limit: int = 20) -> str:
    normalized = sorted({str(k) for k in keys})
    sample = normalized[:limit]
    return f"count={len(normalized)} sample={json.dumps(sample, ensure_ascii=False)}"


def _validate_registration_line_id(line_id: str) -> str:
    value = validate_line_id(line_id)
    if value not in CATEGORY_OVERRIDE_REGISTRATION_LINES:
        raise ValueError(f"registration category overrides are unsupported for line_id={line_id!r}")
    return value


def _utc_now_isoformat() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_registration_category_overrides_payload(
    *,
    repo_root: Path,
    client_id: str,
    line_id: str,
    bookkeeping_mode: str,
) -> dict[str, object]:
    normalized_line_id = _validate_registration_line_id(line_id)
    normalized_mode = validate_bookkeeping_mode(bookkeeping_mode)
    assets = line_asset_paths(repo_root, normalized_line_id, bookkeeping_mode=normalized_mode)
    lex = load_lexicon(assets["lexicon_path"])
    global_defaults = load_category_defaults(assets["defaults_path"])
    keys = sorted({str(key) for key in lex.categories_by_key.keys()})

    missing_defaults = [key for key in keys if key not in global_defaults.defaults]
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

    return {
        "schema": CATEGORY_OVERRIDES_SCHEMA_V2,
        "client_id": str(client_id),
        "generated_at": _utc_now_isoformat(),
        "note_ja": _EDIT_NOTE_JA,
        "overrides": overrides,
    }


def write_registration_category_overrides(
    *,
    path: Path,
    repo_root: Path,
    client_id: str,
    line_id: str,
    bookkeeping_mode: str,
) -> None:
    payload = generate_registration_category_overrides_payload(
        repo_root=repo_root,
        client_id=client_id,
        line_id=line_id,
        bookkeeping_mode=bookkeeping_mode,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def apply_registration_category_override_bootstrap_payload(
    *,
    payload: dict[str, object],
    analysis: CategoryOverrideBootstrapAnalysis,
    line_id: str,
    selected_category_keys: set[str] | None = None,
) -> tuple[CategoryOverrideBootstrapChange, ...]:
    _validate_registration_line_id(line_id)
    changes = apply_category_override_bootstrap_payload(
        payload=payload,
        analysis=analysis,
        payload_label=f"category_overrides[{line_id}]",
        selected_category_keys=selected_category_keys,
    )
    return tuple(changes)


def apply_registration_category_override_bootstrap_file(
    *,
    overrides_path: Path,
    analysis: CategoryOverrideBootstrapAnalysis,
    line_id: str,
    selected_category_keys: set[str] | None = None,
) -> tuple[CategoryOverrideBootstrapChange, ...]:
    _validate_registration_line_id(line_id)
    payload = json.loads(overrides_path.read_text(encoding="utf-8"))
    changes = apply_registration_category_override_bootstrap_payload(
        payload=payload,
        analysis=analysis,
        line_id=line_id,
        selected_category_keys=selected_category_keys,
    )
    if changes:
        overrides_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return changes


def prepare_registration_category_overrides(
    *,
    repo_root: Path,
    client_id: str,
    line_ids: Iterable[str],
    bookkeeping_mode: str,
    teacher_analysis: CategoryOverrideBootstrapAnalysis | None = None,
) -> dict[str, PreparedRegistrationCategoryOverrides]:
    prepared: dict[str, PreparedRegistrationCategoryOverrides] = {}
    for raw_line_id in line_ids:
        line_id = _validate_registration_line_id(raw_line_id)
        payload = generate_registration_category_overrides_payload(
            repo_root=repo_root,
            client_id=client_id,
            line_id=line_id,
            bookkeeping_mode=bookkeeping_mode,
        )
        changes: tuple[CategoryOverrideBootstrapChange, ...] = ()
        if teacher_analysis is not None:
            changes = apply_registration_category_override_bootstrap_payload(
                payload=payload,
                analysis=teacher_analysis,
                line_id=line_id,
            )
        prepared[line_id] = PreparedRegistrationCategoryOverrides(
            line_id=line_id,
            payload=payload,
            changes=changes,
        )
    return prepared
