# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path

from .lexicon import load_lexicon, match_summary
from .yayoi_columns import COL_DEBIT_ACCOUNT, COL_SUMMARY
from .yayoi_csv import read_yayoi_csv, token_to_text

CATEGORY_OVERRIDE_BOOTSTRAP_MATCHED_ROWS_MIN = 2
CATEGORY_OVERRIDE_BOOTSTRAP_MIN_P_MAJORITY = 0.40
CATEGORY_OVERRIDE_BOOTSTRAP_DENYLIST = (
    "現金",
    "普通預金",
    "売掛金",
    "買掛金",
    "未払金",
    "未払費用",
    "預り金",
    "仮払金",
    "仮受金",
    "短期借入金",
    "長期借入金",
    "未払消費税等",
    "前渡金",
    "前払金",
    "前払費用",
    "短期貸付金",
    "長期貸付金",
    "車両運搬具",
)
_SUPPORTED_SUFFIXES = {".csv", ".txt"}


@dataclass(frozen=True)
class CategoryOverrideBootstrapCandidate:
    category_key: str
    category_label: str
    matched_rows: int
    top_account: str
    top_account_count: int
    second_account_count: int
    p_majority: float


@dataclass(frozen=True)
class CategoryOverrideBootstrapAnalysis:
    teacher_source_basename: str
    teacher_source_sha256: str
    row_count: int
    clear_rows: int
    ambiguous_rows: int
    none_rows: int
    candidates_by_category: dict[str, CategoryOverrideBootstrapCandidate]


@dataclass(frozen=True)
class CategoryOverrideBootstrapChange:
    category_key: str
    category_label: str
    from_target_account: str
    to_target_account: str


def category_override_bootstrap_rules_manifest() -> dict[str, object]:
    return {
        "matched_rows_min": CATEGORY_OVERRIDE_BOOTSTRAP_MATCHED_ROWS_MIN,
        "strict_plurality": True,
        "min_p_majority": CATEGORY_OVERRIDE_BOOTSTRAP_MIN_P_MAJORITY,
        "denylist_exact_names": list(CATEGORY_OVERRIDE_BOOTSTRAP_DENYLIST),
    }


def _validate_teacher_path(path: Path) -> None:
    suffix = path.suffix.lower()
    if suffix not in _SUPPORTED_SUFFIXES:
        raise ValueError(
            "category override teacher file must use one of "
            f"{sorted(_SUPPORTED_SUFFIXES)}, got {path.name!r}"
        )
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"category override teacher file not found: {path}")


def analyze_category_override_teacher(
    *,
    teacher_path: Path,
    lexicon_path: Path,
) -> CategoryOverrideBootstrapAnalysis:
    _validate_teacher_path(teacher_path)
    lexicon = load_lexicon(lexicon_path)
    csv_obj = read_yayoi_csv(teacher_path)
    teacher_sha256 = sha256(teacher_path.read_bytes()).hexdigest()

    row_count = 0
    clear_rows = 0
    ambiguous_rows = 0
    none_rows = 0
    category_account_votes: dict[str, Counter[str]] = defaultdict(Counter)
    category_labels: dict[str, str] = {}

    for row in csv_obj.rows:
        row_count += 1
        summary = token_to_text(row.tokens[COL_SUMMARY], csv_obj.encoding)
        debit_account = token_to_text(row.tokens[COL_DEBIT_ACCOUNT], csv_obj.encoding).strip()
        match = match_summary(lexicon, summary)
        if match.quality == "clear":
            clear_rows += 1
        elif match.quality == "ambiguous":
            ambiguous_rows += 1
        else:
            none_rows += 1

        if match.category_key is None or not debit_account:
            continue

        category_account_votes[match.category_key][debit_account] += 1
        category_labels[match.category_key] = str(match.category_label or match.category_key)

    candidates_by_category: dict[str, CategoryOverrideBootstrapCandidate] = {}
    denylist = set(CATEGORY_OVERRIDE_BOOTSTRAP_DENYLIST)
    for category_key, votes in sorted(category_account_votes.items()):
        ranked_accounts = votes.most_common()
        if not ranked_accounts:
            continue

        top_account, top_account_count = ranked_accounts[0]
        second_account_count = ranked_accounts[1][1] if len(ranked_accounts) >= 2 else 0
        matched_rows = sum(votes.values())
        p_majority = float(top_account_count) / float(matched_rows) if matched_rows else 0.0

        if matched_rows < CATEGORY_OVERRIDE_BOOTSTRAP_MATCHED_ROWS_MIN:
            continue
        if top_account_count <= second_account_count:
            continue
        if p_majority < CATEGORY_OVERRIDE_BOOTSTRAP_MIN_P_MAJORITY:
            continue
        if top_account in denylist:
            continue

        candidates_by_category[category_key] = CategoryOverrideBootstrapCandidate(
            category_key=category_key,
            category_label=category_labels.get(category_key, category_key),
            matched_rows=matched_rows,
            top_account=top_account,
            top_account_count=top_account_count,
            second_account_count=second_account_count,
            p_majority=p_majority,
        )

    return CategoryOverrideBootstrapAnalysis(
        teacher_source_basename=teacher_path.name,
        teacher_source_sha256=teacher_sha256,
        row_count=row_count,
        clear_rows=clear_rows,
        ambiguous_rows=ambiguous_rows,
        none_rows=none_rows,
        candidates_by_category=candidates_by_category,
    )


def apply_category_override_bootstrap(
    *,
    overrides_path: Path,
    analysis: CategoryOverrideBootstrapAnalysis,
    selected_category_keys: set[str] | None = None,
) -> list[CategoryOverrideBootstrapChange]:
    payload = json.loads(overrides_path.read_text(encoding="utf-8"))
    changes = apply_category_override_bootstrap_payload(
        payload=payload,
        analysis=analysis,
        payload_label=str(overrides_path),
        selected_category_keys=selected_category_keys,
    )
    if changes:
        overrides_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return changes


def apply_category_override_bootstrap_payload(
    *,
    payload: dict[str, object],
    analysis: CategoryOverrideBootstrapAnalysis,
    payload_label: str = "<payload>",
    selected_category_keys: set[str] | None = None,
) -> list[CategoryOverrideBootstrapChange]:
    if not isinstance(payload, dict):
        raise ValueError(f"category_overrides payload must be an object: {payload_label}")

    overrides = payload.get("overrides")
    if not isinstance(overrides, dict):
        raise ValueError(f"category_overrides.overrides must be an object: {payload_label}")

    changes: list[CategoryOverrideBootstrapChange] = []
    for category_key in sorted(analysis.candidates_by_category.keys()):
        if selected_category_keys is not None and category_key not in selected_category_keys:
            continue
        row = overrides.get(category_key)
        if not isinstance(row, dict):
            raise ValueError(f"category_overrides row is missing or invalid for {category_key!r}: {payload_label}")

        from_target_account = str(row.get("target_account") or "")
        to_target_account = analysis.candidates_by_category[category_key].top_account
        if from_target_account == to_target_account:
            continue

        row["target_account"] = to_target_account
        changes.append(
            CategoryOverrideBootstrapChange(
                category_key=category_key,
                category_label=analysis.candidates_by_category[category_key].category_label,
                from_target_account=from_target_account,
                to_target_account=to_target_account,
            )
        )

    return changes
