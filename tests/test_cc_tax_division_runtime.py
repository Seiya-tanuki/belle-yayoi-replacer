from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.build_cc_cache import ensure_cc_client_cache_updated, load_credit_card_line_config
from belle.cc_replacer import replace_credit_card_yayoi_csv
from belle.defaults import CategoryDefaults, CategoryOverride, DefaultRule, merge_effective_defaults
from belle.lexicon import load_lexicon
from belle.tax_postprocess import (
    BOOKKEEPING_MODE_TAX_EXCLUDED,
    ROUNDING_MODE_FLOOR,
    YayoiTaxPostprocessConfig,
)
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_CREDIT_TAX_AMOUNT,
    COL_CREDIT_TAX_DIVISION,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_DEBIT_TAX_AMOUNT,
    COL_DEBIT_TAX_DIVISION,
    COL_SUMMARY,
)
from belle.yayoi_csv import read_yayoi_csv, token_to_text

PLACEHOLDER_ACCOUNT = "仮払金"
PAYABLE_ACCOUNT = "未払金"
ACCOUNT_TRAVEL = "旅費交通費"
ACCOUNT_SUPPLIES = "消耗品費"
ACCOUNT_MEETING = "会議費"
CATEGORY_KEY = "shopc_category"
TAX_10 = "課対仕入内10%適格"
TAX_8 = "課対仕入内8%軽"

NEW_TAX_COLUMNS = [
    "target_tax_side",
    "target_tax_division_before",
    "target_tax_division_after",
    "target_tax_division_changed",
    "tax_evidence_type",
    "tax_lookup_key",
    "tax_confidence",
    "tax_sample_total",
    "tax_p_majority",
    "tax_reasons",
]
POSTPROCESS_COLUMNS = [
    "debit_tax_amount_before",
    "debit_tax_amount_after",
    "debit_tax_fill_status",
    "debit_tax_rate",
    "debit_tax_calc_mode",
    "credit_tax_amount_before",
    "credit_tax_amount_after",
    "credit_tax_fill_status",
    "credit_tax_rate",
    "credit_tax_calc_mode",
]


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "credit_card_statement"


def _write_cc_config(
    line_root: Path,
    *,
    account_min_count: int = 1,
    account_min_p_majority: float = 0.5,
    tax_min_count: int = 1,
    tax_min_p_majority: float = 0.5,
    partial_enabled: bool = False,
) -> None:
    cfg = {
        "schema": "belle.credit_card_line_config.v1",
        "version": "0.2",
        "placeholder_account_name": PLACEHOLDER_ACCOUNT,
        "target_payable_placeholder_names": [PAYABLE_ACCOUNT],
        "training": {"exclude_counter_accounts": []},
        "thresholds": {
            "merchant_key_account": {"min_count": int(account_min_count), "min_p_majority": float(account_min_p_majority)},
            "merchant_key_payable_subaccount": {
                "min_count": int(account_min_count),
                "min_p_majority": float(account_min_p_majority),
            },
            "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.5},
        },
        "tax_division_thresholds": {
            "merchant_key_target_account_exact": {
                "min_count": int(tax_min_count),
                "min_p_majority": float(tax_min_p_majority),
            },
            "merchant_key_target_account_partial": {
                "min_count": int(tax_min_count),
                "min_p_majority": float(tax_min_p_majority),
            },
        },
        "teacher_extraction": {
            "canonical_payable_thresholds": {"min_count": 1, "min_p_majority": 0.5}
        },
        "candidate_extraction": {
            "min_total_count": 1,
            "min_unique_merchants": 1,
            "min_unique_counter_accounts": 1,
            "manual_allow": [],
        },
        "partial_match": {
            "enabled": bool(partial_enabled),
            "direction": "cache_key_in_input",
            "require_unique_longest": True,
            "min_match_len": 4,
            "min_stats_sample_total": 10,
            "min_stats_p_majority": 0.95,
        },
    }
    cfg_path = line_root / "config" / "credit_card_line_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    ruleset_path = line_root.parents[3] / "rulesets" / "credit_card_statement" / "teacher_extraction_rules_v1.json"
    ruleset_path.parent.mkdir(parents=True, exist_ok=True)
    ruleset_path.write_text(
        json.dumps(
            {
                "schema": "belle.cc_teacher_extraction_rules.v1",
                "version": "1",
                "teacher_payable_candidate_accounts": [PAYABLE_ACCOUNT, "未払費用"],
                "hard_include_terms": ["CARD", "カード"],
                "soft_include_terms": ["VISA"],
                "exclude_terms": ["デビット", "プリペイド", "ローン"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_minimal_lexicon(repo_root: Path) -> Path:
    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    lexicon_path.parent.mkdir(parents=True, exist_ok=True)
    lexicon_path.write_text(
        json.dumps(
            {
                "schema": "belle.lexicon.v1",
                "version": "test",
                "categories": [
                    {
                        "id": 1,
                        "key": CATEGORY_KEY,
                        "label": "SHOPC",
                        "kind": "merchant",
                        "precision_hint": 0.99,
                        "deprecated": False,
                        "negative_terms": {"n0": [], "n1": []},
                    }
                ],
                "term_rows": [["n0", "SHOPC", 1, 1.0, "S"]],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return lexicon_path


def _defaults(*, category_tax: str = "", global_tax: str = "") -> CategoryDefaults:
    return CategoryDefaults(
        schema="belle.category_defaults.v2",
        version="test",
        defaults={
            CATEGORY_KEY: DefaultRule(
                target_account=ACCOUNT_MEETING,
                target_tax_division=category_tax,
                confidence=0.65,
                priority="MED",
                reason_code="category_default",
            )
        },
        global_fallback=DefaultRule(
            target_account=PLACEHOLDER_ACCOUNT,
            target_tax_division=global_tax,
            confidence=0.35,
            priority="HIGH",
            reason_code="global_fallback",
        ),
    )


def _enabled_tax_config() -> YayoiTaxPostprocessConfig:
    return YayoiTaxPostprocessConfig(
        enabled=True,
        bookkeeping_mode=BOOKKEEPING_MODE_TAX_EXCLUDED,
        rounding_mode=ROUNDING_MODE_FLOOR,
    )


def _build_row(
    *,
    summary: str,
    debit_account: str,
    credit_account: str,
    amount: str = "605",
    debit_subaccount: str = "",
    credit_subaccount: str = "",
    debit_tax_division: str = "",
    credit_tax_division: str = "",
    debit_tax_amount: str = "",
    credit_tax_amount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/04/08"
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    cols[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    cols[COL_DEBIT_AMOUNT] = amount
    cols[COL_DEBIT_TAX_AMOUNT] = debit_tax_amount
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_CREDIT_TAX_DIVISION] = credit_tax_division
    cols[COL_CREDIT_AMOUNT] = amount
    cols[COL_CREDIT_TAX_AMOUNT] = credit_tax_amount
    cols[COL_SUMMARY] = summary
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _read_rows(path: Path) -> list[list[str]]:
    csv_obj = read_yayoi_csv(path)
    return [[token_to_text(tok, csv_obj.encoding) for tok in row.tokens] for row in csv_obj.rows]


def _read_review(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), list(reader)


class CCTaxDivisionRuntimeTests(unittest.TestCase):
    def test_cache_build_learns_tax_by_merchant_and_target_account_and_skips_blank_tax(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_TAX_CACHE"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="SHOPA / one", debit_account=ACCOUNT_TRAVEL, credit_account=PAYABLE_ACCOUNT, credit_subaccount="CARD_A", debit_tax_division=TAX_10),
                    _build_row(summary="SHOPA / two", debit_account=ACCOUNT_SUPPLIES, credit_account=PAYABLE_ACCOUNT, credit_subaccount="CARD_A", debit_tax_division=TAX_8),
                    _build_row(summary="SHOPA / three", debit_account=ACCOUNT_TRAVEL, credit_account=PAYABLE_ACCOUNT, credit_subaccount="CARD_A", debit_tax_division=""),
                ],
            )

            cache, summary = ensure_cc_client_cache_updated(repo_root, client_id)

            self.assertEqual("belle.cc_client_cache.v2", cache.schema)
            self.assertEqual("0.3", cache.version)
            self.assertEqual(2, int(summary.get("tax_rows_learned_added") or 0))
            stats_travel = cache.merchant_key_target_account_tax_stats["SHOPA"][ACCOUNT_TRAVEL]
            stats_supplies = cache.merchant_key_target_account_tax_stats["SHOPA"][ACCOUNT_SUPPLIES]
            self.assertEqual(1, int(stats_travel.sample_total))
            self.assertEqual(TAX_10, stats_travel.top_value)
            self.assertEqual({TAX_10: 1}, stats_travel.value_counts)
            self.assertEqual(1, int(stats_supplies.sample_total))
            self.assertEqual(TAX_8, stats_supplies.top_value)
            self.assertEqual({TAX_8: 1}, stats_supplies.value_counts)

    def test_learned_exact_tax_route_writes_target_tax_and_feeds_shared_tax_postprocess(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_TAX_EXACT"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)
            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="SHOPA / learn", debit_account=ACCOUNT_TRAVEL, credit_account=PAYABLE_ACCOUNT, credit_subaccount="CARD_A", debit_tax_division=TAX_10)
                ],
            )
            ensure_cc_client_cache_updated(repo_root, client_id)

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [_build_row(summary="SHOPA / target", debit_account=PLACEHOLDER_ACCOUNT, credit_account=PAYABLE_ACCOUNT, amount="605")],
            )
            run_dir = line_root / "outputs" / "runs" / "R_CC_TAX_EXACT"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "out.csv"
            config = load_credit_card_line_config(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="cc_tax_exact",
                yayoi_tax_config=_enabled_tax_config(),
            )

            rows = _read_rows(out_path)
            self.assertEqual(ACCOUNT_TRAVEL, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(TAX_10, rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("55", rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(1, int(manifest.get("changed_count") or 0))
            tax_manifest = manifest.get("tax_division_replacement") or {}
            self.assertEqual(1, int(tax_manifest.get("changed_count") or 0))
            self.assertEqual(1, int((tax_manifest.get("route_counts") or {}).get("merchant_key_target_account_exact") or 0))

    def test_partial_tax_route_reuses_account_partial_lookup_key(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_TAX_PARTIAL"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(
                line_root,
                account_min_count=3,
                account_min_p_majority=0.9,
                tax_min_count=3,
                tax_min_p_majority=0.9,
                partial_enabled=True,
            )
            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="カインズ", debit_account=ACCOUNT_SUPPLIES, credit_account=PAYABLE_ACCOUNT, credit_subaccount="CARD_A", debit_tax_division=TAX_10)
                    for _ in range(10)
                ],
            )
            ensure_cc_client_cache_updated(repo_root, client_id)

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [_build_row(summary="カインズホーム", debit_account=PLACEHOLDER_ACCOUNT, credit_account=PAYABLE_ACCOUNT, amount="605")],
            )
            run_dir = line_root / "outputs" / "runs" / "R_CC_TAX_PARTIAL"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "out.csv"
            config = load_credit_card_line_config(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="cc_tax_partial",
            )

            review_path = Path(str((manifest.get("reports") or {}).get("review_report_csv") or ""))
            fieldnames, review_rows = _read_review(review_path)
            rows = _read_rows(out_path)
            self.assertEqual(ACCOUNT_SUPPLIES, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(TAX_10, rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("カインズ", review_rows[0]["lookup_key"])
            self.assertEqual("カインズ", review_rows[0]["tax_lookup_key"])
            self.assertEqual("merchant_key_target_account_partial", review_rows[0]["tax_evidence_type"])
            self.assertEqual(NEW_TAX_COLUMNS, fieldnames[-len(POSTPROCESS_COLUMNS) - len(NEW_TAX_COLUMNS) : -len(POSTPROCESS_COLUMNS)])

    def test_effective_override_tax_division_can_supply_target_tax_division(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_TAX_OVERRIDE"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            global_defaults = _defaults(category_tax="")
            effective_defaults = merge_effective_defaults(
                global_defaults,
                {CATEGORY_KEY: CategoryOverride(target_account=ACCOUNT_MEETING, target_tax_division=TAX_10)},
            )

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [_build_row(summary="SHOPC / default", debit_account=PLACEHOLDER_ACCOUNT, credit_account=PAYABLE_ACCOUNT)],
            )
            run_dir = line_root / "outputs" / "runs" / "R_CC_TAX_OVERRIDE"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "out.csv"

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=line_root / "artifacts" / "cache" / "client_cache.json",
                config=load_credit_card_line_config(repo_root, client_id),
                run_dir=run_dir,
                artifact_prefix="cc_tax_override",
                lex=lex,
                defaults=effective_defaults,
            )

            rows = _read_rows(out_path)
            self.assertEqual(ACCOUNT_MEETING, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(TAX_10, rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual(1, int(((manifest.get("tax_division_replacement") or {}).get("route_counts") or {}).get("category_default") or 0))

    def test_blank_default_tax_division_does_not_blank_existing_target_tax_division(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_TAX_KEEP"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)
            lex = load_lexicon(_write_minimal_lexicon(repo_root))

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(
                        summary="SHOPC / keep",
                        debit_account=PLACEHOLDER_ACCOUNT,
                        credit_account=PAYABLE_ACCOUNT,
                        debit_tax_division="KEEP_ME",
                    )
                ],
            )
            run_dir = line_root / "outputs" / "runs" / "R_CC_TAX_KEEP"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "out.csv"

            replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=line_root / "artifacts" / "cache" / "client_cache.json",
                config=load_credit_card_line_config(repo_root, client_id),
                run_dir=run_dir,
                artifact_prefix="cc_tax_keep",
                lex=lex,
                defaults=_defaults(category_tax=""),
            )

            rows = _read_rows(out_path)
            self.assertEqual("KEEP_ME", rows[0][COL_DEBIT_TAX_DIVISION])

    def test_unresolved_tax_decision_preserves_existing_cell_and_tax_only_change_is_counted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_TAX_GLOBAL"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_row(summary="SHOPC / tax only", debit_account=PLACEHOLDER_ACCOUNT, credit_account=PAYABLE_ACCOUNT),
                    _build_row(summary="UNKNOWN / keep", debit_account=PLACEHOLDER_ACCOUNT, credit_account=PAYABLE_ACCOUNT, debit_tax_division="KEEP_UNKNOWN"),
                ],
            )
            run_dir = line_root / "outputs" / "runs" / "R_CC_TAX_GLOBAL"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "out.csv"
            tax_only_defaults = CategoryDefaults(
                schema="belle.category_defaults.v2",
                version="test",
                defaults={
                    CATEGORY_KEY: DefaultRule(
                        target_account=PLACEHOLDER_ACCOUNT,
                        target_tax_division=TAX_10,
                        confidence=0.65,
                        priority="MED",
                        reason_code="category_default",
                    )
                },
                global_fallback=DefaultRule(
                    target_account=PLACEHOLDER_ACCOUNT,
                    target_tax_division="",
                    confidence=0.35,
                    priority="HIGH",
                    reason_code="global_fallback",
                ),
            )

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=line_root / "artifacts" / "cache" / "client_cache.json",
                config=load_credit_card_line_config(repo_root, client_id),
                run_dir=run_dir,
                artifact_prefix="cc_tax_global",
                lex=lex,
                defaults=tax_only_defaults,
            )

            rows = _read_rows(out_path)
            self.assertEqual(TAX_10, rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("KEEP_UNKNOWN", rows[1][COL_DEBIT_TAX_DIVISION])
            self.assertEqual(1, int(manifest.get("changed_count") or 0))
            self.assertEqual(1, int(((manifest.get("tax_division_replacement") or {}).get("route_counts") or {}).get("category_default") or 0))
            self.assertEqual(1, int(((manifest.get("tax_division_replacement") or {}).get("unresolved_count") or 0)))

    def test_credit_placeholder_side_tax_replacement_writes_credit_tax_division(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_TAX_CREDIT"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)
            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_row(summary="SHOPR / learn", debit_account=PAYABLE_ACCOUNT, credit_account=ACCOUNT_TRAVEL, debit_subaccount="CARD_A", credit_tax_division=TAX_10)
                ],
            )
            ensure_cc_client_cache_updated(repo_root, client_id)

            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [_build_row(summary="SHOPR / target", debit_account=PAYABLE_ACCOUNT, credit_account=PLACEHOLDER_ACCOUNT, amount="605")],
            )
            run_dir = line_root / "outputs" / "runs" / "R_CC_TAX_CREDIT"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "out.csv"
            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=line_root / "artifacts" / "cache" / "client_cache.json",
                config=load_credit_card_line_config(repo_root, client_id),
                run_dir=run_dir,
                artifact_prefix="cc_tax_credit",
                yayoi_tax_config=_enabled_tax_config(),
            )

            rows = _read_rows(out_path)
            self.assertEqual(ACCOUNT_TRAVEL, rows[0][COL_CREDIT_ACCOUNT])
            self.assertEqual(TAX_10, rows[0][COL_CREDIT_TAX_DIVISION])
            self.assertEqual("55", rows[0][COL_CREDIT_TAX_AMOUNT])
            self.assertEqual(
                1,
                int((((manifest.get("tax_division_replacement") or {}).get("target_side_counts") or {}).get("credit")) or 0),
            )


if __name__ == "__main__":
    unittest.main()
