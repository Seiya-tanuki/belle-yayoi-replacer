from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from belle.build_client_cache import ensure_client_cache_updated
from belle.defaults import CategoryDefaults, DefaultRule
from belle.lexicon import load_lexicon
from belle.replacer import replace_yayoi_csv
from belle.tax_postprocess import (
    BOOKKEEPING_MODE_TAX_EXCLUDED,
    ROUNDING_MODE_FLOOR,
    YayoiTaxPostprocessConfig,
)
from belle.yayoi_columns import COL_DEBIT_ACCOUNT, COL_DEBIT_TAX_AMOUNT, COL_DEBIT_TAX_DIVISION, COL_SUMMARY
from belle.yayoi_csv import read_yayoi_csv, token_to_text

CATEGORY_KEY = "known_category"
CATEGORY_LABEL = "Known Category"
LEARNED_ACCOUNT_A = "旅費交通費"
LEARNED_ACCOUNT_B = "消耗品費"
DEFAULT_ACCOUNT = "会議費"
GLOBAL_ACCOUNT = "仮払金"
TAX_A = "課対仕入内10%適格"
TAX_B = "課対仕入内8%軽"

NEW_TAX_COLUMNS = [
    "debit_tax_division_before",
    "debit_tax_division_after",
    "debit_tax_division_changed",
    "tax_evidence_type",
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
                        "label": CATEGORY_LABEL,
                        "kind": "expense",
                        "precision_hint": 0.9,
                        "deprecated": False,
                        "negative_terms": {"n0": [], "n1": []},
                    }
                ],
                "term_rows": [["n0", "KNOWNSTORE", 1, 1.0, "S"]],
                "term_buckets_prefix2": {"KN": [0]},
                "learned": {"policy": {"core_weight": 1.0}, "provenance_registry": []},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return lexicon_path


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _receipt_row(
    *,
    summary: str,
    debit_account: str,
    debit_tax_division: str = "",
    debit_amount: str = "605",
    debit_tax_amount: str = "",
) -> list[str]:
    row = [""] * 25
    row[COL_DEBIT_ACCOUNT] = debit_account
    row[COL_DEBIT_TAX_DIVISION] = debit_tax_division
    row[8] = debit_amount
    row[COL_DEBIT_TAX_AMOUNT] = debit_tax_amount
    row[COL_SUMMARY] = summary
    return row


def _load_rows(path: Path) -> list[list[str]]:
    csv_obj = read_yayoi_csv(path)
    return [[token_to_text(token, csv_obj.encoding) for token in row.tokens] for row in csv_obj.rows]


def _load_review(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), list(reader)


def _defaults(*, category_tax: str = "", global_account: str = GLOBAL_ACCOUNT, global_tax: str = "") -> CategoryDefaults:
    return CategoryDefaults(
        schema="belle.category_defaults.v2",
        version="test",
        defaults={
            CATEGORY_KEY: DefaultRule(
                target_account=DEFAULT_ACCOUNT,
                target_tax_division=category_tax,
                confidence=0.55,
                priority="MED",
                reason_code="category_default",
            )
        },
        global_fallback=DefaultRule(
            target_account=global_account,
            target_tax_division=global_tax,
            confidence=0.35,
            priority="HIGH",
            reason_code="global_fallback",
        ),
    )


def _config() -> dict[str, object]:
    return {
        "version": "1.16",
        "csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"},
        "thresholds": {
            "t_number_min_count": 1,
            "t_number_p_majority_min": 0.5,
            "vendor_key_min_count": 1,
            "vendor_key_p_majority_min": 0.5,
            "category_min_count": 1,
            "category_p_majority_min": 0.5,
            "t_number_x_category_min_count": 1,
            "t_number_x_category_p_majority_min": 0.5,
        },
        "tax_division_thresholds": {
            "t_number_x_category_target_account": {"min_count": 1, "min_p_majority": 0.5},
            "t_number_target_account": {"min_count": 1, "min_p_majority": 0.5},
            "vendor_key_target_account": {"min_count": 1, "min_p_majority": 0.5},
            "category_target_account": {"min_count": 1, "min_p_majority": 0.5},
            "global_target_account": {"min_count": 1, "min_p_majority": 0.5},
        },
    }


def _enabled_tax_config() -> YayoiTaxPostprocessConfig:
    return YayoiTaxPostprocessConfig(
        enabled=True,
        bookkeeping_mode=BOOKKEEPING_MODE_TAX_EXCLUDED,
        rounding_mode=ROUNDING_MODE_FLOOR,
    )


class ReceiptTaxDivisionRuntimeTests(unittest.TestCase):
    def test_cache_build_learns_tax_division_conditioned_on_debit_account_and_skips_blank_tax(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_RECEIPT_TAX_CACHE"
            line_root = repo_root / "clients" / client_id / "lines" / "receipt"
            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _receipt_row(
                        summary="KNOWNSTORE / lunch T1234567890123",
                        debit_account=LEARNED_ACCOUNT_A,
                        debit_tax_division=TAX_A,
                    ),
                    _receipt_row(
                        summary="KNOWNSTORE / lunch T1234567890123",
                        debit_account=LEARNED_ACCOUNT_B,
                        debit_tax_division=TAX_B,
                    ),
                    _receipt_row(
                        summary="KNOWNSTORE / lunch T1234567890123",
                        debit_account=LEARNED_ACCOUNT_A,
                        debit_tax_division="",
                    ),
                ],
            )
            lex = load_lexicon(_write_minimal_lexicon(repo_root))

            cache, _summary = ensure_client_cache_updated(
                repo_root=repo_root,
                client_id=client_id,
                lex=lex,
                config=_config(),
                line_id="receipt",
            )

            stats_a = cache.tax_t_numbers_by_category_and_account["T1234567890123"][CATEGORY_KEY][LEARNED_ACCOUNT_A]
            stats_b = cache.tax_t_numbers_by_category_and_account["T1234567890123"][CATEGORY_KEY][LEARNED_ACCOUNT_B]
            self.assertEqual(1, stats_a.sample_total)
            self.assertEqual(TAX_A, stats_a.top_tax_division)
            self.assertEqual({TAX_A: 1}, stats_a.tax_division_counts)
            self.assertEqual(1, stats_b.sample_total)
            self.assertEqual(TAX_B, stats_b.top_tax_division)
            self.assertEqual({TAX_B: 1}, stats_b.tax_division_counts)
            self.assertEqual({}, cache.tax_global_by_account.get("未使用科目", {}))

    def test_live_runtime_learned_tax_route_writes_debit_tax_division(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_RECEIPT_TAX_RUNTIME"
            line_root = repo_root / "clients" / client_id / "lines" / "receipt"
            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _receipt_row(
                        summary="KNOWNSTORE / taxi T1234567890123",
                        debit_account=LEARNED_ACCOUNT_A,
                        debit_tax_division=TAX_A,
                    )
                ],
            )
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            cache, _summary = ensure_client_cache_updated(
                repo_root=repo_root,
                client_id=client_id,
                lex=lex,
                config=_config(),
                line_id="receipt",
            )

            run_dir = repo_root / "run"
            in_path = repo_root / "target.csv"
            out_path = run_dir / "out.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _receipt_row(
                        summary="KNOWNSTORE / taxi T1234567890123",
                        debit_account="BEFORE_ACCOUNT",
                        debit_tax_division="対象外",
                    )
                ],
            )

            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=lex,
                client_cache=cache,
                defaults=_defaults(),
                config=_config(),
                run_dir=run_dir,
                artifact_prefix="receipt_tax_learned",
            )

            rows = _load_rows(out_path)
            self.assertEqual(LEARNED_ACCOUNT_A, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(TAX_A, rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual(
                1,
                int(((manifest.get("tax_division_replacement") or {}).get("route_counts") or {}).get(
                    "t_number_x_category_target_account"
                ) or 0),
            )

    def test_category_default_tax_division_can_supply_receipt_tax_division(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            run_dir = repo_root / "run"
            in_path = repo_root / "input.csv"
            out_path = run_dir / "out.csv"
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            _write_yayoi_rows(
                in_path,
                [_receipt_row(summary="KNOWNSTORE / default", debit_account="BEFORE_ACCOUNT", debit_tax_division="対象外")],
            )

            replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=lex,
                client_cache=None,
                defaults=_defaults(category_tax=TAX_A),
                config=_config(),
                run_dir=run_dir,
                artifact_prefix="receipt_tax_default",
            )

            rows = _load_rows(out_path)
            self.assertEqual(DEFAULT_ACCOUNT, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(TAX_A, rows[0][COL_DEBIT_TAX_DIVISION])

    def test_blank_effective_target_tax_division_does_not_blank_existing_tax_division(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            run_dir = repo_root / "run"
            in_path = repo_root / "input.csv"
            out_path = run_dir / "out.csv"
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            _write_yayoi_rows(
                in_path,
                [_receipt_row(summary="KNOWNSTORE / keep", debit_account="BEFORE_ACCOUNT", debit_tax_division="対象外")],
            )

            replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=lex,
                client_cache=None,
                defaults=_defaults(category_tax=""),
                config=_config(),
                run_dir=run_dir,
                artifact_prefix="receipt_tax_keep_existing",
            )

            rows = _load_rows(out_path)
            self.assertEqual("対象外", rows[0][COL_DEBIT_TAX_DIVISION])

    def test_unresolved_tax_decision_preserves_existing_tax_division(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            run_dir = repo_root / "run"
            in_path = repo_root / "input.csv"
            out_path = run_dir / "out.csv"
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            _write_yayoi_rows(
                in_path,
                [_receipt_row(summary="UNKNOWN SHOP", debit_account="BEFORE_ACCOUNT", debit_tax_division="対象外")],
            )

            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=lex,
                client_cache=None,
                defaults=_defaults(category_tax="", global_tax=""),
                config=_config(),
                run_dir=run_dir,
                artifact_prefix="receipt_tax_unresolved",
            )

            rows = _load_rows(out_path)
            self.assertEqual("対象外", rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual(1, int(((manifest.get("tax_division_replacement") or {}).get("unresolved_count") or 0)))

    def test_non_target_original_tax_division_is_preserved_and_excluded_from_unresolved(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            run_dir = repo_root / "run"
            in_path = repo_root / "input.csv"
            out_path = run_dir / "out.csv"
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            _write_yayoi_rows(
                in_path,
                [
                    _receipt_row(
                        summary="KNOWNSTORE / preserve",
                        debit_account="BEFORE_ACCOUNT",
                        debit_tax_division="課対仕入内10%適格",
                    )
                ],
            )

            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=lex,
                client_cache=None,
                defaults=_defaults(category_tax=TAX_B),
                config=_config(),
                run_dir=run_dir,
                artifact_prefix="receipt_tax_preserve_non_target",
            )

            rows = _load_rows(out_path)
            review_path = Path(str((manifest.get("reports") or {}).get("review_report_csv") or ""))
            fieldnames, review_rows = _load_review(review_path)

            self.assertEqual(1, int(manifest["changed_count"]))
            self.assertEqual("1", review_rows[0]["changed"])
            self.assertEqual(DEFAULT_ACCOUNT, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual("課対仕入内10%適格", rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("課対仕入内10%適格", review_rows[0]["debit_tax_division_after"])
            self.assertEqual("0", review_rows[0]["debit_tax_division_changed"])
            self.assertEqual("none", review_rows[0]["tax_evidence_type"])
            self.assertIn("tax:receipt_original_tax_preserved", review_rows[0]["tax_reasons"])
            self.assertEqual(1, int(((manifest.get("tax_division_replacement") or {}).get("gated_by_original_tax_count") or 0)))
            self.assertEqual(0, int(((manifest.get("tax_division_replacement") or {}).get("unresolved_count") or 0)))
            self.assertEqual({}, (manifest.get("tax_division_replacement") or {}).get("route_counts") or {})
            self.assertEqual(NEW_TAX_COLUMNS, fieldnames[-len(POSTPROCESS_COLUMNS) - len(NEW_TAX_COLUMNS) : -len(POSTPROCESS_COLUMNS)])
            self.assertIn("tax_division_replacement", manifest)

    def test_blank_original_tax_division_is_preserved_and_not_treated_as_replaceable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            run_dir = repo_root / "run"
            in_path = repo_root / "input.csv"
            out_path = run_dir / "out.csv"
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            _write_yayoi_rows(
                in_path,
                [_receipt_row(summary="KNOWNSTORE / blank preserve", debit_account="BEFORE_ACCOUNT", debit_tax_division="")],
            )

            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=lex,
                client_cache=None,
                defaults=_defaults(category_tax=TAX_A),
                config=_config(),
                run_dir=run_dir,
                artifact_prefix="receipt_tax_preserve_blank",
            )

            rows = _load_rows(out_path)
            review_path = Path(str((manifest.get("reports") or {}).get("review_report_csv") or ""))
            _, review_rows = _load_review(review_path)

            self.assertEqual(DEFAULT_ACCOUNT, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual("", rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("", review_rows[0]["debit_tax_division_after"])
            self.assertEqual("0", review_rows[0]["debit_tax_division_changed"])
            self.assertEqual("none", review_rows[0]["tax_evidence_type"])
            self.assertIn("tax:receipt_original_tax_preserved", review_rows[0]["tax_reasons"])
            self.assertEqual(1, int(((manifest.get("tax_division_replacement") or {}).get("gated_by_original_tax_count") or 0)))
            self.assertEqual(0, int(((manifest.get("tax_division_replacement") or {}).get("unresolved_count") or 0)))

    def test_normalized_original_target_tax_division_passes_gate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            run_dir = repo_root / "run"
            in_path = repo_root / "input.csv"
            out_path = run_dir / "out.csv"
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            _write_yayoi_rows(
                in_path,
                [_receipt_row(summary="KNOWNSTORE / normalized", debit_account="BEFORE_ACCOUNT", debit_tax_division="　対象外　")],
            )

            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=lex,
                client_cache=None,
                defaults=_defaults(category_tax=TAX_A),
                config=_config(),
                run_dir=run_dir,
                artifact_prefix="receipt_tax_normalized_gate",
            )

            rows = _load_rows(out_path)
            self.assertEqual(DEFAULT_ACCOUNT, rows[0][COL_DEBIT_ACCOUNT])
            self.assertEqual(TAX_A, rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual(1, int(((manifest.get("tax_division_replacement") or {}).get("category_default_applied_count") or 0)))
            self.assertEqual(0, int(((manifest.get("tax_division_replacement") or {}).get("gated_by_original_tax_count") or 0)))

    def test_tax_only_change_updates_changed_and_observability(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            run_dir = repo_root / "run"
            in_path = repo_root / "input.csv"
            out_path = run_dir / "out.csv"
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            _write_yayoi_rows(
                in_path,
                [_receipt_row(summary="UNKNOWN SHOP", debit_account=GLOBAL_ACCOUNT, debit_tax_division="対象外")],
            )

            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=lex,
                client_cache=None,
                defaults=_defaults(category_tax="", global_account=GLOBAL_ACCOUNT, global_tax=TAX_A),
                config=_config(),
                run_dir=run_dir,
                artifact_prefix="receipt_tax_only",
            )

            review_path = Path(str((manifest.get("reports") or {}).get("review_report_csv") or ""))
            fieldnames, review_rows = _load_review(review_path)

            self.assertEqual(1, int(manifest["changed_count"]))
            self.assertEqual("1", review_rows[0]["changed"])
            self.assertEqual(TAX_A, review_rows[0]["debit_tax_division_after"])
            self.assertEqual("1", review_rows[0]["debit_tax_division_changed"])
            self.assertEqual("global_fallback", review_rows[0]["tax_evidence_type"])
            self.assertEqual(0, int(((manifest.get("tax_division_replacement") or {}).get("gated_by_original_tax_count") or 0)))
            self.assertEqual(NEW_TAX_COLUMNS, fieldnames[-len(POSTPROCESS_COLUMNS) - len(NEW_TAX_COLUMNS) : -len(POSTPROCESS_COLUMNS)])
            self.assertIn("tax_division_replacement", manifest)

    def test_learned_tax_division_can_feed_shared_tax_postprocess(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_RECEIPT_TAX_POSTPROCESS"
            line_root = repo_root / "clients" / client_id / "lines" / "receipt"
            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _receipt_row(
                        summary="KNOWNSTORE / meal T1234567890123",
                        debit_account=LEARNED_ACCOUNT_A,
                        debit_tax_division=TAX_A,
                    )
                ],
            )
            lex = load_lexicon(_write_minimal_lexicon(repo_root))
            cache, _summary = ensure_client_cache_updated(
                repo_root=repo_root,
                client_id=client_id,
                lex=lex,
                config=_config(),
                line_id="receipt",
            )

            run_dir = repo_root / "run"
            in_path = repo_root / "input.csv"
            out_path = run_dir / "out.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _receipt_row(
                        summary="KNOWNSTORE / meal T1234567890123",
                        debit_account="BEFORE_ACCOUNT",
                        debit_tax_division="対象外",
                        debit_amount="605",
                    )
                ],
            )

            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=lex,
                client_cache=cache,
                defaults=_defaults(),
                config=_config(),
                run_dir=run_dir,
                artifact_prefix="receipt_tax_postprocess",
                yayoi_tax_config=_enabled_tax_config(),
            )

            rows = _load_rows(out_path)
            self.assertEqual(TAX_A, rows[0][COL_DEBIT_TAX_DIVISION])
            self.assertEqual("55", rows[0][COL_DEBIT_TAX_AMOUNT])
            self.assertEqual(1, int(manifest["changed_count"]))


if __name__ == "__main__":
    unittest.main()
