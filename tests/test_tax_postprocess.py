from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from belle.tax_postprocess import (
    BOOKKEEPING_MODE_TAX_EXCLUDED,
    ROUNDING_MODE_FLOOR,
    STATUS_AMOUNT_BLANK,
    STATUS_AMOUNT_PARSE_FAILED,
    STATUS_APPLIED_INNER_FLOOR,
    STATUS_DISABLED,
    STATUS_NON_TARGET_TAX_DIVISION,
    STATUS_TAX_AMOUNT_ALREADY_PRESENT,
    STATUS_UNSUPPORTED_CALC_MODE,
    YayoiTaxPostprocessConfig,
    apply_yayoi_tax_postprocess,
    compute_inner_tax_floor,
    default_yayoi_tax_postprocess_config,
    load_yayoi_tax_postprocess_config,
    parse_tax_division,
)
from belle.yayoi_columns import (
    COL_CREDIT_AMOUNT,
    COL_CREDIT_TAX_AMOUNT,
    COL_CREDIT_TAX_DIVISION,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_TAX_AMOUNT,
    COL_DEBIT_TAX_DIVISION,
)
from belle.yayoi_csv import EXPECTED_COLS, YAYOI_ENCODING, YAYOI_LINE_ENDING, YayoiCSV, YayoiRow, token_to_text


def _qualifying_config(*, enabled: bool = True) -> YayoiTaxPostprocessConfig:
    return YayoiTaxPostprocessConfig(
        enabled=enabled,
        bookkeeping_mode=BOOKKEEPING_MODE_TAX_EXCLUDED,
        rounding_mode=ROUNDING_MODE_FLOOR,
    )


def _blank_row() -> list[str]:
    return [""] * EXPECTED_COLS


def _make_csv(rows: list[list[str]]) -> YayoiCSV:
    csv_rows: list[YayoiRow] = []
    for row in rows:
        csv_rows.append(
            YayoiRow(tokens=[text.encode(YAYOI_ENCODING, errors="strict") for text in row], eol=YAYOI_LINE_ENDING)
        )
    return YayoiCSV(
        path=Path("synthetic.csv"),
        encoding=YAYOI_ENCODING,
        line_ending=YAYOI_LINE_ENDING,
        rows=csv_rows,
    )


def _cell_text(csv_obj: YayoiCSV, row_idx: int, col_idx: int) -> str:
    return token_to_text(csv_obj.rows[row_idx].tokens[col_idx], csv_obj.encoding)


def _find_side_result(summary, *, row_index_1b: int, side: str):
    for result in summary.side_results:
        if result.row_index_1b == row_index_1b and result.side == side:
            return result
    raise AssertionError(f"missing side result: row={row_index_1b} side={side}")


class TaxPostprocessTests(unittest.TestCase):
    def test_missing_config_returns_default_disabled_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            loaded = load_yayoi_tax_postprocess_config(Path(td), "ACME")

        self.assertEqual(default_yayoi_tax_postprocess_config(), loaded)
        self.assertFalse(loaded.enabled)

    def test_invalid_config_value_raises_value_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            cfg_path = repo_root / "clients" / "ACME" / "config" / "yayoi_tax_config.json"
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(
                json.dumps(
                    {
                        "schema": "belle.yayoi_tax_config.v1",
                        "version": "1.0",
                        "enabled": True,
                        "bookkeeping_mode": "broken_mode",
                        "rounding_mode": "floor",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "bookkeeping_mode"):
                load_yayoi_tax_postprocess_config(repo_root, "ACME")

    def test_parse_inner_with_qualified_invoice_suffix(self) -> None:
        parsed = parse_tax_division("課対仕入内10%適格")
        self.assertEqual("inner", parsed.calc_mode)
        self.assertEqual(10, parsed.rate_percent)

    def test_parse_inner_with_control_rate_suffix(self) -> None:
        parsed = parse_tax_division("課対仕入内10%区分80%")
        self.assertEqual("inner", parsed.calc_mode)
        self.assertEqual(10, parsed.rate_percent)

    def test_parse_inner_with_reduced_rate(self) -> None:
        parsed = parse_tax_division("課税売上内軽減8%")
        self.assertEqual("inner", parsed.calc_mode)
        self.assertEqual(8, parsed.rate_percent)

    def test_compute_inner_tax_floor_examples(self) -> None:
        self.assertEqual(55, compute_inner_tax_floor(605, 10))
        self.assertEqual(3628, compute_inner_tax_floor(39916, 10))

    def test_existing_tax_amount_is_preserved(self) -> None:
        row = _blank_row()
        row[COL_DEBIT_TAX_DIVISION] = "課対仕入内10%適格"
        row[COL_DEBIT_AMOUNT] = "605"
        row[COL_DEBIT_TAX_AMOUNT] = "99"
        csv_obj = _make_csv([row])

        summary = apply_yayoi_tax_postprocess(csv_obj, _qualifying_config())

        self.assertEqual("99", _cell_text(csv_obj, 0, COL_DEBIT_TAX_AMOUNT))
        self.assertEqual(0, summary.total_rows_changed)
        self.assertEqual(STATUS_TAX_AMOUNT_ALREADY_PRESENT, _find_side_result(summary, row_index_1b=1, side="debit").status)

    def test_blank_amount_produces_amount_blank(self) -> None:
        row = _blank_row()
        row[COL_DEBIT_TAX_DIVISION] = "課対仕入内10%適格"
        csv_obj = _make_csv([row])

        summary = apply_yayoi_tax_postprocess(csv_obj, _qualifying_config())

        self.assertEqual(STATUS_AMOUNT_BLANK, _find_side_result(summary, row_index_1b=1, side="debit").status)

    def test_unparseable_amount_produces_amount_parse_failed(self) -> None:
        row = _blank_row()
        row[COL_DEBIT_TAX_DIVISION] = "課対仕入内10%適格"
        row[COL_DEBIT_AMOUNT] = "ABC"
        csv_obj = _make_csv([row])

        summary = apply_yayoi_tax_postprocess(csv_obj, _qualifying_config())

        self.assertEqual(STATUS_AMOUNT_PARSE_FAILED, _find_side_result(summary, row_index_1b=1, side="debit").status)

    def test_non_target_or_empty_tax_division_produces_non_target_status(self) -> None:
        row = _blank_row()
        csv_obj = _make_csv([row])

        summary = apply_yayoi_tax_postprocess(csv_obj, _qualifying_config())

        self.assertEqual(STATUS_NON_TARGET_TAX_DIVISION, _find_side_result(summary, row_index_1b=1, side="debit").status)

    def test_outer_or_separate_calc_mode_produces_unsupported_calc_mode(self) -> None:
        outer_row = _blank_row()
        outer_row[COL_DEBIT_TAX_DIVISION] = "課対仕入外10%"
        outer_row[COL_DEBIT_AMOUNT] = "605"
        separate_row = _blank_row()
        separate_row[COL_DEBIT_TAX_DIVISION] = "課対仕入別10%"
        separate_row[COL_DEBIT_AMOUNT] = "605"
        csv_obj = _make_csv([outer_row, separate_row])

        summary = apply_yayoi_tax_postprocess(csv_obj, _qualifying_config())

        self.assertEqual(STATUS_UNSUPPORTED_CALC_MODE, _find_side_result(summary, row_index_1b=1, side="debit").status)
        self.assertEqual(STATUS_UNSUPPORTED_CALC_MODE, _find_side_result(summary, row_index_1b=2, side="debit").status)

    def test_disabled_config_returns_disabled_status_and_no_changes(self) -> None:
        row = _blank_row()
        row[COL_DEBIT_TAX_DIVISION] = "課対仕入内10%適格"
        row[COL_DEBIT_AMOUNT] = "605"
        csv_obj = _make_csv([row])

        summary = apply_yayoi_tax_postprocess(csv_obj, _qualifying_config(enabled=False))

        self.assertEqual("", _cell_text(csv_obj, 0, COL_DEBIT_TAX_AMOUNT))
        self.assertEqual(0, summary.total_rows_changed)
        self.assertEqual(STATUS_DISABLED, _find_side_result(summary, row_index_1b=1, side="debit").status)

    def test_apply_can_fill_debit_and_credit_tax_amount_cells(self) -> None:
        debit_row = _blank_row()
        debit_row[COL_DEBIT_TAX_DIVISION] = "課対仕入内10%適格"
        debit_row[COL_DEBIT_AMOUNT] = "605"
        credit_row = _blank_row()
        credit_row[COL_CREDIT_TAX_DIVISION] = "課税売上内10%"
        credit_row[COL_CREDIT_AMOUNT] = "39916"
        csv_obj = _make_csv([debit_row, credit_row])

        summary = apply_yayoi_tax_postprocess(csv_obj, _qualifying_config())

        self.assertEqual("55", _cell_text(csv_obj, 0, COL_DEBIT_TAX_AMOUNT))
        self.assertEqual("3628", _cell_text(csv_obj, 1, COL_CREDIT_TAX_AMOUNT))
        self.assertEqual(2, summary.total_rows_changed)
        self.assertEqual(1, summary.debit_filled_count)
        self.assertEqual(1, summary.credit_filled_count)
        self.assertEqual(STATUS_APPLIED_INNER_FLOOR, _find_side_result(summary, row_index_1b=1, side="debit").status)
        self.assertEqual(STATUS_APPLIED_INNER_FLOOR, _find_side_result(summary, row_index_1b=2, side="credit").status)


if __name__ == "__main__":
    unittest.main()
