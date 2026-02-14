from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from belle.yayoi_csv import read_yayoi_csv, token_to_text, write_yayoi_csv


def _build_row(*, summary: str, debit: str = "旅費交通費") -> list[str]:
    cols = [""] * 25
    cols[4] = debit
    cols[16] = summary
    return cols


def _write_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _rows_as_text(csv_obj) -> list[list[str]]:
    out: list[list[str]] = []
    for row in csv_obj.rows:
        out.append([token_to_text(tok, csv_obj.encoding) for tok in row.tokens])
    return out


class YayoiCSVParserTests(unittest.TestCase):
    def test_quoted_comma_in_summary_is_single_field(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "quoted_comma.csv"
            _write_rows(src, [_build_row(summary="ABC,DEF")])

            csv_obj = read_yayoi_csv(src)

            self.assertEqual(len(csv_obj.rows), 1)
            self.assertEqual(len(csv_obj.rows[0].tokens), 25)
            self.assertEqual(token_to_text(csv_obj.rows[0].tokens[16], csv_obj.encoding), "ABC,DEF")

    def test_quoted_newline_in_summary_stays_single_record(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "quoted_newline.csv"
            _write_rows(src, [_build_row(summary="LINE1\nLINE2")])

            csv_obj = read_yayoi_csv(src)

            self.assertEqual(len(csv_obj.rows), 1)
            self.assertEqual(len(csv_obj.rows[0].tokens), 25)
            self.assertEqual(token_to_text(csv_obj.rows[0].tokens[16], csv_obj.encoding), "LINE1\nLINE2")

    def test_round_trip_parse_write_parse_preserves_25_columns(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "src.csv"
            dst = Path(td) / "dst.csv"
            original_rows = [
                _build_row(summary="A,B"),
                _build_row(summary='LINE1\nLINE2 "Q"'),
            ]
            _write_rows(src, original_rows)

            first = read_yayoi_csv(src)
            write_yayoi_csv(first, dst)
            second = read_yayoi_csv(dst)

            self.assertEqual(_rows_as_text(first), _rows_as_text(second))
            self.assertTrue(all(len(r.tokens) == 25 for r in second.rows))

    def test_fail_closed_when_column_count_is_not_25(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "bad_cols.csv"
            with src.open("w", encoding="cp932", newline="") as f:
                writer = csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
                writer.writerow([""] * 24)

            with self.assertRaisesRegex(ValueError, "expected 25, got 24"):
                read_yayoi_csv(src)


if __name__ == "__main__":
    unittest.main()
