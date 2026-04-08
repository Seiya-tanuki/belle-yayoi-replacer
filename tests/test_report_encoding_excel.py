from __future__ import annotations

import csv
import shutil
import unittest
from pathlib import Path
from uuid import uuid4

from belle.defaults import CategoryDefaults, DefaultRule
from belle.lexicon import Lexicon
from belle.paths import build_input_artifact_prefix
from belle.replacer import replace_yayoi_csv

UTF8_BOM = b"\xEF\xBB\xBF"


def _write_input_csv(path: Path, *, summary: str, debit: str = "BEFORE_DEBIT") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [""] * 25
    cols[4] = debit
    cols[16] = summary
    with path.open("w", encoding="cp932", newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(cols)


def _build_test_lexicon() -> Lexicon:
    return Lexicon(
        schema="belle.lexicon.v1",
        version="test",
        categories_by_id={},
        categories_by_key={},
        terms_by_field={"n0": [], "n1": []},
    )


def _build_test_defaults() -> CategoryDefaults:
    return CategoryDefaults(
        schema="belle.category_defaults.v2",
        version="test",
        defaults={},
        global_fallback=DefaultRule(
            target_account="AFTER_DEBIT",
            target_tax_division="",
            confidence=0.5,
            priority="HIGH",
            reason_code="global_fallback",
        ),
    )


class ReportEncodingExcelTests(unittest.TestCase):
    def test_review_report_has_utf8_bom_and_replaced_csv_stays_cp932(self) -> None:
        root = Path(__file__).resolve().parents[1] / ".tmp" / f"report_encoding_excel_{uuid4().hex}"
        root.mkdir(parents=True, exist_ok=False)
        try:
            run_dir = root / "run"
            run_dir.mkdir(parents=True, exist_ok=True)
            in_path = root / "input.csv"
            _write_input_csv(in_path, summary="SUMMARY_TEST")

            run_id = "20260218T000000Z_TEST"
            out_path = run_dir / f"{in_path.stem}_replaced_{run_id}.csv"
            manifest = replace_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                lex=_build_test_lexicon(),
                client_cache=None,
                defaults=_build_test_defaults(),
                config={"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}},
                run_dir=run_dir,
                artifact_prefix=build_input_artifact_prefix(in_path=in_path, input_index=1, run_id=run_id),
            )

            review_path = Path(manifest["reports"]["review_report_csv"])
            replaced_path = Path(manifest["output_file"])

            review_bytes = review_path.read_bytes()
            replaced_bytes = replaced_path.read_bytes()

            self.assertTrue(review_bytes.startswith(UTF8_BOM))
            self.assertFalse(replaced_bytes.startswith(UTF8_BOM))

            with replaced_path.open("r", encoding="cp932", newline="") as f:
                row = next(csv.reader(f))
                self.assertEqual(25, len(row))
                self.assertEqual("AFTER_DEBIT", row[4])
                self.assertEqual("SUMMARY_TEST", row[16])
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
