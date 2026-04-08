from __future__ import annotations

import csv
import json
import shutil
import unittest
from pathlib import Path
from uuid import uuid4

from belle.defaults import CategoryDefaults, DefaultRule
from belle.lexicon import Lexicon
from belle.paths import build_input_artifact_prefix
from belle.replacer import replace_yayoi_csv


def _write_input_csv(path: Path, *, summary: str, debit: str = "BEFORE") -> None:
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
            target_account="AFTER",
            target_tax_division="",
            confidence=0.5,
            priority="HIGH",
            reason_code="global_fallback",
        ),
    )


class ReviewReportNamingTests(unittest.TestCase):
    def test_review_report_and_manifest_are_unique_for_same_stem_inputs(self) -> None:
        root = Path(__file__).resolve().parents[1] / ".tmp" / f"review_report_naming_{uuid4().hex}"
        root.mkdir(parents=True, exist_ok=False)
        try:
            run_dir = root / "run"
            run_dir.mkdir(parents=True, exist_ok=True)

            in_paths = [root / "a" / "input.csv", root / "b" / "input.csv"]
            _write_input_csv(in_paths[0], summary="SUMMARY_A")
            _write_input_csv(in_paths[1], summary="SUMMARY_B")

            run_id = "20260214T000000Z_TEST"
            lex = _build_test_lexicon()
            defaults = _build_test_defaults()
            config = {"csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"}}

            manifests = []
            for idx, in_path in enumerate(in_paths, start=1):
                out_path = run_dir / f"{in_path.stem}_replaced_{run_id}.csv"
                if out_path.exists():
                    out_path = run_dir / f"{in_path.stem}_replaced_{run_id}_{idx:02d}.csv"

                artifact_prefix = build_input_artifact_prefix(
                    in_path=in_path,
                    input_index=idx,
                    run_id=run_id,
                )
                manifest = replace_yayoi_csv(
                    in_path=in_path,
                    out_path=out_path,
                    lex=lex,
                    client_cache=None,
                    defaults=defaults,
                    config=config,
                    run_dir=run_dir,
                    artifact_prefix=artifact_prefix,
                )
                manifests.append(manifest)

            review_paths = [Path(m["reports"]["review_report_csv"]) for m in manifests]
            manifest_paths = [Path(m["reports"]["manifest_json"]) for m in manifests]

            self.assertEqual(2, len({p.name for p in review_paths}))
            self.assertEqual(2, len({p.name for p in manifest_paths}))
            self.assertEqual(2, len(list(run_dir.glob("*_review_report.csv"))))
            self.assertEqual(2, len(list(run_dir.glob("*_manifest.json"))))

            self.assertIn("SUMMARY_A", review_paths[0].read_text(encoding="utf-8"))
            self.assertIn("SUMMARY_B", review_paths[1].read_text(encoding="utf-8"))

            for manifest, manifest_path in zip(manifests, manifest_paths):
                stored = json.loads(manifest_path.read_text(encoding="utf-8"))
                self.assertEqual(
                    manifest["reports"]["review_report_csv"],
                    stored["reports"]["review_report_csv"],
                )
                self.assertTrue(Path(stored["reports"]["review_report_csv"]).exists())
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
