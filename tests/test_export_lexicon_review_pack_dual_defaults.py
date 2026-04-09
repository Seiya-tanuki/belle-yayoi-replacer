from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from uuid import uuid4


def _load_export_pack_module(real_repo_root: Path):
    script_path = (
        real_repo_root
        / ".agents"
        / "skills"
        / "export-lexicon-review-pack"
        / "scripts"
        / "export_pack.py"
    )
    spec = importlib.util.spec_from_file_location(f"export_pack_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class ExportLexiconReviewPackDualDefaultsTests(unittest.TestCase):
    def test_receipt_pack_fixed_paths_include_dual_defaults_variants(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        module = _load_export_pack_module(real_repo_root)

        fixed_paths = module._fixed_paths("receipt")
        included = {rel_path: required for rel_path, required in fixed_paths}

        self.assertIn("defaults/receipt/category_defaults_tax_excluded.json", included)
        self.assertIn("defaults/receipt/category_defaults_tax_included.json", included)
        self.assertNotIn("defaults/receipt/category_defaults.json", included)
        self.assertTrue(included["defaults/receipt/category_defaults_tax_excluded.json"])
        self.assertTrue(included["defaults/receipt/category_defaults_tax_included.json"])


if __name__ == "__main__":
    unittest.main()
