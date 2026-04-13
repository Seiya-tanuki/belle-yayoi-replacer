from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock
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

    def test_receipt_manifest_drops_repo_config_version(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        module = _load_export_pack_module(real_repo_root)

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            for rel_path, required in module._fixed_paths("receipt"):
                if not required:
                    continue
                abs_path = repo_root / rel_path
                abs_path.parent.mkdir(parents=True, exist_ok=True)
                abs_path.write_text("fixture\n", encoding="utf-8")
            fake_script = repo_root / ".agents" / "skills" / "export-lexicon-review-pack" / "scripts" / "export_pack.py"
            fake_script.parent.mkdir(parents=True, exist_ok=True)
            fake_script.write_text("# fixture\n", encoding="utf-8")
            module.__file__ = str(fake_script)

            output = io.StringIO()
            with mock.patch.object(module, "_read_git_head", return_value="deadbeef"):
                with mock.patch.object(module, "label_queue_lock", side_effect=lambda **kwargs: contextlib.nullcontext()):
                    with mock.patch.object(sys, "argv", ["export_pack.py", "--line", "receipt"]):
                        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                            rc = module.main()

            self.assertEqual(0, rc, msg=output.getvalue())
            latest_name = (repo_root / "exports" / "gpts_lexicon_review" / "LATEST.txt").read_text(
                encoding="utf-8"
            ).strip()
            zip_path = repo_root / "exports" / "gpts_lexicon_review" / latest_name
            self.assertTrue(zip_path.exists(), msg=output.getvalue())

            with zipfile.ZipFile(zip_path) as zf:
                manifest_obj = json.loads(zf.read("MANIFEST.json").decode("utf-8"))

            self.assertEqual("receipt", manifest_obj.get("line_id"))
            self.assertEqual("deadbeef", manifest_obj.get("git_commit"))
            self.assertEqual({"python"}, set((manifest_obj.get("tool_versions") or {}).keys()))
            self.assertNotIn("repo", manifest_obj.get("tool_versions") or {})


if __name__ == "__main__":
    unittest.main()
