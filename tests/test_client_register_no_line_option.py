from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_register_module(real_repo_root: Path):
    script_path = real_repo_root / ".agents" / "skills" / "client-register" / "register_client.py"
    spec = importlib.util.spec_from_file_location(f"register_client_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _prepare_template(real_repo_root: Path, repo_root: Path) -> None:
    src = real_repo_root / "clients" / "TEMPLATE"
    dst = repo_root / "clients" / "TEMPLATE"
    shutil.copytree(src, dst)


def _write_mode_aware_defaults(repo_root: Path, line_id: str, payload: dict) -> None:
    base_dir = repo_root / "defaults" / line_id
    _write_json(base_dir / "category_defaults_tax_excluded.json", payload)
    _write_json(base_dir / "category_defaults_tax_included.json", payload)


def _prepare_shared_assets(repo_root: Path) -> None:
    _write_mode_aware_defaults(
        repo_root,
        "receipt",
        {
            "schema": "belle.category_defaults.v2",
            "version": "0.1",
            "defaults": {
                "misc": {
                    "target_account": "髮題ｲｻ",
                    "target_tax_division": "",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                }
            },
            "global_fallback": {
                "target_account": "莉ｮ謇暮≡",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    _write_mode_aware_defaults(
        repo_root,
        "credit_card_statement",
        {
            "schema": "belle.category_defaults.v2",
            "version": "0.1",
            "defaults": {
                "misc": {
                    "target_account": "鬮ｮ鬘鯉ｽｲ・ｻ",
                    "target_tax_division": "",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                }
            },
            "global_fallback": {
                "target_account": "闔会ｽｮ隰・坩竕｡",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    _write_json(
        repo_root / "lexicon" / "lexicon.json",
        {
            "schema": "belle.lexicon.v1",
            "version": "0.1",
            "categories": [
                {
                    "id": 1,
                    "key": "misc",
                    "label": "髮題ｲｻ",
                    "kind": "expense",
                    "precision_hint": 0.5,
                    "deprecated": False,
                    "negative_terms": {"n0": [], "n1": []},
                }
            ],
            "term_rows": [["n0", "dummy", 1, 1.0, "S"]],
            "learned": {"policy": {"core_weight": 1.0}},
        },
    )


def _run_register(module, repo_root: Path, *, client_id: str) -> tuple[int, str]:
    fake_script_path = repo_root / ".agents" / "skills" / "client-register" / "register_client.py"
    fake_script_path.parent.mkdir(parents=True, exist_ok=True)
    module.__file__ = str(fake_script_path)

    output_buffer = io.StringIO()
    original_sys_path = list(sys.path)
    try:
        with mock.patch.object(sys, "argv", ["register_client.py"]):
            with mock.patch("builtins.input", side_effect=[client_id, "1"]):
                with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(output_buffer):
                    rc = module.main()
    finally:
        sys.path[:] = original_sys_path
    return rc, output_buffer.getvalue()


class ClientRegisterNoLineOptionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def test_register_always_provisions_all_lines(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_no_line_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            module = _load_register_module(self.real_repo_root)

            rc, output = _run_register(module, repo_root, client_id="C_ALL_LINES")
            self.assertEqual(0, rc, msg=output)

            client_root = repo_root / "clients" / "C_ALL_LINES"
            shared_tax_config_path = client_root / "config" / "yayoi_tax_config.json"
            self.assertTrue(shared_tax_config_path.exists())
            self.assertEqual(True, bool(json.loads(shared_tax_config_path.read_text(encoding="utf-8")).get("enabled")))
            self.assertTrue((client_root / "lines" / "receipt").is_dir())
            self.assertTrue((client_root / "lines" / "bank_statement").is_dir())
            self.assertTrue((client_root / "lines" / "credit_card_statement").is_dir())
            self.assertEqual(1, output.count("- shared: clients/<CLIENT_ID>/config/yayoi_tax_config.json"))

            bank_root = client_root / "lines" / "bank_statement"
            self.assertFalse((bank_root / "inputs" / "ledger_ref").exists())
            self.assertFalse((bank_root / "artifacts" / "ingest" / "ledger_ref").exists())
            self.assertTrue((bank_root / "config" / "bank_line_config.json").exists())
            self.assertTrue((client_root / "lines" / "receipt" / "config" / "category_overrides.json").exists())
            self.assertTrue(
                (client_root / "lines" / "credit_card_statement" / "config" / "category_overrides.json").exists()
            )
            self.assertFalse((client_root / "lines" / "bank_statement" / "config" / "category_overrides.json").exists())
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
