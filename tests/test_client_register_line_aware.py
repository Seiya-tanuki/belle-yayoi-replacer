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


def _prepare_receipt_assets(repo_root: Path) -> None:
    _write_json(
        repo_root / "defaults" / "receipt" / "category_defaults.json",
        {
            "schema": "belle.category_defaults.v1",
            "version": "0.1",
            "defaults": {
                "misc": {
                    "debit_account": "雑費",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                }
            },
            "global_fallback": {
                "debit_account": "仮払金",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    _write_json(
        repo_root / "lexicon" / "receipt" / "lexicon.json",
        {
            "schema": "belle.lexicon.v1",
            "version": "0.1",
            "categories": [
                {
                    "id": 1,
                    "key": "misc",
                    "label": "雑費",
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


def _run_register(module, repo_root: Path, *, line_id: str, client_id: str) -> tuple[int, str]:
    fake_script_path = repo_root / ".agents" / "skills" / "client-register" / "register_client.py"
    fake_script_path.parent.mkdir(parents=True, exist_ok=True)
    module.__file__ = str(fake_script_path)

    output_buffer = io.StringIO()
    original_sys_path = list(sys.path)
    try:
        with mock.patch.object(sys, "argv", ["register_client.py", "--line", line_id]):
            with mock.patch("builtins.input", side_effect=[client_id]):
                with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(output_buffer):
                    rc = module.main()
    finally:
        sys.path[:] = original_sys_path
    return rc, output_buffer.getvalue()


class ClientRegisterLineAwareTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def test_bank_statement_register_creates_bank_layout_without_receipt_assumptions(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_bank_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            module = _load_register_module(self.real_repo_root)

            self.assertFalse((repo_root / "lexicon" / "bank_statement" / "lexicon.json").exists())
            self.assertFalse((repo_root / "defaults" / "bank_statement" / "category_defaults.json").exists())
            self.assertFalse(
                (repo_root / "rulesets" / "bank_statement" / "replacer_config_v1_15.json").exists()
            )

            rc, output = _run_register(
                module,
                repo_root,
                line_id="bank_statement",
                client_id="C_BANK_LINE_AWARE",
            )
            self.assertEqual(0, rc, msg=output)

            client_root = repo_root / "clients" / "C_BANK_LINE_AWARE"
            bank_root = client_root / "lines" / "bank_statement"

            self.assertTrue((bank_root / "config" / "bank_line_config.json").exists())
            self.assertTrue((bank_root / "inputs" / "training" / "ocr_kari_shiwake").is_dir())
            self.assertTrue((bank_root / "inputs" / "training" / "reference_yayoi").is_dir())
            self.assertTrue((bank_root / "inputs" / "kari_shiwake").is_dir())
            self.assertTrue((bank_root / "artifacts" / "ingest" / "training_ocr").is_dir())
            self.assertTrue((bank_root / "artifacts" / "ingest" / "training_reference").is_dir())
            self.assertTrue((bank_root / "artifacts" / "ingest" / "kari_shiwake").is_dir())

            self.assertFalse((bank_root / "inputs" / "ledger_ref").exists())
            self.assertFalse((bank_root / "config" / "category_overrides.json").exists())

            self.assertTrue((client_root / "lines" / "receipt" / "inputs" / "kari_shiwake").is_dir())
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_bank_statement_register_generates_default_config_if_template_file_missing(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_bank_fallback_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            (repo_root / "clients" / "TEMPLATE" / "lines" / "bank_statement" / "config" / "bank_line_config.json").unlink()
            module = _load_register_module(self.real_repo_root)

            rc, output = _run_register(
                module,
                repo_root,
                line_id="bank_statement",
                client_id="C_BANK_CFG_FALLBACK",
            )
            self.assertEqual(0, rc, msg=output)

            config_path = (
                repo_root
                / "clients"
                / "C_BANK_CFG_FALLBACK"
                / "lines"
                / "bank_statement"
                / "config"
                / "bank_line_config.json"
            )
            self.assertTrue(config_path.exists())
            config_obj = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual("belle.bank_line_config.v0", config_obj.get("schema"))
            self.assertEqual("0.1", config_obj.get("version"))
            self.assertEqual("仮払金", config_obj.get("placeholder_account_name"))
            self.assertEqual("普通預金", config_obj.get("bank_account_name"))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_receipt_register_still_initializes_category_overrides(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_receipt_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_receipt_assets(repo_root)
            module = _load_register_module(self.real_repo_root)

            rc, output = _run_register(
                module,
                repo_root,
                line_id="receipt",
                client_id="C_RECEIPT_LINE_AWARE",
            )
            self.assertEqual(0, rc, msg=output)

            overrides_path = (
                repo_root
                / "clients"
                / "C_RECEIPT_LINE_AWARE"
                / "lines"
                / "receipt"
                / "config"
                / "category_overrides.json"
            )
            self.assertTrue(overrides_path.exists())
            overrides_obj = json.loads(overrides_path.read_text(encoding="utf-8"))
            self.assertIn("misc", (overrides_obj.get("overrides") or {}))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
