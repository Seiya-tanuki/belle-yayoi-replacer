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


def _prepare_shared_assets(repo_root: Path) -> None:
    _write_json(
        repo_root / "defaults" / "receipt" / "category_defaults.json",
        {
            "schema": "belle.category_defaults.v2",
            "version": "0.1",
            "defaults": {
                "misc": {
                    "target_account": "雑費",
                    "target_tax_division": "",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                }
            },
            "global_fallback": {
                "target_account": "仮払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    _write_json(
        repo_root / "defaults" / "credit_card_statement" / "category_defaults.json",
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
    _write_json(
        repo_root / "lexicon" / "lexicon.json",
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


def _run_register(module, repo_root: Path, *, client_id: str, line: str | None = None) -> tuple[int, str]:
    fake_script_path = repo_root / ".agents" / "skills" / "client-register" / "register_client.py"
    fake_script_path.parent.mkdir(parents=True, exist_ok=True)
    module.__file__ = str(fake_script_path)

    output_buffer = io.StringIO()
    original_sys_path = list(sys.path)
    try:
        argv = ["register_client.py"]
        if line is not None:
            argv.extend(["--line", line])
        with mock.patch.object(sys, "argv", argv):
            with mock.patch("builtins.input", side_effect=[client_id]):
                with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(output_buffer):
                    rc = module.main()
    finally:
        sys.path[:] = original_sys_path
    return rc, output_buffer.getvalue()


def _client_dir_names(repo_root: Path) -> list[str]:
    clients_dir = repo_root / "clients"
    if not clients_dir.exists():
        return []
    return sorted(path.name for path in clients_dir.iterdir() if path.is_dir())


class ClientRegisterLineAwareTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def test_register_creates_bank_layout_without_receipt_artifacts_in_bank_line(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_bank_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            module = _load_register_module(self.real_repo_root)

            self.assertFalse((repo_root / "lexicon" / "bank_statement" / "lexicon.json").exists())
            self.assertFalse((repo_root / "defaults" / "bank_statement" / "category_defaults.json").exists())
            self.assertFalse(
                (repo_root / "rulesets" / "bank_statement" / "replacer_config_v1_15.json").exists()
            )

            rc, output = _run_register(
                module,
                repo_root,
                client_id="C_BANK_LINE_AWARE",
            )
            self.assertEqual(0, rc, msg=output)

            client_root = repo_root / "clients" / "C_BANK_LINE_AWARE"
            bank_root = client_root / "lines" / "bank_statement"

            self.assertTrue((client_root / "config" / "yayoi_tax_config.json").exists())
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
            self.assertTrue((client_root / "lines" / "receipt" / "config" / "category_overrides.json").exists())
            self.assertTrue(
                (client_root / "lines" / "credit_card_statement" / "config" / "category_overrides.json").exists()
            )
            self.assertEqual(1, output.count("- shared: clients/<CLIENT_ID>/config/yayoi_tax_config.json"))
            self.assertEqual(["C_BANK_LINE_AWARE", "TEMPLATE"], _client_dir_names(repo_root))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_bank_statement_register_generates_default_config_if_template_file_missing(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_bank_fallback_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            (repo_root / "clients" / "TEMPLATE" / "lines" / "bank_statement" / "config" / "bank_line_config.json").unlink()
            module = _load_register_module(self.real_repo_root)

            rc, output = _run_register(
                module,
                repo_root,
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
            thresholds = config_obj.get("thresholds") if isinstance(config_obj.get("thresholds"), dict) else {}
            file_level = (
                thresholds.get("file_level_bank_sub_inference")
                if isinstance(thresholds.get("file_level_bank_sub_inference"), dict)
                else {}
            )
            self.assertEqual(3, int(file_level.get("min_votes") or 0))
            self.assertEqual(0.9, float(file_level.get("min_p_majority") or 0.0))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_receipt_register_still_initializes_category_overrides(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_receipt_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            module = _load_register_module(self.real_repo_root)

            rc, output = _run_register(
                module,
                repo_root,
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

    def test_credit_card_line_option_creates_only_cc_line_with_overrides(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_cc_only_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            module = _load_register_module(self.real_repo_root)

            rc, output = _run_register(
                module,
                repo_root,
                client_id="C_CC_ONLY",
                line="credit_card_statement",
            )
            self.assertEqual(0, rc, msg=output)

            client_root = repo_root / "clients" / "C_CC_ONLY"
            self.assertFalse((client_root / "lines" / "receipt").exists())
            self.assertFalse((client_root / "lines" / "bank_statement").exists())
            self.assertTrue((client_root / "lines" / "credit_card_statement").is_dir())
            self.assertTrue(
                (client_root / "lines" / "credit_card_statement" / "config" / "category_overrides.json").exists()
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_receipt_registration_rolls_back_when_category_overrides_init_fails(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_receipt_rollback_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            module = _load_register_module(self.real_repo_root)

            with mock.patch.object(
                module,
                "_initialize_category_overrides",
                side_effect=RuntimeError("category init failed"),
            ):
                rc, output = _run_register(
                    module,
                    repo_root,
                    client_id="C_RECEIPT_ROLLBACK",
                    line="receipt",
                )

            self.assertEqual(2, rc, msg=output)
            self.assertIn("Failed to initialize category_overrides.json for line=receipt.", output)
            self.assertFalse((repo_root / "clients" / "C_RECEIPT_ROLLBACK").exists())
            self.assertEqual(["TEMPLATE"], _client_dir_names(repo_root))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_bank_registration_rolls_back_when_bank_config_init_fails(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_bank_rollback_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            module = _load_register_module(self.real_repo_root)

            with mock.patch.object(
                module,
                "_ensure_bank_line_config",
                side_effect=RuntimeError("bank init failed"),
            ):
                rc, output = _run_register(
                    module,
                    repo_root,
                    client_id="C_BANK_ROLLBACK",
                    line="bank_statement",
                )

            self.assertEqual(2, rc, msg=output)
            self.assertIn("Failed to initialize bank_line_config.json.", output)
            self.assertFalse((repo_root / "clients" / "C_BANK_ROLLBACK").exists())
            self.assertEqual(["TEMPLATE"], _client_dir_names(repo_root))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_registration_fails_when_shared_tax_config_is_missing_after_staging(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_missing_shared_tax_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            (repo_root / "clients" / "TEMPLATE" / "config" / "yayoi_tax_config.json").unlink()
            module = _load_register_module(self.real_repo_root)

            rc, output = _run_register(
                module,
                repo_root,
                client_id="C_MISSING_SHARED_TAX",
                line="receipt",
            )

            self.assertEqual(2, rc, msg=output)
            self.assertIn("Shared Yayoi tax config is missing after staging.", output)
            self.assertIn("Expected staged path: clients/C_MISSING_SHARED_TAX/config/yayoi_tax_config.json", output)
            self.assertFalse((repo_root / "clients" / "C_MISSING_SHARED_TAX").exists())
            self.assertEqual(["TEMPLATE"], _client_dir_names(repo_root))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_existing_client_failure_leaves_existing_directory_unchanged(self) -> None:
        repo_root = self.test_tmp_root / f"client_register_existing_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            module = _load_register_module(self.real_repo_root)

            existing_root = repo_root / "clients" / "C_EXISTING"
            sentinel_path = existing_root / "sentinel.txt"
            sentinel_path.parent.mkdir(parents=True, exist_ok=True)
            sentinel_path.write_text("keep-me", encoding="utf-8")

            rc, output = _run_register(
                module,
                repo_root,
                client_id="C_EXISTING",
            )

            self.assertEqual(1, rc, msg=output)
            self.assertIn("Already exists: clients\\C_EXISTING", output)
            self.assertTrue(existing_root.exists())
            self.assertEqual("keep-me", sentinel_path.read_text(encoding="utf-8"))
            self.assertEqual(["C_EXISTING", "TEMPLATE"], _client_dir_names(repo_root))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
