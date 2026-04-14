from __future__ import annotations

import csv
import importlib
import json
import shutil
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from uuid import uuid4


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _prepare_template(real_repo_root: Path, repo_root: Path) -> None:
    shutil.copytree(real_repo_root / "clients" / "TEMPLATE", repo_root / "clients" / "TEMPLATE")


def _write_mode_aware_defaults(repo_root: Path, line_id: str, *, excluded: dict, included: dict) -> None:
    base_dir = repo_root / "defaults" / line_id
    _write_json(base_dir / "category_defaults_tax_excluded.json", excluded)
    _write_json(base_dir / "category_defaults_tax_included.json", included)


def _prepare_shared_assets(repo_root: Path) -> None:
    _write_mode_aware_defaults(
        repo_root,
        "receipt",
        excluded={
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
        included={
            "schema": "belle.category_defaults.v2",
            "version": "0.1",
            "defaults": {
                "misc": {
                    "target_account": "租税公課",
                    "target_tax_division": "課対仕入込10%",
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
    _write_mode_aware_defaults(
        repo_root,
        "credit_card_statement",
        excluded={
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
                "target_account": "未払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
        included={
            "schema": "belle.category_defaults.v2",
            "version": "0.1",
            "defaults": {
                "misc": {
                    "target_account": "諸会費",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                }
            },
            "global_fallback": {
                "target_account": "未払金",
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
            "term_rows": [["n0", "DUMMY", 1, 1.0, "S"]],
            "learned": {"policy": {"core_weight": 1.0}},
        },
    )


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _teacher_row(summary: str, debit_account: str) -> list[str]:
    row = [""] * 25
    row[4] = debit_account
    row[16] = summary
    return row


class LocalUiClientServicesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def test_list_client_ids_excludes_template_and_sorts(self) -> None:
        repo_root = self.test_tmp_root / f"local_ui_clients_list_{uuid4().hex}"
        (repo_root / "clients" / "zeta").mkdir(parents=True, exist_ok=False)
        (repo_root / "clients" / "alpha").mkdir(parents=True, exist_ok=False)
        (repo_root / "clients" / "TEMPLATE").mkdir(parents=True, exist_ok=False)
        try:
            from belle.local_ui.services.clients import list_client_ids

            self.assertEqual(["alpha", "zeta"], list_client_ids(repo_root))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_create_client_returns_canonicalized_client_id_and_stdout(self) -> None:
        repo_root = self.test_tmp_root / f"local_ui_clients_create_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            from belle.local_ui.services.clients import create_client

            result = create_client("ＡＢＣ", "tax_excluded", repo_root)
            self.assertTrue(result.ok, msg=result.stdout)
            self.assertEqual("ABC", result.client_id)
            self.assertIn("[OK] Created: clients\\ABC", result.stdout)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_create_client_requires_bookkeeping_mode(self) -> None:
        repo_root = self.test_tmp_root / f"local_ui_clients_requires_mode_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            from belle.local_ui.services.clients import create_client

            result = create_client("ABC", "", repo_root)
            self.assertFalse(result.ok)
            self.assertEqual("", result.client_id)
            self.assertEqual("帳簿方式を選択してください。", result.error_message)
            self.assertIn("bookkeeping_mode is required.", result.stdout)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_create_client_tax_included_passes_selected_mode_to_register(self) -> None:
        repo_root = self.test_tmp_root / f"local_ui_clients_included_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            from belle.local_ui.services.clients import create_client

            result = create_client("ABC_INCLUDED", "tax_included", repo_root)
            self.assertTrue(result.ok, msg=result.stdout)

            config_obj = json.loads(
                (repo_root / "clients" / "ABC_INCLUDED" / "config" / "yayoi_tax_config.json").read_text(
                    encoding="utf-8"
                )
            )
            receipt_overrides = json.loads(
                (
                    repo_root
                    / "clients"
                    / "ABC_INCLUDED"
                    / "lines"
                    / "receipt"
                    / "config"
                    / "category_overrides.json"
                ).read_text(encoding="utf-8")
            )
            cc_overrides = json.loads(
                (
                    repo_root
                    / "clients"
                    / "ABC_INCLUDED"
                    / "lines"
                    / "credit_card_statement"
                    / "config"
                    / "category_overrides.json"
                ).read_text(encoding="utf-8")
            )

            self.assertEqual(False, bool(config_obj.get("enabled")))
            self.assertEqual("tax_included", config_obj.get("bookkeeping_mode"))
            self.assertEqual(
                {"target_account": "租税公課", "target_tax_division": "課対仕入込10%"},
                (receipt_overrides.get("overrides") or {}).get("misc"),
            )
            self.assertEqual(
                {"target_account": "諸会費", "target_tax_division": "課対仕入込10%"},
                (cc_overrides.get("overrides") or {}).get("misc"),
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_create_client_duplicate_returns_failure_result(self) -> None:
        repo_root = self.test_tmp_root / f"local_ui_clients_duplicate_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            existing_root = repo_root / "clients" / "ABC"
            existing_root.mkdir(parents=True, exist_ok=False)
            from belle.local_ui.services.clients import create_client

            result = create_client("ABC", "tax_excluded", repo_root)
            self.assertFalse(result.ok)
            self.assertEqual("", result.client_id)
            self.assertEqual("クライアントを作成できませんでした。入力内容を確認してください。", result.error_message)
            self.assertIn("Already exists", result.stdout)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_create_client_normalizes_system_exit_from_register_module_to_failure_result(self) -> None:
        repo_root = self.test_tmp_root / f"local_ui_clients_system_exit_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            from belle.local_ui.services.clients import create_client

            fake_module = SimpleNamespace(
                validate_and_canonicalize=lambda raw_name: SimpleNamespace(ok=True, canonical="ABC", reason=""),
                main=mock.Mock(side_effect=SystemExit("register fail-closed")),
            )

            with mock.patch("belle.local_ui.services.clients._load_register_module", return_value=fake_module):
                result = create_client("ABC", "tax_excluded", repo_root)

            self.assertFalse(result.ok)
            self.assertEqual("", result.client_id)
            self.assertEqual("クライアントを作成できませんでした。入力内容を確認してください。", result.error_message)
            self.assertIn("register fail-closed", result.stdout)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_create_client_with_teacher_path_bootstraps_override_accounts(self) -> None:
        repo_root = self.test_tmp_root / f"local_ui_clients_teacher_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            teacher_path = repo_root / ".tmp" / "local_ui" / "client_register_bootstrap" / "session_1" / "teacher.csv"
            _write_yayoi_rows(
                teacher_path,
                [
                    _teacher_row("dummy lunch", "交際費"),
                    _teacher_row("dummy taxi", "交際費"),
                ],
            )
            from belle.local_ui.services.clients import create_client

            result = create_client("ABC_TEACHER", "tax_excluded", repo_root, teacher_path=teacher_path)
            self.assertTrue(result.ok, msg=result.stdout)

            receipt_overrides = json.loads(
                (
                    repo_root
                    / "clients"
                    / "ABC_TEACHER"
                    / "lines"
                    / "receipt"
                    / "config"
                    / "category_overrides.json"
                ).read_text(encoding="utf-8")
            )
            cc_overrides = json.loads(
                (
                    repo_root
                    / "clients"
                    / "ABC_TEACHER"
                    / "lines"
                    / "credit_card_statement"
                    / "config"
                    / "category_overrides.json"
                ).read_text(encoding="utf-8")
            )

            self.assertEqual(
                {"target_account": "交際費", "target_tax_division": ""},
                (receipt_overrides.get("overrides") or {}).get("misc"),
            )
            self.assertEqual(
                {"target_account": "交際費", "target_tax_division": ""},
                (cc_overrides.get("overrides") or {}).get("misc"),
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_create_client_with_selected_bootstrap_categories_limits_applied_changes(self) -> None:
        repo_root = self.test_tmp_root / f"local_ui_clients_selected_teacher_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            teacher_path = repo_root / ".tmp" / "local_ui" / "client_register_bootstrap" / "session_1" / "teacher.csv"
            _write_yayoi_rows(
                teacher_path,
                [
                    _teacher_row("dummy lunch", "交際費"),
                    _teacher_row("dummy taxi", "交際費"),
                ],
            )
            from belle.local_ui.services.clients import create_client

            result = create_client(
                "ABC_SELECTED_TEACHER",
                "tax_excluded",
                repo_root,
                teacher_path=teacher_path,
                selected_bootstrap_categories={
                    "receipt": ["misc"],
                    "credit_card_statement": [],
                },
            )
            self.assertTrue(result.ok, msg=result.stdout)

            receipt_overrides = json.loads(
                (
                    repo_root
                    / "clients"
                    / "ABC_SELECTED_TEACHER"
                    / "lines"
                    / "receipt"
                    / "config"
                    / "category_overrides.json"
                ).read_text(encoding="utf-8")
            )
            cc_overrides = json.loads(
                (
                    repo_root
                    / "clients"
                    / "ABC_SELECTED_TEACHER"
                    / "lines"
                    / "credit_card_statement"
                    / "config"
                    / "category_overrides.json"
                ).read_text(encoding="utf-8")
            )

            self.assertEqual(
                {"target_account": "交際費", "target_tax_division": ""},
                (receipt_overrides.get("overrides") or {}).get("misc"),
            )
            self.assertEqual(
                {"target_account": "雑費", "target_tax_division": ""},
                (cc_overrides.get("overrides") or {}).get("misc"),
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_create_app_registers_client_routes(self) -> None:
        import pkgutil

        if not hasattr(pkgutil, "find_loader"):
            import importlib.util

            def _find_loader(name: str):
                spec = importlib.util.find_spec(name)
                return None if spec is None else spec.loader

            pkgutil.find_loader = _find_loader  # type: ignore[attr-defined]

        from nicegui import Client
        import belle.local_ui.pages as pages_module
        from belle.local_ui.app import create_app

        Client.page_routes.clear()
        importlib.reload(pages_module)
        create_app()

        registered_paths = set(Client.page_routes.values())
        self.assertIn("/", registered_paths)
        self.assertIn("/clients/new", registered_paths)
        self.assertIn("/flow/types", registered_paths)


if __name__ == "__main__":
    unittest.main()
