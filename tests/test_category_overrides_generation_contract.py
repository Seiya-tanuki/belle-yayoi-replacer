from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4


REPRESENTATIVE_DEBIT_ACCOUNTS = {
    "restaurant_izakaya": "交際費",
    "apps_subscriptions_software": "通信費",
    "utilities": "水道光熱費",
    "banks_credit_unions": "支払手数料",
    "membership_fees": "諸会費",
}


def _load_register_module(real_repo_root: Path):
    script_path = real_repo_root / ".agents" / "skills" / "client-register" / "register_client.py"
    spec = importlib.util.spec_from_file_location(f"register_client_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _prepare_repo_subset(real_repo_root: Path, repo_root: Path) -> None:
    shutil.copytree(real_repo_root / "clients" / "TEMPLATE", repo_root / "clients" / "TEMPLATE")
    shutil.copytree(real_repo_root / "lexicon", repo_root / "lexicon")
    shutil.copytree(real_repo_root / "defaults", repo_root / "defaults")


def _run_register(module, repo_root: Path, *, client_id: str) -> tuple[int, str]:
    fake_script_path = repo_root / ".agents" / "skills" / "client-register" / "register_client.py"
    fake_script_path.parent.mkdir(parents=True, exist_ok=True)
    module.__file__ = str(fake_script_path)

    output_buffer = io.StringIO()
    original_sys_path = list(sys.path)
    try:
        with mock.patch.object(sys, "argv", ["register_client.py"]):
            with mock.patch("builtins.input", side_effect=[client_id]):
                with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(output_buffer):
                    rc = module.main()
    finally:
        sys.path[:] = original_sys_path
    return rc, output_buffer.getvalue()


def _override_paths_in_repo_subset(repo_root: Path) -> list[str]:
    tracked = [
        path.relative_to(repo_root).as_posix()
        for path in repo_root.glob("clients/**/config/category_overrides.json")
        if path.is_file()
    ]
    return sorted(
        path
        for path in tracked
        if path.endswith("/lines/receipt/config/category_overrides.json")
        or path.endswith("/lines/credit_card_statement/config/category_overrides.json")
    )


def _load_lexicon_keys(repo_root: Path) -> list[str]:
    payload = json.loads((repo_root / "lexicon" / "lexicon.json").read_text(encoding="utf-8"))
    return sorted(str(category["key"]) for category in payload.get("categories") or [])


def _load_expected_override_map(repo_root: Path, line_id: str, lexicon_keys: list[str]) -> dict[str, str]:
    payload = json.loads((repo_root / "defaults" / line_id / "category_defaults.json").read_text(encoding="utf-8"))
    defaults = payload.get("defaults") or {}
    fallback = str(((payload.get("global_fallback") or {}).get("debit_account")) or "")
    return {
        key: str(((defaults.get(key) or {}).get("debit_account")) or fallback)
        for key in lexicon_keys
    }


class CategoryOverridesGenerationContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def test_client_register_generates_expected_overrides_from_repo_assets(self) -> None:
        repo_root = Path(
            tempfile.mkdtemp(
                prefix="category_overrides_generation_contract_",
                dir=self.test_tmp_root,
            )
        )
        try:
            _prepare_repo_subset(self.real_repo_root, repo_root)
            self.assertEqual([], _override_paths_in_repo_subset(repo_root))
            module = _load_register_module(self.real_repo_root)

            rc, output = _run_register(module, repo_root, client_id="C_OVERRIDES_CONTRACT")
            self.assertEqual(0, rc, msg=output)

            lexicon_keys = _load_lexicon_keys(repo_root)
            self.assertEqual(69, len(lexicon_keys))
            self.assertEqual(69, len(set(lexicon_keys)))

            bank_overrides_path = (
                repo_root
                / "clients"
                / "C_OVERRIDES_CONTRACT"
                / "lines"
                / "bank_statement"
                / "config"
                / "category_overrides.json"
            )
            self.assertFalse(bank_overrides_path.exists())

            for line_id in ("receipt", "credit_card_statement"):
                overrides_path = (
                    repo_root
                    / "clients"
                    / "C_OVERRIDES_CONTRACT"
                    / "lines"
                    / line_id
                    / "config"
                    / "category_overrides.json"
                )
                self.assertTrue(overrides_path.exists())

                payload = json.loads(overrides_path.read_text(encoding="utf-8"))
                self.assertEqual("belle.category_overrides.v1", payload.get("schema"))

                overrides = payload.get("overrides")
                self.assertIsInstance(overrides, dict)
                self.assertEqual(69, len(overrides))
                self.assertSetEqual(set(lexicon_keys), {str(key) for key in overrides.keys()})

                actual = {
                    str(key): str((row or {}).get("debit_account"))
                    for key, row in overrides.items()
                }
                expected = _load_expected_override_map(repo_root, line_id, lexicon_keys)
                self.assertEqual(expected, actual)

                for key, debit_account in REPRESENTATIVE_DEBIT_ACCOUNTS.items():
                    self.assertEqual(debit_account, actual.get(key), msg=f"line_id={line_id} key={key}")
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
