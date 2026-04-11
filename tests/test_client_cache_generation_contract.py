from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4


COL_DATE = 0
COL_DEBIT_ACCOUNT = 4
COL_DEBIT_AMOUNT = 6
COL_CREDIT_ACCOUNT = 10
COL_CREDIT_SUBACCOUNT = 11
COL_SUMMARY = 16


def _copy_path(src_root: Path, dst_root: Path, rel_path: str) -> None:
    src = src_root / rel_path
    dst = dst_root / rel_path
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    if src.is_dir():
        shutil.copytree(src, dst, ignore=ignore)
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_repo_assets(src_root: Path, dst_root: Path) -> None:
    for rel_path in (
        "belle",
        "lexicon",
        "rulesets",
        ".agents/skills/client-cache-builder",
    ):
        _copy_path(src_root, dst_root, rel_path)


def _clear_belle_modules() -> None:
    for name in list(sys.modules):
        if name == "belle" or name.startswith("belle."):
            del sys.modules[name]


@contextlib.contextmanager
def _preserve_interpreter_state():
    original_cwd = Path.cwd()
    original_sys_path = list(sys.path)
    original_environ = os.environ.copy()
    original_belle_modules = {
        name: module
        for name, module in sys.modules.items()
        if name == "belle" or name.startswith("belle.")
    }
    try:
        yield
    finally:
        os.chdir(original_cwd)
        sys.path[:] = original_sys_path
        os.environ.clear()
        os.environ.update(original_environ)
        for name in list(sys.modules):
            if name == "belle" or name.startswith("belle."):
                del sys.modules[name]
        sys.modules.update(original_belle_modules)


def _load_script_module(repo_root: Path, rel_path: str):
    _clear_belle_modules()
    script_path = repo_root / rel_path
    spec = importlib.util.spec_from_file_location(f"phase4_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _run_script_main(module, argv: list[str]) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        with contextlib.redirect_stderr(buf):
            with mock.patch.object(sys, "argv", argv):
                module.main()
    return buf.getvalue()


def _write_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n")
        writer.writerows(rows)


def _receipt_row(*, summary: str, debit_account: str) -> list[str]:
    row = [""] * 25
    row[COL_DEBIT_ACCOUNT] = debit_account
    row[COL_SUMMARY] = summary
    return row


def _credit_card_row(
    *,
    date_text: str,
    summary: str,
    debit_account: str,
    credit_account: str,
    credit_subaccount: str,
) -> list[str]:
    row = [""] * 25
    row[COL_DATE] = date_text
    row[COL_DEBIT_ACCOUNT] = debit_account
    row[COL_DEBIT_AMOUNT] = "1000"
    row[COL_CREDIT_ACCOUNT] = credit_account
    row[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    row[COL_SUMMARY] = summary
    return row


def _pick_current_term(repo_root: Path) -> tuple[str, str]:
    lexicon_obj = json.loads((repo_root / "lexicon" / "lexicon.json").read_text(encoding="utf-8"))
    categories_by_id = {
        int(category["id"]): str(category["key"])
        for category in (lexicon_obj.get("categories") or [])
    }
    for row in lexicon_obj.get("term_rows") or []:
        if not isinstance(row, list) or len(row) < 5:
            continue
        field, needle, category_id, _weight, _kind = row[:5]
        if field != "n0":
            continue
        if not isinstance(needle, str) or not needle.strip():
            continue
        category_key = categories_by_id.get(int(category_id))
        if category_key:
            return needle, category_key
    raise AssertionError("No usable n0 term row found in current lexicon.")


def _current_category_keys(repo_root: Path) -> set[str]:
    lexicon_obj = json.loads((repo_root / "lexicon" / "lexicon.json").read_text(encoding="utf-8"))
    return {
        str(category.get("key") or "")
        for category in (lexicon_obj.get("categories") or [])
        if str(category.get("key") or "")
    }


class ClientCacheGenerationContractTests(unittest.TestCase):
    def test_receipt_client_cache_generation_from_empty_state_uses_current_taxonomy(self) -> None:
        source_repo_root = Path(__file__).resolve().parents[1]
        client_id = "CLEAN_STATE_RECEIPT_CACHE"

        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            _copy_repo_assets(source_repo_root, temp_repo_root)

            with _preserve_interpreter_state():
                term_needle, expected_category_key = _pick_current_term(temp_repo_root)
                current_category_keys = _current_category_keys(temp_repo_root)

                line_root = temp_repo_root / "clients" / client_id / "lines" / "receipt"
                cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
                telemetry_dir = line_root / "artifacts" / "telemetry"
                legacy_cache_path = temp_repo_root / "clients" / client_id / "artifacts" / "cache" / "client_cache.json"

                self.assertFalse(cache_path.exists())
                self.assertFalse(legacy_cache_path.exists())

                _write_rows(
                    line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                    [
                        _receipt_row(
                            summary=f"{term_needle} T1234567890123",
                            debit_account="莠､髫幄ｲｻ",
                        )
                    ],
                )

                module = _load_script_module(
                    temp_repo_root,
                    ".agents/skills/client-cache-builder/scripts/build_client_cache.py",
                )
                output = _run_script_main(
                    module,
                    [
                        "build_client_cache.py",
                        "--client",
                        client_id,
                        "--line",
                        "receipt",
                    ],
                )

                self.assertTrue(cache_path.exists(), msg=output)
                self.assertFalse(legacy_cache_path.exists(), msg=output)

                cache_obj = json.loads(cache_path.read_text(encoding="utf-8"))
                self.assertEqual("belle.client_cache.v2", cache_obj.get("schema"))
                self.assertTrue(cache_obj.get("append_only"))

                applied = cache_obj.get("applied_ledger_ref_sha256") or {}
                self.assertEqual(1, len(applied), msg=output)

                stats = cache_obj.get("stats") or {}
                categories = stats.get("categories") or {}
                self.assertIn(expected_category_key, categories, msg=output)
                self.assertTrue(set(categories).issubset(current_category_keys), msg=output)

                t_numbers_by_category = stats.get("t_numbers_by_category") or {}
                self.assertIn("T1234567890123", t_numbers_by_category, msg=output)
                self.assertIn(expected_category_key, t_numbers_by_category["T1234567890123"], msg=output)
                self.assertTrue(
                    set(t_numbers_by_category["T1234567890123"]).issubset(current_category_keys),
                    msg=output,
                )
                self.assertEqual(
                    {
                        "t_numbers_by_category_and_account": {},
                        "t_numbers_by_account": {},
                        "vendor_keys_by_account": {},
                        "categories_by_account": {},
                        "global_by_account": {},
                    },
                    cache_obj.get("tax_stats") or {},
                )

                telemetry_files = sorted(telemetry_dir.glob("client_cache_update_run_*.json"))
                self.assertEqual(1, len(telemetry_files), msg=output)
                telemetry_obj = json.loads(telemetry_files[0].read_text(encoding="utf-8"))
                self.assertEqual("belle.client_cache_update_run.v1", telemetry_obj.get("schema"))
                self.assertEqual(str(cache_path), ((telemetry_obj.get("paths") or {}).get("client_cache") or ""))

    def test_credit_card_client_cache_generation_from_empty_state_needs_no_prior_artifacts(self) -> None:
        source_repo_root = Path(__file__).resolve().parents[1]
        client_id = "CLEAN_STATE_CC_CACHE"

        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            _copy_repo_assets(source_repo_root, temp_repo_root)

            with _preserve_interpreter_state():
                line_root = temp_repo_root / "clients" / client_id / "lines" / "credit_card_statement"
                cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
                telemetry_dir = line_root / "artifacts" / "telemetry"

                self.assertFalse(cache_path.exists())
                self.assertFalse((line_root / "artifacts").exists())

                config_path = line_root / "config" / "credit_card_line_config.json"
                config_path.parent.mkdir(parents=True, exist_ok=True)
                config_path.write_text(
                    json.dumps(
                        {
                            "schema": "belle.credit_card_line_config.v0",
                            "version": "0.1",
                            "placeholder_account_name": "TEMP_PLACEHOLDER",
                            "payable_account_name": "譛ｪ謇暮≡",
                            "training": {"exclude_counter_accounts": []},
                            "thresholds": {
                                "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
                                "file_level_card_inference": {"min_votes": 1, "min_p_majority": 0.5},
                            },
                            "candidate_extraction": {
                                "min_total_count": 1,
                                "min_unique_merchants": 1,
                                "min_unique_counter_accounts": 1,
                                "manual_allow": [],
                            },
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )

                _write_rows(
                    line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                    [
                        _credit_card_row(
                            date_text="2026/03/01",
                            summary="PHASE4CARDALPHA",
                            debit_account="譌・ｲｻ莠､騾夊ｲｻ",
                            credit_account="譛ｪ謇暮≡",
                            credit_subaccount="CARD_A",
                        ),
                        _credit_card_row(
                            date_text="2026/03/02",
                            summary="PHASE4CARDBETA",
                            debit_account="豸郁怜刀雋ｻ",
                            credit_account="譛ｪ謇暮≡",
                            credit_subaccount="CARD_A",
                        ),
                    ],
                )

                module = _load_script_module(
                    temp_repo_root,
                    ".agents/skills/client-cache-builder/scripts/build_client_cache.py",
                )
                output = _run_script_main(
                    module,
                    [
                        "build_client_cache.py",
                        "--client",
                        client_id,
                        "--line",
                        "credit_card_statement",
                    ],
                )

                self.assertTrue(cache_path.exists(), msg=output)

                cache_obj = json.loads(cache_path.read_text(encoding="utf-8"))
                self.assertEqual("belle.cc_client_cache.v2", cache_obj.get("schema"))
                self.assertEqual("credit_card_statement", cache_obj.get("line_id"))

                applied = cache_obj.get("applied_ledger_ref_sha256") or {}
                self.assertEqual(1, len(applied), msg=output)
                applied_teacher = cache_obj.get("applied_cc_teacher_by_raw_sha256") or {}
                self.assertEqual(1, len(applied_teacher), msg=output)
                self.assertEqual(2, len(cache_obj.get("merchant_key_account_stats") or {}), msg=output)
                self.assertEqual(2, len(cache_obj.get("merchant_key_payable_sub_stats") or {}), msg=output)
                self.assertEqual("REVIEW_REQUIRED", ((cache_obj.get("canonical_payable") or {}).get("status") or ""), msg=output)

                telemetry_files = sorted(telemetry_dir.glob("client_cache_update_run_*.json"))
                self.assertEqual(1, len(telemetry_files), msg=output)
                telemetry_obj = json.loads(telemetry_files[0].read_text(encoding="utf-8"))
                self.assertEqual("belle.cc_client_cache_update_run.v1", telemetry_obj.get("schema"))
                self.assertEqual(str(cache_path), ((telemetry_obj.get("paths") or {}).get("client_cache") or ""))


if __name__ == "__main__":
    unittest.main()
