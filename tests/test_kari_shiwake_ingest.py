from __future__ import annotations

import contextlib
import csv
import hashlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from uuid import uuid4

from belle.line_runners import bank_statement as bank_runner
from belle.line_runners import receipt as receipt_runner
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_MEMO,
    COL_SUMMARY,
)


def _write_yayoi_row(path: Path, *, summary: str, debit: str = "譌・ｲｻ莠､騾夊ｲｻ") -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [""] * 25
    cols[4] = debit
    cols[16] = summary
    payload = (",".join(cols) + "\n").encode("cp932")
    path.write_bytes(payload)
    return payload


def _load_replacer_script_module(repo_root: Path):
    script_path = repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
    spec = importlib.util.spec_from_file_location(f"run_yayoi_replacer_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _prepare_temp_repo_structure(repo_root: Path, client_id: str) -> Path:
    client_dir = repo_root / "clients" / client_id
    (client_dir / "config").mkdir(parents=True, exist_ok=True)
    (client_dir / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
    (client_dir / "inputs" / "ledger_ref").mkdir(parents=True, exist_ok=True)
    (repo_root / "rulesets" / "receipt").mkdir(parents=True, exist_ok=True)
    (repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json").write_text(
        json.dumps({"version": "1.15"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (client_dir / "config" / "category_overrides.json").write_text("{}", encoding="utf-8")
    return client_dir


def _prepare_temp_repo_structure_line(repo_root: Path, client_id: str, *, line_id: str = "receipt") -> Path:
    client_line_dir = repo_root / "clients" / client_id / "lines" / line_id
    (client_line_dir / "config").mkdir(parents=True, exist_ok=True)
    (client_line_dir / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
    (client_line_dir / "inputs" / "ledger_ref").mkdir(parents=True, exist_ok=True)
    (repo_root / "rulesets" / "receipt").mkdir(parents=True, exist_ok=True)
    (repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json").write_text(
        json.dumps({"version": "1.15"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (client_line_dir / "config" / "category_overrides.json").write_text("{}", encoding="utf-8")
    return client_line_dir


def _prepare_temp_bank_repo_structure_line(repo_root: Path, client_id: str) -> Path:
    client_line_dir = repo_root / "clients" / client_id / "lines" / "bank_statement"
    (client_line_dir / "config").mkdir(parents=True, exist_ok=True)
    (client_line_dir / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
    (client_line_dir / "inputs" / "training" / "ocr_kari_shiwake").mkdir(parents=True, exist_ok=True)
    (client_line_dir / "inputs" / "training" / "reference_yayoi").mkdir(parents=True, exist_ok=True)
    (client_line_dir / "config" / "bank_line_config.json").write_text(
        json.dumps(
            {
                "schema": "belle.bank_line_config.v0",
                "version": "0.1",
                "placeholder_account_name": "TEMP_PLACEHOLDER",
                "bank_account_name": "BANK_ACCOUNT",
                "bank_account_subaccount": "",
                "thresholds": {
                    "kana_sign_amount": {"min_count": 2, "min_p_majority": 0.85},
                    "kana_sign": {"min_count": 3, "min_p_majority": 0.80},
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return client_line_dir


def _build_bank_row(
    *,
    date_text: str,
    summary: str,
    debit_account: str,
    credit_account: str,
    amount: int,
    memo: str = "",
    credit_subaccount: str = "",
) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = date_text
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_AMOUNT] = str(int(amount))
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_AMOUNT] = str(int(amount))
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_SUMMARY] = summary
    cols[COL_MEMO] = memo
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as f:
        writer = csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)
    return path.read_bytes()


def _snapshot_kari_ingest_state(client_dir: Path) -> dict[str, object]:
    manifest_path = client_dir / "artifacts" / "ingest" / "kari_shiwake_ingested.json"
    store_dir = client_dir / "artifacts" / "ingest" / "kari_shiwake"
    stored_files = {}
    if store_dir.exists():
        stored_files = {
            p.name: p.read_bytes()
            for p in sorted(store_dir.iterdir(), key=lambda path: path.name)
            if p.is_file()
        }
    return {
        "manifest_text": manifest_path.read_text(encoding="utf-8") if manifest_path.exists() else None,
        "stored_files": stored_files,
    }


class KariShiwakeIngestTests(unittest.TestCase):
    def test_no_kari_file_skips_without_run_dir(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C1"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            client_dir = _prepare_temp_repo_structure(temp_repo_root, client_id)
            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            buf = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                ["run_yayoi_replacer.py", "--client", client_id, "--line", "receipt"],
            ):
                with contextlib.redirect_stdout(buf):
                    rc = module.main()

            self.assertEqual(rc, 0)
            out = buf.getvalue()
            self.assertIn("[PLAN] client=C1 line=receipt", out)
            self.assertIn("receipt: SKIP (no target input)", out)
            self.assertIn("[OK] nothing to do", out)

            runs_dir = client_dir / "outputs" / "runs"
            run_dirs = [p for p in runs_dir.iterdir() if p.is_dir()] if runs_dir.exists() else []
            self.assertEqual(run_dirs, [])
            self.assertFalse((client_dir / "outputs" / "LATEST.txt").exists())

    def test_multiple_kari_files_fail_without_run_dir(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C1"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            client_dir = _prepare_temp_repo_structure(temp_repo_root, client_id)
            input_dir = client_dir / "inputs" / "kari_shiwake"
            _write_yayoi_row(input_dir / "a.csv", summary="A")
            _write_yayoi_row(input_dir / "b.csv", summary="B")

            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            buf = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                ["run_yayoi_replacer.py", "--client", client_id, "--line", "receipt"],
            ):
                with contextlib.redirect_stdout(buf):
                    rc = module.main()

            self.assertEqual(rc, 1)
            out = buf.getvalue()
            self.assertIn("[PLAN] client=C1 line=receipt", out)
            self.assertIn("receipt: FAIL (multiple target inputs)", out)
            self.assertIn("target=[a.csv, b.csv]", out)

            runs_dir = client_dir / "outputs" / "runs"
            run_dirs = [p for p in runs_dir.iterdir() if p.is_dir()] if runs_dir.exists() else []
            self.assertEqual(run_dirs, [])
            self.assertFalse((client_dir / "outputs" / "LATEST.txt").exists())

    def test_single_kari_file_is_ingested_and_run_uses_it(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C1"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            client_dir = _prepare_temp_repo_structure(temp_repo_root, client_id)
            input_dir = client_dir / "inputs" / "kari_shiwake"
            payload = _write_yayoi_row(input_dir / "target.csv", summary="TEST SHOP")
            input_sha = hashlib.sha256(payload).hexdigest()

            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            replaced_inputs: list[Path] = []
            tm = SimpleNamespace(t_numbers={})
            tm_summary = SimpleNamespace(applied_new_files=[], rows_used_added=0, warnings=[])
            autogrow_summary = SimpleNamespace(
                processed_files=0,
                processed_rows=0,
                unclassified_rows_seen=0,
                new_keys=0,
                updated_keys=0,
                skipped_by_reason={},
                warnings=[],
            )
            lex = SimpleNamespace(categories_by_key={})

            def _fake_replace_yayoi_csv(
                *,
                in_path: Path,
                out_path: Path,
                lex,
                client_cache,
                defaults,
                config,
                run_dir: Path,
                artifact_prefix: str | None = None,
                yayoi_tax_config=None,
            ):
                del lex
                del client_cache
                del defaults
                del config
                del run_dir
                del artifact_prefix
                del yayoi_tax_config
                replaced_inputs.append(in_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(b"")
                return {
                    "changed_ratio": 0.0,
                    "output_file": str(out_path),
                    "analysis": {"rows_with_t_number": 0, "rows_using_t_routes": 0},
                }

            buf = io.StringIO()
            with mock.patch.object(receipt_runner, "load_lexicon", return_value=lex):
                with mock.patch.object(receipt_runner, "load_category_defaults", return_value={}):
                    with mock.patch.object(
                        receipt_runner,
                        "try_load_category_overrides",
                        return_value=({}, []),
                    ):
                        with mock.patch.object(receipt_runner, "merge_effective_defaults", return_value={}):
                            with mock.patch.object(
                                receipt_runner,
                                "ensure_client_cache_updated",
                                return_value=(tm, tm_summary),
                            ):
                                with mock.patch.object(
                                    receipt_runner,
                                    "ensure_lexicon_candidates_updated_from_ledger_ref",
                                    return_value=autogrow_summary,
                                ):
                                    with mock.patch.object(
                                        receipt_runner,
                                        "replace_yayoi_csv",
                                        side_effect=_fake_replace_yayoi_csv,
                                    ):
                                        with mock.patch.object(
                                            sys,
                                            "argv",
                                            [
                                                "run_yayoi_replacer.py",
                                                "--client",
                                                client_id,
                                                "--line",
                                                "receipt",
                                                "--yes",
                                            ],
                                        ):
                                            with contextlib.redirect_stdout(buf):
                                                rc = module.main()

            self.assertEqual(rc, 0, msg=buf.getvalue())

            remaining = [p for p in input_dir.iterdir() if p.is_file() and p.name != ".gitkeep"]
            self.assertEqual(remaining, [])

            ingest_manifest_path = client_dir / "artifacts" / "ingest" / "kari_shiwake_ingested.json"
            self.assertTrue(ingest_manifest_path.exists())
            ingest_manifest = json.loads(ingest_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(ingest_manifest.get("schema"), "belle.kari_shiwake_ingest.v1")

            ingested = ingest_manifest.get("ingested") or {}
            self.assertEqual(len(ingested), 1)
            self.assertIn(input_sha, ingested)
            entry = ingested[input_sha]
            self.assertEqual(entry.get("original_name"), "target.csv")
            self.assertEqual(entry.get("sha256"), input_sha)
            self.assertTrue(str(entry.get("stored_name") or "").startswith("INGESTED_"))
            self.assertEqual(int(entry.get("rows_observed") or -1), 1)
            self.assertEqual(int(entry.get("byte_size") or -1), len(payload))

            ingested_path = client_dir / "artifacts" / "ingest" / "kari_shiwake" / str(entry["stored_name"])
            self.assertTrue(ingested_path.exists())
            self.assertEqual(replaced_inputs, [ingested_path])

            latest_path = client_dir / "outputs" / "LATEST.txt"
            self.assertTrue(latest_path.exists())
            run_id = latest_path.read_text(encoding="utf-8").strip()
            run_manifest_path = client_dir / "outputs" / "runs" / run_id / "run_manifest.json"
            self.assertTrue(run_manifest_path.exists())
            run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
            kari_meta = ((run_manifest.get("inputs") or {}).get("kari_shiwake") or {})
            self.assertEqual(kari_meta.get("original_name"), "target.csv")
            self.assertEqual(kari_meta.get("stored_name"), entry.get("stored_name"))
            self.assertEqual(kari_meta.get("sha256"), input_sha)

    def test_single_kari_file_line_layout_with_line_receipt(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C1"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            client_line_dir = _prepare_temp_repo_structure_line(temp_repo_root, client_id, line_id="receipt")
            input_dir = client_line_dir / "inputs" / "kari_shiwake"
            payload = _write_yayoi_row(input_dir / "target.csv", summary="TEST SHOP LINE")
            input_sha = hashlib.sha256(payload).hexdigest()

            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            replaced_inputs: list[Path] = []
            tm = SimpleNamespace(t_numbers={})
            tm_summary = SimpleNamespace(applied_new_files=[], rows_used_added=0, warnings=[])
            autogrow_summary = SimpleNamespace(
                processed_files=0,
                processed_rows=0,
                unclassified_rows_seen=0,
                new_keys=0,
                updated_keys=0,
                skipped_by_reason={},
                warnings=[],
            )
            lex = SimpleNamespace(categories_by_key={})

            def _fake_replace_yayoi_csv(
                *,
                in_path: Path,
                out_path: Path,
                lex,
                client_cache,
                defaults,
                config,
                run_dir: Path,
                artifact_prefix: str | None = None,
                yayoi_tax_config=None,
            ):
                del lex
                del client_cache
                del defaults
                del config
                del run_dir
                del artifact_prefix
                del yayoi_tax_config
                replaced_inputs.append(in_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(b"")
                return {
                    "changed_ratio": 0.0,
                    "output_file": str(out_path),
                    "analysis": {"rows_with_t_number": 0, "rows_using_t_routes": 0},
                }

            buf = io.StringIO()
            with mock.patch.object(receipt_runner, "load_lexicon", return_value=lex):
                with mock.patch.object(receipt_runner, "load_category_defaults", return_value={}):
                    with mock.patch.object(
                        receipt_runner,
                        "try_load_category_overrides",
                        return_value=({}, []),
                    ):
                        with mock.patch.object(receipt_runner, "merge_effective_defaults", return_value={}):
                            with mock.patch.object(
                                receipt_runner,
                                "ensure_client_cache_updated",
                                return_value=(tm, tm_summary),
                            ):
                                with mock.patch.object(
                                    receipt_runner,
                                    "ensure_lexicon_candidates_updated_from_ledger_ref",
                                    return_value=autogrow_summary,
                                ):
                                    with mock.patch.object(
                                        receipt_runner,
                                        "replace_yayoi_csv",
                                        side_effect=_fake_replace_yayoi_csv,
                                    ):
                                        with mock.patch.object(
                                            sys,
                                            "argv",
                                            [
                                                "run_yayoi_replacer.py",
                                                "--client",
                                                client_id,
                                                "--line",
                                                "receipt",
                                                "--yes",
                                            ],
                                        ):
                                            with contextlib.redirect_stdout(buf):
                                                rc = module.main()

            self.assertEqual(rc, 0, msg=buf.getvalue())

            remaining = [p for p in input_dir.iterdir() if p.is_file() and p.name != ".gitkeep"]
            self.assertEqual(remaining, [])

            ingest_manifest_path = client_line_dir / "artifacts" / "ingest" / "kari_shiwake_ingested.json"
            self.assertTrue(ingest_manifest_path.exists())
            ingest_manifest = json.loads(ingest_manifest_path.read_text(encoding="utf-8"))
            ingested = ingest_manifest.get("ingested") or {}
            self.assertEqual(len(ingested), 1)
            self.assertIn(input_sha, ingested)
            entry = ingested[input_sha]

            ingested_path = client_line_dir / "artifacts" / "ingest" / "kari_shiwake" / str(entry["stored_name"])
            self.assertTrue(ingested_path.exists())
            self.assertEqual(replaced_inputs, [ingested_path])

            latest_path = client_line_dir / "outputs" / "LATEST.txt"
            self.assertTrue(latest_path.exists())
            run_id = latest_path.read_text(encoding="utf-8").strip()
            run_manifest_path = client_line_dir / "outputs" / "runs" / run_id / "run_manifest.json"
            self.assertTrue(run_manifest_path.exists())
            run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(run_manifest.get("line_id"), "receipt")

    def test_receipt_invalid_runtime_config_keeps_target_in_inbox(self) -> None:
        client_id = "C_RECEIPT_BAD_CONFIG"
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_line_dir = _prepare_temp_repo_structure_line(repo_root, client_id, line_id="receipt")
            target_path = client_line_dir / "inputs" / "kari_shiwake" / "target.csv"
            payload = _write_yayoi_row(target_path, summary="BAD CONFIG TARGET")
            config_path = repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json"
            config_path.write_text("{invalid json", encoding="utf-8")
            before_ingest_state = _snapshot_kari_ingest_state(client_line_dir)
            lex = SimpleNamespace(categories_by_key={})

            with mock.patch.object(receipt_runner, "load_lexicon", return_value=lex):
                with mock.patch.object(receipt_runner, "load_category_defaults", return_value={}):
                    with mock.patch.object(receipt_runner, "try_load_category_overrides", return_value=({}, [])):
                        with mock.patch.object(receipt_runner, "merge_effective_defaults", return_value={}):
                            with mock.patch.object(
                                receipt_runner,
                                "replace_yayoi_csv",
                                side_effect=AssertionError("replace_yayoi_csv must not be called"),
                            ):
                                with self.assertRaises(json.JSONDecodeError):
                                    receipt_runner.run_receipt(
                                        repo_root,
                                        client_id,
                                        client_layout_line_id="receipt",
                                        client_dir=client_line_dir,
                                        config_path=config_path,
                                    )

            self.assertTrue(target_path.exists())
            self.assertEqual(payload, target_path.read_bytes())
            self.assertEqual(before_ingest_state, _snapshot_kari_ingest_state(client_line_dir))

    def test_receipt_client_cache_failure_keeps_target_in_inbox(self) -> None:
        client_id = "C_RECEIPT_CACHE_FAIL"
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_line_dir = _prepare_temp_repo_structure_line(repo_root, client_id, line_id="receipt")
            target_path = client_line_dir / "inputs" / "kari_shiwake" / "target.csv"
            payload = _write_yayoi_row(target_path, summary="CACHE FAIL TARGET")
            config_path = repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json"
            before_ingest_state = _snapshot_kari_ingest_state(client_line_dir)
            lex = SimpleNamespace(categories_by_key={})

            with mock.patch.object(receipt_runner, "load_lexicon", return_value=lex):
                with mock.patch.object(receipt_runner, "load_category_defaults", return_value={}):
                    with mock.patch.object(receipt_runner, "try_load_category_overrides", return_value=({}, [])):
                        with mock.patch.object(receipt_runner, "merge_effective_defaults", return_value={}):
                            with mock.patch.object(
                                receipt_runner,
                                "ensure_client_cache_updated",
                                side_effect=RuntimeError("cache boom"),
                            ):
                                with mock.patch.object(
                                    receipt_runner,
                                    "ensure_lexicon_candidates_updated_from_ledger_ref",
                                    side_effect=AssertionError("autogrow must not run"),
                                ):
                                    with mock.patch.object(
                                        receipt_runner,
                                        "replace_yayoi_csv",
                                        side_effect=AssertionError("replace_yayoi_csv must not be called"),
                                    ):
                                        with self.assertRaisesRegex(RuntimeError, "client_cache 更新に失敗しました"):
                                            receipt_runner.run_receipt(
                                                repo_root,
                                                client_id,
                                                client_layout_line_id="receipt",
                                                client_dir=client_line_dir,
                                                config_path=config_path,
                                            )

            self.assertTrue(target_path.exists())
            self.assertEqual(payload, target_path.read_bytes())
            self.assertEqual(before_ingest_state, _snapshot_kari_ingest_state(client_line_dir))

    def test_receipt_lexicon_autogrow_failure_keeps_target_in_inbox(self) -> None:
        client_id = "C_RECEIPT_AUTOGROW_FAIL"
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_line_dir = _prepare_temp_repo_structure_line(repo_root, client_id, line_id="receipt")
            target_path = client_line_dir / "inputs" / "kari_shiwake" / "target.csv"
            payload = _write_yayoi_row(target_path, summary="AUTOGROW FAIL TARGET")
            config_path = repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json"
            before_ingest_state = _snapshot_kari_ingest_state(client_line_dir)
            lex = SimpleNamespace(categories_by_key={})
            tm = SimpleNamespace(t_numbers={})
            tm_summary = SimpleNamespace(applied_new_files=[], rows_used_added=0, warnings=[])

            with mock.patch.object(receipt_runner, "load_lexicon", return_value=lex):
                with mock.patch.object(receipt_runner, "load_category_defaults", return_value={}):
                    with mock.patch.object(receipt_runner, "try_load_category_overrides", return_value=({}, [])):
                        with mock.patch.object(receipt_runner, "merge_effective_defaults", return_value={}):
                            with mock.patch.object(
                                receipt_runner,
                                "ensure_client_cache_updated",
                                return_value=(tm, tm_summary),
                            ):
                                with mock.patch.object(
                                    receipt_runner,
                                    "ensure_lexicon_candidates_updated_from_ledger_ref",
                                    side_effect=RuntimeError("autogrow boom"),
                                ):
                                    with mock.patch.object(
                                        receipt_runner,
                                        "replace_yayoi_csv",
                                        side_effect=AssertionError("replace_yayoi_csv must not be called"),
                                    ):
                                        with self.assertRaisesRegex(
                                            RuntimeError,
                                            "label_queue 自動更新に失敗しました。出力は作成しません",
                                        ):
                                            receipt_runner.run_receipt(
                                                repo_root,
                                                client_id,
                                                client_layout_line_id="receipt",
                                                client_dir=client_line_dir,
                                                config_path=config_path,
                                            )

            self.assertTrue(target_path.exists())
            self.assertEqual(payload, target_path.read_bytes())
            self.assertEqual(before_ingest_state, _snapshot_kari_ingest_state(client_line_dir))

    def test_bank_client_cache_failure_keeps_target_in_inbox(self) -> None:
        client_id = "C_BANK_CACHE_FAIL"
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_line_dir = _prepare_temp_bank_repo_structure_line(repo_root, client_id)
            target_path = client_line_dir / "inputs" / "kari_shiwake" / "target.csv"
            payload = _write_yayoi_rows(
                target_path,
                [
                    _build_bank_row(
                        date_text="2026/03/01",
                        summary="BANK TARGET CACHE FAIL",
                        debit_account="TEMP_PLACEHOLDER",
                        credit_account="BANK_ACCOUNT",
                        amount=1200,
                        memo="SIGN=debit",
                    )
                ],
            )
            before_ingest_state = _snapshot_kari_ingest_state(client_line_dir)

            with mock.patch.object(
                bank_runner,
                "ensure_bank_client_cache_updated",
                side_effect=RuntimeError("bank cache boom"),
            ):
                with mock.patch.object(
                    bank_runner,
                    "replace_bank_yayoi_csv",
                    side_effect=AssertionError("replace_bank_yayoi_csv must not be called"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "bank client_cache 更新に失敗しました"):
                        bank_runner.run_bank(
                            repo_root,
                            client_id,
                            client_dir=client_line_dir,
                        )

            self.assertTrue(target_path.exists())
            self.assertEqual(payload, target_path.read_bytes())
            self.assertEqual(before_ingest_state, _snapshot_kari_ingest_state(client_line_dir))

    def test_bank_zero_usable_training_pairs_keeps_target_in_inbox(self) -> None:
        client_id = "C_BANK_ZERO_PAIRS"
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_line_dir = _prepare_temp_bank_repo_structure_line(repo_root, client_id)
            target_path = client_line_dir / "inputs" / "kari_shiwake" / "target.csv"
            payload = _write_yayoi_rows(
                target_path,
                [
                    _build_bank_row(
                        date_text="2026/03/05",
                        summary="BANK TARGET ZERO PAIRS",
                        debit_account="TEMP_PLACEHOLDER",
                        credit_account="BANK_ACCOUNT",
                        amount=1200,
                        memo="SIGN=debit",
                    )
                ],
            )
            _write_yayoi_rows(
                client_line_dir / "inputs" / "training" / "ocr_kari_shiwake" / "training_ocr.csv",
                [
                    _build_bank_row(
                        date_text="2026/02/01",
                        summary="OCR DUP",
                        debit_account="TEMP_PLACEHOLDER",
                        credit_account="BANK_ACCOUNT",
                        amount=1200,
                        memo="SIGN=debit",
                    ),
                    _build_bank_row(
                        date_text="2026/02/01",
                        summary="OCR DUP",
                        debit_account="TEMP_PLACEHOLDER",
                        credit_account="BANK_ACCOUNT",
                        amount=1200,
                        memo="SIGN=debit",
                    ),
                ],
            )
            _write_yayoi_rows(
                client_line_dir / "inputs" / "training" / "reference_yayoi" / "teacher.csv",
                [
                    _build_bank_row(
                        date_text="2026/02/01",
                        summary="TEACHER UNIQUE",
                        debit_account="COUNTER_ACCOUNT",
                        credit_account="BANK_ACCOUNT",
                        amount=1200,
                        credit_subaccount="BANK_SUB",
                    )
                ],
            )
            before_ingest_state = _snapshot_kari_ingest_state(client_line_dir)

            with mock.patch.object(
                bank_runner,
                "replace_bank_yayoi_csv",
                side_effect=AssertionError("replace_bank_yayoi_csv must not be called"),
            ):
                with self.assertRaisesRegex(SystemExit, "zero usable pairs"):
                    bank_runner.run_bank(
                        repo_root,
                        client_id,
                        client_dir=client_line_dir,
                    )

            self.assertTrue(target_path.exists())
            self.assertEqual(payload, target_path.read_bytes())
            self.assertEqual(before_ingest_state, _snapshot_kari_ingest_state(client_line_dir))

    def test_bank_invalid_runtime_config_keeps_target_in_inbox(self) -> None:
        client_id = "C_BANK_BAD_CONFIG"
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_line_dir = _prepare_temp_bank_repo_structure_line(repo_root, client_id)
            target_path = client_line_dir / "inputs" / "kari_shiwake" / "target.csv"
            payload = _write_yayoi_rows(
                target_path,
                [
                    _build_bank_row(
                        date_text="2026/03/09",
                        summary="BANK TARGET BAD CONFIG",
                        debit_account="TEMP_PLACEHOLDER",
                        credit_account="BANK_ACCOUNT",
                        amount=2200,
                        memo="SIGN=debit",
                    )
                ],
            )
            (client_line_dir / "config" / "bank_line_config.json").write_text("{invalid json", encoding="utf-8")
            before_ingest_state = _snapshot_kari_ingest_state(client_line_dir)

            with mock.patch.object(
                bank_runner,
                "ensure_bank_client_cache_updated",
                return_value={"cache_path": str(client_line_dir / "artifacts" / "cache" / "client_cache.json")},
            ):
                with mock.patch.object(
                    bank_runner,
                    "replace_bank_yayoi_csv",
                    side_effect=AssertionError("replace_bank_yayoi_csv must not be called"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "bank_line_config 読み込みに失敗しました"):
                        bank_runner.run_bank(
                            repo_root,
                            client_id,
                            client_dir=client_line_dir,
                        )

            self.assertTrue(target_path.exists())
            self.assertEqual(payload, target_path.read_bytes())
            self.assertEqual(before_ingest_state, _snapshot_kari_ingest_state(client_line_dir))


if __name__ == "__main__":
    unittest.main()
