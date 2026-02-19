from __future__ import annotations

import contextlib
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


def _write_yayoi_row(path: Path, *, summary: str, debit: str = "消耗品費") -> bytes:
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


class KariShiwakeIngestTests(unittest.TestCase):
    def test_no_kari_file_fails_without_run_dir(self) -> None:
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
            with mock.patch.object(sys, "argv", ["run_yayoi_replacer.py", "--client", client_id]):
                with contextlib.redirect_stdout(buf):
                    rc = module.main()

            self.assertEqual(rc, 1)
            self.assertIn(
                f"[ERROR] 置換対象の仮仕訳CSVが見つかりません。clients/{client_id}/inputs/kari_shiwake/ に1ファイル配置してください。",
                buf.getvalue(),
            )

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
            with mock.patch.object(sys, "argv", ["run_yayoi_replacer.py", "--client", client_id]):
                with contextlib.redirect_stdout(buf):
                    rc = module.main()

            self.assertEqual(rc, 1)
            out = buf.getvalue()
            self.assertIn("[ERROR] 置換対象の仮仕訳CSVが複数あります。1ファイルにしてください:", out)
            self.assertIn("  - a.csv", out)
            self.assertIn("  - b.csv", out)

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
            ):
                replaced_inputs.append(in_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(b"")
                return {
                    "changed_ratio": 0.0,
                    "output_file": str(out_path),
                    "analysis": {"rows_with_t_number": 0, "rows_using_t_routes": 0},
                }

            buf = io.StringIO()
            with mock.patch.object(module, "load_lexicon", return_value=lex):
                with mock.patch.object(module, "load_category_defaults", return_value={}):
                    with mock.patch.object(module, "load_category_overrides", return_value={}):
                        with mock.patch.object(module, "merge_effective_defaults", return_value={}):
                            with mock.patch.object(module, "ensure_client_cache_updated", return_value=(tm, tm_summary)):
                                with mock.patch.object(
                                    module,
                                    "ensure_lexicon_candidates_updated_from_ledger_ref",
                                    return_value=autogrow_summary,
                                ):
                                    with mock.patch.object(module, "replace_yayoi_csv", side_effect=_fake_replace_yayoi_csv):
                                        with mock.patch.object(
                                            sys,
                                            "argv",
                                            ["run_yayoi_replacer.py", "--client", client_id],
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
            ):
                replaced_inputs.append(in_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(b"")
                return {
                    "changed_ratio": 0.0,
                    "output_file": str(out_path),
                    "analysis": {"rows_with_t_number": 0, "rows_using_t_routes": 0},
                }

            buf = io.StringIO()
            with mock.patch.object(module, "load_lexicon", return_value=lex):
                with mock.patch.object(module, "load_category_defaults", return_value={}):
                    with mock.patch.object(module, "load_category_overrides", return_value={}):
                        with mock.patch.object(module, "merge_effective_defaults", return_value={}):
                            with mock.patch.object(module, "ensure_client_cache_updated", return_value=(tm, tm_summary)):
                                with mock.patch.object(
                                    module,
                                    "ensure_lexicon_candidates_updated_from_ledger_ref",
                                    return_value=autogrow_summary,
                                ):
                                    with mock.patch.object(module, "replace_yayoi_csv", side_effect=_fake_replace_yayoi_csv):
                                        with mock.patch.object(
                                            sys,
                                            "argv",
                                            ["run_yayoi_replacer.py", "--client", client_id, "--line", "receipt"],
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


if __name__ == "__main__":
    unittest.main()
