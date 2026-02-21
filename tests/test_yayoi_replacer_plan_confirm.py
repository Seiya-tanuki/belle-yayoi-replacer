from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4


def _load_replacer_script_module(repo_root: Path):
    script_path = repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
    spec = importlib.util.spec_from_file_location(f"run_yayoi_replacer_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _write_yayoi_row(path: Path, *, summary: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [""] * 25
    cols[4] = "仮払金"
    cols[16] = summary
    path.write_bytes((",".join(cols) + "\n").encode("cp932"))


def _prepare_line_dirs(repo_root: Path, client_id: str) -> tuple[Path, Path]:
    receipt_root = repo_root / "clients" / client_id / "lines" / "receipt"
    bank_root = repo_root / "clients" / client_id / "lines" / "bank_statement"
    (receipt_root / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
    (bank_root / "inputs" / "kari_shiwake").mkdir(parents=True, exist_ok=True)
    return receipt_root, bank_root


def _prepare_receipt_config(repo_root: Path) -> None:
    ruleset_dir = repo_root / "rulesets" / "receipt"
    ruleset_dir.mkdir(parents=True, exist_ok=True)
    (ruleset_dir / "replacer_config_v1_15.json").write_text("{\"version\":\"1.15\"}\n", encoding="utf-8")


class YayoiReplacerPlanConfirmTests(unittest.TestCase):
    def test_yayoi_replacer_dry_run_plan_all_ok(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C1"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            _prepare_line_dirs(temp_repo_root, client_id)
            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            buf = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                ["run_yayoi_replacer.py", "--client", client_id, "--line", "all", "--dry-run"],
            ):
                with contextlib.redirect_stdout(buf):
                    rc = module.main()

            out = buf.getvalue()
            self.assertEqual(0, rc, msg=out)
            self.assertIn("[PLAN] client=C1 line=all", out)
            self.assertIn("receipt: SKIP (no target input)", out)
            self.assertIn("bank_statement: SKIP (no target input)", out)
            self.assertIn("credit_card_statement: SKIP (unimplemented)", out)

    def test_yayoi_replacer_non_interactive_requires_yes(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C1"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            receipt_root, _ = _prepare_line_dirs(temp_repo_root, client_id)
            _prepare_receipt_config(temp_repo_root)
            _write_yayoi_row(receipt_root / "inputs" / "kari_shiwake" / "target.csv", summary="NON TTY TEST")

            script_src = real_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            script_dst = (
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )
            script_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(script_src, script_dst)

            env = os.environ.copy()
            py_path = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = str(real_repo_root) if not py_path else f"{real_repo_root}{os.pathsep}{py_path}"

            proc = subprocess.run(
                [
                    sys.executable,
                    str(script_dst),
                    "--client",
                    client_id,
                    "--line",
                    "receipt",
                ],
                cwd=temp_repo_root,
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=60,
            )

            out = (proc.stdout or "") + "\n" + (proc.stderr or "")
            self.assertEqual(2, proc.returncode, msg=out)
            self.assertIn("--yes", out)
            self.assertIn("non-interactive", out)

    def test_yayoi_replacer_all_skips_when_no_targets(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C1"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            _prepare_line_dirs(temp_repo_root, client_id)
            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            buf = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                ["run_yayoi_replacer.py", "--client", client_id, "--line", "all"],
            ):
                with contextlib.redirect_stdout(buf):
                    rc = module.main()

            out = buf.getvalue()
            self.assertEqual(0, rc, msg=out)
            self.assertIn("[OK] nothing to do", out)

    def test_yayoi_replacer_all_fails_on_multiple_targets(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C1"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            receipt_root, _ = _prepare_line_dirs(temp_repo_root, client_id)
            _write_yayoi_row(receipt_root / "inputs" / "kari_shiwake" / "a.csv", summary="A")
            _write_yayoi_row(receipt_root / "inputs" / "kari_shiwake" / "b.csv", summary="B")
            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            buf = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                ["run_yayoi_replacer.py", "--client", client_id, "--line", "all"],
            ):
                with contextlib.redirect_stdout(buf):
                    rc = module.main()

            out = buf.getvalue()
            self.assertEqual(1, rc, msg=out)
            self.assertIn("receipt: FAIL (multiple target inputs)", out)
            self.assertIn("[ERROR] PLAN contains FAIL.", out)


if __name__ == "__main__":
    unittest.main()
