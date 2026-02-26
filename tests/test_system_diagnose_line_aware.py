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


SPEC_FILES = [
    "FILE_LAYOUT.md",
    "REPLACER_SPEC.md",
    "CLIENT_CACHE_SPEC.md",
    "LEXICON_PENDING_SPEC.md",
    "CATEGORY_OVERRIDES_SPEC.md",
]

SKILL_DIRS = [
    "yayoi-replacer",
    "client-register",
    "client-cache-builder",
    "lexicon-apply",
    "lexicon-extract",
    "export-lexicon-review-pack",
]


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _load_system_diagnose_module(real_repo_root: Path):
    script_path = (
        real_repo_root
        / ".agents"
        / "skills"
        / "system-diagnose"
        / "scripts"
        / "system_diagnose.py"
    )
    spec = importlib.util.spec_from_file_location(f"system_diagnose_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _prepare_common_repo_layout(repo_root: Path, line_id: str) -> None:
    for name in SPEC_FILES:
        _write_text(repo_root / "spec" / name, f"# {name}\n")
    for name in SKILL_DIRS:
        (repo_root / ".agents" / "skills" / name).mkdir(parents=True, exist_ok=True)
    (repo_root / "clients" / "TEMPLATE" / "lines" / line_id / "artifacts" / "ingest").mkdir(
        parents=True,
        exist_ok=True,
    )
    if line_id == "bank_statement":
        bank_template_root = repo_root / "clients" / "TEMPLATE" / "lines" / "bank_statement"
        for rel in [
            Path("inputs/training/ocr_kari_shiwake"),
            Path("inputs/training/reference_yayoi"),
            Path("inputs/kari_shiwake"),
            Path("artifacts/ingest/training_ocr"),
            Path("artifacts/ingest/training_reference"),
            Path("artifacts/ingest/kari_shiwake"),
        ]:
            (bank_template_root / rel).mkdir(parents=True, exist_ok=True)
        _write_text(
            bank_template_root / "config" / "bank_line_config.json",
            json.dumps({"schema": "belle.bank_line_config.v0", "version": "0.1"}, ensure_ascii=False),
        )
        for module_name in [
            "build_bank_cache.py",
            "bank_replacer.py",
            "bank_cache.py",
            "bank_pairing.py",
        ]:
            _write_text(repo_root / "belle" / module_name, "# test fixture\n")


def _prepare_receipt_assets(repo_root: Path, *, with_lexicon: bool) -> None:
    _write_text(repo_root / "defaults" / "receipt" / "category_defaults.json", "{}\n")
    _write_text(repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json", "{}\n")
    if with_lexicon:
        _write_text(repo_root / "lexicon" / "lexicon.json", "{}\n")


def _prepare_bank_client(
    repo_root: Path,
    client_id: str,
    *,
    teacher_count: int,
    target_count: int,
    ocr_file_count: int,
    with_config: bool = True,
    with_cache: bool = False,
) -> None:
    line_root = repo_root / "clients" / client_id / "lines" / "bank_statement"
    ocr_dir = line_root / "inputs" / "training" / "ocr_kari_shiwake"
    ref_dir = line_root / "inputs" / "training" / "reference_yayoi"
    target_dir = line_root / "inputs" / "kari_shiwake"
    ocr_dir.mkdir(parents=True, exist_ok=True)
    ref_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    for idx in range(ocr_file_count):
        _write_text(ocr_dir / f"ocr_{idx + 1}.csv", "dummy\n")
    for idx in range(teacher_count):
        _write_text(ref_dir / f"teacher_{idx + 1}.csv", "dummy\n")
    for idx in range(target_count):
        _write_text(target_dir / f"target_{idx + 1}.csv", "dummy\n")

    if with_config:
        _write_text(
            line_root / "config" / "bank_line_config.json",
            json.dumps({"schema": "belle.bank_line_config.v0", "version": "0.1"}, ensure_ascii=False),
        )
    if with_cache:
        _write_text(
            line_root / "artifacts" / "cache" / "client_cache.json",
            json.dumps({"updated_at": "2026-02-20T00:00:00Z"}, ensure_ascii=False),
        )


def _make_fake_run_command(module):
    command_outputs = {
        "git rev-parse --is-inside-work-tree": (0, "true\n"),
        "git rev-parse HEAD": (0, "0123456789abcdef\n"),
        "git status --porcelain=v1 -uall": (0, ""),
        "git --version": (0, "git version 2.47.0\n"),
        "python --version": (0, "Python 3.12.1\n"),
        'python -c "import sys; print(sys.executable); print(sys.version)"': (
            0,
            "C:\\Python312\\python.exe\n3.12.1\n",
        ),
        "python tools/bom_guard.py --check": (0, "UTF-8 BOM files: 0\n"),
        "python -m compileall belle tools .agents/skills tests": (0, "ok\n"),
        "python -m unittest discover -s tests -v": (0, "ok\n"),
        'python -c "import codecs; codecs.lookup(\'cp932\'); print(\'cp932 OK\')"': (0, "cp932 OK\n"),
        "py -0p": (0, " -V:3.12 * C:\\Python312\\python.exe\n"),
        "git config --get core.hooksPath": (0, ".githooks\n"),
        "where.exe python": (1, ""),
        "python3 --version": (0, "Python 3.12.1\n"),
    }

    def _fake_run(command: str, cwd: Path, timeout_sec: int = 30):
        returncode, stdout = command_outputs.get(command, (0, ""))
        return module.CommandResult(
            command=command,
            returncode=returncode,
            stdout=stdout,
            stderr="",
            timed_out=False,
            error=None,
            duration_sec=0.001,
        )

    return _fake_run


def _run_main(module, repo_root: Path, line_id: str) -> tuple[int, str]:
    fake_script_path = (
        repo_root
        / ".agents"
        / "skills"
        / "system-diagnose"
        / "scripts"
        / "system_diagnose.py"
    )
    fake_script_path.parent.mkdir(parents=True, exist_ok=True)
    module.__file__ = str(fake_script_path)
    fake_run_command = _make_fake_run_command(module)
    output_buffer = io.StringIO()
    with mock.patch.object(module, "_run_command", side_effect=fake_run_command):
        with mock.patch.object(sys, "argv", ["system_diagnose.py", "--line", line_id]):
            with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(output_buffer):
                rc = module.main()
    return rc, output_buffer.getvalue()


def _read_latest_report(repo_root: Path) -> str:
    latest_path = repo_root / "exports" / "system_diagnose" / "LATEST.txt"
    if not latest_path.exists():
        return ""
    report_name = latest_path.read_text(encoding="utf-8").strip()
    report_path = repo_root / "exports" / "system_diagnose" / report_name
    if not report_path.exists():
        return ""
    return report_path.read_text(encoding="utf-8")


class SystemDiagnoseLineAwareTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def test_bank_statement_go_without_bank_receipt_assets(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_bank_go_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "bank_statement")
            _prepare_bank_client(
                repo_root,
                "C_BANK_GO",
                teacher_count=1,
                target_count=1,
                ocr_file_count=0,
                with_config=True,
                with_cache=False,
            )
            self.assertFalse((repo_root / "lexicon" / "bank_statement" / "lexicon.json").exists())
            self.assertFalse((repo_root / "defaults" / "bank_statement" / "category_defaults.json").exists())
            self.assertFalse(
                (repo_root / "rulesets" / "bank_statement" / "replacer_config_v1_15.json").exists()
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "bank_statement")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertNotIn("lexicon/bank_statement/lexicon.json exists", report)
            self.assertNotIn("defaults/bank_statement/category_defaults.json exists", report)
            self.assertNotIn("rulesets/bank_statement/replacer_config_v1_15.json", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_bank_statement_go_with_empty_teacher_and_target_dirs(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_bank_empty_input_dirs_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "bank_statement")
            _prepare_bank_client(
                repo_root,
                "C_BANK_EMPTY",
                teacher_count=0,
                target_count=0,
                ocr_file_count=0,
                with_config=True,
                with_cache=False,
            )
            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "bank_statement")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn("C18 bank_statement teacher reference directory exists", report)
            self.assertIn("C19 bank_statement target kari_shiwake directory exists", report)
            self.assertNotIn("file count is exactly 1", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_receipt_still_enforces_lexicon_requirement(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_receipt_ng_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "receipt")
            _prepare_receipt_assets(repo_root, with_lexicon=False)

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "receipt")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn("C1 lexicon/lexicon.json exists", report)
            self.assertIn("| C1 lexicon/lexicon.json exists | FAIL |", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
