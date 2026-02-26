from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
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


class SystemDiagnoseDefaultAllTests(unittest.TestCase):
    def test_default_all_is_bootstrap_safe_and_reports_each_line(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        temp_root = real_repo_root / ".tmp" / f"diagnose_default_all_{uuid4().hex}"
        temp_root.mkdir(parents=True, exist_ok=False)
        try:
            script_source = (
                real_repo_root
                / ".agents"
                / "skills"
                / "system-diagnose"
                / "scripts"
                / "system_diagnose.py"
            )
            script_target = (
                temp_root
                / ".agents"
                / "skills"
                / "system-diagnose"
                / "scripts"
                / "system_diagnose.py"
            )
            script_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(script_source, script_target)

            for name in SPEC_FILES:
                _write_text(temp_root / "spec" / name, f"# {name}\n")
            for skill_name in SKILL_DIRS:
                (temp_root / ".agents" / "skills" / skill_name).mkdir(parents=True, exist_ok=True)

            # receipt template (used by write probe)
            (temp_root / "clients" / "TEMPLATE" / "lines" / "receipt" / "artifacts" / "ingest").mkdir(
                parents=True,
                exist_ok=True,
            )

            bank_template_root = temp_root / "clients" / "TEMPLATE" / "lines" / "bank_statement"
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

            card_template_root = temp_root / "clients" / "TEMPLATE" / "lines" / "credit_card_statement"
            for rel in [
                Path("inputs/kari_shiwake"),
                Path("inputs/ledger_ref"),
                Path("artifacts/ingest/kari_shiwake"),
                Path("artifacts/ingest/ledger_ref"),
                Path("artifacts/cache"),
                Path("outputs/runs"),
            ]:
                (card_template_root / rel).mkdir(parents=True, exist_ok=True)

            _write_text(temp_root / "lexicon" / "lexicon.json", "{}\n")
            _write_text(temp_root / "defaults" / "receipt" / "category_defaults.json", "{}\n")
            _write_text(temp_root / "rulesets" / "receipt" / "replacer_config_v1_15.json", "{}\n")

            _write_text(temp_root / "belle" / "__init__.py", "")
            _write_text(
                temp_root / "belle" / "lines.py",
                "\n".join(
                    [
                        "from __future__ import annotations",
                        "",
                        "CANONICAL_LINE_IDS = ['receipt', 'bank_statement', 'credit_card_statement']",
                        "",
                        "def validate_line_id(line_id: str) -> str:",
                        "    value = str(line_id or '').strip().lower()",
                        "    if value in CANONICAL_LINE_IDS:",
                        "        return value",
                        "    raise ValueError(f\"invalid line_id: {line_id!r}\")",
                        "",
                        "def is_line_implemented(line_id: str) -> bool:",
                        "    return validate_line_id(line_id) in {'receipt', 'bank_statement', 'credit_card_statement'}",
                        "",
                    ]
                ),
            )
            for module_name in [
                "build_bank_cache.py",
                "bank_replacer.py",
                "bank_cache.py",
                "bank_pairing.py",
            ]:
                _write_text(temp_root / "belle" / module_name, "# fixture\n")
            # cp932 では表現できない文字を含むファイル名で、all-mode 子実行の UTF-8 強制を検証する。
            _write_text(temp_root / "belle" / "emoji_\U0001F680.py", "VALUE = 'ok'\n")

            _write_text(
                temp_root / "tools" / "bom_guard.py",
                "\n".join(
                    [
                        "from __future__ import annotations",
                        "",
                        "if __name__ == '__main__':",
                        "    print('UTF-8 BOM files: 0')",
                        "",
                    ]
                ),
            )

            _write_text(
                temp_root / "tests" / "test_smoke.py",
                "\n".join(
                    [
                        "import unittest",
                        "",
                        "class SmokeTests(unittest.TestCase):",
                        "    def test_ok(self) -> None:",
                        "        self.assertTrue(True)",
                        "",
                        "if __name__ == '__main__':",
                        "    unittest.main()",
                        "",
                    ]
                ),
            )

            subprocess.run(["git", "init"], cwd=temp_root, check=True, capture_output=True, text=True)
            subprocess.run(
                ["git", "config", "user.email", "diagnose-test@example.com"],
                cwd=temp_root,
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Diagnose Test"],
                cwd=temp_root,
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(["git", "add", "."], cwd=temp_root, check=True, capture_output=True, text=True)
            subprocess.run(
                ["git", "commit", "-m", "init diagnose fixture"],
                cwd=temp_root,
                check=True,
                capture_output=True,
                text=True,
            )

            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "cp932"
            env["PYTHONUTF8"] = "0"
            proc = subprocess.run(
                [sys.executable, str(script_target)],
                cwd=temp_root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                timeout=480,
                check=False,
            )
            combined = f"{proc.stdout}\n{proc.stderr}"
            self.assertEqual(0, proc.returncode, msg=combined)
            self.assertIn("- receipt: GO", combined)
            self.assertIn("- bank_statement: GO", combined)
            self.assertIn("- credit_card_statement: GO", combined)

            latest_path = temp_root / "exports" / "system_diagnose" / "LATEST.txt"
            self.assertTrue(latest_path.exists(), msg=combined)
            report_name = latest_path.read_text(encoding="utf-8").strip()
            report_text = (temp_root / "exports" / "system_diagnose" / report_name).read_text(encoding="utf-8")
            report_files = sorted((temp_root / "exports" / "system_diagnose").glob("system_diagnose_*.md"))
            self.assertEqual(1, len(report_files), msg=[p.name for p in report_files])
            self.assertIn("| receipt | GO |", report_text)
            self.assertIn("| bank_statement | GO |", report_text)
            self.assertIn("| credit_card_statement | GO |", report_text)
            self.assertNotIn("template-only check; unimplemented is warn-only", report_text)
            self.assertIn("## receipt", report_text)
            self.assertIn("## bank_statement", report_text)
            self.assertIn("## credit_card_statement", report_text)
            self.assertIn("- Line ID: receipt", report_text)
            self.assertIn("- Line ID: bank_statement", report_text)
            self.assertIn("- Line ID: credit_card_statement", report_text)
            self.assertNotIn("Internal capture warning", report_text)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
