from __future__ import annotations

import json
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from uuid import uuid4


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


class SystemDiagnoseBankForbiddenResidueWarnTests(unittest.TestCase):
    def test_bank_statement_warns_for_forbidden_residue_but_stays_go(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        temp_root = real_repo_root / ".tmp" / f"diagnose_bank_forbidden_warn_{uuid4().hex}"
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

            for name in [
                "FILE_LAYOUT.md",
                "REPLACER_SPEC.md",
                "CLIENT_CACHE_SPEC.md",
                "LEXICON_PENDING_SPEC.md",
                "CATEGORY_OVERRIDES_SPEC.md",
            ]:
                _write_text(temp_root / "spec" / name, f"# {name}\n")

            for skill_name in [
                "yayoi-replacer",
                "client-register",
                "client-cache-builder",
                "lexicon-apply",
                "lexicon-extract",
                "export-lexicon-review-pack",
            ]:
                (temp_root / ".agents" / "skills" / skill_name).mkdir(parents=True, exist_ok=True)

            (temp_root / "clients" / "TEMPLATE" / "lines" / "bank_statement" / "artifacts" / "ingest").mkdir(
                parents=True,
                exist_ok=True,
            )

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
                        "    return validate_line_id(line_id) in {'receipt', 'bank_statement'}",
                        "",
                    ]
                ),
            )

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

            line_root = temp_root / "clients" / "C_WARN" / "lines" / "bank_statement"
            _write_text(line_root / "inputs" / "training" / "ocr_kari_shiwake" / "ocr_1.csv", "dummy\n")
            _write_text(line_root / "inputs" / "training" / "reference_yayoi" / "teacher_1.csv", "dummy\n")
            _write_text(line_root / "inputs" / "kari_shiwake" / "target_1.csv", "dummy\n")
            _write_text(
                line_root / "config" / "bank_line_config.json",
                json.dumps({"schema": "belle.bank_line_config.v0", "version": "0.1"}, ensure_ascii=False),
            )
            _write_text(
                line_root / "artifacts" / "cache" / "client_cache.json",
                json.dumps({"updated_at": "2026-02-20T00:00:00Z"}, ensure_ascii=False),
            )

            # Forbidden residue directories are intentionally created (empty) and must trigger WARN only.
            (line_root / "inputs" / "ledger_ref").mkdir(parents=True, exist_ok=True)
            (line_root / "artifacts" / "ingest" / "ledger_ref").mkdir(parents=True, exist_ok=True)

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

            proc = subprocess.run(
                [sys.executable, str(script_target), "--line", "bank_statement"],
                cwd=temp_root,
                capture_output=True,
                text=True,
                timeout=240,
                check=False,
            )
            combined = f"{proc.stdout}\n{proc.stderr}"
            self.assertEqual(0, proc.returncode, msg=combined)

            latest_path = temp_root / "exports" / "system_diagnose" / "LATEST.txt"
            self.assertTrue(latest_path.exists(), msg=combined)
            report_name = latest_path.read_text(encoding="utf-8").strip()
            report_text = (temp_root / "exports" / "system_diagnose" / report_name).read_text(encoding="utf-8")
            self.assertIn("WARN", report_text)
            self.assertIn("forbidden bank ledger_ref residue", report_text)
            self.assertIn("inputs/ledger_ref/**", report_text)
            self.assertIn("artifacts/ingest/ledger_ref/**", report_text)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
