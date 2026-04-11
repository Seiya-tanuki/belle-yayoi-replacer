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


def _write_valid_shared_tax_config(
    repo_root: Path,
    client_id: str,
    *,
    bookkeeping_mode: str = "tax_excluded",
    enabled: bool | None = None,
) -> None:
    if enabled is None:
        enabled = bookkeeping_mode == "tax_excluded"
    _write_text(
        repo_root / "clients" / client_id / "config" / "yayoi_tax_config.json",
        json.dumps(
            {
                "schema": "belle.yayoi_tax_config.v1",
                "version": "1.0",
                "enabled": enabled,
                "bookkeeping_mode": bookkeeping_mode,
                "rounding_mode": "floor",
            },
            ensure_ascii=False,
        ),
    )


def _minimal_receipt_replacer_config_json() -> str:
    return json.dumps(
        {
            "schema": "belle.replacer_config.v1",
            "version": "1.16",
            "csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"},
            "tax_division_thresholds": {
                "t_number_x_category_target_account": {"min_count": 2, "min_p_majority": 0.75},
                "t_number_target_account": {"min_count": 3, "min_p_majority": 0.7},
                "vendor_key_target_account": {"min_count": 3, "min_p_majority": 0.7},
                "category_target_account": {"min_count": 3, "min_p_majority": 0.7},
                "global_target_account": {"min_count": 3, "min_p_majority": 0.7},
            },
            "tax_division_confidence": {
                "t_number_x_category_target_account_strength": 0.97,
                "t_number_target_account_strength": 0.95,
                "vendor_key_target_account_strength": 0.85,
                "category_target_account_strength": 0.65,
                "global_target_account_strength": 0.55,
                "category_default_strength": 0.55,
                "global_fallback_strength": 0.35,
                "learned_weight_multiplier": 0.85,
            },
        },
        ensure_ascii=False,
    )


def _minimal_credit_card_line_config_json() -> str:
    return json.dumps(
        {
            "schema": "belle.credit_card_line_config.v1",
            "version": "0.2",
            "placeholder_account_name": "仮払金",
            "target_payable_placeholder_names": ["未払金"],
            "thresholds": {
                "merchant_key_account": {"min_count": 3, "min_p_majority": 0.9},
                "file_level_card_inference": {"min_votes": 3, "min_p_majority": 0.9},
            },
            "teacher_extraction": {
                "canonical_payable_thresholds": {"min_count": 3, "min_p_majority": 0.9}
            },
            "tax_division_thresholds": {
                "merchant_key_target_account_exact": {"min_count": 3, "min_p_majority": 0.9},
                "merchant_key_target_account_partial": {"min_count": 3, "min_p_majority": 0.9},
            },
            "candidate_extraction": {
                "min_total_count": 5,
                "min_unique_merchants": 3,
                "min_unique_counter_accounts": 2,
            },
        },
        ensure_ascii=False,
    )


def _write_mode_aware_defaults(repo_root: Path, line_id: str, text: str) -> None:
    defaults_dir = repo_root / "defaults" / line_id
    _write_text(defaults_dir / "category_defaults_tax_excluded.json", text)
    _write_text(defaults_dir / "category_defaults_tax_included.json", text)


class SystemDiagnoseDefaultAllTests(unittest.TestCase):
    def test_default_all_is_bootstrap_safe_and_reports_each_line(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        if shutil.which("git") is None:
            self.skipTest("git executable is required for self-contained diagnose fixture setup")
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
            run_tests_source = real_repo_root / "tools" / "run_tests.py"
            run_tests_target = temp_root / "tools" / "run_tests.py"
            run_tests_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(run_tests_source, run_tests_target)

            for name in SPEC_FILES:
                _write_text(temp_root / "spec" / name, f"# {name}\n")
            for skill_name in SKILL_DIRS:
                (temp_root / ".agents" / "skills" / skill_name).mkdir(parents=True, exist_ok=True)

            # receipt template (used by write probe)
            (temp_root / "clients" / "TEMPLATE" / "lines" / "receipt" / "artifacts" / "ingest").mkdir(
                parents=True,
                exist_ok=True,
            )
            _write_valid_shared_tax_config(temp_root, "TEMPLATE")

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
            _write_mode_aware_defaults(temp_root, "receipt", "{}\n")
            _write_mode_aware_defaults(temp_root, "credit_card_statement", "{}\n")
            _write_text(
                temp_root / "rulesets" / "receipt" / "replacer_config_v1_15.json",
                _minimal_receipt_replacer_config_json(),
            )
            _write_text(
                card_template_root / "config" / "credit_card_line_config.json",
                _minimal_credit_card_line_config_json(),
            )

            _write_text(temp_root / "belle" / "__init__.py", "")
            _write_text(
                temp_root / "belle" / "lines.py",
                "\n".join(
                    [
                        "from __future__ import annotations",
                        "",
                        "from pathlib import Path",
                        "",
                        "CANONICAL_LINE_IDS = ['receipt', 'bank_statement', 'credit_card_statement']",
                        "SUPPORTED_BOOKKEEPING_MODES = ['tax_excluded', 'tax_included']",
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
                        "def tracked_category_defaults_relpaths(line_id: str) -> list[Path]:",
                        "    line = validate_line_id(line_id)",
                        "    if line == 'receipt' or line == 'credit_card_statement':",
                        "        return [",
                        "            Path('defaults') / line / 'category_defaults_tax_excluded.json',",
                        "            Path('defaults') / line / 'category_defaults_tax_included.json',",
                        "        ]",
                        "    return [Path('defaults') / line / 'category_defaults.json']",
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
                        "from belle.lines import validate_line_id",
                        "",
                        "class SmokeTests(unittest.TestCase):",
                        "    def test_ok(self) -> None:",
                        "        self.assertEqual('receipt', validate_line_id('receipt'))",
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
