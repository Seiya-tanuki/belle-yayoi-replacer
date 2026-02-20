from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


class LexiconSkillsReceiptOnlyTests(unittest.TestCase):
    def test_bank_statement_invocation_fails_closed_with_exit_2(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        scripts = [
            ".agents/skills/lexicon-extract/scripts/run_lexicon_extract.py",
            ".agents/skills/lexicon-apply/scripts/run_lexicon_apply.py",
            ".agents/skills/export-lexicon-review-pack/scripts/export_pack.py",
        ]

        for rel_script in scripts:
            with self.subTest(script=rel_script):
                proc = subprocess.run(
                    [sys.executable, str(repo_root / rel_script), "--line", "bank_statement"],
                    cwd=repo_root,
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                combined = f"{proc.stdout}\n{proc.stderr}".lower()
                self.assertEqual(2, proc.returncode, msg=combined)
                self.assertIn("receipt-only", combined, msg=combined)


if __name__ == "__main__":
    unittest.main()
