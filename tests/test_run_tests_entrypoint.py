from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


class RunTestsEntrypointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]

    def _prepare_repo(self, temp_root: Path, *, passing: bool) -> Path:
        run_tests_source = self.real_repo_root / "tools" / "run_tests.py"
        run_tests_target = temp_root / "tools" / "run_tests.py"
        run_tests_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(run_tests_source, run_tests_target)

        _write_text(temp_root / "belle" / "__init__.py", "")
        _write_text(temp_root / "belle" / "bootstrap_fixture.py", "VALUE = 'ok'\n")
        assertion = "self.assertEqual('ok', VALUE)" if passing else "self.assertEqual('ng', VALUE)"
        _write_text(
            temp_root / "tests" / "test_smoke.py",
            textwrap.dedent(
                f"""\
                import unittest

                from belle.bootstrap_fixture import VALUE


                class SmokeTests(unittest.TestCase):
                    def test_import_from_repo_root(self) -> None:
                        {assertion}


                if __name__ == "__main__":
                    unittest.main()
                """
            ),
        )
        return run_tests_target

    def _run_entrypoint(self, script_path: Path, cwd: Path) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)
        return subprocess.run(
            [sys.executable, str(script_path)],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )

    def test_entrypoint_runs_without_pythonpath_in_zip_like_repo(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            script_path = self._prepare_repo(temp_root, passing=True)

            proc = self._run_entrypoint(script_path, temp_root)
            combined = f"{proc.stdout}\n{proc.stderr}"

            self.assertEqual(0, proc.returncode, msg=combined)
            self.assertIn("test_import_from_repo_root", combined)
            self.assertNotIn("ModuleNotFoundError", combined)

    def test_entrypoint_returns_nonzero_when_suite_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            script_path = self._prepare_repo(temp_root, passing=False)

            proc = self._run_entrypoint(script_path, temp_root)
            combined = f"{proc.stdout}\n{proc.stderr}"

            self.assertNotEqual(0, proc.returncode, msg=combined)
            self.assertIn("FAILED", combined)


if __name__ == "__main__":
    unittest.main()
