#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> int:
    repo_root = _repo_root()
    tests_dir = repo_root / "tests"

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    sys.dont_write_bytecode = True
    os.chdir(repo_root)

    suite = unittest.defaultTestLoader.discover(
        start_dir=str(tests_dir),
        pattern="test*.py",
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
