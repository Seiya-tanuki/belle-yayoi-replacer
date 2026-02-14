#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Set

UTF8_BOM = b"\xEF\xBB\xBF"
ALLOWED_SUFFIXES = {".md", ".json", ".yaml", ".yml"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _tracked_candidates(repo_root: Path) -> Set[Path]:
    proc = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "-z"],
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"git ls-files failed: {err or 'unknown error'}")

    out = proc.stdout
    rel_paths = [p for p in out.split(b"\0") if p]
    candidates: Set[Path] = set()
    for raw_rel in rel_paths:
        rel = Path(raw_rel.decode("utf-8", errors="surrogateescape"))
        if rel.suffix.lower() in ALLOWED_SUFFIXES:
            candidates.add(rel)
    return candidates


def _skill_md_candidates(repo_root: Path) -> Set[Path]:
    candidates: Set[Path] = set()
    for path in (repo_root / ".agents" / "skills").glob("**/SKILL.md"):
        if path.is_file():
            candidates.add(path.relative_to(repo_root))
    return candidates


def _scan_bom(repo_root: Path, rel_paths: Iterable[Path]) -> List[Path]:
    offenders: List[Path] = []
    for rel in sorted(set(rel_paths)):
        abs_path = repo_root / rel
        if not abs_path.is_file():
            continue
        if abs_path.read_bytes().startswith(UTF8_BOM):
            offenders.append(rel)
    return offenders


def _fix_bom(repo_root: Path, rel_paths: Iterable[Path]) -> List[Path]:
    fixed: List[Path] = []
    for rel in sorted(set(rel_paths)):
        abs_path = repo_root / rel
        if not abs_path.is_file():
            continue
        raw = abs_path.read_bytes()
        if raw.startswith(UTF8_BOM):
            abs_path.write_bytes(raw[len(UTF8_BOM):])
            fixed.append(rel)
    return fixed


def main() -> int:
    parser = argparse.ArgumentParser(description="Check/fix UTF-8 BOM in tracked critical files.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="fail if UTF-8 BOM is found")
    mode.add_argument("--fix", action="store_true", help="remove UTF-8 BOM in place")
    args = parser.parse_args()

    repo_root = _repo_root()
    try:
        candidates = _tracked_candidates(repo_root) | _skill_md_candidates(repo_root)
    except RuntimeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    if args.check:
        offenders = _scan_bom(repo_root, candidates)
        for rel in offenders:
            print(rel.as_posix())
        print(f"[SUMMARY] UTF-8 BOM files: {len(offenders)}")
        return 1 if offenders else 0

    fixed = _fix_bom(repo_root, candidates)
    for rel in fixed:
        print(rel.as_posix())
    print(f"[SUMMARY] UTF-8 BOM files fixed: {len(fixed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
