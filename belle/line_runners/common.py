# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from belle.paths import get_client_root


@dataclass
class LinePlan:
    line_id: str
    status: str  # "RUN" | "SKIP" | "FAIL"
    reason: str
    target_files: list[str]
    details: dict[str, object] = field(default_factory=dict)


def list_input_files(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    files: list[Path] = []
    for p in dir_path.iterdir():
        if not p.is_file():
            continue
        if p.name == ".gitkeep":
            continue
        if p.name.endswith(".tmp"):
            continue
        files.append(p)
    return sorted(files, key=lambda x: x.name)


def resolve_client_layout(
    repo_root: Path,
    client_id: str,
    line_id: str,
) -> tuple[str | None, Path]:
    line_dir = get_client_root(repo_root, client_id, line_id=line_id)
    if line_dir.exists():
        return line_id, line_dir
    if line_id == "receipt":
        legacy_dir = get_client_root(repo_root, client_id)
        if legacy_dir.exists():
            return None, legacy_dir
    raise FileNotFoundError(f"client dir not found: {line_dir}")


def compute_target_file_status(client_dir: Path) -> tuple[str, str, list[str]]:
    input_files = list_input_files(client_dir / "inputs" / "kari_shiwake")
    target_names = [p.name for p in input_files]
    if not input_files:
        return "SKIP", "no target input", target_names
    if len(input_files) >= 2:
        return "FAIL", "multiple target inputs", target_names
    return "OK", "single target input", target_names
