from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .io_atomic import atomic_write_text


def write_text_atomic(path: Path, text: str, encoding: str = "utf-8") -> None:
    atomic_write_text(path, text, encoding=encoding)


def write_json_atomic(path: Path, obj: Any) -> None:
    json_text = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False)
    if not json_text.endswith("\n"):
        json_text += "\n"
    write_text_atomic(path, json_text, encoding="utf-8")


def update_latest_run_id(latest_path: Path, run_id: str) -> None:
    write_text_atomic(latest_path, f"{run_id}\n", encoding="utf-8")
