# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

RECEIPT_LINE_CONFIG_FILENAME = "receipt_line_config.json"


def receipt_line_config_path(client_dir: Path) -> Path:
    return client_dir / "config" / RECEIPT_LINE_CONFIG_FILENAME


def load_receipt_line_config(client_dir: Path) -> dict[str, Any]:
    config_path = receipt_line_config_path(client_dir)
    if not config_path.exists():
        raise FileNotFoundError(f"{RECEIPT_LINE_CONFIG_FILENAME} not found: {config_path}")
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{RECEIPT_LINE_CONFIG_FILENAME} must be a JSON object: {config_path}")
    return raw
