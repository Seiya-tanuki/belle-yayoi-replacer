from __future__ import annotations

import importlib.util
import pkgutil


def ensure_nicegui_compat() -> None:
    if hasattr(pkgutil, "find_loader"):
        return

    def _find_loader(name: str):
        spec = importlib.util.find_spec(name)
        return None if spec is None else spec.loader

    pkgutil.find_loader = _find_loader  # type: ignore[attr-defined]
