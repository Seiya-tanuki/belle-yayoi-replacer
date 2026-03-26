from __future__ import annotations

import contextlib
import importlib.util
import io
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _register_script_path() -> Path:
    return repo_root() / ".agents" / "skills" / "client-register" / "register_client.py"


def _load_register_module():
    script_path = _register_script_path()
    spec = importlib.util.spec_from_file_location(f"register_client_ui_{uuid4().hex}", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load register_client module: {script_path}")
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


@dataclass(frozen=True)
class ClientCreateResult:
    ok: bool
    client_id: str
    stdout: str
    error_message: str


def list_client_ids(root: Path | None = None) -> list[str]:
    current_root = root or repo_root()
    clients_dir = current_root / "clients"
    if not clients_dir.exists():
        return []

    names = [
        path.name
        for path in clients_dir.iterdir()
        if path.is_dir() and path.name != "TEMPLATE"
    ]
    return sorted(names)


def preview_client_id(raw_name: str, root: Path | None = None) -> str:
    module = _load_register_module()
    result = module.validate_and_canonicalize(raw_name)
    return result.canonical if result.ok else ""


def create_client(raw_name: str, root: Path | None = None) -> ClientCreateResult:
    current_root = root or repo_root()
    module = _load_register_module()
    validation = module.validate_and_canonicalize(raw_name)
    if not validation.ok:
        return ClientCreateResult(
            ok=False,
            client_id="",
            stdout=validation.reason,
            error_message="クライアントを作成できませんでした。入力内容を確認してください。",
        )

    buffer = io.StringIO()
    original_sys_path = list(sys.path)
    try:
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            rc = module.main(
                argv=["--client-id", raw_name, "--yes"],
                repo_root=current_root,
            )
    finally:
        sys.path[:] = original_sys_path

    stdout = buffer.getvalue()
    return ClientCreateResult(
        ok=rc == 0,
        client_id=validation.canonical if rc == 0 else "",
        stdout=stdout,
        error_message="" if rc == 0 else "クライアントを作成できませんでした。入力内容を確認してください。",
    )
