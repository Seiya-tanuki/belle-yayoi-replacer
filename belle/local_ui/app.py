from __future__ import annotations

import socket
from typing import Any


def _import_ui():
    from nicegui import ui

    return ui


def pick_port(host: str, preferred_port: int, attempts: int = 5) -> int:
    for offset in range(max(attempts, 1)):
        candidate = preferred_port + offset
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, candidate))
            except OSError:
                continue
        return candidate
    raise RuntimeError(f"No available port found for host={host!r} near {preferred_port}.")


def create_app() -> Any:
    from belle.local_ui.pages import register_routes

    register_routes()
    return {"app_name": "Belle ローカルUI"}


def run_local_ui(*, host: str = "127.0.0.1", port: int = 8080, open_browser: bool = True) -> None:
    ui = _import_ui()
    create_app()
    ui.run(
        host=host,
        port=pick_port(host, port),
        show=open_browser,
        reload=False,
    )
