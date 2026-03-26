#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

UI_DEPENDENCY_ERROR = (
    "UI の起動に必要なパッケージが見つかりません。"
    "'python -m pip install -r requirements-ui.txt' を実行してください。"
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start Belle local UI.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-browser", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        from belle.local_ui.app import run_local_ui
    except ModuleNotFoundError as exc:
        if exc.name and exc.name.startswith("nicegui"):
            print(UI_DEPENDENCY_ERROR)
            return 1
        raise

    run_local_ui(host=args.host, port=args.port, open_browser=not args.no_browser)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
