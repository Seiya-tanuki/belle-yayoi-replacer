from __future__ import annotations

from belle.local_ui.theme import page_shell


def register_routes() -> None:
    from nicegui import ui

    @ui.page("/")
    def home_page() -> None:
        with page_shell(
            "手順 1 / 6",
            "クライアントを選んでください",
            "作業するクライアントを選ぶと、次の画面へ進みます。",
        ):
            ui.label("Phase 1 foundation complete.").classes("text-sm text-slate-500")
