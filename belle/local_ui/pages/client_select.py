from __future__ import annotations

from belle.local_ui.services.clients import list_client_ids
from belle.local_ui.state import get_state
from belle.local_ui.theme import page_shell, primary_button, secondary_button


def build() -> None:
    from nicegui import ui

    state = get_state()
    client_ids = list_client_ids()

    def select_client(client_id: str) -> None:
        state.selected_client_id = client_id
        ui.navigate.to("/flow/types")

    with page_shell(
        "手順 1 / 6",
        "クライアントを選んでください",
        "作業するクライアントを選ぶと、次の画面へ進みます。",
    ):
        if client_ids:
            ui.select(
                client_ids,
                label="クライアントを選ぶ",
                value=state.selected_client_id if state.selected_client_id in client_ids else None,
                on_change=lambda e: select_client(e.value) if e.value else None,
                with_input=True,
                clearable=True,
            ).props("outlined").classes("w-full")
        else:
            ui.label("まだクライアントがありません").classes("text-xl font-semibold")
            ui.label("まずは新しいクライアントを作ってください。").classes("text-sm text-slate-600")
        with ui.row().classes("w-full items-center justify-between gap-3"):
            secondary_button("新しいクライアントを作る", lambda: ui.navigate.to("/clients/new"))
            with ui.row().classes("justify-end"):
                if state.selected_client_id:
                    primary_button("次へ", lambda: ui.navigate.to("/flow/types"))
