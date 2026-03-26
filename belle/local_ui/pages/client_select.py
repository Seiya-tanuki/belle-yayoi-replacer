from __future__ import annotations

from belle.local_ui.services.clients import list_client_ids
from belle.local_ui.state import get_state
from belle.local_ui.theme import card_container, page_shell, primary_button, secondary_button


def build() -> None:
    from nicegui import ui

    state = get_state()
    search_value = {"text": ""}
    clients_column = ui.column().classes("w-full gap-3")

    def render_clients() -> None:
        clients_column.clear()
        current_search = search_value["text"].strip().lower()
        client_ids = list_client_ids()
        visible_ids = [client_id for client_id in client_ids if current_search in client_id.lower()]
        with clients_column:
            if visible_ids:
                for client_id in visible_ids:
                    with card_container(selected=state.selected_client_id == client_id).on(
                        "click", lambda _=None, client_id=client_id: select_client(client_id)
                    ):
                        ui.label(client_id).classes("text-lg font-semibold")
            else:
                ui.label("まだクライアントがありません").classes("text-xl font-semibold")
                ui.label("まずは新しいクライアントを作ってください。").classes("text-sm text-slate-600")

    def select_client(client_id: str) -> None:
        state.selected_client_id = client_id
        ui.navigate.to("/flow/types")

    with page_shell(
        "手順 1 / 6",
        "クライアントを選んでください",
        "作業するクライアントを選ぶと、次の画面へ進みます。",
    ):
        ui.input(
            label="クライアント名で探す",
            placeholder="クライアント名で探す",
            on_change=lambda e: (search_value.__setitem__("text", e.value or ""), render_clients()),
        ).props("outlined clearable").classes("w-full")
        render_clients()
        secondary_button("新しいクライアントを作る", lambda: ui.navigate.to("/clients/new"))
        if state.selected_client_id:
            primary_button("次へ", lambda: ui.navigate.to("/flow/types"))
        else:
            ui.space()
