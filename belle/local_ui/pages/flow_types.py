from __future__ import annotations

from belle.local_ui.state import get_state
from belle.local_ui.theme import card_container, page_shell, secondary_button


def build() -> None:
    from nicegui import ui

    state = get_state()
    with page_shell(
        "手順 2 / 6",
        "今回の処理種類を選んでください",
        "必要な種類だけを選ぶと、次の画面で入れるファイルが分かりやすくなります。",
    ):
        if not state.selected_client_id:
            ui.label("先にクライアントを選んでください。").classes("text-sm text-red-600")
            secondary_button("戻る", lambda: ui.navigate.to("/"))
            return

        for title, description in [
            ("領収書", "領収書CSVを置換します"),
            ("銀行明細", "銀行明細CSVを置換します"),
            ("クレジットカード", "カード明細CSVを置換します"),
        ]:
            with card_container():
                ui.label(title).classes("text-lg font-semibold")
                ui.label(description).classes("text-sm text-slate-600")
        secondary_button("戻る", lambda: ui.navigate.to("/"))
