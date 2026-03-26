from __future__ import annotations

from belle.local_ui.state import LINE_ORDER, get_state, normalize_selected_lines
from belle.local_ui.theme import card_container, page_shell, primary_button, secondary_button


LINE_CHOICES = {
    "receipt": ("領収書", "領収書CSVを置換します"),
    "bank_statement": ("銀行明細", "銀行明細CSVを置換します"),
    "credit_card_statement": ("クレジットカード", "カード明細CSVを置換します"),
}


def build() -> None:
    from nicegui import ui

    state = get_state()
    selected = set(state.selected_lines)
    button_holder = ui.row().classes("w-full justify-start")

    def refresh_button() -> None:
        button_holder.clear()
        with button_holder:
            if selected:
                primary_button("次へ", go_next)
            else:
                ui.button("次へ").props("unelevated color=primary").classes("w-full sm:w-auto").disable()

    def toggle_line(line_id: str) -> None:
        if line_id in selected:
            selected.remove(line_id)
        else:
            selected.add(line_id)
        state.selected_lines = normalize_selected_lines(list(selected))
        refresh_page()

    def go_next() -> None:
        normalized = normalize_selected_lines(list(selected))
        if not normalized:
            ui.notify("先に処理種類を選んでください。", type="warning")
            return
        state.selected_lines = normalized
        state.current_line_index = 0
        ui.navigate.to(f"/flow/upload/{normalized[0]}")

    def refresh_page() -> None:
        cards_column.clear()
        with cards_column:
            for line_id in LINE_ORDER:
                title, description = LINE_CHOICES[line_id]
                with card_container(selected=line_id in selected).on(
                    "click", lambda _=None, line_id=line_id: toggle_line(line_id)
                ):
                    ui.label(title).classes("text-lg font-semibold")
                    ui.label(description).classes("text-sm text-slate-600")
        refresh_button()

    with page_shell(
        "手順 2 / 6",
        "今回の処理種類を選んでください",
        "必要な種類だけを選ぶと、次の画面で入れるファイルが分かりやすくなります。",
    ):
        if not state.selected_client_id:
            ui.label("先にクライアントを選んでください。").classes("text-sm text-red-600")
            secondary_button("戻る", lambda: ui.navigate.to("/"))
            return

        cards_column = ui.column().classes("w-full gap-3")
        refresh_page()
        secondary_button("戻る", lambda: ui.navigate.to("/"))
