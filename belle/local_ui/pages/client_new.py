from __future__ import annotations

from belle.local_ui.services.clients import create_client, preview_client_id
from belle.local_ui.state import get_state
from belle.local_ui.theme import card_container, page_shell, primary_button, secondary_button


BOOKKEEPING_MODE_CHOICES = {
    "tax_included": (
        "税込経理",
        "税込金額のまま扱う設定です。",
    ),
    "tax_excluded": (
        "税抜経理",
        "税額を本体金額と分けて扱う設定です。",
    ),
}


def build() -> None:
    from nicegui import ui

    state = get_state()
    model = {"raw_name": "", "preview": "", "bookkeeping_mode": "", "stdout": "", "error": ""}

    def update_preview(value: str) -> None:
        model["raw_name"] = value or ""
        model["preview"] = preview_client_id(model["raw_name"])
        preview_label.set_text(model["preview"] or "-")

    def update_bookkeeping_mode(value: str) -> None:
        model["bookkeeping_mode"] = value or ""

    def refresh_bookkeeping_cards() -> None:
        cards_row.clear()
        with cards_row:
            for value, (label, description) in BOOKKEEPING_MODE_CHOICES.items():
                is_selected = model["bookkeeping_mode"] == value
                with card_container(selected=is_selected).classes("sm:flex-1").on(
                    "click",
                    lambda _=None, value=value: select_bookkeeping_mode(value),
                ):
                    ui.label(label).classes(
                        "text-lg font-semibold text-white" if is_selected else "text-lg font-semibold"
                    )
                    ui.label(description).classes(
                        "text-sm text-white" if is_selected else "text-sm text-slate-600"
                    )

    def select_bookkeeping_mode(value: str) -> None:
        update_bookkeeping_mode(value)
        refresh_bookkeeping_cards()

    def submit() -> None:
        model["error"] = ""
        model["stdout"] = ""
        if not model["bookkeeping_mode"]:
            model["error"] = "帳簿方式を選択してください。"
            error_label.set_text(model["error"])
            error_label.visible = True
            return

        result = create_client(model["raw_name"], model["bookkeeping_mode"])
        model["stdout"] = result.stdout
        if result.ok:
            state.selected_client_id = result.client_id
            ui.notify("クライアントを作成しました。", type="positive")
            ui.navigate.to("/flow/types")
            return

        model["error"] = "クライアントを作成できませんでした。入力内容を確認してください。"
        error_label.set_text(model["error"])
        error_label.visible = True

    with page_shell(
        "手順 1 / 6",
        "新しいクライアントを作ります",
        "入力した名前は、保存用に自動で整えられます。\n消費税の経理方式は必ず事前に弥生会計のデータを確認し、正しい方を選択してください",
    ):
        ui.label("登録したいクライアント名").classes("text-sm font-semibold text-slate-700")
        ui.input(
            placeholder="ここにクライアント名を入力してください",
            on_change=lambda e: update_preview(e.value or ""),
        ).props("outlined").classes("w-full")
        ui.label("消費税の経理方式").classes("text-sm font-semibold text-slate-700")
        cards_row = ui.row().classes("w-full items-stretch gap-3 flex-col sm:flex-row")
        refresh_bookkeeping_cards()
        with ui.card().classes("w-full rounded-2xl border border-slate-200 p-4 gap-2 shadow-sm"):
            ui.label("保存される名前").classes("text-sm text-slate-500")
            preview_label = ui.label("-").classes("text-lg font-semibold")
        error_label = ui.label("").classes("text-sm text-red-600")
        error_label.visible = False
        with ui.row().classes("w-full items-center justify-between gap-3"):
            secondary_button("戻る", lambda: ui.navigate.to("/"))
            with ui.row().classes("justify-end"):
                primary_button("この名前で作成する", submit)
