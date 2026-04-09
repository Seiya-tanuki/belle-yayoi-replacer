from __future__ import annotations

from belle.local_ui.services.clients import create_client, preview_client_id
from belle.local_ui.state import get_state
from belle.local_ui.theme import page_shell, primary_button, secondary_button


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

    def submit() -> None:
        model["error"] = ""
        model["stdout"] = ""
        if not model["bookkeeping_mode"]:
            model["error"] = "帳簿方式を選択してください。"
            error_label.set_text(model["error"])
            error_label.visible = True
            detail_log.set_content("詳細はありません。")
            return

        result = create_client(model["raw_name"], model["bookkeeping_mode"])
        model["stdout"] = result.stdout
        detail_log.set_content(result.stdout or "詳細はありません。")
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
        "入力した名前は、保存用に自動で整えられます。\n帳簿方式は作成時に必ず選択してください。",
    ):
        ui.input(
            label="クライアント名",
            placeholder="クライアント名",
            on_change=lambda e: update_preview(e.value or ""),
        ).props("outlined").classes("w-full")
        ui.radio(
            {
                "tax_excluded": "税抜き",
                "tax_included": "税込み",
            },
            value=None,
            on_change=lambda e: update_bookkeeping_mode(e.value or ""),
        ).props("inline").classes("w-full")
        with ui.card().classes("w-full rounded-2xl border border-slate-200 p-4 gap-2 shadow-sm"):
            ui.label("保存される名前").classes("text-sm text-slate-500")
            preview_label = ui.label("-").classes("text-lg font-semibold")
        error_label = ui.label("").classes("text-sm text-red-600")
        error_label.visible = False
        with ui.expansion("詳細ログを見る", value=False).classes("w-full"):
            detail_log = ui.markdown("詳細はありません。")
        with ui.row().classes("w-full items-center justify-between gap-3"):
            secondary_button("戻る", lambda: ui.navigate.to("/"))
            with ui.row().classes("justify-end"):
                primary_button("この名前で作成する", submit)
