from __future__ import annotations

from belle.local_ui.services import uploads
from belle.local_ui.state import get_state
from belle.local_ui.theme import card_container, page_shell, primary_button, secondary_button


def build(line_id: str) -> None:
    from nicegui import ui

    state = get_state()
    if not state.selected_client_id:
        with page_shell("手順 3 / 6", "必要なファイルを入れてください", ""):
            ui.label("先にクライアントを選んでください。").classes("text-sm text-red-600")
            secondary_button("戻る", lambda: ui.navigate.to("/"))
        return

    if line_id not in uploads.LINE_PAGE_COPY:
        with page_shell("手順 3 / 6", "必要なファイルを入れてください", ""):
            ui.label("先に処理種類を選んでください。").classes("text-sm text-red-600")
            secondary_button("戻る", lambda: ui.navigate.to("/flow/types"))
        return

    if line_id not in state.selected_lines:
        if state.selected_lines:
            ui.navigate.to(f"/flow/upload/{state.selected_lines[0]}")
        else:
            ui.navigate.to("/flow/types")
        return

    line_copy = uploads.line_copy(line_id)
    slots = uploads.slot_keys_for_line(line_id)
    file_lists: dict[str, object] = {}
    upload_widgets: dict[str, object] = {}
    error_box = {"element": None}

    def refresh_files(slot_key: str) -> None:
        names = uploads.list_slot_files(state.selected_client_id, slot_key)
        state.uploads[slot_key] = names
        list_column = file_lists[slot_key]
        list_column.clear()
        with list_column:
            if names:
                for name in names:
                    ui.label(name).classes("text-sm text-slate-700")
            else:
                ui.label("まだファイルはありません").classes("text-sm text-slate-500")

    def clear_error() -> None:
        if error_box["element"] is not None:
            error_box["element"].set_text("")
            error_box["element"].visible = False

    def show_error(message: str) -> None:
        if error_box["element"] is not None:
            error_box["element"].set_text(message)
            error_box["element"].visible = True

    def handle_upload(slot_key: str, event) -> None:
        clear_error()
        try:
            uploads.save_uploaded_file(
                state.selected_client_id,
                slot_key,
                event.name,
                event.content.read(),
            )
        except ValueError:
            show_error("このファイル形式は使えません。")
            ui.notify("このファイル形式は使えません。", type="warning")
            return
        refresh_files(slot_key)

    def go_back() -> None:
        index = state.selected_lines.index(line_id)
        if index == 0:
            ui.navigate.to("/flow/types")
            return
        ui.navigate.to(f"/flow/upload/{state.selected_lines[index - 1]}")

    def go_next() -> None:
        validation = uploads.validate_line_uploads(state.selected_client_id, line_id)
        if not validation.ok:
            show_error(validation.errors[0])
            ui.notify(validation.errors[0], type="warning")
            return
        clear_error()
        index = state.selected_lines.index(line_id)
        state.current_line_index = index
        if index + 1 < len(state.selected_lines):
            ui.navigate.to(f"/flow/upload/{state.selected_lines[index + 1]}")
            return
        ui.navigate.to("/flow/check")

    with page_shell(
        str(line_copy["step"]),
        str(line_copy["title"]),
        str(line_copy["subtitle"]),
    ):
        if line_copy["extra_note"]:
            ui.label(str(line_copy["extra_note"])).classes("text-sm text-slate-600")
        error_label = ui.label("").classes("text-sm text-red-600")
        error_label.visible = False
        error_box["element"] = error_label
        for slot_key in slots:
            slot_config = uploads.SLOT_CONFIG[slot_key]
            with card_container():
                ui.label(str(slot_config["title"])).classes("text-lg font-semibold")
                ui.label(str(slot_config["description"])).classes("text-sm text-slate-600")
                upload_widgets[slot_key] = ui.upload(
                    label="ここにファイルをドラッグ&ドロップしてください",
                    multiple=bool(slot_config["multiple"]),
                    max_files=None if bool(slot_config["multiple"]) else 1,
                    auto_upload=True,
                    on_upload=lambda event, slot_key=slot_key: handle_upload(slot_key, event),
                ).props("flat bordered").classes("w-full")
                file_lists[slot_key] = ui.column().classes("w-full gap-1")
                refresh_files(slot_key)

                def confirm_clear(slot_key: str = slot_key) -> None:
                    with ui.dialog() as dialog, ui.card().classes("w-full max-w-md"):
                        ui.label("この欄のファイルを空にしますか？").classes("text-lg font-semibold")
                        ui.label("この画面で入れたファイルだけを消します。").classes("text-sm text-slate-600")
                        with ui.row().classes("w-full justify-end gap-2"):
                            secondary_button("やめる", dialog.close)

                            def clear_and_close() -> None:
                                uploads.clear_slot(state.selected_client_id, slot_key)
                                refresh_files(slot_key)
                                upload_widget = upload_widgets.get(slot_key)
                                if upload_widget is not None:
                                    upload_widget.reset()
                                dialog.close()

                            ui.button("空にする", on_click=clear_and_close).props("flat color=negative")
                    dialog.open()

                ui.button("ファイルをリセットする", on_click=confirm_clear).props(
                    "outline color=negative"
                ).classes("w-full sm:w-auto")
        with ui.row().classes("w-full items-center justify-between gap-3"):
            secondary_button("戻る", go_back)
            with ui.row().classes("justify-end"):
                primary_button("次へ", go_next)
