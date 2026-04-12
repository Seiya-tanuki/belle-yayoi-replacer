from __future__ import annotations

from belle.local_ui.services import client_bootstrap
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

BOOKKEEPING_MODE_REQUIRED_MESSAGE = (
    "先に消費税の経理方式を選択してください。選択後に学習データをアップロードできます。"
)


def build() -> None:
    from nicegui import ui

    state = get_state()
    model = {"raw_name": "", "preview": "", "bookkeeping_mode": "", "stdout": "", "error": ""}
    teacher_state = {"value": client_bootstrap.empty_teacher_file_state()}

    def update_preview(value: str) -> None:
        model["raw_name"] = value or ""
        model["preview"] = preview_client_id(model["raw_name"])
        preview_label.set_text(model["preview"] or "-")

    def update_bookkeeping_mode(value: str) -> None:
        model["bookkeeping_mode"] = value or ""

    def render_teacher_preview() -> None:
        preview = teacher_state["value"].preview
        preview_box.clear()
        with preview_box:
            if preview.note:
                ui.label(preview.note).classes("text-sm text-slate-600")
                return
            for section in preview.sections:
                with ui.column().classes("w-full gap-2"):
                    if section.title:
                        ui.label(section.title).classes("text-sm font-semibold text-slate-700")
                    for row in section.rows:
                        with ui.row().classes("w-full items-center justify-between gap-3 rounded-xl bg-slate-50 px-3 py-2"):
                            ui.label(row.category_label).classes("text-sm text-slate-700")
                            ui.label(row.replacement_account).classes("text-sm font-semibold text-slate-900")

    def refresh_teacher_panel() -> None:
        current_state = teacher_state["value"]
        requires_bookkeeping_mode = not model["bookkeeping_mode"]
        teacher_file_label.set_text(current_state.original_basename or "まだ選んでいません")
        teacher_clear_button.visible = bool(current_state.staged_path)
        teacher_error_label.set_text(current_state.error_message)
        teacher_error_label.visible = bool(current_state.error_message)
        teacher_locked_label.set_text(BOOKKEEPING_MODE_REQUIRED_MESSAGE if requires_bookkeeping_mode else "")
        teacher_locked_label.visible = requires_bookkeeping_mode
        if requires_bookkeeping_mode:
            teacher_upload_widget.disable()
        else:
            teacher_upload_widget.enable()
        render_teacher_preview()
        if current_state.submit_blocked:
            submit_button.disable()
        else:
            submit_button.enable()

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
        teacher_state["value"] = client_bootstrap.refresh_teacher_file(
            teacher_state["value"],
            bookkeeping_mode=model["bookkeeping_mode"],
        )
        refresh_teacher_panel()

    def handle_teacher_upload(event) -> None:
        model["error"] = ""
        error_label.set_text("")
        error_label.visible = False
        try:
            teacher_state["value"] = client_bootstrap.stage_teacher_file(
                teacher_state["value"],
                filename=event.name,
                content=event.content.read(),
                bookkeeping_mode=model["bookkeeping_mode"],
            )
        except ValueError as exc:
            teacher_error_label.set_text(str(exc))
            teacher_error_label.visible = True
            ui.notify(str(exc), type="warning")
            return
        refresh_teacher_panel()

    def clear_teacher_file() -> None:
        teacher_state["value"] = client_bootstrap.clear_teacher_file(teacher_state["value"])
        teacher_upload_widget.reset()
        refresh_teacher_panel()

    def go_back() -> None:
        if teacher_state["value"].staged_path is not None:
            clear_teacher_file()
        ui.navigate.to("/")

    def submit() -> None:
        model["error"] = ""
        model["stdout"] = ""
        error_label.set_text("")
        error_label.visible = False
        if not model["bookkeeping_mode"]:
            model["error"] = "帳簿方式を選択してください。"
            error_label.set_text(model["error"])
            error_label.visible = True
            return
        if teacher_state["value"].submit_blocked:
            model["error"] = teacher_state["value"].error_message or "このファイルは使えません。別のファイルにしてください。"
            error_label.set_text(model["error"])
            error_label.visible = True
            return

        result = create_client(
            model["raw_name"],
            model["bookkeeping_mode"],
            teacher_path=teacher_state["value"].staged_path,
        )
        model["stdout"] = result.stdout
        if result.ok:
            teacher_state["value"] = client_bootstrap.cleanup_after_success(teacher_state["value"])
            state.selected_client_id = result.client_id
            ui.notify("クライアントを作成しました。", type="positive")
            ui.navigate.to("/flow/types")
            return

        model["error"] = result.error_message or "クライアントを作成できませんでした。入力内容を確認してください。"
        error_label.set_text(model["error"])
        error_label.visible = True

    with page_shell(
        "手順 1 / 6",
        "新しいクライアントを作ります",
        "入力した名前は、保存用に自動で整えられます。\n消費税の経理方式は必ず事前に弥生会計のデータを確認し、正しい方を選択してください",
    ):
        with ui.card().classes("w-full rounded-2xl border border-slate-200 p-4 gap-2 shadow-sm"):
            ui.label("保存されるクライアント名").classes("text-sm text-slate-500")
            preview_label = ui.label("-").classes("text-lg font-semibold")
        ui.label("登録したいクライアント名").classes("text-sm font-semibold text-slate-700")
        ui.input(
            placeholder="ここにクライアント名を入力してください",
            on_change=lambda e: update_preview(e.value or ""),
        ).props("outlined").classes("w-full")
        ui.label("消費税の経理方式").classes("text-sm font-semibold text-slate-700")
        cards_row = ui.row().classes("w-full items-stretch gap-3 flex-col sm:flex-row")
        refresh_bookkeeping_cards()
        with ui.card().classes("w-full rounded-2xl border border-slate-200 p-4 gap-3 shadow-sm"):
            ui.label("カテゴリと勘定科目の自動設定").classes("text-lg font-semibold")
            ui.label(
                "弥生から出力した学習データをアップロードするだけでカテゴリと置換科目を自動で設定できます。"
            ).classes("text-sm text-slate-600")
            teacher_locked_label = ui.label("").classes("text-sm text-amber-700")
            teacher_locked_label.visible = False
            teacher_upload_widget = ui.upload(
                label="ここにファイルをドラッグ&ドロップしてください",
                multiple=False,
                max_files=1,
                auto_upload=True,
                on_upload=handle_teacher_upload,
            ).props("flat bordered").classes("w-full")
            with ui.card().classes("w-full rounded-2xl border border-slate-200 bg-slate-50 p-4 gap-2 shadow-none"):
                ui.label("選んだファイル").classes("text-sm text-slate-500")
                teacher_file_label = ui.label("まだ選んでいません").classes("text-base font-semibold")
            teacher_error_label = ui.label("").classes("text-sm text-red-600")
            teacher_error_label.visible = False
            with ui.column().classes("w-full gap-3"):
                ui.label("自動で設定されるカテゴリと置換先の勘定科目の一覧").classes("text-sm font-semibold text-slate-700")
                preview_box = ui.column().classes("w-full gap-3")
            teacher_clear_button = ui.button("このファイルを外す", on_click=clear_teacher_file).props(
                "outline color=negative"
            ).classes("w-full sm:w-auto")
            teacher_clear_button.visible = False
        error_label = ui.label("").classes("text-sm text-red-600")
        error_label.visible = False
        with ui.row().classes("w-full items-center justify-between gap-3"):
            secondary_button("戻る", go_back)
            with ui.row().classes("justify-end"):
                submit_button = ui.button("この名前で作成する", on_click=submit).props("unelevated").classes(
                    "w-full sm:w-auto bg-sky-600 text-white"
                )
        refresh_teacher_panel()
