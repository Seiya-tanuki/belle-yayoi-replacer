from __future__ import annotations

from dataclasses import dataclass

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

NO_SELECTED_BOOTSTRAP_ROWS_MESSAGE = "登録される自動設定カテゴリはありません。"


@dataclass(frozen=True)
class ClientRegistrationConfirmationSection:
    title: str
    rows: tuple[client_bootstrap.ClientBootstrapPreviewRow, ...]


@dataclass(frozen=True)
class ClientRegistrationConfirmationSummary:
    client_directory_name: str
    bookkeeping_mode_label: str
    sections: tuple[ClientRegistrationConfirmationSection, ...]
    note: str = ""


def _bookkeeping_mode_label(value: str) -> str:
    return BOOKKEEPING_MODE_CHOICES.get(value, ("", ""))[0]


def _line_ids_label(line_ids: tuple[str, ...]) -> str:
    labels = [client_bootstrap.PREVIEW_LINE_LABELS.get(line_id, line_id) for line_id in line_ids]
    return " / ".join(labels)


def build_confirmation_summary(
    *,
    client_directory_name: str,
    bookkeeping_mode: str,
    preview: client_bootstrap.ClientBootstrapPreview,
    selected_row_keys: set[tuple[tuple[str, ...], str]],
) -> ClientRegistrationConfirmationSummary:
    sections: list[ClientRegistrationConfirmationSection] = []
    for section in preview.sections:
        selected_rows = tuple(
            row for row in section.rows if ((row.line_ids, row.category_key) in selected_row_keys)
        )
        if not selected_rows:
            continue
        title = section.title
        if not title and len(selected_rows) == 1 and len(selected_rows[0].line_ids) == 1:
            title = _line_ids_label(selected_rows[0].line_ids)
        sections.append(ClientRegistrationConfirmationSection(title=title, rows=selected_rows))

    note = preview.note
    if not sections:
        note = note or NO_SELECTED_BOOTSTRAP_ROWS_MESSAGE

    return ClientRegistrationConfirmationSummary(
        client_directory_name=client_directory_name or "-",
        bookkeeping_mode_label=_bookkeeping_mode_label(bookkeeping_mode) or "-",
        sections=tuple(sections),
        note=note,
    )


def build() -> None:
    from nicegui import ui

    state = get_state()
    model = {"raw_name": "", "preview": "", "bookkeeping_mode": "", "stdout": "", "error": ""}
    teacher_state = {"value": client_bootstrap.empty_teacher_file_state()}
    selected_preview_rows = {"value": set()}
    registration_state = {"submitting": False}
    confirmation_summary = {
        "value": build_confirmation_summary(
            client_directory_name="",
            bookkeeping_mode="",
            preview=client_bootstrap.ClientBootstrapPreview(),
            selected_row_keys=set(),
        )
    }

    def update_preview(value: str) -> None:
        model["raw_name"] = value or ""
        model["preview"] = preview_client_id(model["raw_name"])

    def update_bookkeeping_mode(value: str) -> None:
        model["bookkeeping_mode"] = value or ""

    def _preview_row_key(row: client_bootstrap.ClientBootstrapPreviewRow) -> tuple[tuple[str, ...], str]:
        return (row.line_ids, row.category_key)

    def reset_preview_selection() -> None:
        selected_preview_rows["value"] = {
            _preview_row_key(row)
            for section in teacher_state["value"].preview.sections
            for row in section.rows
        }

    def selected_bootstrap_categories() -> dict[str, list[str]]:
        selected_by_line: dict[str, set[str]] = {}
        for section in teacher_state["value"].preview.sections:
            for row in section.rows:
                if _preview_row_key(row) not in selected_preview_rows["value"]:
                    continue
                for line_id in row.line_ids:
                    selected_by_line.setdefault(line_id, set()).add(row.category_key)
        return {
            line_id: sorted(category_keys)
            for line_id, category_keys in selected_by_line.items()
        }

    def set_preview_row_selected(row: client_bootstrap.ClientBootstrapPreviewRow, selected: bool) -> None:
        next_selected = set(selected_preview_rows["value"])
        row_key = _preview_row_key(row)
        if selected:
            next_selected.add(row_key)
        else:
            next_selected.discard(row_key)
        selected_preview_rows["value"] = next_selected
        render_teacher_preview()

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
                        row_selected = _preview_row_key(row) in selected_preview_rows["value"]
                        with ui.row().classes(
                            "w-full items-center justify-between gap-3 rounded-xl bg-slate-50 px-3 py-2"
                        ):
                            ui.checkbox(
                                value=row_selected,
                                on_change=lambda e, row=row: set_preview_row_selected(row, bool(e.value)),
                            ).props("dense")
                            with ui.row().classes("min-w-0 flex-1 items-center justify-between gap-3"):
                                ui.label(row.category_label).classes("text-sm text-slate-700")
                                ui.label(row.replacement_account).classes("text-sm font-semibold text-slate-900")

    def refresh_teacher_panel() -> None:
        current_state = teacher_state["value"]
        requires_bookkeeping_mode = not model["bookkeeping_mode"]
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
        reset_preview_selection()
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
        reset_preview_selection()
        refresh_teacher_panel()

    def clear_teacher_file() -> None:
        teacher_state["value"] = client_bootstrap.clear_teacher_file(teacher_state["value"])
        selected_preview_rows["value"] = set()
        teacher_upload_widget.reset()
        refresh_teacher_panel()

    def set_registration_submitting(submitting: bool) -> None:
        registration_state["submitting"] = submitting
        if submitting:
            submit_button.disable()
            confirm_submit_button.disable()
            close_dialog_button.disable()
        else:
            refresh_teacher_panel()
            confirm_submit_button.enable()
            close_dialog_button.enable()

    def validate_before_confirmation() -> bool:
        model["error"] = ""
        model["stdout"] = ""
        error_label.set_text("")
        error_label.visible = False
        if not model["preview"]:
            model["error"] = "クライアント名を入力してください。"
            error_label.set_text(model["error"])
            error_label.visible = True
            return False
        if not model["bookkeeping_mode"]:
            model["error"] = "帳簿方式を選択してください。"
            error_label.set_text(model["error"])
            error_label.visible = True
            return False
        if teacher_state["value"].submit_blocked:
            model["error"] = teacher_state["value"].error_message or "このファイルは使えません。別のファイルにしてください。"
            error_label.set_text(model["error"])
            error_label.visible = True
            return False
        return True

    def update_confirmation_summary() -> None:
        confirmation_summary["value"] = build_confirmation_summary(
            client_directory_name=model["preview"],
            bookkeeping_mode=model["bookkeeping_mode"],
            preview=teacher_state["value"].preview,
            selected_row_keys=set(selected_preview_rows["value"]),
        )

    def open_confirmation_dialog() -> None:
        if not validate_before_confirmation():
            return
        update_confirmation_summary()
        render_confirmation_preview.refresh()
        confirmation_dialog.open()

    def confirm_submit() -> None:
        if registration_state["submitting"]:
            return
        if not validate_before_confirmation():
            return

        result = None
        set_registration_submitting(True)
        try:
            result = create_client(
                model["raw_name"],
                model["bookkeeping_mode"],
                teacher_path=teacher_state["value"].staged_path,
                selected_bootstrap_categories=(
                    selected_bootstrap_categories() if teacher_state["value"].staged_path is not None else None
                ),
            )
        finally:
            model["stdout"] = result.stdout if result is not None else ""
            set_registration_submitting(False)

        if result is None:
            model["error"] = "クライアントを作成できませんでした。入力内容を確認してください。"
            error_label.set_text(model["error"])
            error_label.visible = True
            return

        if result.ok:
            teacher_state["value"] = client_bootstrap.cleanup_after_success(teacher_state["value"])
            state.selected_client_id = result.client_id
            confirmation_dialog.close()
            ui.notify("クライアントを作成しました。", type="positive")
            ui.navigate.to("/flow/types")
            return

        model["error"] = result.error_message or "クライアントを作成できませんでした。入力内容を確認してください。"
        error_label.set_text(model["error"])
        error_label.visible = True

    def go_back() -> None:
        if teacher_state["value"].staged_path is not None:
            clear_teacher_file()
        ui.navigate.to("/")

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
            teacher_error_label = ui.label("").classes("text-sm text-red-600")
            teacher_error_label.visible = False
            with ui.column().classes("w-full gap-3"):
                ui.label(
                    "自動で設定されるカテゴリと置換先の勘定科目の一覧\n"
                    "チェックマークのついた対象だけが登録されます。登録したくないカテゴリや科目がある場合はチェックを外してください"
                ).classes("text-sm font-semibold text-slate-700 whitespace-pre-line")
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
                submit_button = ui.button("登録内容を確認する", on_click=open_confirmation_dialog).props(
                    "unelevated"
                ).classes(
                    "w-full sm:w-auto bg-sky-600 text-white"
                )
        with ui.dialog() as confirmation_dialog, ui.card().classes("w-full max-w-2xl gap-4"):
            ui.label("登録内容を確認してください").classes("text-lg font-semibold")
            with ui.column().classes("w-full gap-3 rounded-2xl bg-slate-50 p-4"):
                ui.label("登録されるクライアント名").classes("text-sm text-slate-500")
                confirmation_client_name = ui.label("-").classes("text-base font-semibold text-slate-900")
            with ui.column().classes("w-full gap-3 rounded-2xl bg-slate-50 p-4"):
                ui.label("選択された経理方式").classes("text-sm text-slate-500")
                confirmation_bookkeeping_mode = ui.label("-").classes("text-base font-semibold text-slate-900")
            with ui.column().classes("w-full gap-3 rounded-2xl bg-slate-50 p-4"):
                ui.label("自動設定されるカテゴリと置換先の勘定科目").classes("text-sm text-slate-500")
                confirmation_preview_box = ui.column().classes("w-full gap-3")
            with ui.row().classes("w-full justify-end gap-2 flex-wrap"):
                close_dialog_button = ui.button("戻って修正する", on_click=confirmation_dialog.close).props(
                    "outline"
                ).classes("w-full sm:w-auto border-sky-600 text-sky-600")
                confirm_submit_button = ui.button("この内容で登録する", on_click=confirm_submit).props(
                    "unelevated"
                ).classes("w-full sm:w-auto bg-sky-600 text-white")

        @ui.refreshable
        def render_confirmation_preview() -> None:
            summary = confirmation_summary["value"]
            confirmation_client_name.set_text(summary.client_directory_name)
            confirmation_bookkeeping_mode.set_text(summary.bookkeeping_mode_label)
            confirmation_preview_box.clear()
            with confirmation_preview_box:
                if summary.note:
                    ui.label(summary.note).classes("text-sm text-slate-600")
                    return
                for section in summary.sections:
                    with ui.column().classes("w-full gap-2"):
                        if section.title:
                            ui.label(section.title).classes("text-sm font-semibold text-slate-700")
                        for row in section.rows:
                            with ui.row().classes(
                                "w-full items-center justify-between gap-3 rounded-xl border border-slate-200 bg-white px-3 py-2"
                            ):
                                ui.label(row.category_label).classes("text-sm text-slate-700")
                                ui.label(row.replacement_account).classes("text-sm font-semibold text-slate-900")

        refresh_teacher_panel()
        render_confirmation_preview()
