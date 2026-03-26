from __future__ import annotations

from belle.local_ui.services.replacer import run_selected_lines, serialize_run_results, utc_now_iso
from belle.local_ui.state import get_state
from belle.local_ui.theme import page_shell


def build() -> None:
    from nicegui import ui

    state = get_state()
    with page_shell("手順 5 / 6", "処理を実行しています", "画面を閉じずに、そのままお待ちください。"):
        ui.label("現在の処理").classes("text-sm text-slate-500")
        progress_label = ui.label("開始しています...").classes("text-lg font-semibold")

        if not state.selected_client_id or not state.selected_lines:
            progress_label.set_text("先にクライアントと処理種類を選んでください。")
            return

        def execute() -> None:
            state.session_started_at_utc = utc_now_iso()
            progress_label.set_text("置換を実行しています")
            results = run_selected_lines(state.selected_client_id, state.selected_lines)
            state.run_results = serialize_run_results(results)
            state.session_finished_at_utc = utc_now_iso()
            ui.navigate.to("/flow/done")

        ui.timer(0.1, execute, once=True)
