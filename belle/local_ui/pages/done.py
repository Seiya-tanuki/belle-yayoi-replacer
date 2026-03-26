from __future__ import annotations

from pathlib import Path

from belle.local_ui.services.collect import overall_result_title, run_collect, serialize_collect_result
from belle.local_ui.state import get_state, line_label, reset_state
from belle.local_ui.theme import page_shell, primary_button, secondary_button


def build() -> None:
    from nicegui import ui

    state = get_state()
    title = overall_result_title(state.run_results)

    with page_shell("手順 6 / 6", title, "今回の結果を確認し、必要なら成果物ZIPを作ります。"):
        collect_message = ui.label("").classes("text-sm")
        collect_message.visible = False

        def update_collect_message() -> None:
            result = state.collect_result
            if not result:
                collect_message.visible = False
                return
            collect_message.set_text(str(result.get("message") or ""))
            status = str(result.get("status") or "")
            if status == "success":
                collect_message.classes(replace="text-sm text-green-700")
            elif status == "warning":
                collect_message.classes(replace="text-sm text-amber-700")
            else:
                collect_message.classes(replace="text-sm text-red-700")
            collect_message.visible = True

        for result in state.run_results:
            with ui.card().classes("w-full rounded-2xl border border-slate-200 p-4 gap-2 shadow-sm"):
                ui.label(line_label(str(result.get("line_id") or ""))).classes("text-sm text-slate-500")
                ui.label(str(result.get("status_label") or "")).classes("text-lg font-semibold")
                with ui.expansion("詳細ログを見る", value=False).classes("w-full"):
                    ui.markdown(f"```\n{result.get('stdout') or result.get('stderr') or 'ログはありません。'}\n```")
        if state.collect_result:
            with ui.expansion("詳細ログを見る", value=False).classes("w-full"):
                ui.markdown(
                    "```\n"
                    f"{state.collect_result.get('stdout') or state.collect_result.get('stderr') or 'ログはありません。'}\n"
                    "```"
                )
        update_collect_message()

        def collect_zip() -> None:
            result = run_collect(
                client_id=state.selected_client_id,
                run_results=state.run_results,
                session_started_at_utc=state.session_started_at_utc,
                session_finished_at_utc=state.session_finished_at_utc,
            )
            state.collect_result = serialize_collect_result(result)
            update_collect_message()

        def download_zip() -> None:
            zip_path = state.collect_result.get("zip_path") if state.collect_result else ""
            if zip_path:
                ui.download(Path(str(zip_path)))

        if state.collect_result and state.collect_result.get("zip_path"):
            primary_button("成果物ZIPをダウンロード", download_zip)
        else:
            primary_button("成果物ZIPを作る", collect_zip)
        secondary_button("最初に戻る", lambda: (reset_state(), ui.navigate.to("/")))
