from __future__ import annotations

from belle.local_ui.services.replacer import run_precheck_for_lines, serialize_precheck_results
from belle.local_ui.state import get_state
from belle.local_ui.theme import card_container, page_shell, primary_button, secondary_button


def build() -> None:
    from nicegui import ui

    state = get_state()
    with page_shell("手順 4 / 6", "事前確認", "このまま進めるかを先に確認します。"):
        if not state.selected_client_id:
            ui.label("先にクライアントを選んでください。").classes("text-sm text-red-600")
            secondary_button("ファイルを直す", lambda: ui.navigate.to("/"))
            return
        if not state.selected_lines:
            ui.label("先に処理種類を選んでください。").classes("text-sm text-red-600")
            secondary_button("ファイルを直す", lambda: ui.navigate.to("/flow/types"))
            return

        results = run_precheck_for_lines(state.selected_client_id, state.selected_lines)
        state.precheck_results = serialize_precheck_results(results)
        has_fail = any(result.status == "FAIL" or result.returncode != 0 for result in results)

        for result in results:
            with card_container():
                ui.label(result.line_id).classes("text-sm text-slate-500")
                ui.label(result.status_label).classes("text-lg font-semibold")
                ui.label(result.reason).classes("text-sm text-slate-600")
                with ui.expansion("詳細ログを見る", value=False).classes("w-full"):
                    ui.markdown(f"```\n{result.stdout or result.stderr or 'ログはありません。'}\n```")

        if has_fail:
            ui.button("置換を実行する").props("unelevated color=primary").classes("w-full sm:w-auto").disable()
        else:
            primary_button("置換を実行する", lambda: ui.navigate.to("/flow/run"))
        secondary_button(
            "ファイルを直す",
            lambda: ui.navigate.to(f"/flow/upload/{state.selected_lines[-1]}"),
        )
