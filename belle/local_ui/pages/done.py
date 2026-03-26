from __future__ import annotations

from belle.local_ui.state import get_state, reset_state
from belle.local_ui.theme import page_shell, secondary_button


def build() -> None:
    from nicegui import ui

    state = get_state()
    statuses = {result.get("status") for result in state.run_results}
    if "failure" in statuses:
        title = "処理を完了できませんでした"
    elif "needs_review" in statuses:
        title = "処理は完了しましたが、確認が必要です"
    else:
        title = "処理が完了しました"

    with page_shell("手順 6 / 6", title, "Phase 5 で成果物収集を実装します。"):
        for result in state.run_results:
            with ui.card().classes("w-full rounded-2xl border border-slate-200 p-4 gap-2 shadow-sm"):
                ui.label(str(result.get("line_id") or "")).classes("text-sm text-slate-500")
                ui.label(str(result.get("status_label") or "")).classes("text-lg font-semibold")
                with ui.expansion("詳細ログを見る", value=False).classes("w-full"):
                    ui.markdown(f"```\n{result.get('stdout') or result.get('stderr') or 'ログはありません。'}\n```")
        secondary_button("最初に戻る", lambda: (reset_state(), ui.navigate.to("/")))
