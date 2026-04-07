from __future__ import annotations

from pathlib import Path

from belle.local_ui.services.collect import overall_result_title, run_collect, serialize_collect_result
from belle.local_ui.services.replacer import SESSION_FATAL_DETAIL_TEXT
from belle.local_ui.state import get_state, line_label, reset_state
from belle.local_ui.theme import page_shell, primary_button, secondary_button
from belle.ui_reason_codes import (
    RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED,
    RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED,
    SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID,
)

NEEDS_REVIEW_SECTION_SUFFIX = "（詳細を見るボタンをクリック）"


def collect_zip_path(collect_result: dict[str, object] | None) -> Path | None:
    zip_path = str((collect_result or {}).get("zip_path") or "").strip()
    if not zip_path:
        return None
    return Path(zip_path)


def requested_run_refs_for_results(client_id: str, run_results: list[dict[str, object]]) -> list[str]:
    refs: list[str] = []
    seen = set()
    for result in run_results:
        run_id = str(result.get("run_id") or "").strip()
        if not run_id:
            continue
        run_ref = f"{client_id}:{run_id}"
        if run_ref in seen:
            continue
        seen.add(run_ref)
        refs.append(run_ref)
    return refs


def markdown_code_block(text: str) -> str:
    return f"```text\n{text}\n```"


def detail_markdown_for_result(result: dict[str, object]) -> str:
    ui_reason_code = str(result.get("ui_reason_code") or "").strip()
    if ui_reason_code == SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID:
        return (
            "注意事項:\n"
            f"{SESSION_FATAL_DETAIL_TEXT}\n\n"
            "発生したエラー:\n"
            f"{markdown_code_block(ui_reason_code)}"
        )
    if ui_reason_code == RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED:
        return (
            "注意事項:\n"
            "カードを推定する十分な根拠が得られず、補助科目が置換されませんでした。\n\n"
            "操作マニュアル（NotebookLM）に以下のメッセージをそのまま貼り付ければ詳細な原因が確認できます:\n"
            f"{markdown_code_block('RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED が発生しました。原因と対処法を教えてください。')}"
        )
    if ui_reason_code == RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED:
        return (
            "注意事項:\n"
            "銀行を推定する十分な根拠が得られず、補助科目が置換されませんでした。\n\n"
            "操作マニュアル（NotebookLM）に以下のメッセージをそのまま貼り付ければ詳細な原因が確認できます:\n"
            f"{markdown_code_block('RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED が発生しました。原因と対処法を教えてください。')}"
        )
    if str(result.get("status") or "") == "failure":
        prompt = f"{ui_reason_code or 'RUN_FAIL_UNKNOWN'} が発生しました。原因と対処法を教えてください。"
        return (
            "注意事項:\n"
            "処理時にエラーが発生しました。\n\n"
            "操作マニュアル（NotebookLM）に以下のメッセージをそのまま貼り付ければ詳細な原因が確認できます:\n"
            f"{markdown_code_block(prompt)}"
        )

    detail_text = str(result.get("stdout") or result.get("stderr") or "ログはありません。")
    return f"```\n{detail_text}\n```"


def detail_markdown_for_collect_result(result: dict[str, object]) -> str:
    ui_reason_code = str(result.get("ui_reason_code") or "").strip() or "COLLECT_FAIL_UNKNOWN"
    return (
        "予期しないエラーが発生しました。システム管理者に問い合わせてください。\n\n"
        "発生したエラー:\n"
        f"{markdown_code_block(ui_reason_code)}"
    )


def build() -> None:
    from nicegui import ui

    state = get_state()
    title = overall_result_title(state.run_results)

    with page_shell("手順 6 / 6", title, "今回の結果を確認し、必要なら成果物ZIPを作ります。"):
        has_session_fatal = bool(state.session_fatal)
        collect_message = ui.label("").classes("text-sm")
        collect_message.visible = False
        collect_download_hint = ui.label(
            "zipは自動でダウンロードされます。開始されない場合は右下の「成果物ZIPをダウンロード」ボタンをクリックしてください"
        ).classes("text-sm text-slate-600 whitespace-pre-line")
        collect_download_hint.visible = False

        def update_collect_message() -> None:
            result = state.collect_result
            if has_session_fatal or not result:
                collect_message.visible = False
                collect_download_hint.visible = False
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
            collect_download_hint.visible = collect_zip_path(result) is not None

        for result in state.run_results:
            with ui.card().classes("w-full rounded-2xl border border-slate-200 p-4 gap-2 shadow-sm"):
                ui.label(line_label(str(result.get("line_id") or ""))).classes("text-sm text-slate-500")
                section_status_label = str(result.get("status_label") or "")
                if str(result.get("status") or "") == "needs_review":
                    section_status_label += NEEDS_REVIEW_SECTION_SUFFIX
                ui.label(section_status_label).classes("text-lg font-semibold")
                if str(result.get("status") or "") != "success":
                    with ui.expansion("詳細を見る", value=False).classes("w-full"):
                        ui.markdown(detail_markdown_for_result(result))
        if state.collect_result and str(state.collect_result.get("status") or "") == "error":
            with ui.expansion("詳細を見る", value=False).classes("w-full"):
                ui.markdown(detail_markdown_for_collect_result(state.collect_result))
        update_collect_message()

        def collect_zip(
            requested_run_refs: list[str] | None = None,
            *,
            collect_today_all: bool = False,
            collect_today_all_clients: bool = False,
        ) -> None:
            result = run_collect(
                client_id=state.selected_client_id,
                run_results=state.run_results,
                session_started_at_utc=state.session_started_at_utc,
                session_finished_at_utc=state.session_finished_at_utc,
                requested_run_refs=requested_run_refs,
                collect_today_all=collect_today_all,
                collect_today_all_clients=collect_today_all_clients,
            )
            state.collect_result = serialize_collect_result(result)
            update_collect_message()
            zip_path = collect_zip_path(state.collect_result)
            if zip_path is not None:
                ui.download(zip_path)
            render_action_buttons.refresh()

        def open_collect_dialog() -> None:
            current_run_refs = requested_run_refs_for_results(state.selected_client_id, state.run_results)
            with ui.dialog() as dialog, ui.card().classes("w-full max-w-lg gap-4"):
                ui.label("zipを作成する対象を選択してください").classes("text-lg font-semibold")
                with ui.row().classes("w-full justify-end gap-2 flex-wrap"):
                    primary_button("今回の分のみ", lambda: (dialog.close(), collect_zip(current_run_refs)))
                    secondary_button(
                        "この顧客の今日の分",
                        lambda: (dialog.close(), collect_zip(collect_today_all=True)),
                    )
                    secondary_button(
                        "全顧客の今日の分",
                        lambda: (dialog.close(), collect_zip(collect_today_all_clients=True)),
                    )
            dialog.open()

        def download_zip() -> None:
            zip_path = collect_zip_path(state.collect_result)
            if zip_path is not None:
                ui.download(zip_path)

        @ui.refreshable
        def render_action_buttons() -> None:
            with ui.row().classes("w-full items-center justify-between gap-3"):
                secondary_button("最初に戻る", lambda: (reset_state(), ui.navigate.to("/")))
                with ui.row().classes("justify-end"):
                    if has_session_fatal:
                        return
                    if collect_zip_path(state.collect_result) is not None:
                        primary_button("成果物ZIPをダウンロード", download_zip)
                    else:
                        primary_button("成果物ZIPを作る", open_collect_dialog)

        render_action_buttons()
