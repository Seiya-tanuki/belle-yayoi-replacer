from __future__ import annotations

from collections.abc import Mapping

from belle.local_ui.services.replacer import SESSION_FATAL_DETAIL_TEXT
from belle.ui_reason_codes import (
    RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED,
    RUN_NEEDS_REVIEW_CARD_CANONICAL_PAYABLE_FAILED,
    RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED,
    SESSION_FATAL_APPLICATION_CALL_FAILED,
)


def markdown_code_block(text: str) -> str:
    return f"```text\n{text}\n```"


def notebooklm_prompt_for_reason_code(
    ui_reason_code: str,
    *,
    session_fatal: bool = False,
) -> str:
    normalized = str(ui_reason_code or "").strip() or "RUN_FAIL_UNKNOWN"
    if session_fatal:
        return f"{normalized} が発生しました。再起動手順と、管理者に伝える内容を教えてください。"
    return f"{normalized} が発生しました。原因と対処法を教えてください。"


def _query_prompt_markdown(prompt: str) -> str:
    return (
        "操作マニュアル（NotebookLM）に以下のメッセージをそのまま貼り付ければ詳細な原因が確認できます:\n"
        f"{markdown_code_block(prompt)}"
    )


def _result_get(result: Mapping[str, object], key: str) -> str:
    return str(result.get(key) or "").strip()


def detail_markdown_for_run_result(result: Mapping[str, object]) -> str:
    ui_reason_code = _result_get(result, "ui_reason_code")
    if ui_reason_code == SESSION_FATAL_APPLICATION_CALL_FAILED:
        return (
            "注意事項:\n"
            f"{SESSION_FATAL_DETAIL_TEXT}\n\n"
            "再起動後も同じ場合は、"
            f"{_query_prompt_markdown(notebooklm_prompt_for_reason_code(ui_reason_code, session_fatal=True))}"
        )
    if ui_reason_code == RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED:
        return (
            "注意事項:\n"
            "カードを推定する十分な根拠が得られず、補助科目が置換されませんでした。\n\n"
            f"{_query_prompt_markdown(notebooklm_prompt_for_reason_code(ui_reason_code))}"
        )
    if ui_reason_code == RUN_NEEDS_REVIEW_CARD_CANONICAL_PAYABLE_FAILED:
        return (
            "注意事項:\n"
            "貸借の未払側は見つかりましたが、最終出力に使う canonical payable account を安全に確定できませんでした。\n\n"
            f"{_query_prompt_markdown(notebooklm_prompt_for_reason_code(ui_reason_code))}"
        )
    if ui_reason_code == RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED:
        return (
            "注意事項:\n"
            "銀行を推定する十分な根拠が得られず、補助科目が置換されませんでした。\n\n"
            f"{_query_prompt_markdown(notebooklm_prompt_for_reason_code(ui_reason_code))}"
        )
    if _result_get(result, "status") == "failure":
        return (
            "注意事項:\n"
            "処理時にエラーが発生しました。\n\n"
            f"{_query_prompt_markdown(notebooklm_prompt_for_reason_code(ui_reason_code))}"
        )

    detail_text = _result_get(result, "stdout") or _result_get(result, "stderr") or "ログはありません。"
    return f"```\n{detail_text}\n```"


def detail_markdown_for_collect_result(result: Mapping[str, object]) -> str:
    ui_reason_code = _result_get(result, "ui_reason_code") or "COLLECT_FAIL_UNKNOWN"
    return (
        "注意事項:\n"
        "成果物ZIPを作成できませんでした。\n\n"
        f"{_query_prompt_markdown(notebooklm_prompt_for_reason_code(ui_reason_code))}"
    )


def detail_markdown_for_precheck_result(result: Mapping[str, object]) -> str:
    ui_reason_code = _result_get(result, "ui_reason_code")
    if ui_reason_code == SESSION_FATAL_APPLICATION_CALL_FAILED:
        return (
            "注意事項:\n"
            f"{SESSION_FATAL_DETAIL_TEXT}\n\n"
            "再起動後も同じ場合は、"
            f"{_query_prompt_markdown(notebooklm_prompt_for_reason_code(ui_reason_code, session_fatal=True))}"
        )

    return (
        "注意事項:\n"
        "このままでは進めません。入力ファイルや設定内容を確認してください。\n\n"
        f"{_query_prompt_markdown(notebooklm_prompt_for_reason_code(ui_reason_code or 'PRECHECK_FAIL_UNKNOWN'))}"
    )
