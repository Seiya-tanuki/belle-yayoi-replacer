from __future__ import annotations

import unittest


class LocalUiDetailMessagesTests(unittest.TestCase):
    def test_precheck_detail_uses_notebooklm_prompt(self) -> None:
        from belle.local_ui.services.detail_messages import detail_markdown_for_precheck_result

        text = detail_markdown_for_precheck_result(
            {
                "ui_reason_code": "PRECHECK_FAIL_BANK_TRAINING_PAIR_INCOMPLETE",
                "status": "FAIL",
            }
        )
        self.assertIn("このままでは進めません。入力ファイルや設定内容を確認してください。", text)
        self.assertIn(
            "PRECHECK_FAIL_BANK_TRAINING_PAIR_INCOMPLETE が発生しました。原因と対処法を教えてください。",
            text,
        )

    def test_precheck_session_fatal_uses_restart_guidance_and_notebooklm_prompt(self) -> None:
        from belle.local_ui.services.detail_messages import detail_markdown_for_precheck_result

        text = detail_markdown_for_precheck_result(
            {
                "ui_reason_code": "SESSION_FATAL_APPLICATION_CALL_FAILED",
                "status": "FAIL",
            }
        )
        self.assertIn("想定外の問題が発生したため、今回の処理は完了できませんでした。", text)
        self.assertIn(
            "SESSION_FATAL_APPLICATION_CALL_FAILED が発生しました。再起動手順と、管理者に伝える内容を教えてください。",
            text,
        )

    def test_collect_detail_uses_notebooklm_prompt(self) -> None:
        from belle.local_ui.services.detail_messages import detail_markdown_for_collect_result

        text = detail_markdown_for_collect_result({"ui_reason_code": "COLLECT_FAIL_UNKNOWN"})
        self.assertIn("成果物ZIPを作成できませんでした。", text)
        self.assertIn("COLLECT_FAIL_UNKNOWN が発生しました。原因と対処法を教えてください。", text)


if __name__ == "__main__":
    unittest.main()
