from __future__ import annotations

import unittest
from pathlib import Path


class LocalUiDonePageTests(unittest.TestCase):
    def test_collect_zip_path_returns_none_when_missing(self) -> None:
        from belle.local_ui.pages.done import collect_zip_path

        self.assertIsNone(collect_zip_path({}))
        self.assertIsNone(collect_zip_path({"zip_path": ""}))
        self.assertIsNone(collect_zip_path(None))

    def test_collect_zip_path_returns_path_when_present(self) -> None:
        from belle.local_ui.pages.done import collect_zip_path

        result = collect_zip_path({"zip_path": "C:/tmp/result.zip"})
        self.assertEqual(Path("C:/tmp/result.zip"), result)

    def test_detail_markdown_for_card_needs_review_uses_notebooklm_message(self) -> None:
        from belle.local_ui.pages.done import detail_markdown_for_result

        text = detail_markdown_for_result(
            {"ui_reason_code": "RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED"}
        )
        self.assertIn("カードを推定する十分な根拠が得られず、補助科目が置換されませんでした。", text)
        self.assertIn(
            "操作マニュアル（NotebookLM）に以下のメッセージをそのまま貼り付ければ詳細な原因が確認できます:",
            text,
        )
        self.assertIn(
            "RUN_NEEDS_REVIEW_CARD_SUBACCOUNT_INFERENCE_FAILED が発生しました。原因と対処法を教えてください。",
            text,
        )

    def test_detail_markdown_for_bank_needs_review_uses_notebooklm_message(self) -> None:
        from belle.local_ui.pages.done import detail_markdown_for_result

        text = detail_markdown_for_result(
            {"ui_reason_code": "RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED"}
        )
        self.assertIn("銀行を推定する十分な根拠が得られず、補助科目が置換されませんでした。", text)
        self.assertIn(
            "操作マニュアル（NotebookLM）に以下のメッセージをそのまま貼り付ければ詳細な原因が確認できます:",
            text,
        )
        self.assertIn(
            "RUN_NEEDS_REVIEW_BANK_SUBACCOUNT_INFERENCE_FAILED が発生しました。原因と対処法を教えてください。",
            text,
        )

    def test_detail_markdown_for_other_results_falls_back_to_logs(self) -> None:
        from belle.local_ui.pages.done import detail_markdown_for_result

        text = detail_markdown_for_result({"ui_reason_code": "RUN_OK", "stdout": "raw log"})
        self.assertEqual("```\nraw log\n```", text)

    def test_detail_markdown_for_failure_uses_notebooklm_message(self) -> None:
        from belle.local_ui.pages.done import detail_markdown_for_result

        text = detail_markdown_for_result(
            {"status": "failure", "ui_reason_code": "RUN_FAIL_CARD_CONFIG_MISSING"}
        )
        self.assertIn("処理時にエラーが発生しました。", text)
        self.assertIn(
            "操作マニュアル（NotebookLM）に以下のメッセージをそのまま貼り付ければ詳細な原因が確認できます:",
            text,
        )
        self.assertIn(
            "RUN_FAIL_CARD_CONFIG_MISSING が発生しました。原因と対処法を教えてください。",
            text,
        )


if __name__ == "__main__":
    unittest.main()
