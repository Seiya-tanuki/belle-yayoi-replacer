from __future__ import annotations

import unittest
from pathlib import Path


class LocalUiDonePageTests(unittest.TestCase):
    def test_markdown_code_block_wraps_text(self) -> None:
        from belle.local_ui.pages.done import markdown_code_block

        self.assertEqual("```text\nABC_DEF\n```", markdown_code_block("ABC_DEF"))

    def test_needs_review_section_suffix_constant(self) -> None:
        from belle.local_ui.pages.done import NEEDS_REVIEW_SECTION_SUFFIX

        self.assertEqual("（詳細を見るボタンをクリック）", NEEDS_REVIEW_SECTION_SUFFIX)

    def test_requested_run_refs_for_results_uses_nonempty_run_ids(self) -> None:
        from belle.local_ui.pages.done import requested_run_refs_for_results

        refs = requested_run_refs_for_results(
            "C1",
            [
                {"run_id": "RID1"},
                {"run_id": ""},
                {"run_id": "RID2"},
                {"run_id": "RID1"},
            ],
        )
        self.assertEqual(["C1:RID1", "C1:RID2"], refs)

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

    def test_detail_markdown_for_collect_result_uses_fixed_error_message(self) -> None:
        from belle.local_ui.pages.done import detail_markdown_for_collect_result

        text = detail_markdown_for_collect_result({"ui_reason_code": "COLLECT_FAIL_UNKNOWN"})
        self.assertIn("予期しないエラーが発生しました。システム管理者に問い合わせてください。", text)
        self.assertIn("発生したエラー:", text)
        self.assertIn("```text\nCOLLECT_FAIL_UNKNOWN\n```", text)

    def test_detail_markdown_for_session_fatal_uses_restart_guidance(self) -> None:
        from belle.local_ui.pages.done import detail_markdown_for_result

        text = detail_markdown_for_result(
            {"status": "failure", "ui_reason_code": "SESSION_FATAL_SUBPROCESS_OUTPUT_INVALID"}
        )
        self.assertIn("想定外の問題が発生したため、今回の処理は完了できませんでした。", text)
        self.assertIn(
            "コマンドプロンプト（システム起動時に表示されるテキストだけの黒い画面）を右上のバツボタンを押して終了してください。",
            text,
        )
        self.assertIn("さらにこのブラウザも終了し、その後改めてデスクトップからシステムを起動してください。", text)
        self.assertIn("```text\nSESSION_FATAL_SUBPROCESS_OUTPUT_INVALID\n```", text)


if __name__ == "__main__":
    unittest.main()
