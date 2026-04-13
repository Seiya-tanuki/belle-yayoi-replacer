from __future__ import annotations

import unittest

from belle.local_ui.pages.client_new import (
    NO_SELECTED_BOOTSTRAP_ROWS_MESSAGE,
    build_confirmation_summary,
)
from belle.local_ui.services.client_bootstrap import (
    ClientBootstrapPreview,
    ClientBootstrapPreviewRow,
    ClientBootstrapPreviewSection,
)


class LocalUiClientNewConfirmationTests(unittest.TestCase):
    def test_build_confirmation_summary_keeps_only_checked_rows(self) -> None:
        food_row = ClientBootstrapPreviewRow(
            line_ids=("receipt",),
            category_key="food",
            category_label="飲食",
            replacement_account="交際費",
        )
        travel_row = ClientBootstrapPreviewRow(
            line_ids=("credit_card_statement",),
            category_key="travel",
            category_label="交通",
            replacement_account="旅費交通費",
        )
        preview = ClientBootstrapPreview(
            sections=(
                ClientBootstrapPreviewSection(title="領収書", rows=(food_row,)),
                ClientBootstrapPreviewSection(title="クレジットカード", rows=(travel_row,)),
            )
        )

        summary = build_confirmation_summary(
            client_directory_name="ABC",
            bookkeeping_mode="tax_excluded",
            preview=preview,
            selected_row_keys={(("receipt",), "food")},
        )

        self.assertEqual("ABC", summary.client_directory_name)
        self.assertEqual("税抜経理", summary.bookkeeping_mode_label)
        self.assertEqual(1, len(summary.sections))
        self.assertEqual("領収書", summary.sections[0].title)
        self.assertEqual(("飲食",), tuple(row.category_label for row in summary.sections[0].rows))
        self.assertEqual("", summary.note)

    def test_build_confirmation_summary_keeps_shared_line_title_blank_when_preview_section_has_no_title(self) -> None:
        shared_row = ClientBootstrapPreviewRow(
            line_ids=("receipt", "credit_card_statement"),
            category_key="food",
            category_label="飲食",
            replacement_account="交際費",
        )
        preview = ClientBootstrapPreview(
            sections=(
                ClientBootstrapPreviewSection(title="", rows=(shared_row,)),
            )
        )

        summary = build_confirmation_summary(
            client_directory_name="ABC",
            bookkeeping_mode="tax_included",
            preview=preview,
            selected_row_keys={(("receipt", "credit_card_statement"), "food")},
        )

        self.assertEqual("税込経理", summary.bookkeeping_mode_label)
        self.assertEqual(1, len(summary.sections))
        self.assertEqual("", summary.sections[0].title)
        self.assertEqual("交際費", summary.sections[0].rows[0].replacement_account)

    def test_build_confirmation_summary_uses_empty_state_message_when_no_rows_selected(self) -> None:
        preview = ClientBootstrapPreview(
            sections=(
                ClientBootstrapPreviewSection(
                    title="領収書",
                    rows=(
                        ClientBootstrapPreviewRow(
                            line_ids=("receipt",),
                            category_key="food",
                            category_label="飲食",
                            replacement_account="交際費",
                        ),
                    ),
                ),
            )
        )

        summary = build_confirmation_summary(
            client_directory_name="",
            bookkeeping_mode="",
            preview=preview,
            selected_row_keys=set(),
        )

        self.assertEqual("-", summary.client_directory_name)
        self.assertEqual("-", summary.bookkeeping_mode_label)
        self.assertEqual(0, len(summary.sections))
        self.assertEqual(NO_SELECTED_BOOTSTRAP_ROWS_MESSAGE, summary.note)


if __name__ == "__main__":
    unittest.main()
