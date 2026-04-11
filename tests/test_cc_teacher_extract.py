from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from belle.cc_teacher_extract import (
    extract_cc_teacher_rows,
    load_cc_teacher_extraction_ruleset,
    load_credit_card_teacher_extraction_config,
    resolve_cc_teacher_ruleset_path,
)
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_SUMMARY,
)


def _base_config(
    *,
    manual_include: list[str] | None = None,
    manual_exclude: list[str] | None = None,
    min_total_count: int = 2,
    min_unique_counter_accounts: int = 2,
    min_unique_summaries: int = 2,
) -> dict:
    return {
        "schema": "belle.credit_card_line_config.v1",
        "version": "0.3",
        "target_payable_placeholder_names": ["未払金"],
        "teacher_extraction": {
            "enabled": True,
            "ruleset_relpath": "rulesets/credit_card_statement/teacher_extraction_rules_v1.json",
            "payable_candidate_accounts": ["未払費用", "未払金"],
            "manual_include_subaccounts": list(manual_include or []),
            "manual_exclude_subaccounts": list(manual_exclude or []),
            "soft_match_thresholds": {
                "min_total_count": int(min_total_count),
                "min_unique_counter_accounts": int(min_unique_counter_accounts),
                "min_unique_summaries": int(min_unique_summaries),
            },
            "canonical_payable_thresholds": {
                "min_count": 3,
                "min_p_majority": 0.9,
            },
        },
    }


def _base_ruleset() -> dict:
    return {
        "schema": "belle.cc_teacher_extraction_rules.v1",
        "version": "2",
        "teacher_payable_candidate_accounts": ["未払費用", "未払金"],
        "hard_include_terms": ["CARD", "カード"],
        "soft_include_terms": [
            "VISA",
            "LUXURY",
            "MASTERCARD",
            "オーナーズ",
            "アメックス",
            "セゾン",
            "ビジネスクラシック",
            "ラグジュアリー",
            "三井住友",
        ],
        "exclude_terms": ["デビット", "給与", "報酬", "源泉", "社会保険"],
        "soft_negative_terms": ["ファイナンス"],
    }


def _row(*, debit_account: str, debit_subaccount: str, credit_account: str, credit_subaccount: str, summary: str) -> list[str]:
    cols = [""] * 25
    cols[COL_DEBIT_ACCOUNT] = debit_account
    cols[COL_DEBIT_SUBACCOUNT] = debit_subaccount
    cols[COL_CREDIT_ACCOUNT] = credit_account
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_SUMMARY] = summary
    return cols


class CCTeacherExtractTests(unittest.TestCase):
    def test_payable_candidate_gate_filters_non_candidate_accounts(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="法人カード",
                    summary="S1",
                ),
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="買掛金",
                    credit_subaccount="法人カード",
                    summary="S2",
                ),
            ],
            source_identity={"source": "gate"},
            config=_base_config(manual_include=["法人カード"]),
            ruleset=_base_ruleset(),
        )

        self.assertEqual(1, len(result["selected_rows"]))
        manifest = result["manifest"]
        self.assertEqual(2, manifest["row_counts"]["source_rows_total"])
        self.assertEqual(1, manifest["row_counts"]["payable_candidate_rows"])
        self.assertEqual(1, manifest["reasons"]["row_reason_counts"]["payable_account_not_candidate"])

    def test_payable_candidate_gate_accepts_both_unpaid_accounts(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="法人カードA",
                    summary="S1",
                ),
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払費用",
                    credit_subaccount="法人カードB",
                    summary="S2",
                ),
            ],
            source_identity={"source": "dual_payable_accounts"},
            config=_base_config(manual_include=["法人カードA", "法人カードB"]),
            ruleset=_base_ruleset(),
        )

        self.assertEqual(2, len(result["selected_rows"]))
        self.assertEqual(2, result["manifest"]["row_counts"]["payable_candidate_rows"])
        selected_accounts = sorted(
            {
                account
                for detail in result["manifest"]["selected_subaccounts"]
                for account in detail["payable_accounts_seen"]
            }
        )
        self.assertEqual(["未払費用", "未払金"], selected_accounts)

    def test_manual_exclude_precedence_wins_over_manual_include_and_hard_match(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="法人カード",
                    summary="S1",
                )
            ],
            source_identity={"source": "manual_exclude"},
            config=_base_config(manual_include=["法人カード"], manual_exclude=["法人カード"]),
            ruleset=_base_ruleset(),
        )

        self.assertEqual([], result["selected_rows"])
        excluded = result["manifest"]["excluded_subaccounts"]
        self.assertEqual("manual_exclude", excluded[0]["reason"])

    def test_manual_include_precedence_selects_without_term_match(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="CORP_MAIN",
                    summary="S1",
                )
            ],
            source_identity={"source": "manual_include"},
            config=_base_config(manual_include=["CORP_MAIN"]),
            ruleset=_base_ruleset(),
        )

        self.assertEqual(1, len(result["selected_rows"]))
        self.assertEqual("manual_include", result["manifest"]["selected_subaccounts"][0]["reason"])

    def test_hard_include_term_selects_group(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="事業用カード",
                    summary="S1",
                )
            ],
            source_identity={"source": "hard_term"},
            config=_base_config(),
            ruleset=_base_ruleset(),
        )

        self.assertEqual(1, len(result["selected_rows"]))
        selected = result["manifest"]["selected_subaccounts"][0]
        self.assertEqual("hard_include_term", selected["reason"])
        self.assertEqual(["カード"], selected["matched_terms"]["hard_include_terms"])

    def test_soft_include_term_requires_thresholds(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="VISA MAIN",
                    summary="SHOP_A",
                ),
                _row(
                    debit_account="旅費交通費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="VISA MAIN",
                    summary="SHOP_B",
                ),
            ],
            source_identity={"source": "soft_term_ok"},
            config=_base_config(min_total_count=2, min_unique_counter_accounts=2, min_unique_summaries=2),
            ruleset=_base_ruleset(),
        )

        self.assertEqual(2, len(result["selected_rows"]))
        self.assertEqual("soft_include_term", result["manifest"]["selected_subaccounts"][0]["reason"])

    def test_rejection_path_reports_soft_threshold_failure(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="VISA MAIN",
                    summary="SHOP_A",
                )
            ],
            source_identity={"source": "soft_term_ng"},
            config=_base_config(min_total_count=2, min_unique_counter_accounts=2, min_unique_summaries=2),
            ruleset=_base_ruleset(),
        )

        self.assertEqual([], result["selected_rows"])
        excluded = result["manifest"]["excluded_subaccounts"][0]
        self.assertEqual("soft_include_threshold_failed", excluded["reason"])
        self.assertEqual(1, result["manifest"]["reasons"]["row_reason_counts"]["soft_include_threshold_failed"])

    def test_saison_family_soft_include_selects_when_thresholds_met(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="セゾンプラチナビジネス",
                    summary="SHOP_A",
                ),
                _row(
                    debit_account="旅費交通費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="セゾンプラチナビジネス",
                    summary="SHOP_B",
                ),
            ],
            source_identity={"source": "saison"},
            config=_base_config(min_total_count=2, min_unique_counter_accounts=2, min_unique_summaries=2),
            ruleset=_base_ruleset(),
        )

        selected = result["manifest"]["selected_subaccounts"][0]
        self.assertEqual("soft_include_term", selected["reason"])
        self.assertEqual(["セゾン"], selected["matched_terms"]["soft_include_terms"])

    def test_mitsui_sumitomo_soft_include_selects_when_thresholds_met(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="三井住友",
                    summary="SHOP_A",
                ),
                _row(
                    debit_account="旅費交通費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="三井住友",
                    summary="SHOP_B",
                ),
            ],
            source_identity={"source": "mitsui_sumitomo"},
            config=_base_config(min_total_count=2, min_unique_counter_accounts=2, min_unique_summaries=2),
            ruleset=_base_ruleset(),
        )

        selected = result["manifest"]["selected_subaccounts"][0]
        self.assertEqual("soft_include_term", selected["reason"])
        self.assertEqual(["三井住友"], selected["matched_terms"]["soft_include_terms"])

    def test_amex_family_soft_include_selects_when_thresholds_met(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="アメックス21008",
                    summary="SHOP_A",
                ),
                _row(
                    debit_account="旅費交通費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="アメックス21008",
                    summary="SHOP_B",
                ),
            ],
            source_identity={"source": "amex"},
            config=_base_config(min_total_count=2, min_unique_counter_accounts=2, min_unique_summaries=2),
            ruleset=_base_ruleset(),
        )

        selected = result["manifest"]["selected_subaccounts"][0]
        self.assertEqual("soft_include_term", selected["reason"])
        self.assertEqual(["アメックス"], selected["matched_terms"]["soft_include_terms"])

    def test_finance_only_soft_negative_rejects_without_positive_evidence(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="トヨタファイナンス",
                    summary="SHOP_A",
                )
            ],
            source_identity={"source": "finance_only"},
            config=_base_config(),
            ruleset=_base_ruleset(),
        )

        self.assertEqual([], result["selected_rows"])
        excluded = result["manifest"]["excluded_subaccounts"][0]
        self.assertEqual("soft_negative_only", excluded["reason"])
        self.assertEqual(["ファイナンス"], excluded["matched_terms"]["soft_negative_terms"])

    def test_positive_card_and_finance_soft_negative_can_still_select(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="トヨタファイナンス/カード6322",
                    summary="SHOP_A",
                )
            ],
            source_identity={"source": "finance_with_card_positive"},
            config=_base_config(),
            ruleset=_base_ruleset(),
        )

        selected = result["manifest"]["selected_subaccounts"][0]
        self.assertEqual(1, len(result["selected_rows"]))
        self.assertEqual("soft_negative_overridden_by_hard_include", selected["reason"])
        self.assertEqual(["カード"], selected["matched_terms"]["hard_include_terms"])
        self.assertEqual(["ファイナンス"], selected["matched_terms"]["soft_negative_terms"])
        self.assertEqual("hard_include_term", selected["evaluation"]["positive_reason"])

    def test_salary_social_insurance_and_withholding_terms_are_rejected(self) -> None:
        rows = [
            _row(
                debit_account="消耗品費",
                debit_subaccount="",
                credit_account="未払金",
                credit_subaccount="給与",
                summary="S1",
            ),
            _row(
                debit_account="消耗品費",
                debit_subaccount="",
                credit_account="未払金",
                credit_subaccount="役員報酬",
                summary="S2",
            ),
            _row(
                debit_account="消耗品費",
                debit_subaccount="",
                credit_account="未払金",
                credit_subaccount="社会保険料",
                summary="S3",
            ),
            _row(
                debit_account="消耗品費",
                debit_subaccount="",
                credit_account="未払金",
                credit_subaccount="源泉所得税",
                summary="S4",
            ),
        ]

        result = extract_cc_teacher_rows(
            rows,
            source_identity={"source": "negative_terms"},
            config=_base_config(),
            ruleset=_base_ruleset(),
        )

        self.assertEqual([], result["selected_rows"])
        self.assertEqual(
            {"exclude_term"},
            {detail["reason"] for detail in result["manifest"]["excluded_subaccounts"]},
        )

    def test_etc_is_not_treated_as_unconditional_hard_positive(self) -> None:
        result = extract_cc_teacher_rows(
            [
                _row(
                    debit_account="消耗品費",
                    debit_subaccount="",
                    credit_account="未払金",
                    credit_subaccount="ETC協同組合",
                    summary="SHOP_A",
                )
            ],
            source_identity={"source": "etc_context_sensitive"},
            config=_base_config(min_total_count=2, min_unique_counter_accounts=2, min_unique_summaries=2),
            ruleset=_base_ruleset(),
        )

        self.assertEqual([], result["selected_rows"])
        self.assertEqual("no_include_match", result["manifest"]["excluded_subaccounts"][0]["reason"])

    def test_source_rows_are_not_mutated(self) -> None:
        rows = [
            _row(
                debit_account="消耗品費",
                debit_subaccount="",
                credit_account="未払金",
                credit_subaccount="事業用カード",
                summary="S1",
            )
        ]
        before = copy.deepcopy(rows)

        result = extract_cc_teacher_rows(
            rows,
            source_identity={"source": "immutability"},
            config=_base_config(),
            ruleset=_base_ruleset(),
        )

        self.assertEqual(before, rows)
        self.assertEqual(before[0], result["selected_rows"][0])

    def test_manifest_and_reason_reporting_are_deterministic(self) -> None:
        rows = [
            _row(
                debit_account="旅費交通費",
                debit_subaccount="",
                credit_account="未払金",
                credit_subaccount="VISA MAIN",
                summary="SHOP_B",
            ),
            _row(
                debit_account="消耗品費",
                debit_subaccount="",
                credit_account="未払金",
                credit_subaccount="VISA MAIN",
                summary="SHOP_A",
            ),
            _row(
                debit_account="消耗品費",
                debit_subaccount="",
                credit_account="未払金",
                credit_subaccount="法人デビット",
                summary="SHOP_C",
            ),
        ]

        result1 = extract_cc_teacher_rows(
            rows,
            source_identity={"source": "deterministic"},
            config=_base_config(min_total_count=2, min_unique_counter_accounts=2, min_unique_summaries=2),
            ruleset=_base_ruleset(),
        )
        result2 = extract_cc_teacher_rows(
            rows,
            source_identity={"source": "deterministic"},
            config=_base_config(min_total_count=2, min_unique_counter_accounts=2, min_unique_summaries=2),
            ruleset=_base_ruleset(),
        )

        self.assertEqual(result1["manifest"], result2["manifest"])
        self.assertEqual({"exclude_term": 1, "soft_include_term": 1}, result1["manifest"]["reasons"]["group_reason_counts"])
        self.assertEqual(2, result1["manifest"]["row_counts"]["selected_rows"])
        self.assertEqual(1, result1["manifest"]["row_counts"]["rejected_rows"])

    def test_load_helpers_resolve_template_ruleset_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_LOAD_HELPERS"
            config_path = (
                repo_root
                / "clients"
                / client_id
                / "lines"
                / "credit_card_statement"
                / "config"
                / "credit_card_line_config.json"
            )
            ruleset_path = repo_root / "rulesets" / "credit_card_statement" / "teacher_extraction_rules_v1.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            ruleset_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(json.dumps(_base_config(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            ruleset_path.write_text(json.dumps(_base_ruleset(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            config = load_credit_card_teacher_extraction_config(repo_root, client_id)
            resolved_ruleset_path = resolve_cc_teacher_ruleset_path(repo_root, config)
            ruleset = load_cc_teacher_extraction_ruleset(resolved_ruleset_path)

            self.assertEqual(ruleset_path, resolved_ruleset_path)
            self.assertEqual(["未払金"], config["target_payable_placeholder_names"])
            teacher_extraction = config["teacher_extraction"]
            self.assertEqual(["未払費用", "未払金"], teacher_extraction["payable_candidate_accounts"])
            self.assertEqual({"min_count": 3, "min_p_majority": 0.9}, teacher_extraction["canonical_payable_thresholds"])
            self.assertEqual(["未払費用", "未払金"], ruleset["teacher_payable_candidate_accounts"])


if __name__ == "__main__":
    unittest.main()
