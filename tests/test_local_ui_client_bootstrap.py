from __future__ import annotations

import csv
from dataclasses import asdict
import json
import shutil
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

from belle.local_ui.services.client_bootstrap import (
    _NO_VISIBLE_CHANGES_MESSAGE,
    clear_teacher_file,
    cleanup_after_success,
    empty_teacher_file_state,
    refresh_teacher_file,
    session_dir_for,
    stage_root,
    stage_teacher_file,
)
from belle.local_ui.services.clients import create_client


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _teacher_row(summary: str, debit_account: str) -> list[str]:
    row = [""] * 25
    row[4] = debit_account
    row[16] = summary
    return row


def _prepare_template(real_repo_root: Path, repo_root: Path) -> None:
    shutil.copytree(real_repo_root / "clients" / "TEMPLATE", repo_root / "clients" / "TEMPLATE")


def _write_mode_aware_defaults(repo_root: Path, line_id: str, *, excluded: dict, included: dict) -> None:
    base_dir = repo_root / "defaults" / line_id
    _write_json(base_dir / "category_defaults_tax_excluded.json", excluded)
    _write_json(base_dir / "category_defaults_tax_included.json", included)


def _prepare_shared_assets(repo_root: Path) -> None:
    _write_mode_aware_defaults(
        repo_root,
        "receipt",
        excluded={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "food": {
                    "target_account": "旅費交通費",
                    "target_tax_division": "",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "travel": {
                    "target_account": "旅費交通費",
                    "target_tax_division": "",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
            },
            "global_fallback": {
                "target_account": "仮払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
        included={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "food": {
                    "target_account": "交際費",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "travel": {
                    "target_account": "旅費交通費",
                    "target_tax_division": "対象外",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
            },
            "global_fallback": {
                "target_account": "仮払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    _write_mode_aware_defaults(
        repo_root,
        "credit_card_statement",
        excluded={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "food": {
                    "target_account": "通信費",
                    "target_tax_division": "",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "travel": {
                    "target_account": "諸会費",
                    "target_tax_division": "",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
            },
            "global_fallback": {
                "target_account": "未払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
        included={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "food": {
                    "target_account": "交際費",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "travel": {
                    "target_account": "旅費交通費",
                    "target_tax_division": "対象外",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
            },
            "global_fallback": {
                "target_account": "未払金",
                "target_tax_division": "",
                "confidence": 0.35,
                "priority": "HIGH",
                "reason_code": "global_fallback",
            },
        },
    )
    _write_json(
        repo_root / "lexicon" / "lexicon.json",
        {
            "schema": "belle.lexicon.v1",
            "version": "test",
            "categories": [
                {
                    "id": 1,
                    "key": "food",
                    "label": "飲食",
                    "kind": "expense",
                    "precision_hint": 0.9,
                    "deprecated": False,
                    "negative_terms": {"n0": [], "n1": []},
                },
                {
                    "id": 2,
                    "key": "travel",
                    "label": "交通",
                    "kind": "expense",
                    "precision_hint": 0.8,
                    "deprecated": False,
                    "negative_terms": {"n0": [], "n1": []},
                },
            ],
            "term_rows": [
                ["n0", "LUNCH", 1, 1.0, "S"],
                ["n0", "MEAL", 1, 1.0, "S"],
                ["n0", "ALPHA", 2, 1.0, "S"],
                ["n0", "BRAVO", 2, 1.0, "S"],
            ],
            "learned": {"policy": {"core_weight": 1.0}},
        },
    )


class LocalUiClientBootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def _make_repo_root(self, suffix: str) -> Path:
        repo_root = self.test_tmp_root / f"{suffix}_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        _prepare_template(self.real_repo_root, repo_root)
        _prepare_shared_assets(repo_root)
        return repo_root

    def test_temp_staging_creates_missing_tmp_parent_directories(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_missing_tmp")
        try:
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=b"",
                bookkeeping_mode="",
                root=repo_root,
            )

            self.assertTrue((repo_root / ".tmp").is_dir())
            self.assertTrue(state.staged_path is not None and state.staged_path.exists())
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_temp_staging_reuses_existing_tmp_directory(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_reuse_tmp")
        try:
            existing_tmp = repo_root / ".tmp"
            existing_tmp.mkdir(parents=True, exist_ok=False)

            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=b"",
                bookkeeping_mode="",
                root=repo_root,
            )

            self.assertTrue(existing_tmp.exists())
            self.assertTrue(state.staged_path is not None)
            self.assertTrue(str(state.staged_path).startswith(str(existing_tmp)))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_staged_temp_path_preserves_original_basename_safely(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_basename")
        try:
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="nested/../safe_teacher.txt",
                content=b"",
                bookkeeping_mode="",
                root=repo_root,
            )

            self.assertEqual("safe_teacher.txt", state.original_basename)
            self.assertIsNotNone(state.staged_path)
            self.assertEqual("safe_teacher.txt", state.staged_path.name if state.staged_path is not None else "")
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_replacing_teacher_file_cleans_previous_session_directory(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_replace_cleanup")
        try:
            first_state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="first.csv",
                content=b"",
                bookkeeping_mode="",
                root=repo_root,
            )
            first_session_dir = session_dir_for(first_state.session_token, repo_root)

            second_state = stage_teacher_file(
                first_state,
                filename="second.csv",
                content=b"",
                bookkeeping_mode="",
                root=repo_root,
            )

            self.assertFalse(first_session_dir.exists())
            self.assertNotEqual(first_state.session_token, second_state.session_token)
            self.assertTrue(session_dir_for(second_state.session_token, repo_root).exists())
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_clearing_teacher_file_cleans_current_session_directory(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_clear_cleanup")
        try:
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=b"",
                bookkeeping_mode="",
                root=repo_root,
            )
            current_session_dir = session_dir_for(state.session_token, repo_root)

            cleared = clear_teacher_file(state, repo_root)

            self.assertFalse(current_session_dir.exists())
            self.assertEqual("", cleared.session_token)
            self.assertIsNone(cleared.staged_path)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_back_navigation_cleanup_contract_clears_current_session_directory(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_back_cleanup")
        try:
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=self._teacher_bytes_for_food("交際費"),
                bookkeeping_mode="tax_excluded",
                root=repo_root,
            )
            current_session_dir = session_dir_for(state.session_token, repo_root)

            cleared = clear_teacher_file(state, repo_root)

            self.assertFalse(current_session_dir.exists())
            self.assertEqual("", cleared.session_token)
            self.assertIsNone(cleared.staged_path)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_successful_create_cleans_current_session_directory(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_success_cleanup")
        try:
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=self._teacher_bytes_for_food("交際費"),
                bookkeeping_mode="tax_excluded",
                root=repo_root,
            )
            current_session_dir = session_dir_for(state.session_token, repo_root)

            result = create_client("BOOTSTRAP_OK", "tax_excluded", repo_root, teacher_path=state.staged_path)
            cleaned = cleanup_after_success(state, repo_root)

            self.assertTrue(result.ok, msg=result.stdout)
            self.assertFalse(current_session_dir.exists())
            self.assertEqual("", cleaned.session_token)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_failed_create_keeps_staged_temp_file_for_retry(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_failed_retry")
        try:
            (repo_root / "clients" / "DUPLICATE").mkdir(parents=True, exist_ok=False)
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=self._teacher_bytes_for_food("交際費"),
                bookkeeping_mode="tax_excluded",
                root=repo_root,
            )

            result = create_client("DUPLICATE", "tax_excluded", repo_root, teacher_path=state.staged_path)

            self.assertFalse(result.ok)
            self.assertTrue(state.staged_path is not None and state.staged_path.exists())
            self.assertTrue(session_dir_for(state.session_token, repo_root).exists())
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_invalid_teacher_file_produces_blocking_error_state(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_invalid")
        try:
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="invalid.csv",
                content="a,b\n".encode("cp932"),
                bookkeeping_mode="tax_excluded",
                root=repo_root,
            )

            self.assertTrue(state.submit_blocked)
            self.assertEqual("このファイルは使えません。別のファイルにしてください。", state.error_message)
            self.assertEqual((), state.preview.sections)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_valid_teacher_with_zero_visible_changes_is_handled_cleanly(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_zero_visible")
        try:
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=self._teacher_bytes_for_food("交際費"),
                bookkeeping_mode="tax_included",
                root=repo_root,
            )

            self.assertFalse(state.submit_blocked)
            self.assertEqual("", state.error_message)
            self.assertEqual((), state.preview.sections)
            self.assertEqual(_NO_VISIBLE_CHANGES_MESSAGE, state.preview.note)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_preview_output_shape_contains_only_minimal_display_fields(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_preview_shape")
        try:
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=self._teacher_bytes_for_food("交際費"),
                bookkeeping_mode="tax_excluded",
                root=repo_root,
            )

            payload = asdict(state.preview)
            self.assertEqual({"sections", "note"}, set(payload.keys()))
            self.assertEqual("", payload["note"])
            self.assertEqual(2, len(payload["sections"]))
            self.assertEqual({"title", "rows"}, set(payload["sections"][0].keys()))
            self.assertEqual(
                {"line_id", "category_key", "category_label", "replacement_account"},
                set(payload["sections"][0]["rows"][0].keys()),
            )
            self.assertEqual("receipt", payload["sections"][0]["rows"][0]["line_id"])
            self.assertEqual("food", payload["sections"][0]["rows"][0]["category_key"])
            self.assertEqual("飲食", payload["sections"][0]["rows"][0]["category_label"])
            self.assertEqual("交際費", payload["sections"][0]["rows"][0]["replacement_account"])
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_preview_prefers_label_ja_for_display_when_available(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_label_ja")
        try:
            _write_json(
                repo_root / "lexicon" / "lexicon.json",
                {
                    "schema": "belle.lexicon.v1",
                    "version": "test",
                    "categories": [
                        {
                            "id": 1,
                            "key": "food",
                            "label": "FOOD_LABEL",
                            "label_ja": "飲食カテゴリ",
                            "kind": "expense",
                            "precision_hint": 0.9,
                            "deprecated": False,
                            "negative_terms": {"n0": [], "n1": []},
                        },
                        {
                            "id": 2,
                            "key": "travel",
                            "label": "TRAVEL_LABEL",
                            "label_ja": "交通カテゴリ",
                            "kind": "expense",
                            "precision_hint": 0.8,
                            "deprecated": False,
                            "negative_terms": {"n0": [], "n1": []},
                        },
                    ],
                    "term_rows": [
                        ["n0", "LUNCH", 1, 1.0, "S"],
                        ["n0", "MEAL", 1, 1.0, "S"],
                        ["n0", "ALPHA", 2, 1.0, "S"],
                        ["n0", "BRAVO", 2, 1.0, "S"],
                    ],
                    "learned": {"policy": {"core_weight": 1.0}},
                },
            )

            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=self._teacher_bytes_for_food("交際費"),
                bookkeeping_mode="tax_excluded",
                root=repo_root,
            )

            self.assertEqual("飲食カテゴリ", state.preview.sections[0].rows[0].category_label)
            self.assertEqual("receipt", state.preview.sections[0].rows[0].line_id)
            self.assertEqual("food", state.preview.sections[0].rows[0].category_key)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_bookkeeping_mode_changes_recompute_preview_output(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_recompute")
        try:
            state = stage_teacher_file(
                empty_teacher_file_state(),
                filename="teacher.csv",
                content=self._teacher_bytes_for_food("交際費"),
                bookkeeping_mode="tax_excluded",
                root=repo_root,
            )
            recomputed = refresh_teacher_file(state, bookkeeping_mode="tax_included", root=repo_root)

            self.assertNotEqual(state.preview, recomputed.preview)
            self.assertEqual(2, len(state.preview.sections))
            self.assertEqual(_NO_VISIBLE_CHANGES_MESSAGE, recomputed.preview.note)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_preview_generation_does_not_require_register_client_private_helpers(self) -> None:
        repo_root = self._make_repo_root("local_ui_bootstrap_no_register_private")
        try:
            with mock.patch(
                "belle.local_ui.services.clients._load_register_module",
                side_effect=RuntimeError("register_client private helper should not be used"),
            ):
                state = stage_teacher_file(
                    empty_teacher_file_state(),
                    filename="teacher.csv",
                    content=self._teacher_bytes_for_food("交際費"),
                    bookkeeping_mode="tax_excluded",
                    root=repo_root,
                )

            self.assertFalse(state.submit_blocked)
            self.assertEqual("", state.error_message)
            self.assertEqual(2, len(state.preview.sections))
            self.assertEqual("交際費", state.preview.sections[0].rows[0].replacement_account)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def _teacher_bytes_for_food(self, target_account: str) -> bytes:
        rows = [
            _teacher_row("LUNCH TOKYO", target_account),
            _teacher_row("MEAL OSAKA", target_account),
        ]
        temp_dir = self.test_tmp_root / f"local_ui_bootstrap_teacher_bytes_{uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=False)
        try:
            teacher_path = temp_dir / "teacher.csv"
            _write_yayoi_rows(teacher_path, rows)
            return teacher_path.read_bytes()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
