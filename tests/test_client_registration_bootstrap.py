from __future__ import annotations

import contextlib
import csv
from hashlib import sha256
import importlib.util
import io
import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

from belle.category_override_bootstrap import (
    analyze_category_override_teacher,
    category_override_bootstrap_rules_manifest,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


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
                    "target_tax_division": "課対仕入内10%適格",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "software": {
                    "target_account": "通信費",
                    "target_tax_division": "課対仕入内10%適格",
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
        included={
            "schema": "belle.category_defaults.v2",
            "version": "test",
            "defaults": {
                "food": {
                    "target_account": "租税公課",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "software": {
                    "target_account": "支払手数料",
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
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "software": {
                    "target_account": "諸会費",
                    "target_tax_division": "課対仕入込10%",
                    "confidence": 0.7,
                    "priority": "MED",
                    "reason_code": "category_default",
                },
                "travel": {
                    "target_account": "諸会費",
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
                "software": {
                    "target_account": "支払手数料",
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
                    "key": "software",
                    "label": "ソフトウェア",
                    "kind": "expense",
                    "precision_hint": 0.8,
                    "deprecated": False,
                    "negative_terms": {"n0": [], "n1": []},
                },
                {
                    "id": 3,
                    "key": "travel",
                    "label": "交通",
                    "kind": "expense",
                    "precision_hint": 0.95,
                    "deprecated": False,
                    "negative_terms": {"n0": [], "n1": []},
                },
            ],
            "term_rows": [
                ["n0", "LUNCH", 1, 1.0, "S"],
                ["n0", "MEAL", 1, 1.0, "S"],
                ["n0", "SAAS", 2, 1.0, "S"],
                ["n0", "SOFT", 2, 1.0, "S"],
                ["n0", "ALPHA", 3, 1.0, "S"],
                ["n0", "BRAVO", 1, 1.0, "S"],
            ],
            "learned": {"policy": {"core_weight": 1.0}},
        },
    )


def _load_register_module(real_repo_root: Path):
    script_path = real_repo_root / ".agents" / "skills" / "client-register" / "register_client.py"
    spec = importlib.util.spec_from_file_location(f"register_client_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _run_register(
    module,
    repo_root: Path,
    *,
    client_id: str,
    bookkeeping_mode: str = "tax_excluded",
    line: str | None = None,
    teacher_relpath: str | None = None,
    selected_json_relpath: str | None = None,
) -> tuple[int, str]:
    fake_script_path = repo_root / ".agents" / "skills" / "client-register" / "register_client.py"
    fake_script_path.parent.mkdir(parents=True, exist_ok=True)
    module.__file__ = str(fake_script_path)

    argv = ["register_client.py", "--client-id", client_id, "--bookkeeping-mode", bookkeeping_mode]
    if line is not None:
        argv.extend(["--line", line])
    if teacher_relpath is not None:
        argv.extend(["--category-override-teacher-path", teacher_relpath])
    if selected_json_relpath is not None:
        argv.extend(["--category-override-selected-json", selected_json_relpath])

    buffer = io.StringIO()
    original_sys_path = list(sys.path)
    try:
        with mock.patch.object(sys, "argv", argv):
            with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
                rc = module.main()
    finally:
        sys.path[:] = original_sys_path
    return rc, buffer.getvalue()


def _teacher_row(summary: str, debit_account: str) -> list[str]:
    row = [""] * 25
    row[4] = debit_account
    row[16] = summary
    return row


def _override_row(repo_root: Path, client_id: str, line_id: str, category_key: str) -> dict[str, str]:
    payload = json.loads(
        (
            repo_root
            / "clients"
            / client_id
            / "lines"
            / line_id
            / "config"
            / "category_overrides.json"
        ).read_text(encoding="utf-8")
    )
    return dict((payload.get("overrides") or {}).get(category_key) or {})


def _load_client_registration_manifest(repo_root: Path, client_id: str) -> dict:
    audit_root = repo_root / "clients" / client_id / "artifacts" / "client_registration"
    run_id = (audit_root / "LATEST.txt").read_text(encoding="utf-8").strip()
    return json.loads((audit_root / "runs" / run_id / "run_manifest.json").read_text(encoding="utf-8"))


class ClientRegistrationBootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)
        self.register_module = _load_register_module(self.real_repo_root)

    def test_no_teacher_path_preserves_defaults_and_writes_audit_manifest(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_bootstrap_none_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)

            rc, output = _run_register(self.register_module, repo_root, client_id="C_BOOTSTRAP_NONE")

            self.assertEqual(0, rc, msg=output)
            self.assertEqual(
                {"target_account": "旅費交通費", "target_tax_division": "課対仕入内10%適格"},
                _override_row(repo_root, "C_BOOTSTRAP_NONE", "receipt", "food"),
            )
            self.assertEqual(
                {"target_account": "通信費", "target_tax_division": "課対仕入込10%"},
                _override_row(repo_root, "C_BOOTSTRAP_NONE", "credit_card_statement", "food"),
            )

            manifest = _load_client_registration_manifest(repo_root, "C_BOOTSTRAP_NONE")
            bootstrap = manifest.get("category_override_bootstrap") or {}
            self.assertEqual("belle.client_registration_init.run_manifest.v1", manifest.get("schema"))
            self.assertEqual("tax_excluded", manifest.get("bookkeeping_mode"))
            self.assertEqual(
                ["receipt", "bank_statement", "credit_card_statement"],
                manifest.get("selected_lines"),
            )
            self.assertEqual(False, bool(bootstrap.get("requested")))
            self.assertEqual("skipped_no_teacher", bootstrap.get("status"))
            self.assertEqual("", bootstrap.get("teacher_source_basename"))
            self.assertEqual("", bootstrap.get("teacher_source_sha256"))
            self.assertEqual(0, int(bootstrap.get("row_count") or 0))
            self.assertEqual(category_override_bootstrap_rules_manifest(), bootstrap.get("rules_used"))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_teacher_bootstraps_target_account_only_for_receipt_and_credit_card_and_manifest_matches(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_bootstrap_apply_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            teacher_path = repo_root / "teacher" / "bootstrap_teacher.txt"
            _write_yayoi_rows(
                teacher_path,
                [
                    _teacher_row("LUNCH TOKYO", "交際費"),
                    _teacher_row("MEAL OSAKA", "交際費"),
                    _teacher_row("SAAS MONTHLY", "消耗品費"),
                    _teacher_row("SOFT LICENSE", "消耗品費"),
                ],
            )

            rc, output = _run_register(
                self.register_module,
                repo_root,
                client_id="C_BOOTSTRAP_APPLY",
                teacher_relpath="teacher/bootstrap_teacher.txt",
            )

            self.assertEqual(0, rc, msg=output)
            self.assertEqual(
                {"target_account": "交際費", "target_tax_division": "課対仕入内10%適格"},
                _override_row(repo_root, "C_BOOTSTRAP_APPLY", "receipt", "food"),
            )
            self.assertEqual(
                {"target_account": "消耗品費", "target_tax_division": "課対仕入内10%適格"},
                _override_row(repo_root, "C_BOOTSTRAP_APPLY", "receipt", "software"),
            )
            self.assertEqual(
                {"target_account": "交際費", "target_tax_division": "課対仕入込10%"},
                _override_row(repo_root, "C_BOOTSTRAP_APPLY", "credit_card_statement", "food"),
            )
            self.assertEqual(
                {"target_account": "消耗品費", "target_tax_division": "課対仕入込10%"},
                _override_row(repo_root, "C_BOOTSTRAP_APPLY", "credit_card_statement", "software"),
            )

            manifest = _load_client_registration_manifest(repo_root, "C_BOOTSTRAP_APPLY")
            bootstrap = manifest.get("category_override_bootstrap") or {}
            teacher_sha = sha256(teacher_path.read_bytes()).hexdigest()
            self.assertEqual(True, bool(bootstrap.get("requested")))
            self.assertEqual("applied", bootstrap.get("status"))
            self.assertEqual("bootstrap_teacher.txt", bootstrap.get("teacher_source_basename"))
            self.assertEqual(teacher_sha, bootstrap.get("teacher_source_sha256"))
            self.assertEqual(4, int(bootstrap.get("row_count") or 0))
            self.assertEqual(4, int(bootstrap.get("clear_rows") or 0))
            self.assertEqual(0, int(bootstrap.get("ambiguous_rows") or 0))
            self.assertEqual(0, int(bootstrap.get("none_rows") or 0))
            per_line = bootstrap.get("per_line") or {}
            self.assertEqual(2, int(((per_line.get("receipt") or {}).get("applied_count")) or 0))
            self.assertEqual(2, int(((per_line.get("credit_card_statement") or {}).get("applied_count")) or 0))
            self.assertEqual(
                [
                    {
                        "category_key": "food",
                        "category_label": "飲食",
                        "from_target_account": "旅費交通費",
                        "to_target_account": "交際費",
                    },
                    {
                        "category_key": "software",
                        "category_label": "ソフトウェア",
                        "from_target_account": "通信費",
                        "to_target_account": "消耗品費",
                    },
                ],
                (per_line.get("receipt") or {}).get("changes"),
            )
            self.assertEqual(
                [
                    {
                        "category_key": "food",
                        "category_label": "飲食",
                        "from_target_account": "通信費",
                        "to_target_account": "交際費",
                    },
                    {
                        "category_key": "software",
                        "category_label": "ソフトウェア",
                        "from_target_account": "諸会費",
                        "to_target_account": "消耗品費",
                    },
                ],
                (per_line.get("credit_card_statement") or {}).get("changes"),
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_teacher_bootstrap_applies_only_selected_categories_by_line(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_bootstrap_selective_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            teacher_path = repo_root / "teacher" / "bootstrap_teacher.txt"
            _write_yayoi_rows(
                teacher_path,
                [
                    _teacher_row("LUNCH TOKYO", "交際費"),
                    _teacher_row("MEAL OSAKA", "交際費"),
                    _teacher_row("SAAS MONTHLY", "消耗品費"),
                    _teacher_row("SOFT LICENSE", "消耗品費"),
                ],
            )
            _write_json(
                repo_root / "teacher" / "selected.json",
                {
                    "receipt": ["food"],
                    "credit_card_statement": ["software"],
                },
            )

            rc, output = _run_register(
                self.register_module,
                repo_root,
                client_id="C_BOOTSTRAP_SELECTIVE",
                teacher_relpath="teacher/bootstrap_teacher.txt",
                selected_json_relpath="teacher/selected.json",
            )

            self.assertEqual(0, rc, msg=output)
            self.assertEqual(
                {"target_account": "交際費", "target_tax_division": "課対仕入内10%適格"},
                _override_row(repo_root, "C_BOOTSTRAP_SELECTIVE", "receipt", "food"),
            )
            self.assertEqual(
                {"target_account": "通信費", "target_tax_division": "課対仕入内10%適格"},
                _override_row(repo_root, "C_BOOTSTRAP_SELECTIVE", "receipt", "software"),
            )
            self.assertEqual(
                {"target_account": "通信費", "target_tax_division": "課対仕入込10%"},
                _override_row(repo_root, "C_BOOTSTRAP_SELECTIVE", "credit_card_statement", "food"),
            )
            self.assertEqual(
                {"target_account": "消耗品費", "target_tax_division": "課対仕入込10%"},
                _override_row(repo_root, "C_BOOTSTRAP_SELECTIVE", "credit_card_statement", "software"),
            )

            manifest = _load_client_registration_manifest(repo_root, "C_BOOTSTRAP_SELECTIVE")
            bootstrap = manifest.get("category_override_bootstrap") or {}
            per_line = bootstrap.get("per_line") or {}
            self.assertEqual(1, int(((per_line.get("receipt") or {}).get("applied_count")) or 0))
            self.assertEqual(1, int(((per_line.get("credit_card_statement") or {}).get("applied_count")) or 0))
            self.assertEqual(
                [
                    {
                        "category_key": "food",
                        "category_label": "飲食",
                        "from_target_account": "旅費交通費",
                        "to_target_account": "交際費",
                    }
                ],
                (per_line.get("receipt") or {}).get("changes"),
            )
            self.assertEqual(
                [
                    {
                        "category_key": "software",
                        "category_label": "ソフトウェア",
                        "from_target_account": "諸会費",
                        "to_target_account": "消耗品費",
                    }
                ],
                (per_line.get("credit_card_statement") or {}).get("changes"),
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_invalid_teacher_file_fails_closed(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_bootstrap_invalid_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            invalid_path = repo_root / "teacher" / "invalid.csv"
            invalid_path.parent.mkdir(parents=True, exist_ok=True)
            invalid_path.write_text("a,b\n", encoding="cp932", newline="")

            rc, output = _run_register(
                self.register_module,
                repo_root,
                client_id="C_BOOTSTRAP_INVALID",
                teacher_relpath="teacher/invalid.csv",
            )

            self.assertEqual(2, rc, msg=output)
            self.assertIn("Failed to bootstrap category_overrides.json from teacher file.", output)
            self.assertFalse((repo_root / "clients" / "C_BOOTSTRAP_INVALID").exists())
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_valid_teacher_with_zero_candidates_succeeds_without_changes(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_bootstrap_zero_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            _write_yayoi_rows(
                repo_root / "teacher" / "zero.csv",
                [
                    _teacher_row("LUNCH ONLYONCE", "交際費"),
                    _teacher_row("UNKNOWN STORE", "交際費"),
                ],
            )

            rc, output = _run_register(
                self.register_module,
                repo_root,
                client_id="C_BOOTSTRAP_ZERO",
                teacher_relpath="teacher/zero.csv",
            )

            self.assertEqual(0, rc, msg=output)
            self.assertEqual(
                {"target_account": "旅費交通費", "target_tax_division": "課対仕入内10%適格"},
                _override_row(repo_root, "C_BOOTSTRAP_ZERO", "receipt", "food"),
            )
            manifest = _load_client_registration_manifest(repo_root, "C_BOOTSTRAP_ZERO")
            bootstrap = manifest.get("category_override_bootstrap") or {}
            self.assertEqual("no_changes", bootstrap.get("status"))
            self.assertEqual(2, int(bootstrap.get("row_count") or 0))
            self.assertEqual(1, int(bootstrap.get("clear_rows") or 0))
            self.assertEqual(0, int(bootstrap.get("ambiguous_rows") or 0))
            self.assertEqual(1, int(bootstrap.get("none_rows") or 0))
            self.assertEqual(0, int((((bootstrap.get("per_line") or {}).get("receipt") or {}).get("applied_count")) or 0))
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_bank_statement_line_with_teacher_fails_cleanly(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_bootstrap_bank_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_template(self.real_repo_root, repo_root)
            _prepare_shared_assets(repo_root)
            _write_yayoi_rows(
                repo_root / "teacher" / "bank.csv",
                [_teacher_row("LUNCH TOKYO", "交際費"), _teacher_row("MEAL OSAKA", "交際費")],
            )

            rc, output = _run_register(
                self.register_module,
                repo_root,
                client_id="C_BOOTSTRAP_BANK",
                line="bank_statement",
                teacher_relpath="teacher/bank.csv",
            )

            self.assertEqual(1, rc, msg=output)
            self.assertIn("--category-override-teacher-path is unsupported", output)
            self.assertFalse((repo_root / "clients" / "C_BOOTSTRAP_BANK").exists())
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_denylisted_top_account_is_rejected(self) -> None:
        with self.subTest("analysis"):
            repo_root = self.test_tmp_root / f"client_registration_bootstrap_deny_{uuid4().hex}"
            repo_root.mkdir(parents=True, exist_ok=False)
            try:
                _prepare_shared_assets(repo_root)
                teacher_path = repo_root / "teacher" / "deny.csv"
                _write_yayoi_rows(
                    teacher_path,
                    [
                        _teacher_row("LUNCH TOKYO", "現金"),
                        _teacher_row("MEAL OSAKA", "現金"),
                    ],
                )

                analysis = analyze_category_override_teacher(
                    teacher_path=teacher_path,
                    lexicon_path=repo_root / "lexicon" / "lexicon.json",
                )
                self.assertEqual({}, analysis.candidates_by_category)
            finally:
                shutil.rmtree(repo_root, ignore_errors=True)

    def test_tie_non_strict_plurality_is_rejected(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_bootstrap_tie_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_shared_assets(repo_root)
            teacher_path = repo_root / "teacher" / "tie.csv"
            _write_yayoi_rows(
                teacher_path,
                [
                    _teacher_row("LUNCH TOKYO", "交際費"),
                    _teacher_row("MEAL OSAKA", "交際費"),
                    _teacher_row("LUNCH NAGOYA", "会議費"),
                    _teacher_row("MEAL KYOTO", "会議費"),
                ],
            )

            analysis = analyze_category_override_teacher(
                teacher_path=teacher_path,
                lexicon_path=repo_root / "lexicon" / "lexicon.json",
            )
            self.assertEqual({}, analysis.candidates_by_category)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_p_majority_below_threshold_is_rejected(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_bootstrap_pmajority_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_shared_assets(repo_root)
            teacher_path = repo_root / "teacher" / "pmajority.csv"
            _write_yayoi_rows(
                teacher_path,
                [
                    _teacher_row("LUNCH TOKYO", "交際費"),
                    _teacher_row("MEAL OSAKA", "交際費"),
                    _teacher_row("LUNCH NAGOYA", "会議費"),
                    _teacher_row("MEAL KYOTO", "消耗品費"),
                    _teacher_row("LUNCH KOBE", "旅費交通費"),
                    _teacher_row("MEAL FUKUOKA", "雑費"),
                ],
            )

            analysis = analyze_category_override_teacher(
                teacher_path=teacher_path,
                lexicon_path=repo_root / "lexicon" / "lexicon.json",
            )
            self.assertEqual({}, analysis.candidates_by_category)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_ambiguous_matches_are_counted_and_included_in_aggregation(self) -> None:
        repo_root = self.test_tmp_root / f"client_registration_bootstrap_ambiguous_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_shared_assets(repo_root)
            teacher_path = repo_root / "teacher" / "ambiguous.txt"
            _write_yayoi_rows(
                teacher_path,
                [
                    _teacher_row("ALPHA MOVE", "旅費交通費"),
                    _teacher_row("ALPHA BRAVO", "旅費交通費"),
                ],
            )

            analysis = analyze_category_override_teacher(
                teacher_path=teacher_path,
                lexicon_path=repo_root / "lexicon" / "lexicon.json",
            )
            candidate = analysis.candidates_by_category.get("travel")
            self.assertIsNotNone(candidate)
            self.assertEqual(2, analysis.row_count)
            self.assertEqual(1, analysis.clear_rows)
            self.assertEqual(1, analysis.ambiguous_rows)
            self.assertEqual(0, analysis.none_rows)
            self.assertEqual("旅費交通費", candidate.top_account if candidate is not None else "")
            self.assertEqual(2, candidate.matched_rows if candidate is not None else 0)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
