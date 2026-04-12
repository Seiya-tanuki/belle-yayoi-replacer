from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4


SPEC_FILES = [
    "FILE_LAYOUT.md",
    "REPLACER_SPEC.md",
    "CLIENT_CACHE_SPEC.md",
    "LEXICON_PENDING_SPEC.md",
    "CATEGORY_OVERRIDES_SPEC.md",
]

SKILL_DIRS = [
    "yayoi-replacer",
    "client-register",
    "client-cache-builder",
    "lexicon-apply",
    "lexicon-extract",
    "export-lexicon-review-pack",
]


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _write_valid_shared_tax_config(
    repo_root: Path,
    client_id: str,
    *,
    bookkeeping_mode: str = "tax_excluded",
    enabled: bool | None = None,
) -> None:
    if enabled is None:
        enabled = bookkeeping_mode == "tax_excluded"
    _write_text(
        repo_root / "clients" / client_id / "config" / "yayoi_tax_config.json",
        json.dumps(
            {
                "schema": "belle.yayoi_tax_config.v1",
                "version": "1.0",
                "enabled": enabled,
                "bookkeeping_mode": bookkeeping_mode,
                "rounding_mode": "floor",
            },
            ensure_ascii=False,
        ),
    )


def _minimal_receipt_replacer_config_json(*, include_tax_sections: bool = True) -> str:
    payload = {
        "schema": "belle.replacer_config.v1",
        "version": "1.16",
        "csv_contract": {"dummy_summary_exact": "##DUMMY_OCR_UNREADABLE##"},
    }
    if include_tax_sections:
        payload["tax_division_thresholds"] = {
            "t_number_x_category_target_account": {"min_count": 2, "min_p_majority": 0.75},
            "t_number_target_account": {"min_count": 3, "min_p_majority": 0.7},
            "vendor_key_target_account": {"min_count": 3, "min_p_majority": 0.7},
            "category_target_account": {"min_count": 3, "min_p_majority": 0.7},
            "global_target_account": {"min_count": 3, "min_p_majority": 0.7},
        }
        payload["tax_division_confidence"] = {
            "t_number_x_category_target_account_strength": 0.97,
            "t_number_target_account_strength": 0.95,
            "vendor_key_target_account_strength": 0.85,
            "category_target_account_strength": 0.65,
            "global_target_account_strength": 0.55,
            "category_default_strength": 0.55,
            "global_fallback_strength": 0.35,
            "learned_weight_multiplier": 0.85,
        }
    return json.dumps(payload, ensure_ascii=False)


def _minimal_credit_card_line_config_json(*, include_tax_sections: bool = True) -> str:
    payload = {
        "schema": "belle.credit_card_line_config.v1",
        "version": "0.2",
        "placeholder_account_name": "仮払金",
        "target_payable_placeholder_names": ["未払金"],
        "thresholds": {
            "merchant_key_account": {"min_count": 3, "min_p_majority": 0.9},
            "file_level_card_inference": {"min_votes": 3, "min_p_majority": 0.9},
        },
        "teacher_extraction": {
            "canonical_payable_thresholds": {"min_count": 3, "min_p_majority": 0.9}
        },
        "candidate_extraction": {
            "min_total_count": 5,
            "min_unique_merchants": 3,
            "min_unique_counter_accounts": 2,
        },
    }
    if include_tax_sections:
        payload["tax_division_thresholds"] = {
            "merchant_key_target_account_exact": {"min_count": 3, "min_p_majority": 0.9},
            "merchant_key_target_account_partial": {"min_count": 3, "min_p_majority": 0.9},
        }
    return json.dumps(payload, ensure_ascii=False)


def _write_credit_card_template_config(
    repo_root: Path,
    *,
    config_obj: dict | None = None,
    include_tax_sections: bool = True,
) -> None:
    if config_obj is None:
        config_obj = json.loads(_minimal_credit_card_line_config_json(include_tax_sections=include_tax_sections))
    _write_text(
        repo_root / "clients" / "TEMPLATE" / "lines" / "credit_card_statement" / "config" / "credit_card_line_config.json",
        json.dumps(config_obj, ensure_ascii=False),
    )


def _write_minimal_lexicon_with_category(repo_root: Path, category_key: str = "known_a") -> None:
    _write_text(
        repo_root / "lexicon" / "lexicon.json",
        json.dumps(
            {
                "schema": "belle.lexicon.v1",
                "version": "test",
                "categories": [
                    {
                        "id": 1,
                        "key": category_key,
                        "label": "Known A",
                        "kind": "expense",
                        "precision_hint": 0.9,
                        "deprecated": False,
                        "negative_terms": {"n0": [], "n1": []},
                    }
                ],
                "term_rows": [],
            },
            ensure_ascii=False,
        ),
    )


def _write_mode_aware_defaults(repo_root: Path, line_id: str, text: str) -> None:
    defaults_dir = repo_root / "defaults" / line_id
    _write_text(defaults_dir / "category_defaults_tax_excluded.json", text)
    _write_text(defaults_dir / "category_defaults_tax_included.json", text)


def _load_system_diagnose_module(real_repo_root: Path):
    script_path = (
        real_repo_root
        / ".agents"
        / "skills"
        / "system-diagnose"
        / "scripts"
        / "system_diagnose.py"
    )
    spec = importlib.util.spec_from_file_location(f"system_diagnose_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _prepare_common_repo_layout(repo_root: Path, line_id: str) -> None:
    for name in SPEC_FILES:
        _write_text(repo_root / "spec" / name, f"# {name}\n")
    for name in SKILL_DIRS:
        (repo_root / ".agents" / "skills" / name).mkdir(parents=True, exist_ok=True)
    (repo_root / "clients" / "TEMPLATE" / "lines" / line_id / "artifacts" / "ingest").mkdir(
        parents=True,
        exist_ok=True,
    )
    _write_valid_shared_tax_config(repo_root, "TEMPLATE")
    if line_id == "bank_statement":
        bank_template_root = repo_root / "clients" / "TEMPLATE" / "lines" / "bank_statement"
        for rel in [
            Path("inputs/training/ocr_kari_shiwake"),
            Path("inputs/training/reference_yayoi"),
            Path("inputs/kari_shiwake"),
            Path("artifacts/ingest/training_ocr"),
            Path("artifacts/ingest/training_reference"),
            Path("artifacts/ingest/kari_shiwake"),
        ]:
            (bank_template_root / rel).mkdir(parents=True, exist_ok=True)
        _write_text(
            bank_template_root / "config" / "bank_line_config.json",
            json.dumps({"schema": "belle.bank_line_config.v0", "version": "0.1"}, ensure_ascii=False),
        )
        for module_name in [
            "build_bank_cache.py",
            "bank_replacer.py",
            "bank_cache.py",
            "bank_pairing.py",
        ]:
            _write_text(repo_root / "belle" / module_name, "# test fixture\n")
    if line_id == "credit_card_statement":
        card_template_root = repo_root / "clients" / "TEMPLATE" / "lines" / "credit_card_statement"
        for rel in [
            Path("inputs/kari_shiwake"),
            Path("inputs/ledger_ref"),
            Path("artifacts/ingest/kari_shiwake"),
            Path("artifacts/ingest/ledger_ref"),
            Path("artifacts/cache"),
            Path("outputs/runs"),
        ]:
            (card_template_root / rel).mkdir(parents=True, exist_ok=True)
        _write_text(
            card_template_root / "config" / "credit_card_line_config.json",
            _minimal_credit_card_line_config_json(),
        )


def _prepare_receipt_assets(repo_root: Path, *, with_lexicon: bool) -> None:
    _write_mode_aware_defaults(repo_root, "receipt", "{}\n")
    _write_text(
        repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json",
        _minimal_receipt_replacer_config_json(),
    )
    if with_lexicon:
        _write_minimal_lexicon_with_category(repo_root)


def _prepare_bank_client(
    repo_root: Path,
    client_id: str,
    *,
    teacher_count: int,
    target_count: int,
    ocr_file_count: int,
    with_config: bool = True,
    with_cache: bool = False,
    with_shared_tax_config: bool = True,
) -> None:
    line_root = repo_root / "clients" / client_id / "lines" / "bank_statement"
    ocr_dir = line_root / "inputs" / "training" / "ocr_kari_shiwake"
    ref_dir = line_root / "inputs" / "training" / "reference_yayoi"
    target_dir = line_root / "inputs" / "kari_shiwake"
    ocr_dir.mkdir(parents=True, exist_ok=True)
    ref_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    for idx in range(ocr_file_count):
        _write_text(ocr_dir / f"ocr_{idx + 1}.csv", "dummy\n")
    for idx in range(teacher_count):
        _write_text(ref_dir / f"teacher_{idx + 1}.csv", "dummy\n")
    for idx in range(target_count):
        _write_text(target_dir / f"target_{idx + 1}.csv", "dummy\n")

    if with_config:
        _write_text(
            line_root / "config" / "bank_line_config.json",
            json.dumps({"schema": "belle.bank_line_config.v0", "version": "0.1"}, ensure_ascii=False),
        )
    if with_cache:
        _write_text(
            line_root / "artifacts" / "cache" / "client_cache.json",
            json.dumps({"updated_at": "2026-02-20T00:00:00Z"}, ensure_ascii=False),
        )
    if with_shared_tax_config:
        _write_valid_shared_tax_config(repo_root, client_id)


def _make_fake_run_command(module):
    command_outputs = {
        "git rev-parse --is-inside-work-tree": (0, "true\n"),
        "git rev-parse HEAD": (0, "0123456789abcdef\n"),
        "git status --porcelain=v1 -uall": (0, ""),
        "git --version": (0, "git version 2.47.0\n"),
        "python --version": (0, "Python 3.12.1\n"),
        'python -c "import sys; print(sys.executable); print(sys.version)"': (
            0,
            "C:\\Python312\\python.exe\n3.12.1\n",
        ),
        "python tools/bom_guard.py --check": (0, "UTF-8 BOM files: 0\n"),
        "python -m compileall belle tools .agents/skills tests": (0, "ok\n"),
        "python tools/run_tests.py": (0, "ok\n"),
        'python -c "import codecs; codecs.lookup(\'cp932\'); print(\'cp932 OK\')"': (0, "cp932 OK\n"),
        "py -0p": (0, " -V:3.12 * C:\\Python312\\python.exe\n"),
        "git config --get core.hooksPath": (0, ".githooks\n"),
        "where.exe python": (1, ""),
        "python3 --version": (0, "Python 3.12.1\n"),
    }

    def _fake_run(command: str, cwd: Path, timeout_sec: int = 30):
        returncode, stdout = command_outputs.get(command, (0, ""))
        return module.CommandResult(
            command=command,
            returncode=returncode,
            stdout=stdout,
            stderr="",
            timed_out=False,
            error=None,
            duration_sec=0.001,
        )

    return _fake_run


def _run_main(module, repo_root: Path, line_id: str) -> tuple[int, str]:
    fake_script_path = (
        repo_root
        / ".agents"
        / "skills"
        / "system-diagnose"
        / "scripts"
        / "system_diagnose.py"
    )
    fake_script_path.parent.mkdir(parents=True, exist_ok=True)
    module.__file__ = str(fake_script_path)
    fake_run_command = _make_fake_run_command(module)
    output_buffer = io.StringIO()
    with mock.patch.object(module, "_run_command", side_effect=fake_run_command):
        with mock.patch.object(sys, "argv", ["system_diagnose.py", "--line", line_id]):
            with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(output_buffer):
                rc = module.main()
    return rc, output_buffer.getvalue()


def _read_latest_report(repo_root: Path) -> str:
    latest_path = repo_root / "exports" / "system_diagnose" / "LATEST.txt"
    if not latest_path.exists():
        return ""
    report_name = latest_path.read_text(encoding="utf-8").strip()
    report_path = repo_root / "exports" / "system_diagnose" / report_name
    if not report_path.exists():
        return ""
    return report_path.read_text(encoding="utf-8")


class SystemDiagnoseLineAwareTests(unittest.TestCase):
    def setUp(self) -> None:
        self.real_repo_root = Path(__file__).resolve().parents[1]
        self.test_tmp_root = self.real_repo_root / ".tmp"
        self.test_tmp_root.mkdir(parents=True, exist_ok=True)

    def test_bank_statement_go_without_bank_receipt_assets(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_bank_go_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "bank_statement")
            _prepare_bank_client(
                repo_root,
                "C_BANK_GO",
                teacher_count=1,
                target_count=1,
                ocr_file_count=0,
                with_config=True,
                with_cache=False,
            )
            self.assertFalse((repo_root / "lexicon" / "bank_statement" / "lexicon.json").exists())
            self.assertFalse((repo_root / "defaults" / "bank_statement" / "category_defaults.json").exists())
            self.assertFalse(
                (repo_root / "rulesets" / "bank_statement" / "replacer_config_v1_15.json").exists()
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "bank_statement")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn("C41 clients/TEMPLATE/config/yayoi_tax_config.json exists", report)
            self.assertIn("C42 clients/TEMPLATE/config/yayoi_tax_config.json is valid", report)
            self.assertNotIn("lexicon/bank_statement/lexicon.json exists", report)
            self.assertNotIn("defaults/bank_statement/category_defaults.json exists", report)
            self.assertNotIn("rulesets/bank_statement/replacer_config_v1_15.json", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_bank_statement_go_with_empty_teacher_and_target_dirs(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_bank_empty_input_dirs_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "bank_statement")
            _prepare_bank_client(
                repo_root,
                "C_BANK_EMPTY",
                teacher_count=0,
                target_count=0,
                ocr_file_count=0,
                with_config=True,
                with_cache=False,
            )
            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "bank_statement")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn("C18 bank_statement teacher reference directory exists", report)
            self.assertIn("C19 bank_statement target kari_shiwake directory exists", report)
            self.assertNotIn("file count is exactly 1", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_receipt_still_enforces_lexicon_requirement(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_receipt_ng_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "receipt")
            _prepare_receipt_assets(repo_root, with_lexicon=False)

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "receipt")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn("C1 lexicon/lexicon.json exists", report)
            self.assertIn("| C1 lexicon/lexicon.json exists | FAIL |", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_missing_existing_client_shared_tax_config_is_warn_only(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_shared_tax_missing_warn_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "bank_statement")
            (repo_root / "clients" / "C_MISSING_SHARED").mkdir(parents=True, exist_ok=True)

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "bank_statement")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "| S10 shared Yayoi tax config presence for non-TEMPLATE clients (warn-only when missing) | FAIL |",
                report,
            )
            self.assertIn("missing shared tax config for 1 client(s): C_MISSING_SHARED", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_invalid_present_shared_tax_config_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_shared_tax_invalid_fail_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "bank_statement")
            _write_text(
                repo_root / "clients" / "C_INVALID" / "config" / "yayoi_tax_config.json",
                json.dumps(
                    {
                        "schema": "belle.yayoi_tax_config.v1",
                        "version": "1.0",
                        "enabled": True,
                        "bookkeeping_mode": "broken_mode",
                        "rounding_mode": "floor",
                    },
                    ensure_ascii=False,
                ),
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "bank_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "| C43 shared Yayoi tax config is valid for non-TEMPLATE clients when present | FAIL |",
                report,
            )
            self.assertIn("C_INVALID:", report)
            self.assertIn("bookkeeping_mode", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_template_shared_tax_config_semantic_inconsistency_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_template_shared_tax_inconsistent_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "bank_statement")
            _write_text(
                repo_root / "clients" / "TEMPLATE" / "config" / "yayoi_tax_config.json",
                json.dumps(
                    {
                        "schema": "belle.yayoi_tax_config.v1",
                        "version": "1.0",
                        "enabled": False,
                        "bookkeeping_mode": "tax_excluded",
                        "rounding_mode": "floor",
                    },
                    ensure_ascii=False,
                ),
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "bank_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "| C42B clients/TEMPLATE/config/yayoi_tax_config.json matches the bookkeeping-mode bootstrap policy | FAIL |",
                report,
            )
            self.assertIn("present_inconsistent", report)
            self.assertIn("expected enabled=true for bookkeeping_mode=tax_excluded", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_non_template_shared_tax_config_semantic_inconsistency_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_client_shared_tax_inconsistent_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "bank_statement")
            _write_valid_shared_tax_config(repo_root, "C_OK", bookkeeping_mode="tax_excluded")
            _write_valid_shared_tax_config(
                repo_root,
                "C_BAD",
                bookkeeping_mode="tax_included",
                enabled=True,
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "bank_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "| C43B shared Yayoi tax config matches the bookkeeping-mode bootstrap policy for non-TEMPLATE clients when present | FAIL |",
                report,
            )
            self.assertIn("valid_mode_consistent", report)
            self.assertIn("C_OK(enabled=True, bookkeeping_mode=tax_excluded, rounding_mode=floor)", report)
            self.assertIn("C_BAD(enabled=True, bookkeeping_mode=tax_included, rounding_mode=floor)", report)
            self.assertIn("expected enabled=false for bookkeeping_mode=tax_included", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_valid_present_shared_tax_config_evidence_lists_modes(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_shared_tax_valid_evidence_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "bank_statement")
            _write_valid_shared_tax_config(repo_root, "C_VALID", bookkeeping_mode="tax_excluded")
            _write_valid_shared_tax_config(repo_root, "C_INCLUDED", bookkeeping_mode="tax_included")

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "bank_statement")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "C_VALID(enabled=True, bookkeeping_mode=tax_excluded, rounding_mode=floor)",
                report,
            )
            self.assertIn(
                "C_INCLUDED(enabled=False, bookkeeping_mode=tax_included, rounding_mode=floor)",
                report,
            )
            self.assertIn("valid_mode_consistent", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_receipt_ruleset_missing_tax_sections_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_receipt_tax_sections_fail_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "receipt")
            _write_mode_aware_defaults(repo_root, "receipt", "{}\n")
            _write_minimal_lexicon_with_category(repo_root)
            _write_text(
                repo_root / "rulesets" / "receipt" / "replacer_config_v1_15.json",
                _minimal_receipt_replacer_config_json(include_tax_sections=False),
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "receipt")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "| C44 active receipt replacer config contains required tax_division_thresholds and tax_division_confidence sections | FAIL |",
                report,
            )
            self.assertIn("tax_division_thresholds=missing_or_invalid", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_receipt_old_shape_category_overrides_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_receipt_old_override_fail_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "receipt")
            _prepare_receipt_assets(repo_root, with_lexicon=True)
            _write_text(
                repo_root / "clients" / "C_RECEIPT_BAD" / "lines" / "receipt" / "config" / "category_overrides.json",
                json.dumps(
                    {
                        "schema": "belle.category_overrides.v2",
                        "client_id": "C_RECEIPT_BAD",
                        "generated_at": "2026-04-09T00:00:00Z",
                        "note_ja": "old shape",
                        "overrides": {
                            "known_a": {
                                "debit_account": "旅費交通費",
                                "debit_tax_division": "課対仕入内10%適格",
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "receipt")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "| C45 receipt category_overrides.json follows the target_account/target_tax_division row contract when present | FAIL |",
                report,
            )
            self.assertIn("invalid_present", report)
            self.assertIn("row_missing_target_keys=1", report)
            self.assertIn("row_extra_keys=1", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_receipt_missing_optional_override_and_valid_present_override_are_distinguished(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_receipt_override_states_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "receipt")
            _prepare_receipt_assets(repo_root, with_lexicon=True)
            (repo_root / "clients" / "C_RECEIPT_MISSING" / "lines" / "receipt").mkdir(parents=True, exist_ok=True)
            _write_text(
                repo_root / "clients" / "C_RECEIPT_VALID" / "lines" / "receipt" / "config" / "category_overrides.json",
                json.dumps(
                    {
                        "schema": "belle.category_overrides.v2",
                        "client_id": "C_RECEIPT_VALID",
                        "generated_at": "2026-04-09T00:00:00Z",
                        "note_ja": "valid shape",
                        "overrides": {
                            "known_a": {
                                "target_account": "旅費交通費",
                                "target_tax_division": "課対仕入内10%適格",
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "receipt")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn("valid_present: layout=line; path=clients/C_RECEIPT_VALID/lines/receipt/config/category_overrides.json", report)
            self.assertIn("optional_missing: layout=line; path=clients/C_RECEIPT_MISSING/lines/receipt/config/category_overrides.json", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_receipt_root_level_override_is_ignored_without_line_root(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_receipt_root_override_ignored_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "receipt")
            _prepare_receipt_assets(repo_root, with_lexicon=True)
            _write_text(
                repo_root / "clients" / "C_RECEIPT_LEGACY" / "config" / "category_overrides.json",
                json.dumps(
                    {
                        "schema": "belle.category_overrides.v2",
                        "client_id": "C_RECEIPT_LEGACY",
                        "generated_at": "2026-04-09T00:00:00Z",
                        "note_ja": "legacy root shape",
                        "overrides": {
                            "known_a": {
                                "target_account": "旅費交通費",
                                "target_tax_division": "課対仕入内10%適格",
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "receipt")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn("N/A: no receipt category_overrides targets detected", report)
            self.assertNotIn("clients/C_RECEIPT_LEGACY/config/category_overrides.json", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_credit_card_template_config_missing_tax_sections_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_cc_tax_sections_fail_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "credit_card_statement")
            _write_credit_card_template_config(repo_root, include_tax_sections=False)
            _write_minimal_lexicon_with_category(repo_root)
            _write_mode_aware_defaults(repo_root, "credit_card_statement", "{}\n")

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "credit_card_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "| C47 clients/TEMPLATE/lines/credit_card_statement/config/credit_card_line_config.json contains required credit-card v2 payable/canonical/tax config sections | FAIL |",
                report,
            )
            self.assertIn("tax_division_thresholds=missing_or_invalid", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_credit_card_template_config_missing_target_payable_placeholder_names_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_cc_missing_payable_placeholders_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "credit_card_statement")
            config_obj = json.loads(_minimal_credit_card_line_config_json())
            config_obj.pop("target_payable_placeholder_names", None)
            _write_credit_card_template_config(repo_root, config_obj=config_obj)
            _write_minimal_lexicon_with_category(repo_root)
            _write_mode_aware_defaults(repo_root, "credit_card_statement", "{}\n")

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "credit_card_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "target_payable_placeholder_names is required and must be a list of non-blank strings",
                report,
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_credit_card_template_config_blank_only_target_payable_placeholder_names_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_cc_blank_payable_placeholders_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "credit_card_statement")
            config_obj = json.loads(_minimal_credit_card_line_config_json())
            config_obj["target_payable_placeholder_names"] = [" ", ""]
            _write_credit_card_template_config(repo_root, config_obj=config_obj)
            _write_minimal_lexicon_with_category(repo_root)
            _write_mode_aware_defaults(repo_root, "credit_card_statement", "{}\n")

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "credit_card_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn("target_payable_placeholder_names must contain at least one non-blank value", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_credit_card_template_config_missing_canonical_payable_thresholds_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_cc_missing_canonical_thresholds_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "credit_card_statement")
            config_obj = json.loads(_minimal_credit_card_line_config_json())
            teacher_extraction = dict(config_obj.get("teacher_extraction") or {})
            teacher_extraction.pop("canonical_payable_thresholds", None)
            config_obj["teacher_extraction"] = teacher_extraction
            _write_credit_card_template_config(repo_root, config_obj=config_obj)
            _write_minimal_lexicon_with_category(repo_root)
            _write_mode_aware_defaults(repo_root, "credit_card_statement", "{}\n")

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "credit_card_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "teacher_extraction.canonical_payable_thresholds is required and must be an object",
                report,
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_credit_card_template_config_invalid_canonical_payable_min_count_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_cc_invalid_canonical_min_count_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "credit_card_statement")
            config_obj = json.loads(_minimal_credit_card_line_config_json())
            config_obj["teacher_extraction"]["canonical_payable_thresholds"]["min_count"] = 0
            _write_credit_card_template_config(repo_root, config_obj=config_obj)
            _write_minimal_lexicon_with_category(repo_root)
            _write_mode_aware_defaults(repo_root, "credit_card_statement", "{}\n")

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "credit_card_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "teacher_extraction.canonical_payable_thresholds.min_count must be an integer >= 1",
                report,
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_credit_card_template_config_invalid_canonical_payable_min_p_majority_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_cc_invalid_canonical_min_p_majority_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "credit_card_statement")
            config_obj = json.loads(_minimal_credit_card_line_config_json())
            config_obj["teacher_extraction"]["canonical_payable_thresholds"]["min_p_majority"] = 0
            _write_credit_card_template_config(repo_root, config_obj=config_obj)
            _write_minimal_lexicon_with_category(repo_root)
            _write_mode_aware_defaults(repo_root, "credit_card_statement", "{}\n")

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "credit_card_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "teacher_extraction.canonical_payable_thresholds.min_p_majority must be > 0 and <= 1",
                report,
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_credit_card_template_config_valid_v2_contract_passes(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_cc_valid_v2_contract_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "credit_card_statement")
            _write_credit_card_template_config(repo_root)
            _write_minimal_lexicon_with_category(repo_root)
            _write_mode_aware_defaults(repo_root, "credit_card_statement", "{}\n")

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "credit_card_statement")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "| C47 clients/TEMPLATE/lines/credit_card_statement/config/credit_card_line_config.json contains required credit-card v2 payable/canonical/tax config sections | PASS |",
                report,
            )
            self.assertIn("target_payable_placeholder_names=ok", report)
            self.assertIn("teacher_extraction.canonical_payable_thresholds=ok", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_credit_card_old_shape_category_overrides_is_hard_failure(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_cc_old_override_fail_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "credit_card_statement")
            _write_minimal_lexicon_with_category(repo_root)
            _write_mode_aware_defaults(repo_root, "credit_card_statement", "{}\n")
            _write_text(
                repo_root
                / "clients"
                / "C_CC_BAD"
                / "lines"
                / "credit_card_statement"
                / "config"
                / "category_overrides.json",
                json.dumps(
                    {
                        "schema": "belle.category_overrides.v2",
                        "client_id": "C_CC_BAD",
                        "generated_at": "2026-04-09T00:00:00Z",
                        "note_ja": "old shape",
                        "overrides": {
                            "known_a": {
                                "debit_account": "通信費",
                                "debit_tax_division": "課対仕入内10%適格",
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "credit_card_statement")

            self.assertNotEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "| C48 credit_card_statement category_overrides.json follows the target_account/target_tax_division row contract when present | FAIL |",
                report,
            )
            self.assertIn("invalid_present", report)
            self.assertIn("row_missing_target_keys=1", report)
            self.assertIn("row_extra_keys=1", report)
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)

    def test_credit_card_missing_optional_override_and_valid_present_override_are_distinguished(self) -> None:
        repo_root = self.test_tmp_root / f"diagnose_cc_override_states_{uuid4().hex}"
        repo_root.mkdir(parents=True, exist_ok=False)
        try:
            _prepare_common_repo_layout(repo_root, "credit_card_statement")
            _write_minimal_lexicon_with_category(repo_root)
            _write_mode_aware_defaults(repo_root, "credit_card_statement", "{}\n")
            (
                repo_root / "clients" / "C_CC_MISSING" / "lines" / "credit_card_statement"
            ).mkdir(parents=True, exist_ok=True)
            _write_text(
                repo_root
                / "clients"
                / "C_CC_VALID"
                / "lines"
                / "credit_card_statement"
                / "config"
                / "category_overrides.json",
                json.dumps(
                    {
                        "schema": "belle.category_overrides.v2",
                        "client_id": "C_CC_VALID",
                        "generated_at": "2026-04-09T00:00:00Z",
                        "note_ja": "valid shape",
                        "overrides": {
                            "known_a": {
                                "target_account": "通信費",
                                "target_tax_division": "課対仕入内10%適格",
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
            )

            module = _load_system_diagnose_module(self.real_repo_root)
            rc, output = _run_main(module, repo_root, "credit_card_statement")

            self.assertEqual(0, rc, msg=output)
            report = _read_latest_report(repo_root)
            self.assertIn(
                "valid_present: layout=line; path=clients/C_CC_VALID/lines/credit_card_statement/config/category_overrides.json",
                report,
            )
            self.assertIn(
                "optional_missing: layout=line; path=clients/C_CC_MISSING/lines/credit_card_statement/config/category_overrides.json",
                report,
            )
        finally:
            shutil.rmtree(repo_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
