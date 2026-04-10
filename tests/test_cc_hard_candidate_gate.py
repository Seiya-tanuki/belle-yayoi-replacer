from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

from belle.build_cc_cache import ensure_cc_client_cache_updated, load_credit_card_line_config
from belle.cc_replacer import replace_credit_card_yayoi_csv
from belle.yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_SUMMARY,
)
from belle.yayoi_csv import read_yayoi_csv, token_to_text

PLACEHOLDER_ACCOUNT = "仮払金"
PAYABLE_ACCOUNT = "未払金"
ACCOUNT_TRAVEL = "旅費交通費"
ACCOUNT_SUPPLIES = "消耗品費"


def _line_root(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "lines" / "credit_card_statement"


def _load_replacer_script_module(repo_root: Path):
    script_path = repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
    spec = importlib.util.spec_from_file_location(f"run_yayoi_replacer_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _write_min_shared_assets(repo_root: Path) -> None:
    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    lexicon_path.parent.mkdir(parents=True, exist_ok=True)
    lexicon_path.write_text(
        json.dumps(
            {
                "schema": "belle.lexicon.v1",
                "version": "test",
                "categories": [],
                "term_rows": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    defaults_payload = {
        "schema": "belle.category_defaults.v2",
        "version": "test",
        "defaults": {},
        "global_fallback": {
            "target_account": PLACEHOLDER_ACCOUNT,
            "target_tax_division": "",
            "confidence": 0.35,
            "priority": "HIGH",
            "reason_code": "global_fallback",
        },
    }
    defaults_dir = repo_root / "defaults" / "credit_card_statement"
    defaults_dir.mkdir(parents=True, exist_ok=True)
    for filename in ("category_defaults_tax_excluded.json", "category_defaults_tax_included.json"):
        (defaults_dir / filename).write_text(
            json.dumps(defaults_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _write_cc_config(
    line_root: Path,
    *,
    candidate_extraction: dict[str, object] | None = None,
    file_min_votes: int = 1,
    file_min_p_majority: float = 0.5,
) -> None:
    cfg = {
        "schema": "belle.credit_card_line_config.v0",
        "version": "0.1",
        "placeholder_account_name": PLACEHOLDER_ACCOUNT,
        "payable_account_name": PAYABLE_ACCOUNT,
        "training": {"exclude_counter_accounts": []},
        "thresholds": {
            "merchant_key_account": {"min_count": 1, "min_p_majority": 0.5},
            "merchant_key_payable_subaccount": {"min_count": 1, "min_p_majority": 0.5},
            "file_level_card_inference": {
                "min_votes": int(file_min_votes),
                "min_p_majority": float(file_min_p_majority),
            },
        },
        "candidate_extraction": candidate_extraction
        if candidate_extraction is not None
        else {
            "min_total_count": 5,
            "min_unique_merchants": 3,
            "min_unique_counter_accounts": 2,
            "manual_allow": [],
        },
    }
    cfg_path = line_root / "config" / "credit_card_line_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_learning_row(*, summary: str, counter_account: str, payable_subaccount: str) -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/03/01"
    cols[COL_DEBIT_ACCOUNT] = counter_account
    cols[COL_DEBIT_SUBACCOUNT] = ""
    cols[COL_DEBIT_AMOUNT] = "1000"
    cols[COL_CREDIT_ACCOUNT] = PAYABLE_ACCOUNT
    cols[COL_CREDIT_SUBACCOUNT] = payable_subaccount
    cols[COL_CREDIT_AMOUNT] = "1000"
    cols[COL_SUMMARY] = summary
    return cols


def _build_target_row(*, summary: str, credit_subaccount: str = "") -> list[str]:
    cols = [""] * 25
    cols[COL_DATE] = "2026/03/15"
    cols[COL_DEBIT_ACCOUNT] = PLACEHOLDER_ACCOUNT
    cols[COL_DEBIT_SUBACCOUNT] = ""
    cols[COL_DEBIT_AMOUNT] = "1000"
    cols[COL_CREDIT_ACCOUNT] = PAYABLE_ACCOUNT
    cols[COL_CREDIT_SUBACCOUNT] = credit_subaccount
    cols[COL_CREDIT_AMOUNT] = "1000"
    cols[COL_SUMMARY] = summary
    return cols


def _write_yayoi_rows(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)


def _read_rows(path: Path) -> list[list[str]]:
    csv_obj = read_yayoi_csv(path)
    return [[token_to_text(tok, csv_obj.encoding) for tok in row.tokens] for row in csv_obj.rows]


class CCHardCandidateGateTests(unittest.TestCase):
    def test_candidate_becomes_true_at_5_3_2_thresholds(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_532_OK"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            rows = [
                _build_learning_row(summary="ALPHA MART / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="ALPHA MART / two", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="BETA SHOP / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="GAMMA STORE / one", counter_account=ACCOUNT_SUPPLIES, payable_subaccount="CARD_A"),
                _build_learning_row(summary="GAMMA STORE / two", counter_account=ACCOUNT_SUPPLIES, payable_subaccount="CARD_A"),
            ]
            _write_yayoi_rows(line_root / "inputs" / "ledger_ref" / "ledger_ref.csv", rows)

            cache, _ = ensure_cc_client_cache_updated(repo_root, client_id)

            candidate = (cache.card_subaccount_candidates or {}).get("CARD_A") or {}
            self.assertTrue(bool(candidate.get("is_candidate")))
            self.assertEqual(5, int(candidate.get("total_count") or 0))
            self.assertEqual(3, int(candidate.get("unique_merchants") or 0))
            self.assertEqual(2, int(candidate.get("unique_counter_accounts") or 0))

    def test_candidate_stays_false_when_unique_counter_accounts_is_one(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_COUNTER_ONE"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            rows = [
                _build_learning_row(summary="ALPHA MART / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="ALPHA MART / two", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="BETA SHOP / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="GAMMA STORE / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="GAMMA STORE / two", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
            ]
            _write_yayoi_rows(line_root / "inputs" / "ledger_ref" / "ledger_ref.csv", rows)

            cache, _ = ensure_cc_client_cache_updated(repo_root, client_id)

            candidate = (cache.card_subaccount_candidates or {}).get("CARD_A") or {}
            self.assertFalse(bool(candidate.get("is_candidate")))
            self.assertEqual(5, int(candidate.get("total_count") or 0))
            self.assertEqual(3, int(candidate.get("unique_merchants") or 0))
            self.assertEqual(1, int(candidate.get("unique_counter_accounts") or 0))

    def test_manual_allow_still_forces_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_MANUAL_ALLOW"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(
                line_root,
                candidate_extraction={
                    "min_total_count": 99,
                    "min_unique_merchants": 99,
                    "min_unique_counter_accounts": 99,
                    "manual_allow": ["CARD_MANUAL"],
                },
            )

            rows = [
                _build_learning_row(
                    summary="ALPHA MART / one",
                    counter_account=ACCOUNT_TRAVEL,
                    payable_subaccount="CARD_MANUAL",
                )
            ]
            _write_yayoi_rows(line_root / "inputs" / "ledger_ref" / "ledger_ref.csv", rows)

            cache, _ = ensure_cc_client_cache_updated(repo_root, client_id)

            candidate = (cache.card_subaccount_candidates or {}).get("CARD_MANUAL") or {}
            self.assertTrue(bool(candidate.get("is_candidate")))
            self.assertIn("manual_allow", list(candidate.get("notes") or []))

    def test_default_min_unique_counter_accounts_is_two_when_unspecified(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_DEFAULT_COUNTERS"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(
                line_root,
                candidate_extraction={
                    "min_total_count": 5,
                    "min_unique_merchants": 3,
                    "manual_allow": [],
                },
            )

            config = load_credit_card_line_config(repo_root, client_id)
            self.assertEqual(2, int(((config.get("candidate_extraction") or {}).get("min_unique_counter_accounts")) or 0))

            rows = [
                _build_learning_row(summary="ALPHA MART / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="ALPHA MART / two", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="BETA SHOP / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="GAMMA STORE / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="GAMMA STORE / two", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
            ]
            _write_yayoi_rows(line_root / "inputs" / "ledger_ref" / "ledger_ref.csv", rows)

            cache, _ = ensure_cc_client_cache_updated(repo_root, client_id)
            candidate = (cache.card_subaccount_candidates or {}).get("CARD_A") or {}
            self.assertFalse(bool(candidate.get("is_candidate")))
            self.assertEqual(1, int(candidate.get("unique_counter_accounts") or 0))

    def test_zero_candidates_fail_closed_and_do_not_infer_payable_subaccount(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_ZERO_GATE"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(
                line_root,
                candidate_extraction={
                    "min_total_count": 5,
                    "min_unique_merchants": 3,
                    "min_unique_counter_accounts": 2,
                    "manual_allow": [],
                },
            )

            rows = [
                _build_learning_row(summary="SHOPA / learn", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="SHOPB / learn", counter_account=ACCOUNT_SUPPLIES, payable_subaccount="CARD_A"),
            ]
            _write_yayoi_rows(line_root / "inputs" / "ledger_ref" / "ledger_ref.csv", rows)

            ensure_cc_client_cache_updated(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_target_row(summary="SHOPA / target"),
                    _build_target_row(summary="SHOPB / target"),
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_CC_ZERO_GATE"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            config = load_credit_card_line_config(repo_root, client_id)

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_CC_ZERO_GATE",
            )

            file_inference = manifest.get("file_card_inference") or {}
            self.assertNotEqual("OK", file_inference.get("status"))
            self.assertIsNone(file_inference.get("inferred_payable_subaccount"))
            self.assertEqual(["no_candidates_flagged"], list(file_inference.get("reasons") or []))
            self.assertNotIn("no_candidates_flagged_fallback", list(file_inference.get("reasons") or []))
            self.assertTrue(bool(manifest.get("payable_sub_fill_required_failed")))

            rows_out = _read_rows(out_path)
            self.assertEqual("", rows_out[0][COL_CREDIT_SUBACCOUNT])
            self.assertEqual("", rows_out[1][COL_CREDIT_SUBACCOUNT])

    def test_candidate_success_path_still_returns_ok_when_candidate_exists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_CC_GATE_OK"
            line_root = _line_root(repo_root, client_id)
            _write_cc_config(line_root)

            rows = [
                _build_learning_row(summary="ALPHA MART / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="ALPHA MART / two", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="BETA SHOP / one", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                _build_learning_row(summary="GAMMA STORE / one", counter_account=ACCOUNT_SUPPLIES, payable_subaccount="CARD_A"),
                _build_learning_row(summary="GAMMA STORE / two", counter_account=ACCOUNT_SUPPLIES, payable_subaccount="CARD_A"),
            ]
            _write_yayoi_rows(line_root / "inputs" / "ledger_ref" / "ledger_ref.csv", rows)

            ensure_cc_client_cache_updated(repo_root, client_id)
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            in_path = line_root / "inputs" / "kari_shiwake" / "target.csv"
            _write_yayoi_rows(
                in_path,
                [
                    _build_target_row(summary="ALPHA MART / target"),
                    _build_target_row(summary="BETA SHOP / target"),
                    _build_target_row(summary="GAMMA STORE / target"),
                ],
            )

            run_dir = line_root / "outputs" / "runs" / "R_TEST_CC_GATE_OK"
            run_dir.mkdir(parents=True, exist_ok=True)
            out_path = run_dir / "target_replaced.csv"
            config = load_credit_card_line_config(repo_root, client_id)

            manifest = replace_credit_card_yayoi_csv(
                in_path=in_path,
                out_path=out_path,
                cache_path=cache_path,
                config=config,
                run_dir=run_dir,
                artifact_prefix="target_01_R_TEST_CC_GATE_OK",
            )

            file_inference = manifest.get("file_card_inference") or {}
            self.assertEqual("OK", file_inference.get("status"))
            self.assertEqual("CARD_A", file_inference.get("inferred_payable_subaccount"))
            self.assertFalse(bool(manifest.get("payable_sub_fill_required_failed")))

            rows_out = _read_rows(out_path)
            self.assertEqual(["CARD_A", "CARD_A", "CARD_A"], [row[COL_CREDIT_SUBACCOUNT] for row in rows_out])

    def test_runner_strict_stop_still_applies_when_zero_candidates_block_fill(self) -> None:
        real_repo_root = Path(__file__).resolve().parents[1]
        client_id = "C_CC_ZERO_GATE_RUNNER"
        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            _write_min_shared_assets(temp_repo_root)
            line_root = _line_root(temp_repo_root, client_id)
            _write_cc_config(
                line_root,
                candidate_extraction={
                    "min_total_count": 5,
                    "min_unique_merchants": 3,
                    "min_unique_counter_accounts": 2,
                    "manual_allow": [],
                },
            )

            _write_yayoi_rows(
                line_root / "inputs" / "ledger_ref" / "ledger_ref.csv",
                [
                    _build_learning_row(summary="SHOPA / learn", counter_account=ACCOUNT_TRAVEL, payable_subaccount="CARD_A"),
                    _build_learning_row(summary="SHOPB / learn", counter_account=ACCOUNT_SUPPLIES, payable_subaccount="CARD_A"),
                ],
            )
            _write_yayoi_rows(
                line_root / "inputs" / "kari_shiwake" / "target.csv",
                [
                    _build_target_row(summary="SHOPA / target"),
                    _build_target_row(summary="SHOPB / target"),
                ],
            )

            module = _load_replacer_script_module(real_repo_root)
            module.__file__ = str(
                temp_repo_root / ".agents" / "skills" / "yayoi-replacer" / "scripts" / "run_yayoi_replacer.py"
            )

            buf = io.StringIO()
            with self.assertRaises(SystemExit) as ctx:
                with contextlib.redirect_stdout(buf):
                    with contextlib.redirect_stderr(buf):
                        with mock.patch.object(
                            sys,
                            "argv",
                            [
                                "run_yayoi_replacer.py",
                                "--client",
                                client_id,
                                "--line",
                                "credit_card_statement",
                                "--yes",
                            ],
                        ):
                            module.main()

            self.assertEqual(2, int(ctx.exception.code), msg=buf.getvalue())

            latest_path = line_root / "outputs" / "LATEST.txt"
            self.assertTrue(latest_path.exists(), msg=buf.getvalue())
            run_id = latest_path.read_text(encoding="utf-8").strip()
            run_dir = line_root / "outputs" / "runs" / run_id

            run_manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual("FAIL", run_manifest.get("exit_status"))
            self.assertTrue(bool(run_manifest.get("strict_stop_applied")))

            replacer_manifest_path = Path(str(run_manifest.get("replacer_manifest_path") or ""))
            replacer_manifest = json.loads(replacer_manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(bool(replacer_manifest.get("payable_sub_fill_required_failed")))
            self.assertEqual(
                ["no_candidates_flagged"],
                list(((replacer_manifest.get("file_card_inference") or {}).get("reasons")) or []),
            )


if __name__ == "__main__":
    unittest.main()
