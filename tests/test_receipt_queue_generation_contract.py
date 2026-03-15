from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4


COL_DEBIT_ACCOUNT = 4
COL_SUMMARY = 16


def _copy_path(src_root: Path, dst_root: Path, rel_path: str) -> None:
    src = src_root / rel_path
    dst = dst_root / rel_path
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    if src.is_dir():
        shutil.copytree(src, dst, ignore=ignore)
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_repo_assets(src_root: Path, dst_root: Path) -> None:
    for rel_path in (
        "belle",
        "lexicon",
        "rulesets",
        ".agents/skills/lexicon-apply",
        ".agents/skills/lexicon-extract",
    ):
        _copy_path(src_root, dst_root, rel_path)


def _clear_belle_modules() -> None:
    for name in list(sys.modules):
        if name == "belle" or name.startswith("belle."):
            del sys.modules[name]


def _load_script_module(repo_root: Path, rel_path: str):
    _clear_belle_modules()
    script_path = repo_root / rel_path
    spec = importlib.util.spec_from_file_location(f"phase4_{uuid4().hex}", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _run_script_main(module, argv: list[str]) -> tuple[int | None, str]:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        with contextlib.redirect_stderr(buf):
            with mock.patch.object(sys, "argv", argv):
                result = module.main()
    return result, buf.getvalue()


def _write_receipt_ledger_ref(path: Path, *, summary: str, debit_account: str = "交際費") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = [""] * 25
    row[COL_DEBIT_ACCOUNT] = debit_account
    row[COL_SUMMARY] = summary
    with path.open("w", encoding="cp932", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n")
        writer.writerow(row)


def _read_queue_rows(queue_csv: Path) -> list[dict[str, str]]:
    with queue_csv.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _reset_pending_dir_to_empty_baseline(pending_dir: Path) -> None:
    for path in sorted(pending_dir.rglob("*"), reverse=True):
        if path.is_dir():
            continue
        if path.name == ".gitkeep":
            continue
        path.unlink()


class ReceiptQueueGenerationContractTests(unittest.TestCase):
    def test_receipt_queue_and_apply_generate_from_empty_baseline(self) -> None:
        source_repo_root = Path(__file__).resolve().parents[1]
        client_id = "CLEAN_STATE_RECEIPT"

        with tempfile.TemporaryDirectory() as td:
            temp_repo_root = Path(td)
            _copy_repo_assets(source_repo_root, temp_repo_root)

            pending_dir = temp_repo_root / "lexicon" / "receipt" / "pending"
            _reset_pending_dir_to_empty_baseline(pending_dir)
            self.assertTrue((pending_dir / ".gitkeep").exists())
            self.assertTrue((pending_dir / "locks" / ".gitkeep").exists())
            self.assertFalse((pending_dir / "label_queue.csv").exists())
            self.assertFalse((pending_dir / "label_queue_state.json").exists())
            self.assertFalse((pending_dir / "applied_log.jsonl").exists())

            lexicon_path = temp_repo_root / "lexicon" / "lexicon.json"
            lexicon_before = json.loads(lexicon_path.read_text(encoding="utf-8"))
            learned_before = (lexicon_before.get("learned") or {}).get("provenance_registry") or []
            self.assertEqual([], learned_before)
            self.assertEqual(69, len(lexicon_before.get("categories") or []))

            ledger_ref_path = (
                temp_repo_root
                / "clients"
                / client_id
                / "lines"
                / "receipt"
                / "inputs"
                / "ledger_ref"
                / "batch1.csv"
            )
            _write_receipt_ledger_ref(
                ledger_ref_path,
                summary="PHASE4CLEANSTATEIZAKAYA / fixture",
            )

            extract_module = _load_script_module(
                temp_repo_root,
                ".agents/skills/lexicon-extract/scripts/run_lexicon_extract.py",
            )
            rc_extract, extract_output = _run_script_main(
                extract_module,
                [
                    "run_lexicon_extract.py",
                    "--client",
                    client_id,
                    "--line",
                    "receipt",
                ],
            )

            self.assertEqual(0, rc_extract, msg=extract_output)

            queue_csv = pending_dir / "label_queue.csv"
            queue_state = pending_dir / "label_queue_state.json"
            self.assertTrue(queue_csv.exists(), msg=extract_output)
            self.assertTrue(queue_state.exists(), msg=extract_output)

            queue_rows = _read_queue_rows(queue_csv)
            self.assertEqual(1, len(queue_rows), msg=extract_output)
            generated_row = queue_rows[0]
            norm_key = str(generated_row.get("norm_key") or "")
            self.assertTrue(norm_key, msg=extract_output)
            self.assertEqual("HOLD", generated_row.get("action"))
            self.assertEqual("", generated_row.get("user_category_key"))
            self.assertEqual("1", generated_row.get("count_total"))

            queue_state_obj = json.loads(queue_state.read_text(encoding="utf-8"))
            clients_by_norm_key = queue_state_obj.get("clients_by_norm_key") or {}
            self.assertIn(norm_key, clients_by_norm_key)
            self.assertIn(client_id, clients_by_norm_key[norm_key])

            fieldnames = list(queue_rows[0].keys())
            generated_row["user_category_key"] = "restaurant_izakaya"
            generated_row["action"] = "ADD"
            with queue_csv.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(generated_row)

            apply_module = _load_script_module(
                temp_repo_root,
                ".agents/skills/lexicon-apply/scripts/run_lexicon_apply.py",
            )
            rc_apply, apply_output = _run_script_main(
                apply_module,
                [
                    "run_lexicon_apply.py",
                    "--line",
                    "receipt",
                ],
            )

            self.assertEqual(0, rc_apply, msg=apply_output)

            applied_log = pending_dir / "applied_log.jsonl"
            self.assertTrue(applied_log.exists(), msg=apply_output)
            applied_lines = [line for line in applied_log.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(1, len(applied_lines), msg=apply_output)
            applied_record = json.loads(applied_lines[0])
            self.assertEqual("added", applied_record.get("status"))
            self.assertEqual(norm_key, applied_record.get("norm_key"))
            self.assertEqual("restaurant_izakaya", applied_record.get("category_key"))

            queue_rows_after_apply = _read_queue_rows(queue_csv)
            self.assertEqual([], queue_rows_after_apply)
            queue_state_after_apply = json.loads(queue_state.read_text(encoding="utf-8"))
            self.assertNotIn(norm_key, (queue_state_after_apply.get("clients_by_norm_key") or {}))

            apply_run_files = sorted(pending_dir.glob("apply_run_*.json"))
            self.assertEqual(1, len(apply_run_files), msg=apply_output)

            lexicon_after = json.loads(lexicon_path.read_text(encoding="utf-8"))
            learned_after = (lexicon_after.get("learned") or {}).get("provenance_registry") or []
            self.assertEqual(1, len(learned_after), msg=apply_output)
            self.assertEqual(norm_key, learned_after[0].get("norm_key"))
            self.assertEqual("restaurant_izakaya", learned_after[0].get("category_key"))

            category_id = {
                str(category.get("key") or ""): int(category.get("id"))
                for category in (lexicon_after.get("categories") or [])
            }["restaurant_izakaya"]
            self.assertIn(
                ["n0", norm_key, category_id, 0.85, "S"],
                lexicon_after.get("term_rows") or [],
            )


if __name__ == "__main__":
    unittest.main()
