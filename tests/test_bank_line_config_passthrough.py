from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from belle.build_bank_cache import load_bank_line_config


class BankLineConfigPassthroughTests(unittest.TestCase):
    def test_bank_side_subaccount_dict_is_passthrough(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_CONFIG_PASS"
            cfg_path = (
                repo_root
                / "clients"
                / client_id
                / "lines"
                / "bank_statement"
                / "config"
                / "bank_line_config.json"
            )
            cfg_path.parent.mkdir(parents=True, exist_ok=True)

            bank_side_subaccount = {
                "enabled": False,
                "weak_enabled": True,
                "weak_min_count": 3,
            }
            cfg_path.write_text(
                json.dumps(
                    {
                        "schema": "belle.bank_line_config.v0",
                        "version": "0.1",
                        "bank_side_subaccount": bank_side_subaccount,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            loaded = load_bank_line_config(repo_root, client_id)

            self.assertIn("bank_side_subaccount", loaded)
            self.assertEqual(bank_side_subaccount, loaded["bank_side_subaccount"])

    def test_file_level_bank_sub_inference_thresholds_default_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            client_id = "C_BANK_CONFIG_DEFAULTS"
            cfg_path = (
                repo_root
                / "clients"
                / client_id
                / "lines"
                / "bank_statement"
                / "config"
                / "bank_line_config.json"
            )
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(
                json.dumps(
                    {
                        "schema": "belle.bank_line_config.v0",
                        "version": "0.1",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            loaded = load_bank_line_config(repo_root, client_id)
            thresholds = loaded.get("thresholds") if isinstance(loaded.get("thresholds"), dict) else {}
            file_level = (
                thresholds.get("file_level_bank_sub_inference")
                if isinstance(thresholds.get("file_level_bank_sub_inference"), dict)
                else {}
            )
            self.assertEqual(3, int(file_level.get("min_votes") or 0))
            self.assertEqual(0.9, float(file_level.get("min_p_majority") or 0.0))


if __name__ == "__main__":
    unittest.main()
