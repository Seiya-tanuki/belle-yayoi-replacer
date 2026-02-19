from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _load_restore_module():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / ".agents" / "skills" / "restore-assets" / "scripts" / "restore_assets.py"
    spec = importlib.util.spec_from_file_location("restore_assets_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class RestoreAssetsAtomicityTests(unittest.TestCase):
    def test_clients_swap_rolls_back_when_second_move_fails(self) -> None:
        module = _load_restore_module()
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_text(repo_root / "clients" / "TEMPLATE" / ".gitkeep", "")
            _write_text(repo_root / "clients" / "A" / "old.txt", "OLD")

            stage_clients = repo_root / "staging" / "clients"
            _write_text(stage_clients / "A" / "new.txt", "NEW")
            restore_old_dir = repo_root / "exports" / "backups" / "restore_old_test" / "clients"

            real_move = module.shutil.move
            calls = {"count": 0}

            def _move_with_failure(src: str, dst: str):
                calls["count"] += 1
                if calls["count"] == 2:
                    raise OSError("simulated failure on second move")
                return real_move(src, dst)

            with mock.patch.object(module.shutil, "move", side_effect=_move_with_failure):
                with self.assertRaises(OSError):
                    module._restore_clients_with_swap(
                        repo_root=repo_root,
                        stage_clients=stage_clients,
                        restore_old_dir=restore_old_dir,
                    )

            self.assertTrue((repo_root / "clients" / "A").is_dir())
            self.assertEqual((repo_root / "clients" / "A" / "old.txt").read_text(encoding="utf-8"), "OLD")
            self.assertTrue((repo_root / "clients" / "TEMPLATE" / ".gitkeep").exists())

    def test_pending_swap_rolls_back_when_second_move_fails(self) -> None:
        module = _load_restore_module()
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_text(repo_root / "lexicon" / "receipt" / "pending" / ".gitkeep", "")
            _write_text(repo_root / "lexicon" / "receipt" / "pending" / "locks" / ".gitkeep", "")
            _write_text(repo_root / "lexicon" / "receipt" / "pending" / "label_queue.csv", "old-queue")

            stage_pending = repo_root / "staging" / "lexicon" / "receipt" / "pending"
            _write_text(stage_pending / "label_queue.csv", "new-queue")
            restore_old_dir = repo_root / "exports" / "backups" / "restore_old_test" / "lexicon_receipt_pending"

            real_move = module.shutil.move
            calls = {"count": 0}

            def _move_with_failure(src: str, dst: str):
                calls["count"] += 1
                if calls["count"] == 2:
                    raise OSError("simulated failure on second move")
                return real_move(src, dst)

            with mock.patch.object(module.shutil, "move", side_effect=_move_with_failure):
                with self.assertRaises(OSError):
                    module._restore_pending_with_swap(
                        repo_root=repo_root,
                        stage_pending=stage_pending,
                        restore_old_dir=restore_old_dir,
                    )

            self.assertEqual(
                (repo_root / "lexicon" / "receipt" / "pending" / "label_queue.csv").read_text(encoding="utf-8"),
                "old-queue",
            )
            self.assertTrue((repo_root / "lexicon" / "receipt" / "pending" / ".gitkeep").exists())
            self.assertTrue((repo_root / "lexicon" / "receipt" / "pending" / "locks" / ".gitkeep").exists())
            self.assertFalse(module.get_label_queue_lock_path(repo_root).exists())

    def test_apply_restore_never_overwrites_template(self) -> None:
        module = _load_restore_module()
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            _write_text(repo_root / "clients" / "TEMPLATE" / "template.txt", "tracked-template")
            _write_text(repo_root / "clients" / "A" / "old.txt", "old-client")
            _write_text(repo_root / "lexicon" / "receipt" / "pending" / ".gitkeep", "")
            _write_text(repo_root / "lexicon" / "receipt" / "pending" / "locks" / ".gitkeep", "")
            _write_text(repo_root / "lexicon" / "receipt" / "pending" / "label_queue.csv", "old-queue")

            staging_dir = repo_root / "staging"
            _write_text(staging_dir / "clients" / "TEMPLATE" / "template.txt", "backup-template")
            _write_text(staging_dir / "clients" / "A" / "new.txt", "new-client")
            _write_text(staging_dir / "lexicon" / "receipt" / "pending" / "label_queue.csv", "new-queue")

            module._apply_restore(
                repo_root=repo_root,
                staging_dir=staging_dir,
                restore_old_root=repo_root / "exports" / "backups" / "restore_old_test",
            )

            self.assertEqual(
                (repo_root / "clients" / "TEMPLATE" / "template.txt").read_text(encoding="utf-8"),
                "tracked-template",
            )
            self.assertTrue((repo_root / "clients" / "A" / "new.txt").exists())
            self.assertFalse((repo_root / "clients" / "A" / "old.txt").exists())
            self.assertEqual(
                (repo_root / "lexicon" / "receipt" / "pending" / "label_queue.csv").read_text(encoding="utf-8"),
                "new-queue",
            )


if __name__ == "__main__":
    unittest.main()
