from __future__ import annotations

import importlib
import unittest
from unittest import mock


class LocalUiBootTests(unittest.TestCase):
    def test_app_module_imports_without_running_server(self) -> None:
        with mock.patch("socket.socket.bind", side_effect=AssertionError("server setup should not run on import")):
            module = importlib.import_module("belle.local_ui.app")
        self.assertTrue(hasattr(module, "create_app"))
        self.assertTrue(hasattr(module, "run_local_ui"))

    def test_package_exports_helpers(self) -> None:
        package = importlib.import_module("belle.local_ui")
        self.assertTrue(callable(package.create_app))
        self.assertTrue(callable(package.run_local_ui))


if __name__ == "__main__":
    unittest.main()
