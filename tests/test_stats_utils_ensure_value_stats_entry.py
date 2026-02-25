from __future__ import annotations

import unittest

from belle.stats_utils import ensure_value_stats_entry


class EnsureValueStatsEntryTests(unittest.TestCase):
    def test_creates_and_stores(self) -> None:
        stats_map = {}

        entry = ensure_value_stats_entry(stats_map, "K")

        self.assertIn("K", stats_map)
        self.assertIs(stats_map["K"], entry)

    def test_returns_existing_identity(self) -> None:
        stats_map = {}

        first = ensure_value_stats_entry(stats_map, "K")
        second = ensure_value_stats_entry(stats_map, "K")

        self.assertIs(first, second)

    def test_mutation_reflects_in_map(self) -> None:
        stats_map = {}

        entry = ensure_value_stats_entry(stats_map, "K")
        entry.update("X")

        self.assertIs(stats_map["K"], entry)
        self.assertEqual(1, stats_map["K"].sample_total)
        self.assertEqual(1, stats_map["K"].value_counts["X"])


if __name__ == "__main__":
    unittest.main()
