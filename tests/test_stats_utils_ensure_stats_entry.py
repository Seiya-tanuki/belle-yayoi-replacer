from __future__ import annotations

import unittest

from belle.stats_utils import ensure_stats_entry


class EnsureStatsEntryTests(unittest.TestCase):
    def test_ensure_stats_entry_creates_and_stores(self) -> None:
        stats_map = {}

        entry = ensure_stats_entry(stats_map, "K")

        self.assertIn("K", stats_map)
        self.assertIs(stats_map["K"], entry)

    def test_ensure_stats_entry_returns_existing(self) -> None:
        stats_map = {}

        first = ensure_stats_entry(stats_map, "K")
        second = ensure_stats_entry(stats_map, "K")

        self.assertIs(first, second)

    def test_ensure_stats_entry_allows_mutation_through_reference(self) -> None:
        stats_map = {}

        entry = ensure_stats_entry(stats_map, "K")
        entry.add_account("ACC")

        self.assertIs(stats_map["K"], entry)
        self.assertEqual(1, stats_map["K"].sample_total)
        self.assertEqual(1, stats_map["K"].debit_account_counts["ACC"])


if __name__ == "__main__":
    unittest.main()
