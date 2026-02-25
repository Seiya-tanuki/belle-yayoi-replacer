# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict

from .cc_cache import ValueStatsEntry
from .client_cache import StatsEntry


def ensure_stats_entry(stats_map: Dict[str, StatsEntry], key: str) -> StatsEntry:
    if key not in stats_map:
        stats_map[key] = StatsEntry.empty()
    return stats_map[key]


def ensure_value_stats_entry(stats_map: dict, key: str) -> ValueStatsEntry:
    if key not in stats_map:
        stats_map[key] = ValueStatsEntry.empty()
    return stats_map[key]
