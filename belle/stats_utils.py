# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict

from .client_cache import StatsEntry


def ensure_stats_entry(stats_map: Dict[str, StatsEntry], key: str) -> StatsEntry:
    if key not in stats_map:
        stats_map[key] = StatsEntry.empty()
    return stats_map[key]
