# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

from belle.lines import is_line_implemented

from .common import LinePlan

LINE_ID_CARD = "credit_card_statement"


def plan_card(repo_root: Path, client_id: str) -> LinePlan:
    del repo_root
    del client_id
    if not is_line_implemented(LINE_ID_CARD):
        return LinePlan(
            line_id=LINE_ID_CARD,
            status="SKIP",
            reason="unimplemented",
            target_files=[],
            details={},
        )
    return LinePlan(
        line_id=LINE_ID_CARD,
        status="FAIL",
        reason="runner not implemented yet",
        target_files=[],
        details={},
    )


def run_card(*args, **kwargs):
    del args
    del kwargs
    raise NotImplementedError("credit_card_statement runner not implemented yet")
