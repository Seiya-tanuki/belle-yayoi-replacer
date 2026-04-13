from __future__ import annotations

from pathlib import Path

from .errors import (
    InvalidRequestedLineError,
    ReplacerApplicationError,
    ReplacerPlanBlockedError,
    ReplacerRunFailedError,
)
from .models import (
    LinePlan,
    ReplacerPlanResult,
    ReplacerRunResult,
    RequestedLine,
    RunLineResult,
)

LINE_ORDER = ("receipt", "bank_statement", "credit_card_statement")


def plan_replacer(
    repo_root: Path,
    client_id: str,
    *,
    requested_line: RequestedLine,
) -> ReplacerPlanResult:
    from .replacer import plan_replacer as _plan_replacer

    return _plan_replacer(repo_root, client_id, requested_line=requested_line)


def run_replacer(
    repo_root: Path,
    client_id: str,
    *,
    plan_result: ReplacerPlanResult,
) -> ReplacerRunResult:
    from .replacer import run_replacer as _run_replacer

    return _run_replacer(repo_root, client_id, plan_result=plan_result)


__all__ = [
    "InvalidRequestedLineError",
    "LINE_ORDER",
    "LinePlan",
    "ReplacerApplicationError",
    "ReplacerPlanBlockedError",
    "ReplacerPlanResult",
    "ReplacerRunFailedError",
    "ReplacerRunResult",
    "RequestedLine",
    "RunLineResult",
    "plan_replacer",
    "run_replacer",
]
