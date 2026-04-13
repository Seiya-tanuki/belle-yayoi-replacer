from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from belle.application.models import ReplacerPlanResult, RunLineResult
from belle.ui_reason_codes import RUN_FAIL_UNKNOWN


def _copy_detail(detail: Mapping[str, Any] | None) -> dict[str, Any]:
    if not detail:
        return {}
    return dict(detail)


class ReplacerApplicationError(RuntimeError):
    """Base class for replacer application-layer errors."""


class InvalidRequestedLineError(ReplacerApplicationError):
    pass


class ReplacerPlanBlockedError(ReplacerApplicationError):
    def __init__(self, plan_result: ReplacerPlanResult) -> None:
        super().__init__("plan contains blocking FAIL entries")
        self.plan_result = plan_result


class LineRunnerFailure(RuntimeError):
    def __init__(
        self,
        *,
        line_id: str,
        message: str,
        failure_key: str,
        ui_reason_code: str,
        ui_reason_detail: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.line_id = str(line_id)
        self.failure_key = str(failure_key)
        self.ui_reason_code = str(ui_reason_code)
        self.ui_reason_detail = _copy_detail(ui_reason_detail)


class ReplacerRunFailedError(ReplacerApplicationError):
    def __init__(
        self,
        *,
        line_id: str,
        message: str,
        failure_key: str = "unknown",
        ui_reason_code: str = RUN_FAIL_UNKNOWN,
        ui_reason_detail: Mapping[str, Any] | None = None,
        partial_results: tuple[RunLineResult, ...] = (),
    ) -> None:
        super().__init__(message)
        self.line_id = line_id
        self.failure_key = str(failure_key)
        self.ui_reason_code = str(ui_reason_code)
        self.ui_reason_detail = _copy_detail(ui_reason_detail)
        self.partial_results = tuple(partial_results)
