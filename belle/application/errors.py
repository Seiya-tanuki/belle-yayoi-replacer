from __future__ import annotations

from belle.application.models import ReplacerPlanResult, RunLineResult


class ReplacerApplicationError(RuntimeError):
    """Base class for replacer application-layer errors."""


class InvalidRequestedLineError(ReplacerApplicationError):
    pass


class ReplacerPlanBlockedError(ReplacerApplicationError):
    def __init__(self, plan_result: ReplacerPlanResult) -> None:
        super().__init__("plan contains blocking FAIL entries")
        self.plan_result = plan_result


class ReplacerRunFailedError(ReplacerApplicationError):
    def __init__(
        self,
        *,
        line_id: str,
        message: str,
        partial_results: tuple[RunLineResult, ...] = (),
    ) -> None:
        super().__init__(message)
        self.line_id = line_id
        self.partial_results = tuple(partial_results)
