from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

PlanStatus = Literal["RUN", "SKIP", "FAIL"]
RunOutcome = Literal["success", "needs_review", "skipped"]
RequestedLine = Literal["receipt", "bank_statement", "credit_card_statement", "all"]


def _copy_detail(detail: Mapping[str, Any] | None) -> dict[str, Any]:
    if not detail:
        return {}
    return dict(detail)


@dataclass(frozen=True, slots=True)
class LinePlan:
    line_id: str
    status: PlanStatus
    reason: str
    target_files: tuple[str, ...] = ()
    ui_reason_code: str = ""
    ui_reason_detail: dict[str, Any] = field(default_factory=dict)
    details: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "target_files", tuple(self.target_files))
        object.__setattr__(self, "ui_reason_detail", _copy_detail(self.ui_reason_detail))
        object.__setattr__(self, "details", dict(self.details))


@dataclass(frozen=True, slots=True)
class RunLineResult(Mapping[str, object]):
    line_id: str
    outcome: RunOutcome
    reason: str
    ui_reason_code: str
    ui_reason_detail: dict[str, Any] = field(default_factory=dict)
    run_id: str = ""
    run_dir: str = ""
    run_manifest_path: str = ""
    changed_ratio: float = 0.0
    output_file: str = ""
    warnings: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    target_files: tuple[str, ...] = ()
    input_count: int = 0
    output_count: int = 0
    strict_stop_applied: bool = False
    needs_review: bool = False
    exit_status: str = "OK"
    details: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ui_reason_detail", _copy_detail(self.ui_reason_detail))
        object.__setattr__(self, "warnings", tuple(self.warnings))
        object.__setattr__(self, "reasons", tuple(self.reasons))
        object.__setattr__(self, "target_files", tuple(self.target_files))
        object.__setattr__(self, "details", dict(self.details))
        if self.outcome == "needs_review" and not self.needs_review:
            object.__setattr__(self, "needs_review", True)

    @classmethod
    def success(
        cls,
        *,
        line_id: str,
        ui_reason_code: str,
        ui_reason_detail: Mapping[str, Any] | None = None,
        run_id: str,
        run_dir: str,
        run_manifest_path: str,
        changed_ratio: float,
        output_file: str,
        warnings: tuple[str, ...] | list[str] = (),
        target_files: tuple[str, ...] | list[str] = (),
        input_count: int = 1,
        output_count: int = 1,
        details: Mapping[str, object] | None = None,
        reason: str = "success",
    ) -> RunLineResult:
        return cls(
            line_id=line_id,
            outcome="success",
            reason=reason,
            ui_reason_code=ui_reason_code,
            ui_reason_detail=_copy_detail(ui_reason_detail),
            run_id=run_id,
            run_dir=run_dir,
            run_manifest_path=run_manifest_path,
            changed_ratio=float(changed_ratio),
            output_file=output_file,
            warnings=tuple(warnings),
            target_files=tuple(target_files),
            input_count=int(input_count),
            output_count=int(output_count),
            strict_stop_applied=False,
            needs_review=False,
            exit_status="OK",
            details=dict(details or {}),
        )

    @classmethod
    def needs_review_result(
        cls,
        *,
        line_id: str,
        reason: str,
        ui_reason_code: str,
        ui_reason_detail: Mapping[str, Any] | None = None,
        run_id: str,
        run_dir: str,
        run_manifest_path: str,
        changed_ratio: float,
        output_file: str,
        warnings: tuple[str, ...] | list[str] = (),
        reasons: tuple[str, ...] | list[str] = (),
        target_files: tuple[str, ...] | list[str] = (),
        input_count: int = 1,
        output_count: int = 1,
        details: Mapping[str, object] | None = None,
    ) -> RunLineResult:
        return cls(
            line_id=line_id,
            outcome="needs_review",
            reason=reason,
            ui_reason_code=ui_reason_code,
            ui_reason_detail=_copy_detail(ui_reason_detail),
            run_id=run_id,
            run_dir=run_dir,
            run_manifest_path=run_manifest_path,
            changed_ratio=float(changed_ratio),
            output_file=output_file,
            warnings=tuple(warnings),
            reasons=tuple(reasons),
            target_files=tuple(target_files),
            input_count=int(input_count),
            output_count=int(output_count),
            strict_stop_applied=True,
            needs_review=True,
            exit_status="FAIL",
            details=dict(details or {}),
        )

    @classmethod
    def skipped(
        cls,
        *,
        line_id: str,
        reason: str,
        ui_reason_code: str,
        ui_reason_detail: Mapping[str, Any] | None = None,
        target_files: tuple[str, ...] | list[str] = (),
        details: Mapping[str, object] | None = None,
    ) -> RunLineResult:
        return cls(
            line_id=line_id,
            outcome="skipped",
            reason=reason,
            ui_reason_code=ui_reason_code,
            ui_reason_detail=_copy_detail(ui_reason_detail),
            target_files=tuple(target_files),
            exit_status="SKIP",
            details=dict(details or {}),
        )

    def as_legacy_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "line_id": self.line_id,
            "run_id": self.run_id,
            "run_dir": self.run_dir,
            "run_manifest_path": self.run_manifest_path,
            "changed_ratio": self.changed_ratio,
            "output_file": self.output_file,
            "warnings": list(self.warnings),
            "reasons": list(self.reasons),
            "strict_stop_applied": self.strict_stop_applied,
            "needs_review": self.needs_review,
            "exit_status": self.exit_status,
            "ui_reason_code": self.ui_reason_code,
            "ui_reason_detail": dict(self.ui_reason_detail),
            "reason": self.reason,
            "target_files": list(self.target_files),
            "input_count": self.input_count,
            "output_count": self.output_count,
            "details": dict(self.details),
        }
        for key, value in self.details.items():
            if key not in payload:
                payload[key] = value
        return payload

    def __getitem__(self, key: str) -> object:
        return self.as_legacy_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.as_legacy_dict())

    def __len__(self) -> int:
        return len(self.as_legacy_dict())

    def get(self, key: str, default: object | None = None) -> object | None:
        return self.as_legacy_dict().get(key, default)


@dataclass(frozen=True, slots=True)
class ReplacerPlanResult:
    client_id: str
    requested_line: RequestedLine
    plans: tuple[LinePlan, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "plans", tuple(self.plans))

    @property
    def has_failures(self) -> bool:
        return any(plan.status == "FAIL" for plan in self.plans)

    @property
    def runnable_plans(self) -> tuple[LinePlan, ...]:
        return tuple(plan for plan in self.plans if plan.status == "RUN")

    @property
    def skipped_plans(self) -> tuple[LinePlan, ...]:
        return tuple(plan for plan in self.plans if plan.status == "SKIP")


@dataclass(frozen=True, slots=True)
class ReplacerRunResult:
    client_id: str
    requested_line: RequestedLine
    plan_result: ReplacerPlanResult
    line_results: tuple[RunLineResult, ...]
    stopped_early: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "line_results", tuple(self.line_results))

    @property
    def has_needs_review(self) -> bool:
        return any(result.needs_review for result in self.line_results)

    @property
    def needs_review_result(self) -> RunLineResult | None:
        for result in self.line_results:
            if result.needs_review:
                return result
        return None
