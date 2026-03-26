from __future__ import annotations

from dataclasses import dataclass, field

LINE_ORDER = ("receipt", "bank_statement", "credit_card_statement")
LINE_LABELS = {
    "receipt": "領収書",
    "bank_statement": "銀行明細",
    "credit_card_statement": "クレジットカード",
}


@dataclass
class LocalUiState:
    selected_client_id: str = ""
    selected_lines: list[str] = field(default_factory=list)
    current_line_index: int = 0
    uploads: dict[str, list[str]] = field(default_factory=dict)
    precheck_results: list[dict[str, str]] = field(default_factory=list)
    run_results: list[dict[str, str]] = field(default_factory=list)
    session_started_at_utc: str = ""
    session_finished_at_utc: str = ""
    collect_result: dict[str, object] = field(default_factory=dict)


def create_initial_state() -> LocalUiState:
    return LocalUiState()


_STATE = create_initial_state()


def get_state() -> LocalUiState:
    return _STATE


def reset_state() -> LocalUiState:
    global _STATE
    _STATE = create_initial_state()
    return _STATE


def normalize_selected_lines(line_ids: list[str]) -> list[str]:
    selected = {line_id for line_id in line_ids if line_id in LINE_ORDER}
    return [line_id for line_id in LINE_ORDER if line_id in selected]


def line_label(line_id: str) -> str:
    return LINE_LABELS.get(line_id, line_id)
