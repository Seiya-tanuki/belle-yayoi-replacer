from __future__ import annotations

from dataclasses import dataclass, field


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
    collect_result: dict[str, str] = field(default_factory=dict)


def create_initial_state() -> LocalUiState:
    return LocalUiState()
