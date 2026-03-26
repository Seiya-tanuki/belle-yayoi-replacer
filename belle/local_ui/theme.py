from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator


def page_container_classes() -> str:
    return "w-full max-w-3xl mx-auto px-4 py-8 gap-6"


@contextmanager
def page_shell(step_label: str, title: str, subtitle: str) -> Iterator[None]:
    from nicegui import ui

    with ui.column().classes(page_container_classes()):
        ui.label("Belle ローカルUI").classes("text-2xl font-semibold")
        ui.label("迷わず進めるための簡易操作画面").classes("text-sm text-slate-600")
        ui.label(step_label).classes("text-sm font-medium text-slate-500")
        ui.label(title).classes("text-3xl font-semibold")
        ui.label(subtitle).classes("text-base text-slate-600")
        yield
