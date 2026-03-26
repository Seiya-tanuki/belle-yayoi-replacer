from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from belle.local_ui.nicegui_compat import ensure_nicegui_compat


def page_container_classes() -> str:
    return "w-full max-w-3xl mx-auto px-4 py-8 gap-6"


@contextmanager
def page_shell(step_label: str, title: str, subtitle: str) -> Iterator[None]:
    ensure_nicegui_compat()
    from nicegui import ui

    with ui.column().classes(page_container_classes()):
        ui.label("Belle ローカルUI").classes("text-2xl font-semibold")
        ui.label("迷わず進めるための簡易操作画面").classes("text-sm text-slate-600")
        ui.label(step_label).classes("text-sm font-medium text-slate-500")
        ui.label(title).classes("text-3xl font-semibold")
        ui.label(subtitle).classes("text-base text-slate-600")
        yield


def primary_button(label: str, on_click) -> None:
    ensure_nicegui_compat()
    from nicegui import ui

    ui.button(label, on_click=on_click).props("unelevated color=primary").classes("w-full sm:w-auto")


def secondary_button(label: str, on_click) -> None:
    ensure_nicegui_compat()
    from nicegui import ui

    ui.button(label, on_click=on_click).props("flat color=primary").classes("w-full sm:w-auto")


def card_container(*, selected: bool = False):
    ensure_nicegui_compat()
    from nicegui import ui

    classes = "w-full rounded-2xl border p-4 gap-2 shadow-sm cursor-pointer"
    if selected:
        classes += " border-primary bg-blue-50"
    else:
        classes += " border-slate-200 bg-white"
    return ui.card().classes(classes)
