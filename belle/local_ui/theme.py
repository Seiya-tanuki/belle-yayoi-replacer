from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from belle.local_ui.nicegui_compat import ensure_nicegui_compat
from belle.local_ui.state import get_state


def page_container_classes() -> str:
    return "w-full gap-6 px-6 py-8 sm:px-8 lg:px-10"


@contextmanager
def page_shell(step_label: str, title: str, subtitle: str) -> Iterator[None]:
    ensure_nicegui_compat()
    from nicegui import ui

    state = get_state()

    with ui.column().classes("min-h-screen w-full bg-slate-100 px-4 py-6 sm:px-6 lg:px-8"):
        with ui.column().classes(
            "w-full max-w-5xl mx-auto rounded-3xl border border-slate-200 bg-white shadow-xl"
        ):
            with ui.column().classes(page_container_classes()):
                with ui.row().classes("w-full items-start justify-between gap-4 flex-wrap-reverse sm:flex-nowrap"):
                    with ui.column().classes("gap-2"):
                        ui.label("置換システム操作パネル").classes("text-2xl font-semibold")
                        ui.label(title).classes("text-3xl font-semibold")
                        if subtitle:
                            ui.label(subtitle).classes("text-base text-slate-600 whitespace-pre-line")
                    with ui.column().classes("w-full gap-2 sm:w-auto sm:items-end"):
                        if state.selected_client_id:
                            ui.label(f"対象クライアント : {state.selected_client_id}").classes(
                                "rounded-full bg-sky-600 px-4 py-2 text-sm font-medium text-white shadow-sm"
                            )
                        ui.label(step_label).classes(
                            "rounded-full bg-sky-600 px-4 py-2 text-sm font-semibold text-white shadow-sm"
                        )
                yield


def primary_button(label: str, on_click) -> None:
    ensure_nicegui_compat()
    from nicegui import ui

    ui.button(label, on_click=on_click).props("unelevated").classes("w-full sm:w-auto bg-sky-600 text-white")


def secondary_button(label: str, on_click) -> None:
    ensure_nicegui_compat()
    from nicegui import ui

    ui.button(label, on_click=on_click).props("outline").classes("w-full sm:w-auto border-sky-600 text-sky-600")


def card_container(*, selected: bool = False):
    ensure_nicegui_compat()
    from nicegui import ui

    classes = "w-full rounded-2xl border p-4 gap-2 shadow-sm cursor-pointer"
    if selected:
        classes += " border-sky-600 bg-sky-600 shadow-md ring-2 ring-sky-600"
    else:
        classes += " border-slate-200 bg-white"
    return ui.card().classes(classes)
