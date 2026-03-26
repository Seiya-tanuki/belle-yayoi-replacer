from __future__ import annotations

from belle.local_ui.nicegui_compat import ensure_nicegui_compat

_ROUTES_REGISTERED = False

def register_routes() -> None:
    global _ROUTES_REGISTERED
    if _ROUTES_REGISTERED:
        return

    ensure_nicegui_compat()
    from nicegui import ui
    from belle.local_ui.pages.client_new import build as build_client_new
    from belle.local_ui.pages.client_select import build as build_client_select
    from belle.local_ui.pages.flow_types import build as build_flow_types

    @ui.page("/")
    def home_page() -> None:
        build_client_select()

    @ui.page("/clients/new")
    def client_new_page() -> None:
        build_client_new()

    @ui.page("/flow/types")
    def flow_types_page() -> None:
        build_flow_types()

    _ROUTES_REGISTERED = True
