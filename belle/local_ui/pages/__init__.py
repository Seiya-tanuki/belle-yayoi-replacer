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
    from belle.local_ui.pages.upload_line import build as build_upload_line
    from belle.local_ui.theme import page_shell, secondary_button

    @ui.page("/")
    def home_page() -> None:
        build_client_select()

    @ui.page("/clients/new")
    def client_new_page() -> None:
        build_client_new()

    @ui.page("/flow/types")
    def flow_types_page() -> None:
        build_flow_types()

    @ui.page("/flow/upload/{line_id}")
    def upload_line_page(line_id: str) -> None:
        build_upload_line(line_id)

    @ui.page("/flow/check")
    def check_page() -> None:
        with page_shell("手順 4 / 6", "事前確認", "このまま進めるかを先に確認します。"):
            ui.label("Phase 4 で実装します。").classes("text-sm text-slate-500")
            secondary_button("ファイルを直す", lambda: ui.navigate.to("/flow/types"))

    _ROUTES_REGISTERED = True
