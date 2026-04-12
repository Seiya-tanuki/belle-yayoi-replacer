from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field, replace
from pathlib import Path
from uuid import uuid4

from belle.category_override_bootstrap import (
    CategoryOverrideBootstrapAnalysis,
    analyze_category_override_teacher,
)
from belle.client_registration_overrides import (
    apply_registration_category_override_bootstrap_payload,
    generate_registration_category_overrides_payload,
)
from belle.local_ui.services.clients import repo_root


PREVIEW_LINE_IDS = ("receipt", "credit_card_statement")
PREVIEW_LINE_LABELS = {
    "receipt": "領収書",
    "credit_card_statement": "クレジットカード",
}
_ALLOWED_SUFFIXES = {".csv", ".txt"}
_STAGE_ROOT_RELATIVE = Path(".tmp") / "local_ui" / "client_register_bootstrap"
_INVALID_EXTENSION_MESSAGE = "CSV または TXT を選んでください。"
_INVALID_TEACHER_MESSAGE = "このファイルは使えません。別のファイルにしてください。"
_NO_VISIBLE_CHANGES_MESSAGE = "このファイルでは自動設定は変わりません。"


@dataclass(frozen=True)
class ClientBootstrapPreviewRow:
    category_label: str
    replacement_account: str


@dataclass(frozen=True)
class ClientBootstrapPreviewSection:
    title: str
    rows: tuple[ClientBootstrapPreviewRow, ...]


@dataclass(frozen=True)
class ClientBootstrapPreview:
    sections: tuple[ClientBootstrapPreviewSection, ...] = ()
    note: str = ""


@dataclass(frozen=True)
class StagedTeacherFileState:
    session_token: str = ""
    staged_path: Path | None = None
    original_basename: str = ""
    submit_blocked: bool = False
    error_message: str = ""
    preview: ClientBootstrapPreview = field(default_factory=ClientBootstrapPreview)


def stage_root(root: Path | None = None) -> Path:
    current_root = root or repo_root()
    return current_root / _STAGE_ROOT_RELATIVE


def session_dir_for(session_token: str, root: Path | None = None) -> Path:
    return stage_root(root) / session_token


def empty_teacher_file_state() -> StagedTeacherFileState:
    return StagedTeacherFileState()


def stage_teacher_file(
    current_state: StagedTeacherFileState,
    *,
    filename: str,
    content: bytes,
    bookkeeping_mode: str,
    root: Path | None = None,
) -> StagedTeacherFileState:
    safe_name = _safe_basename(filename)
    _validate_extension(safe_name)

    current_root = root or repo_root()
    _cleanup_session_dir(current_state.session_token, current_root)

    session_token = uuid4().hex
    session_dir = session_dir_for(session_token, current_root)
    session_dir.mkdir(parents=True, exist_ok=True)

    staged_path = session_dir / safe_name
    staged_path.write_bytes(content)

    staged_state = StagedTeacherFileState(
        session_token=session_token,
        staged_path=staged_path,
        original_basename=safe_name,
    )
    return refresh_teacher_file(staged_state, bookkeeping_mode=bookkeeping_mode, root=current_root)


def refresh_teacher_file(
    current_state: StagedTeacherFileState,
    *,
    bookkeeping_mode: str,
    root: Path | None = None,
) -> StagedTeacherFileState:
    current_root = root or repo_root()
    staged_path = current_state.staged_path
    if staged_path is None:
        return empty_teacher_file_state()

    try:
        analysis = analyze_category_override_teacher(
            teacher_path=staged_path,
            lexicon_path=current_root / "lexicon" / "lexicon.json",
        )
    except Exception:
        return replace(
            current_state,
            submit_blocked=True,
            error_message=_INVALID_TEACHER_MESSAGE,
            preview=ClientBootstrapPreview(),
        )

    if not str(bookkeeping_mode or "").strip():
        return replace(
            current_state,
            submit_blocked=False,
            error_message="",
            preview=ClientBootstrapPreview(),
        )

    try:
        preview = _build_preview(
            current_root=current_root,
            bookkeeping_mode=bookkeeping_mode,
            analysis=analysis,
        )
    except Exception:
        return replace(
            current_state,
            submit_blocked=True,
            error_message=_INVALID_TEACHER_MESSAGE,
            preview=ClientBootstrapPreview(),
        )

    return replace(
        current_state,
        submit_blocked=False,
        error_message="",
        preview=preview,
    )


def clear_teacher_file(current_state: StagedTeacherFileState, root: Path | None = None) -> StagedTeacherFileState:
    current_root = root or repo_root()
    _cleanup_session_dir(current_state.session_token, current_root)
    return empty_teacher_file_state()


def cleanup_after_success(current_state: StagedTeacherFileState, root: Path | None = None) -> StagedTeacherFileState:
    return clear_teacher_file(current_state, root=root)


def _load_preview_category_labels(current_root: Path) -> dict[str, str]:
    try:
        raw = json.loads((current_root / "lexicon" / "lexicon.json").read_text(encoding="utf-8"))
    except Exception:
        return {}

    categories = raw.get("categories")
    if not isinstance(categories, list):
        return {}

    labels: dict[str, str] = {}
    for category in categories:
        if not isinstance(category, dict):
            continue
        category_key = str(category.get("key") or "").strip()
        if not category_key:
            continue
        labels[category_key] = (
            str(category.get("label_ja") or "").strip()
            or str(category.get("label") or "").strip()
            or category_key
        )
    return labels


def _build_preview(
    *,
    current_root: Path,
    bookkeeping_mode: str,
    analysis: CategoryOverrideBootstrapAnalysis,
) -> ClientBootstrapPreview:
    category_labels = _load_preview_category_labels(current_root)
    changes_by_line: dict[str, tuple[ClientBootstrapPreviewRow, ...]] = {}
    for line_id in PREVIEW_LINE_IDS:
        payload = generate_registration_category_overrides_payload(
            repo_root=current_root,
            client_id="__local_ui_preview__",
            line_id=line_id,
            bookkeeping_mode=bookkeeping_mode,
        )
        changes = apply_registration_category_override_bootstrap_payload(
            payload=payload,
            analysis=analysis,
            line_id=line_id,
        )
        changes_by_line[line_id] = tuple(
            ClientBootstrapPreviewRow(
                category_label=category_labels.get(change.category_key, change.category_label or change.category_key),
                replacement_account=change.to_target_account,
            )
            for change in changes
        )

    receipt_rows = changes_by_line["receipt"]
    credit_rows = changes_by_line["credit_card_statement"]
    if not receipt_rows and not credit_rows:
        return ClientBootstrapPreview(note=_NO_VISIBLE_CHANGES_MESSAGE)
    if receipt_rows == credit_rows:
        return ClientBootstrapPreview(
            sections=(ClientBootstrapPreviewSection(title="", rows=receipt_rows),),
        )

    sections = tuple(
        ClientBootstrapPreviewSection(title=PREVIEW_LINE_LABELS[line_id], rows=rows)
        for line_id, rows in (
            ("receipt", receipt_rows),
            ("credit_card_statement", credit_rows),
        )
        if rows
    )
    return ClientBootstrapPreview(sections=sections)


def _safe_basename(filename: str) -> str:
    safe_name = Path(filename or "").name
    if safe_name in {"", ".", ".."}:
        raise ValueError(_INVALID_EXTENSION_MESSAGE)
    return safe_name


def _validate_extension(filename: str) -> None:
    if Path(filename).suffix.lower() not in _ALLOWED_SUFFIXES:
        raise ValueError(_INVALID_EXTENSION_MESSAGE)


def _cleanup_session_dir(session_token: str, root: Path) -> None:
    if not session_token:
        return
    session_dir = session_dir_for(session_token, root)
    if not session_dir.exists():
        return
    shutil.rmtree(session_dir, ignore_errors=True)
