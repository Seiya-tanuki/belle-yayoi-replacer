from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path


SLOT_CONFIG = {
    "receipt.target": {
        "line_id": "receipt",
        "slot_name": "target",
        "title": "置換したいCSV",
        "description": "今回置換したい領収書のCSVを1つ入れてください。",
        "relative_dir": Path("inputs/kari_shiwake"),
        "multiple": False,
        "extensions": {".csv"},
    },
    "receipt.ledger_ref": {
        "line_id": "receipt",
        "slot_name": "ledger_ref",
        "title": "学習用参照CSV / TXT",
        "description": "学習をするときだけ、参照CSV または TXT を1つ入れてください。",
        "relative_dir": Path("inputs/ledger_ref"),
        "multiple": True,
        "extensions": {".csv", ".txt"},
    },
    "bank_statement.target": {
        "line_id": "bank_statement",
        "slot_name": "target",
        "title": "置換したいCSV",
        "description": "今回置換したい銀行明細のCSVを1つ入れてください。",
        "relative_dir": Path("inputs/kari_shiwake"),
        "multiple": False,
        "extensions": {".csv"},
    },
    "bank_statement.training_ocr": {
        "line_id": "bank_statement",
        "slot_name": "training_ocr",
        "title": "学習用OCR CSV",
        "description": "学習をするときだけ、OCR CSV を1つ入れてください。",
        "relative_dir": Path("inputs/training/ocr_kari_shiwake"),
        "multiple": False,
        "extensions": {".csv"},
    },
    "bank_statement.training_reference": {
        "line_id": "bank_statement",
        "slot_name": "training_reference",
        "title": "学習用参照CSV / TXT",
        "description": "学習をするときだけ、参照CSV または TXT を1つ入れてください。",
        "relative_dir": Path("inputs/training/reference_yayoi"),
        "multiple": False,
        "extensions": {".csv", ".txt"},
    },
    "credit_card_statement.target": {
        "line_id": "credit_card_statement",
        "slot_name": "target",
        "title": "置換したいCSV",
        "description": "今回置換したいカード明細のCSVを1つ入れてください。",
        "relative_dir": Path("inputs/kari_shiwake"),
        "multiple": False,
        "extensions": {".csv"},
    },
    "credit_card_statement.ledger_ref": {
        "line_id": "credit_card_statement",
        "slot_name": "ledger_ref",
        "title": "学習用参照CSV / TXT",
        "description": "学習をするときだけ、参照CSV または TXT を1つ入れてください。",
        "relative_dir": Path("inputs/ledger_ref"),
        "multiple": True,
        "extensions": {".csv", ".txt"},
    },
}

LINE_PAGE_COPY = {
    "receipt": {
        "step": "手順 3 / 6",
        "title": "領収書のファイルを入れてください",
        "subtitle": "領収書の置換に必要なのは\n「1 : OCRシステムから出力した仮仕訳csv」\n「2 : 弥生会計から出力した過去仕訳の学習データtxt（学習したい場合のみ）」\nの２つです",
        "slots": ["receipt.target", "receipt.ledger_ref"],
        "extra_note": "",
    },
    "bank_statement": {
        "step": "手順 3 / 6",
        "title": "銀行明細のファイルを入れてください",
        "subtitle": "銀行通帳の置換に必要なのは\n「1 : OCRシステムから出力した仮仕訳csv 」\n「2 : 学習対象期間のOCR 仮仕訳csv」\n「3 : 2と同期間の弥生会計から出力した過去仕訳の学習データtxt 」\nの３つです。\n2と3は学習しない場合は不要です",
        "slots": [
            "bank_statement.target",
            "bank_statement.training_ocr",
            "bank_statement.training_reference",
        ],
        "extra_note": "学習をしないときは、学習用の2つの欄を空のままにしてください。どちらか片方だけファイルを入れるとエラーが発生します",
    },
    "credit_card_statement": {
        "step": "手順 3 / 6",
        "title": "クレジットカードのファイルを入れてください",
        "subtitle": "クレジットカードの置換に必要なのは\n「1 : OCRシステムから出力した仮仕訳csv 」\n「2 : 弥生会計から出力した過去仕訳の学習データtxt（学習したい場合のみ） 」\nの２つです",
        "slots": ["credit_card_statement.target", "credit_card_statement.ledger_ref"],
        "extra_note": "",
    },
}


@dataclass(frozen=True)
class UploadValidationResult:
    ok: bool
    errors: list[str]


TARGET_REQUIRED_ERROR = "置換したいCSVを入れてください。"
TARGET_MULTIPLE_ERROR = (
    "置換対象のcsvは一度につき1件のみです。"
    "現在は複数のファイルが存在するため「ファイルをリセットする」ボタンをクリックして"
    "一度ファイルを削除してから再度アップロードしてください"
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def slot_keys_for_line(line_id: str) -> list[str]:
    return list(LINE_PAGE_COPY[line_id]["slots"])


def line_copy(line_id: str) -> dict[str, str | list[str]]:
    return LINE_PAGE_COPY[line_id]


def resolve_slot_dir(client_id: str, slot_key: str, root: Path | None = None) -> Path:
    config = SLOT_CONFIG[slot_key]
    current_root = root or repo_root()
    return current_root / "clients" / client_id / "lines" / config["line_id"] / config["relative_dir"]


def allowed_extensions(slot_key: str) -> set[str]:
    return set(SLOT_CONFIG[slot_key]["extensions"])


def is_allowed_extension(slot_key: str, filename: str) -> bool:
    return Path(filename).suffix.lower() in allowed_extensions(slot_key)


def list_slot_files(client_id: str, slot_key: str, root: Path | None = None) -> list[str]:
    slot_dir = resolve_slot_dir(client_id, slot_key, root)
    if not slot_dir.exists():
        return []
    return sorted(path.name for path in slot_dir.iterdir() if path.is_file() and path.name != ".gitkeep")


def _ensure_slot_dir(slot_dir: Path) -> None:
    slot_dir.mkdir(parents=True, exist_ok=True)


def save_uploaded_file(
    client_id: str,
    slot_key: str,
    filename: str,
    content: bytes,
    root: Path | None = None,
) -> list[str]:
    if not is_allowed_extension(slot_key, filename):
        raise ValueError(f"Unsupported extension for {slot_key}: {filename}")

    slot_dir = resolve_slot_dir(client_id, slot_key, root)
    _ensure_slot_dir(slot_dir)
    destination = slot_dir / Path(filename).name
    if not SLOT_CONFIG[slot_key]["multiple"]:
        for existing in slot_dir.iterdir():
            if existing.name == ".gitkeep":
                continue
            if existing.is_file():
                existing.unlink()
    destination.write_bytes(content)
    return list_slot_files(client_id, slot_key, root)


def clear_slot(client_id: str, slot_key: str, root: Path | None = None) -> list[str]:
    slot_dir = resolve_slot_dir(client_id, slot_key, root)
    if slot_dir.exists():
        for path in slot_dir.iterdir():
            if path.name == ".gitkeep":
                continue
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)
    return list_slot_files(client_id, slot_key, root)


def validate_line_uploads(client_id: str, line_id: str, root: Path | None = None) -> UploadValidationResult:
    errors: list[str] = []
    counts = {slot_key: len(list_slot_files(client_id, slot_key, root)) for slot_key in slot_keys_for_line(line_id)}

    if line_id == "receipt":
        target_count = counts["receipt.target"]
        if target_count == 0:
            errors.append(TARGET_REQUIRED_ERROR)
        elif target_count >= 2:
            errors.append(TARGET_MULTIPLE_ERROR)
    elif line_id == "bank_statement":
        target_count = counts["bank_statement.target"]
        if target_count == 0:
            errors.append(TARGET_REQUIRED_ERROR)
        elif target_count >= 2:
            errors.append(TARGET_MULTIPLE_ERROR)
        ocr_count = counts["bank_statement.training_ocr"]
        reference_count = counts["bank_statement.training_reference"]
        if (ocr_count, reference_count) not in {(0, 0), (1, 1)}:
            if ocr_count > 1 or reference_count > 1:
                errors.append("学習用ファイルはそれぞれ1つだけ入れてください。")
            else:
                errors.append("学習用ファイルは2つそろえて入れてください。")
    elif line_id == "credit_card_statement":
        target_count = counts["credit_card_statement.target"]
        if target_count == 0:
            errors.append(TARGET_REQUIRED_ERROR)
        elif target_count >= 2:
            errors.append(TARGET_MULTIPLE_ERROR)
    else:
        errors.append("先に処理種類を選んでください。")

    return UploadValidationResult(ok=not errors, errors=errors)
