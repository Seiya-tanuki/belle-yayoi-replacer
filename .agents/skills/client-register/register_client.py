#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path

FORBIDDEN_CHARS = set('\\/:*?"<>|')
RESERVED_DEVICE_NAMES = {"CON", "PRN", "AUX", "NUL"}
SUPPORTED_REGISTER_LINES = {"receipt", "bank_statement", "credit_card_statement"}

BANK_LINE_CONFIG_MINIMAL = {
    "schema": "belle.bank_line_config.v0",
    "version": "0.1",
    "placeholder_account_name": "仮払金",
    "bank_account_name": "普通預金",
    "bank_account_subaccount": "",
    "pairing": {
        "join_key": ["date", "sign", "amount"],
        "require_unique_on_both_sides": True,
    },
    "thresholds": {
        "kana_sign_amount": {"min_count": 2, "min_p_majority": 0.85},
        "kana_sign": {"min_count": 3, "min_p_majority": 0.80},
    },
    "notes": {
        "status": "template_only_not_used_yet",
        "source_specs": [
            "spec/BANK_LINE_INPUTS_SPEC.md",
            "spec/BANK_CLIENT_CACHE_SPEC.md",
            "spec/BANK_REPLACER_SPEC.md",
        ],
    },
}


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    original: str
    trimmed: str
    canonical: str
    reason: str = ""
    substantial_change: bool = False


def _contains_control_chars(value: str) -> bool:
    return any(ord(ch) < 0x20 for ch in value)


def _is_reserved_device_name(value: str) -> bool:
    upper = value.upper()
    if upper in RESERVED_DEVICE_NAMES:
        return True
    if re.fullmatch(r"COM[1-9]|LPT[1-9]", upper):
        return True
    return False


def _collapse_underscores(value: str) -> str:
    return re.sub(r"_+", "_", value)


def validate_and_canonicalize(raw_name: str) -> ValidationResult:
    original = raw_name.rstrip("\r\n")
    trimmed = original.strip()

    if not trimmed:
        return ValidationResult(False, original, trimmed, "", "CLIENT_ID は必須です。")
    if len(trimmed) > 64:
        return ValidationResult(False, original, trimmed, "", "CLIENT_ID は64文字以内にしてください。")
    if _contains_control_chars(trimmed):
        return ValidationResult(False, original, trimmed, "", "制御文字は使えません。")

    forbidden_in_input = sorted({ch for ch in trimmed if ch in FORBIDDEN_CHARS})
    if forbidden_in_input:
        chars = " ".join(forbidden_in_input)
        return ValidationResult(False, original, trimmed, "", f"Windows 禁止文字が含まれています: {chars}")

    if trimmed.endswith(".") or trimmed.endswith(" "):
        return ValidationResult(False, original, trimmed, "", "末尾のドット/スペースは使えません。")
    if trimmed in {".", ".."}:
        return ValidationResult(False, original, trimmed, "", "`.` と `..` は使えません。")
    if _is_reserved_device_name(trimmed):
        return ValidationResult(False, original, trimmed, "", "Windows 予約デバイス名は使えません。")

    normalized = unicodedata.normalize("NFKC", trimmed)
    canonical = _collapse_underscores(normalized.replace(" ", "_"))
    if not canonical:
        return ValidationResult(False, original, trimmed, "", "正規化後の CLIENT_ID が空になりました。")
    if len(canonical) > 64:
        return ValidationResult(False, original, trimmed, "", "正規化後の CLIENT_ID が64文字を超えています。")
    if _contains_control_chars(canonical):
        return ValidationResult(False, original, trimmed, "", "正規化後に制御文字が含まれています。")

    forbidden_in_canonical = sorted({ch for ch in canonical if ch in FORBIDDEN_CHARS})
    if forbidden_in_canonical:
        chars = " ".join(forbidden_in_canonical)
        return ValidationResult(False, original, trimmed, "", f"正規化後に禁止文字が含まれています: {chars}")
    if canonical.endswith(".") or canonical.endswith(" "):
        return ValidationResult(False, original, trimmed, "", "正規化後の末尾ドット/スペースは使えません。")
    if canonical in {".", ".."}:
        return ValidationResult(False, original, trimmed, "", "正規化後が `.` または `..` です。")
    if _is_reserved_device_name(canonical):
        return ValidationResult(False, original, trimmed, "", "正規化後が Windows 予約デバイス名です。")

    simple_change = _collapse_underscores(trimmed.replace(" ", "_"))
    substantial_change = canonical != simple_change
    return ValidationResult(True, original, trimmed, canonical, substantial_change=substantial_change)


def _display_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def _required_template_dirs(template_line_root: Path, line_id: str) -> list[Path]:
    if line_id == "receipt":
        return [
            template_line_root / "config",
            template_line_root / "outputs" / "runs",
            template_line_root / "artifacts" / "cache",
            template_line_root / "artifacts" / "ingest",
            template_line_root / "artifacts" / "ingest" / "ledger_ref",
            template_line_root / "artifacts" / "ingest" / "kari_shiwake",
            template_line_root / "artifacts" / "telemetry",
            template_line_root / "inputs" / "kari_shiwake",
            template_line_root / "inputs" / "ledger_ref",
        ]
    if line_id == "bank_statement":
        return [
            template_line_root / "config",
            template_line_root / "outputs" / "runs",
            template_line_root / "artifacts" / "cache",
            template_line_root / "artifacts" / "ingest",
            template_line_root / "artifacts" / "ingest" / "training_ocr",
            template_line_root / "artifacts" / "ingest" / "training_reference",
            template_line_root / "artifacts" / "ingest" / "kari_shiwake",
            template_line_root / "artifacts" / "telemetry",
            template_line_root / "inputs" / "kari_shiwake",
            template_line_root / "inputs" / "training",
            template_line_root / "inputs" / "training" / "ocr_kari_shiwake",
            template_line_root / "inputs" / "training" / "reference_yayoi",
        ]
    if line_id == "credit_card_statement":
        return [template_line_root]
    raise ValueError(f"unsupported line for registration: {line_id}")


def _initialize_receipt_category_overrides(repo_root: Path, client_id: str, line_id: str) -> None:
    from belle.defaults import generate_full_category_overrides, load_category_defaults
    from belle.lexicon import load_lexicon
    from belle.lines import line_asset_paths
    from belle.paths import get_category_overrides_path

    assets = line_asset_paths(repo_root, line_id)
    lex = load_lexicon(assets["lexicon_path"])
    global_defaults = load_category_defaults(assets["defaults_path"])
    generate_full_category_overrides(
        path=get_category_overrides_path(repo_root, client_id, line_id=line_id),
        client_id=client_id,
        global_defaults=global_defaults,
        lexicon_category_keys=set(lex.categories_by_key.keys()),
    )


def _ensure_bank_line_config(template_line_root: Path, destination_line_root: Path) -> None:
    destination_config_path = destination_line_root / "config" / "bank_line_config.json"
    if destination_config_path.exists():
        return

    destination_config_path.parent.mkdir(parents=True, exist_ok=True)
    template_config_path = template_line_root / "config" / "bank_line_config.json"
    if template_config_path.exists():
        shutil.copy2(template_config_path, destination_config_path)
        return

    destination_config_path.write_text(
        json.dumps(BANK_LINE_CONFIG_MINIMAL, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--line", default="receipt", help="Document processing line_id")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    clients_dir = repo_root / "clients"
    template_dir = clients_dir / "TEMPLATE"
    legacy_tenants_dir = repo_root / "tenants"

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from belle.lines import is_line_implemented, validate_line_id

    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        return 2
    if line_id not in SUPPORTED_REGISTER_LINES:
        print(f"[ERROR] line is unsupported for register: {line_id}")
        return 2
    if line_id != "credit_card_statement" and not is_line_implemented(line_id):
        print("[ERROR] line is unimplemented in Phase 1")
        return 2

    if legacy_tenants_dir.exists():
        print("[ERROR] `tenants/` が見つかりました。`clients/` へ移行してください。")
        return 2
    if not template_dir.exists() or not template_dir.is_dir():
        print("[ERROR] `clients/TEMPLATE/` が見つかりません。")
        return 2

    template_line_root = template_dir / "lines" / line_id
    if not template_line_root.exists() or not template_line_root.is_dir():
        print(f"[ERROR] `clients/TEMPLATE/lines/{line_id}/` が見つかりません。")
        return 2

    required_dirs = _required_template_dirs(template_line_root, line_id)
    missing_required = [p for p in required_dirs if not p.exists() or not p.is_dir()]
    if missing_required:
        print(f"[ERROR] `clients/TEMPLATE/lines/{line_id}/` に必要ディレクトリが不足しています。")
        for p in missing_required:
            print(f"  - {_display_path(p, repo_root)}")
        return 2

    print("新しいクライアントディレクトリを作成します。")
    print("`clients/TEMPLATE/` を `clients/<CLIENT_ID>/` にコピーします。")
    print("スペースは `_` に正規化されます。")
    user_input = input("作成する名前(CLIENT_ID): ")
    result = validate_and_canonicalize(user_input)

    if not result.ok:
        print(f"[ERROR] {result.reason}")
        return 1

    if result.trimmed != result.canonical:
        print(f"入力値: {result.trimmed}")
        print(f"正規化: {result.canonical}")
        if result.substantial_change:
            answer = input("この正規化名で作成しますか? [y/N]: ").strip().lower()
            if answer not in {"y", "yes"}:
                print("作成を中止しました。")
                return 1
    else:
        print(f"CLIENT_ID: {result.canonical}")

    destination = clients_dir / result.canonical
    if destination.exists():
        print(f"[ERROR] 既に存在します: {_display_path(destination, repo_root)}")
        return 1

    shutil.copytree(template_dir, destination)
    (destination / "config").mkdir(parents=True, exist_ok=True)

    destination_line_root = destination / "lines" / line_id
    if line_id == "receipt":
        try:
            _initialize_receipt_category_overrides(repo_root, result.canonical, line_id)
        except Exception as exc:
            print("[ERROR] category_overrides.json の初期化に失敗しました。")
            print(f"[ERROR] {exc}")
            return 2
    elif line_id == "bank_statement":
        try:
            _ensure_bank_line_config(template_line_root, destination_line_root)
        except Exception as exc:
            print("[ERROR] bank_line_config.json の初期化に失敗しました。")
            print(f"[ERROR] {exc}")
            return 2

    created_path = _display_path(destination, repo_root)
    print(f"[OK] 作成完了: {created_path}")
    if line_id == "receipt":
        print("- 置換入力: clients/<CLIENT_ID>/lines/receipt/inputs/kari_shiwake/")
        print("- 参照入力: clients/<CLIENT_ID>/lines/receipt/inputs/ledger_ref/")
        print("- 上書き設定: clients/<CLIENT_ID>/lines/receipt/config/category_overrides.json")
    elif line_id == "bank_statement":
        print("- 学習入力(ocr): clients/<CLIENT_ID>/lines/bank_statement/inputs/training/ocr_kari_shiwake/")
        print("- 学習入力(弥生): clients/<CLIENT_ID>/lines/bank_statement/inputs/training/reference_yayoi/")
        print("- 置換入力: clients/<CLIENT_ID>/lines/bank_statement/inputs/kari_shiwake/")
        print("- ライン設定: clients/<CLIENT_ID>/lines/bank_statement/config/bank_line_config.json")
    else:
        print(f"- ライン: clients/<CLIENT_ID>/lines/{line_id}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
