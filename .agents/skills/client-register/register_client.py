#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import re
import shutil
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path

FORBIDDEN_CHARS = set('\\/:*?"<>|')
RESERVED_DEVICE_NAMES = {"CON", "PRN", "AUX", "NUL"}


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
    from belle.defaults import generate_full_category_overrides, load_category_defaults
    from belle.lexicon import load_lexicon
    from belle.lines import is_line_implemented, line_asset_paths, validate_line_id
    from belle.paths import get_category_overrides_path

    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        return 2
    if not is_line_implemented(line_id):
        print("[ERROR] line is unimplemented in Phase 1")
        return 2

    if legacy_tenants_dir.exists():
        print("[ERROR] `tenants/` が見つかりました。`clients/` へ移行してください。")
        return 2
    if not template_dir.exists() or not template_dir.is_dir():
        print("[ERROR] `clients/TEMPLATE/` が見つかりません。")
        return 2

    template_line_root = template_dir / "lines" / line_id
    required_dirs = [
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
    missing_required = [p for p in required_dirs if not p.exists()]
    if missing_required:
        print("[ERROR] `clients/TEMPLATE/` に必要ディレクトリが不足しています。")
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

    try:
        assets = line_asset_paths(repo_root, line_id)
        lex = load_lexicon(assets["lexicon_path"])
        global_defaults = load_category_defaults(assets["defaults_path"])
        generate_full_category_overrides(
            path=get_category_overrides_path(repo_root, result.canonical, line_id=line_id),
            client_id=result.canonical,
            global_defaults=global_defaults,
            lexicon_category_keys=set(lex.categories_by_key.keys()),
        )
    except Exception as exc:
        print("[ERROR] category_overrides.json の初期化に失敗しました。")
        print(f"[ERROR] {exc}")
        return 2

    created_path = _display_path(destination, repo_root)
    print(f"[OK] 作成完了: {created_path}")
    print("- 置換入力: clients/<CLIENT_ID>/lines/receipt/inputs/kari_shiwake/")
    print("- 参照入力: clients/<CLIENT_ID>/lines/receipt/inputs/ledger_ref/")
    print("- 上書き設定: clients/<CLIENT_ID>/lines/receipt/config/category_overrides.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
