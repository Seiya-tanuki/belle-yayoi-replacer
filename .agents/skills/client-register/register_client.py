#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

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
        return ValidationResult(False, original, trimmed, "", "顧客名が空です。")
    if len(trimmed) > 64:
        return ValidationResult(False, original, trimmed, "", "顧客名は64文字以内で入力してください。")
    if _contains_control_chars(trimmed):
        return ValidationResult(False, original, trimmed, "", "制御文字は使用できません。")

    forbidden_in_input = sorted({ch for ch in trimmed if ch in FORBIDDEN_CHARS})
    if forbidden_in_input:
        chars = " ".join(forbidden_in_input)
        return ValidationResult(False, original, trimmed, "", f"Windows禁止文字が含まれています: {chars}")

    if trimmed.endswith(".") or trimmed.endswith(" "):
        return ValidationResult(False, original, trimmed, "", "末尾のドット・スペースは使用できません。")
    if trimmed in {".", ".."}:
        return ValidationResult(False, original, trimmed, "", "`.` と `..` は使用できません。")
    if _is_reserved_device_name(trimmed):
        return ValidationResult(False, original, trimmed, "", "Windows予約デバイス名は使用できません。")

    normalized = unicodedata.normalize("NFKC", trimmed)
    canonical = _collapse_underscores(normalized.replace(" ", "_"))
    if not canonical:
        return ValidationResult(False, original, trimmed, "", "正規化後に空文字になりました。")
    if len(canonical) > 64:
        return ValidationResult(False, original, trimmed, "", "正規化後の顧客名が64文字を超えています。")
    if _contains_control_chars(canonical):
        return ValidationResult(False, original, trimmed, "", "正規化後に制御文字が含まれています。")

    forbidden_in_canonical = sorted({ch for ch in canonical if ch in FORBIDDEN_CHARS})
    if forbidden_in_canonical:
        chars = " ".join(forbidden_in_canonical)
        return ValidationResult(False, original, trimmed, "", f"正規化後に禁止文字が含まれています: {chars}")
    if canonical.endswith(".") or canonical.endswith(" "):
        return ValidationResult(False, original, trimmed, "", "正規化後の末尾ドット・スペースは使用できません。")
    if canonical in {".", ".."}:
        return ValidationResult(False, original, trimmed, "", "正規化後の名前が `.` または `..` です。")
    if _is_reserved_device_name(canonical):
        return ValidationResult(False, original, trimmed, "", "正規化後の名前がWindows予約デバイス名です。")

    simple_change = _collapse_underscores(trimmed.replace(" ", "_"))
    substantial_change = canonical != simple_change
    return ValidationResult(True, original, trimmed, canonical, substantial_change=substantial_change)


def _display_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[3]
    clients_dir = repo_root / "clients"
    template_dir = clients_dir / "TEMPLATE"
    legacy_tenants_dir = repo_root / "tenants"

    if legacy_tenants_dir.exists():
        print("[ERROR] `tenants/` が見つかりました。`clients/` に移行してください。")
        return 2
    if not template_dir.exists() or not template_dir.is_dir():
        print("[ERROR] `clients/TEMPLATE/` が見つかりません。")
        return 2

    required_dirs = [
        template_dir / "config",
        template_dir / "outputs" / "runs",
        template_dir / "artifacts" / "cache",
        template_dir / "artifacts" / "ingest",
        template_dir / "artifacts" / "telemetry",
        template_dir / "inputs" / "kari_shiwake",
        template_dir / "inputs" / "ledger_ref",
    ]
    missing_required = [p for p in required_dirs if not p.exists()]
    if missing_required:
        print("[ERROR] `clients/TEMPLATE/` に必要ディレクトリが不足しています。")
        for p in missing_required:
            print(f"  - {_display_path(p, repo_root)}")
        return 2

    print("新しい顧客ディレクトリを作成します。")
    print("`clients/TEMPLATE/` を `clients/<CLIENT_ID>/` にコピーします。")
    print("スペースは `_` に正規化されます。")
    user_input = input("作成する顧客名 (CLIENT_ID): ")
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
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from belle.defaults import generate_full_category_overrides, load_category_defaults
    from belle.lexicon import load_lexicon
    from belle.paths import get_category_overrides_path

    try:
        lex = load_lexicon(repo_root / "lexicon" / "lexicon.json")
        global_defaults = load_category_defaults(repo_root / "defaults" / "category_defaults.json")
        generate_full_category_overrides(
            path=get_category_overrides_path(repo_root, result.canonical),
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
    print("- 入力ファイル配置先: "
          f"{created_path}/inputs/kari_shiwake/, {created_path}/inputs/ledger_ref/")
    print("- 次に使うスキル: $client-cache-builder, $yayoi-replacer, $lexicon-extract, $lexicon-apply")
    print(f"- クライアント個別設定: {created_path}/config/category_overrides.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
