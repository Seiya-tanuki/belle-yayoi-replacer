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
from uuid import uuid4

FORBIDDEN_CHARS = set('\\/:*?"<>|')
RESERVED_DEVICE_NAMES = {"CON", "PRN", "AUX", "NUL"}
REGISTER_LINES = ("receipt", "bank_statement", "credit_card_statement")
CATEGORY_OVERRIDES_LINES = ("receipt", "credit_card_statement")

BANK_LINE_CONFIG_MINIMAL = {
    "schema": "belle.bank_line_config.v0",
    "version": "0.1",
    "placeholder_account_name": "\u4eee\u6255\u91d1",
    "bank_account_name": "\u666e\u901a\u9810\u91d1",
    "bank_account_subaccount": "",
    "pairing": {
        "join_key": ["date", "sign", "amount"],
        "require_unique_on_both_sides": True,
    },
    "thresholds": {
        "kana_sign_amount": {"min_count": 2, "min_p_majority": 0.85},
        "kana_sign": {"min_count": 3, "min_p_majority": 0.80},
        "file_level_bank_sub_inference": {"min_votes": 3, "min_p_majority": 0.9},
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


@dataclass(frozen=True)
class RegistrationError(RuntimeError):
    headline: str
    detail: str = ""


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
        return ValidationResult(False, original, trimmed, "", "CLIENT_ID must not be empty.")
    if len(trimmed) > 64:
        return ValidationResult(False, original, trimmed, "", "CLIENT_ID must be 64 chars or fewer.")
    if _contains_control_chars(trimmed):
        return ValidationResult(False, original, trimmed, "", "Control characters are not allowed.")

    forbidden_in_input = sorted({ch for ch in trimmed if ch in FORBIDDEN_CHARS})
    if forbidden_in_input:
        chars = " ".join(forbidden_in_input)
        return ValidationResult(False, original, trimmed, "", f"Windows-forbidden characters are included: {chars}")

    if trimmed.endswith(".") or trimmed.endswith(" "):
        return ValidationResult(False, original, trimmed, "", "Trailing dot or space is not allowed.")
    if trimmed in {".", ".."}:
        return ValidationResult(False, original, trimmed, "", "`.` and `..` are not allowed.")
    if _is_reserved_device_name(trimmed):
        return ValidationResult(False, original, trimmed, "", "Reserved Windows device names are not allowed.")

    normalized = unicodedata.normalize("NFKC", trimmed)
    canonical = _collapse_underscores(normalized.replace(" ", "_"))
    if not canonical:
        return ValidationResult(False, original, trimmed, "", "Canonicalized CLIENT_ID became empty.")
    if len(canonical) > 64:
        return ValidationResult(False, original, trimmed, "", "Canonicalized CLIENT_ID exceeds 64 chars.")
    if _contains_control_chars(canonical):
        return ValidationResult(False, original, trimmed, "", "Canonicalized CLIENT_ID has control characters.")

    forbidden_in_canonical = sorted({ch for ch in canonical if ch in FORBIDDEN_CHARS})
    if forbidden_in_canonical:
        chars = " ".join(forbidden_in_canonical)
        return ValidationResult(
            False,
            original,
            trimmed,
            "",
            f"Canonicalized CLIENT_ID includes forbidden characters: {chars}",
        )
    if canonical.endswith(".") or canonical.endswith(" "):
        return ValidationResult(False, original, trimmed, "", "Canonicalized CLIENT_ID has trailing dot/space.")
    if canonical in {".", ".."}:
        return ValidationResult(False, original, trimmed, "", "Canonicalized CLIENT_ID is `.` or `..`.")
    if _is_reserved_device_name(canonical):
        return ValidationResult(False, original, trimmed, "", "Canonicalized CLIENT_ID is a reserved device name.")

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


def _initialize_category_overrides(
    repo_root: Path,
    client_id: str,
    line_id: str,
    destination_line_root: Path,
) -> None:
    from belle.defaults import generate_full_category_overrides, load_category_defaults
    from belle.lexicon import load_lexicon
    from belle.lines import line_asset_paths

    assets = line_asset_paths(repo_root, line_id)
    lex = load_lexicon(assets["lexicon_path"])
    global_defaults = load_category_defaults(assets["defaults_path"])
    generate_full_category_overrides(
        path=destination_line_root / "config" / "category_overrides.json",
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


def _verify_template_contract(template_dir: Path, line_ids: tuple[str, ...]) -> tuple[list[Path], list[Path]]:
    missing_line_roots: list[Path] = []
    missing_required_dirs: list[Path] = []
    for line_id in line_ids:
        line_root = template_dir / "lines" / line_id
        if not line_root.exists() or not line_root.is_dir():
            missing_line_roots.append(line_root)
            continue
        required_dirs = _required_template_dirs(line_root, line_id)
        missing_required_dirs.extend([p for p in required_dirs if not p.exists() or not p.is_dir()])
    return missing_line_roots, missing_required_dirs


def _selected_lines(line_arg: str) -> tuple[str, ...]:
    normalized = str(line_arg or "all").strip().lower()
    if normalized == "all":
        return REGISTER_LINES
    if normalized in REGISTER_LINES:
        return (normalized,)
    raise ValueError(f"unsupported --line value: {line_arg!r}")


def _prune_unselected_lines(destination: Path, selected_lines: tuple[str, ...]) -> None:
    lines_root = destination / "lines"
    selected = set(selected_lines)
    for line_id in REGISTER_LINES:
        if line_id in selected:
            continue
        stale_root = lines_root / line_id
        if stale_root.exists():
            shutil.rmtree(stale_root)


def _make_staging_destination(clients_dir: Path, client_id: str) -> Path:
    prefix = f"__client_register_staging_{client_id}_"
    for _ in range(16):
        candidate = clients_dir / f"{prefix}{uuid4().hex}"
        if not candidate.exists():
            return candidate
    raise RuntimeError("Could not allocate unique staging directory name.")


def _cleanup_staging_directory(staging_dir: Path) -> Exception | None:
    if not staging_dir.exists():
        return None
    try:
        shutil.rmtree(staging_dir)
    except Exception as exc:  # pragma: no cover - defensive path
        return exc
    return None


def _initialize_staged_client(
    *,
    repo_root: Path,
    template_dir: Path,
    staging_dir: Path,
    client_id: str,
    selected_lines: tuple[str, ...],
) -> None:
    shutil.copytree(template_dir, staging_dir)
    (staging_dir / "config").mkdir(parents=True, exist_ok=True)
    _prune_unselected_lines(staging_dir, selected_lines)

    for line_id in selected_lines:
        if line_id not in CATEGORY_OVERRIDES_LINES:
            continue
        try:
            _initialize_category_overrides(
                repo_root,
                client_id,
                line_id,
                staging_dir / "lines" / line_id,
            )
        except Exception as exc:
            raise RegistrationError(
                f"Failed to initialize category_overrides.json for line={line_id}.",
                str(exc),
            ) from exc

    if "bank_statement" in selected_lines:
        bank_template_line_root = template_dir / "lines" / "bank_statement"
        bank_destination_line_root = staging_dir / "lines" / "bank_statement"
        try:
            _ensure_bank_line_config(bank_template_line_root, bank_destination_line_root)
        except Exception as exc:
            raise RegistrationError(
                "Failed to initialize bank_line_config.json.",
                str(exc),
            ) from exc

    for line_id in selected_lines:
        line_root = staging_dir / "lines" / line_id
        if not line_root.exists() or not line_root.is_dir():
            raise RegistrationError(f"{line_id} line directory is missing after registration.")


def _publish_staged_client(staging_dir: Path, destination: Path, repo_root: Path) -> None:
    if destination.exists():
        raise RegistrationError(f"Already exists: {_display_path(destination, repo_root)}")
    try:
        staging_dir.rename(destination)
    except Exception as exc:
        raise RegistrationError("Failed to publish staged client directory.", str(exc)) from exc


def _print_created_paths(selected_lines: tuple[str, ...]) -> None:
    if "receipt" in selected_lines:
        print("- receipt: clients/<CLIENT_ID>/lines/receipt/inputs/kari_shiwake/")
        print("- receipt: clients/<CLIENT_ID>/lines/receipt/inputs/ledger_ref/")
        print("- receipt: clients/<CLIENT_ID>/lines/receipt/config/category_overrides.json")
    if "bank_statement" in selected_lines:
        print("- bank_statement: clients/<CLIENT_ID>/lines/bank_statement/inputs/training/ocr_kari_shiwake/")
        print("- bank_statement: clients/<CLIENT_ID>/lines/bank_statement/inputs/training/reference_yayoi/")
        print("- bank_statement: clients/<CLIENT_ID>/lines/bank_statement/inputs/kari_shiwake/")
        print("- bank_statement: clients/<CLIENT_ID>/lines/bank_statement/config/bank_line_config.json")
    if "credit_card_statement" in selected_lines:
        print("- credit_card_statement: clients/<CLIENT_ID>/lines/credit_card_statement/")
        print("- credit_card_statement: clients/<CLIENT_ID>/lines/credit_card_statement/config/category_overrides.json")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--line",
        default="all",
        choices=("all",) + REGISTER_LINES,
        help="Provision target line. Default: all lines.",
    )
    args = parser.parse_args()
    selected_lines = _selected_lines(args.line)

    repo_root = Path(__file__).resolve().parents[3]
    clients_dir = repo_root / "clients"
    template_dir = clients_dir / "TEMPLATE"
    legacy_tenants_dir = repo_root / "tenants"

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from belle.lines import is_line_implemented

    for line_id in selected_lines:
        if not is_line_implemented(line_id):
            print(f"[ERROR] line is unimplemented: {line_id}")
            return 2

    if legacy_tenants_dir.exists():
        print("[ERROR] `tenants/` was found. Please migrate to `clients/` first.")
        return 2
    if not template_dir.exists() or not template_dir.is_dir():
        print("[ERROR] `clients/TEMPLATE/` is missing.")
        return 2

    missing_line_roots, missing_required_dirs = _verify_template_contract(template_dir, selected_lines)
    if missing_line_roots:
        print("[ERROR] Required line roots are missing under `clients/TEMPLATE/lines/`.")
        for p in missing_line_roots:
            print(f"  - {_display_path(p, repo_root)}")
        return 2
    if missing_required_dirs:
        print("[ERROR] Required template directories are missing.")
        for p in missing_required_dirs:
            print(f"  - {_display_path(p, repo_root)}")
        return 2

    print("Create a new client directory.")
    print("Copy `clients/TEMPLATE/` to `clients/<CLIENT_ID>/`.")
    print("Spaces in CLIENT_ID are canonicalized to `_`.")
    user_input = input("Enter CLIENT_ID: ")
    result = validate_and_canonicalize(user_input)

    if not result.ok:
        print(f"[ERROR] {result.reason}")
        return 1

    if result.trimmed != result.canonical:
        print(f"Input: {result.trimmed}")
        print(f"Canonical: {result.canonical}")
        if result.substantial_change:
            answer = input("Proceed with canonicalized CLIENT_ID? [y/N]: ").strip().lower()
            if answer not in {"y", "yes"}:
                print("Cancelled.")
                return 1
    else:
        print(f"CLIENT_ID: {result.canonical}")

    destination = clients_dir / result.canonical
    if destination.exists():
        print(f"[ERROR] Already exists: {_display_path(destination, repo_root)}")
        return 1

    staging_dir = _make_staging_destination(clients_dir, result.canonical)
    try:
        _initialize_staged_client(
            repo_root=repo_root,
            template_dir=template_dir,
            staging_dir=staging_dir,
            client_id=result.canonical,
            selected_lines=selected_lines,
        )
        _publish_staged_client(staging_dir, destination, repo_root)
    except RegistrationError as exc:
        cleanup_error = _cleanup_staging_directory(staging_dir)
        print(f"[ERROR] {exc.headline}")
        if exc.detail:
            print(f"[ERROR] {exc.detail}")
        if cleanup_error is not None:
            print(f"[ERROR] Failed to clean up staging directory: {_display_path(staging_dir, repo_root)}")
            print(f"[ERROR] {cleanup_error}")
        return 2
    except Exception as exc:
        cleanup_error = _cleanup_staging_directory(staging_dir)
        print("[ERROR] Client registration failed before publish.")
        print(f"[ERROR] {exc}")
        if cleanup_error is not None:
            print(f"[ERROR] Failed to clean up staging directory: {_display_path(staging_dir, repo_root)}")
            print(f"[ERROR] {cleanup_error}")
        return 2

    created_path = _display_path(destination, repo_root)
    print(f"[OK] Created: {created_path}")
    _print_created_paths(selected_lines)
    return 0


if __name__ == "__main__":
    sys.exit(main())

