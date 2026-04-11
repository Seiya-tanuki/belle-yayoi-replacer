#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from belle.lines import is_line_implemented, tracked_category_defaults_relpaths, validate_line_id

_SUPPORTED_LINE_IDS = ("receipt", "bank_statement", "credit_card_statement")
_SUPPORTED_LINE_IDS_WITH_ALL = _SUPPORTED_LINE_IDS + ("all",)
_REPORT_RENDER_ONLY_ENV = "BELLE_SYSTEM_DIAGNOSE_RENDER_ONLY"
_REPORT_BEGIN_MARKER = "<<<SYSTEM_DIAGNOSE_REPORT_BEGIN>>>"
_REPORT_END_MARKER = "<<<SYSTEM_DIAGNOSE_REPORT_END>>>"
_YAYOI_TAX_CONFIG_SCHEMA = "belle.yayoi_tax_config.v1"
_YAYOI_TAX_CONFIG_VERSION = "1.0"
_SUPPORTED_TAX_BOOKKEEPING_MODES = {"tax_excluded", "tax_included"}
_SUPPORTED_TAX_ROUNDING_MODES = {"floor"}
_CATEGORY_OVERRIDES_SCHEMA_V2 = "belle.category_overrides.v2"
_UTF8_BOM = b"\xef\xbb\xbf"
_RECEIPT_TAX_THRESHOLD_ROUTES = (
    "t_number_x_category_target_account",
    "t_number_target_account",
    "vendor_key_target_account",
    "category_target_account",
    "global_target_account",
)
_RECEIPT_TAX_CONFIDENCE_KEYS = (
    "t_number_x_category_target_account_strength",
    "t_number_target_account_strength",
    "vendor_key_target_account_strength",
    "category_target_account_strength",
    "global_target_account_strength",
    "category_default_strength",
    "global_fallback_strength",
    "learned_weight_multiplier",
)
_CC_TAX_THRESHOLD_ROUTES = (
    "merchant_key_target_account_exact",
    "merchant_key_target_account_partial",
)


@dataclass
class CommandResult:
    command: str
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool
    error: str | None
    duration_sec: float


@dataclass
class CheckResult:
    check_id: str
    label: str
    passed: bool
    evidence: str
    remediation: str
    hard: bool


@dataclass
class Risk:
    severity: str
    check_id: str
    title: str
    remediation: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_iso(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _utc_compact(ts: datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M%SZ")


def _run_command(
    command: str,
    cwd: Path,
    timeout_sec: int = 30,
    env: dict[str, str] | None = None,
) -> CommandResult:
    started = time.perf_counter()
    try:
        proc = subprocess.run(
            command,
            cwd=str(cwd),
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_sec,
            check=False,
        )
        duration = time.perf_counter() - started
        return CommandResult(
            command=command,
            returncode=proc.returncode,
            stdout=proc.stdout or "",
            stderr=proc.stderr or "",
            timed_out=False,
            error=None,
            duration_sec=duration,
        )
    except subprocess.TimeoutExpired as exc:
        duration = time.perf_counter() - started
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        return CommandResult(
            command=command,
            returncode=None,
            stdout=stdout,
            stderr=stderr,
            timed_out=True,
            error=f"timeout after {timeout_sec}s",
            duration_sec=duration,
        )
    except Exception as exc:  # pragma: no cover - defensive
        duration = time.perf_counter() - started
        return CommandResult(
            command=command,
            returncode=None,
            stdout="",
            stderr="",
            timed_out=False,
            error=f"{type(exc).__name__}: {exc}",
            duration_sec=duration,
        )


def _trim_text(value: str, max_chars: int = 12000) -> str:
    text = value.strip("\n")
    if not text:
        return "(empty)"
    if len(text) <= max_chars:
        return text
    head = text[: max_chars - 80]
    omitted = len(text) - len(head)
    return f"{head}\n... [trimmed {omitted} chars]"


def _escape_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


def _result_evidence(res: CommandResult) -> str:
    if res.timed_out:
        return f"timeout ({res.error})"
    if res.error:
        return res.error
    output = (res.stdout.strip() or res.stderr.strip() or "(no output)")
    one_line = output.splitlines()[0][:240]
    return f"exit={res.returncode}; {one_line}"


def _parse_porcelain_paths(stdout: str) -> List[str]:
    paths: List[str] = []
    for raw_line in stdout.splitlines():
        line = raw_line.rstrip("\r\n")
        if not line:
            continue
        if line.startswith("?? "):
            path = line[3:].strip()
        else:
            path = line[3:].strip() if len(line) >= 3 else ""
        if path:
            paths.append(path)
    return paths


def _detect_replacer_config(repo_root: Path, line_id: str) -> tuple[Path | None, str]:
    line_rulesets = repo_root / "rulesets" / line_id
    exact = line_rulesets / "replacer_config_v1_15.json"
    if exact.exists():
        return exact, f"found active default: rulesets/{line_id}/replacer_config_v1_15.json"

    readme = line_rulesets / "README.md"
    if readme.exists():
        text = readme.read_text(encoding="utf-8", errors="replace")
        for match in re.findall(r"`(replacer_config_[^`]+\.json)`", text):
            candidate = line_rulesets / match
            if candidate.exists():
                return candidate, f"detected via rulesets/{line_id}/README.md: {match}"

    candidates = sorted(line_rulesets.glob("replacer_config_v*.json"))
    if candidates:
        latest = candidates[-1]
        return latest, f"fallback to latest versioned config: {latest.name}"
    return None, "no replacer_config_v*.json found"


def _probe_write_delete(target_dir: Path) -> tuple[bool, str]:
    stamp = f"{int(time.time() * 1000)}_{os.getpid()}"
    probe_path = target_dir / f".system_diagnose_probe_{stamp}.tmp"
    try:
        probe_path.write_text("probe\n", encoding="utf-8", newline="\n")
        probe_path.unlink()
        return True, "create+delete succeeded"
    except Exception as exc:
        if probe_path.exists():
            try:
                probe_path.unlink()
            except Exception:
                pass
        return False, f"{type(exc).__name__}: {exc}"


def _ensure_required_dirs(repo_root: Path, line_id: str) -> List[Path]:
    required_rel_paths = [
        Path("exports"),
        Path("exports/system_diagnose"),
        Path("exports/gpts_lexicon_review"),
        Path("exports/backups"),
    ]
    if line_id == "receipt":
        required_rel_paths.append(Path("lexicon") / line_id / "pending" / "locks")
    created: List[Path] = []
    for rel_path in required_rel_paths:
        abs_path = repo_root / rel_path
        if not abs_path.exists():
            created.append(rel_path)
        abs_path.mkdir(parents=True, exist_ok=True)
    return created


def _iter_non_placeholder_files(dir_path: Path) -> List[Path]:
    if not dir_path.exists() or not dir_path.is_dir():
        return []
    files: List[Path] = []
    for p in dir_path.iterdir():
        if not p.is_file():
            continue
        if p.name == ".gitkeep":
            continue
        if p.name.endswith(".tmp"):
            continue
        files.append(p)
    return sorted(files, key=lambda p: p.name)


def _count_ingested_entries(manifest_path: Path) -> tuple[int, str | None]:
    if not manifest_path.exists():
        return 0, "manifest_missing"
    try:
        obj = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return 0, f"manifest_parse_error:{type(exc).__name__}"
    ingested = obj.get("ingested")
    if not isinstance(ingested, dict):
        return 0, "manifest_ingested_not_object"
    if not ingested:
        return 0, None
    ingested_order = obj.get("ingested_order")
    if isinstance(ingested_order, list):
        unique_ordered = {str(sha) for sha in ingested_order if str(sha) in ingested}
        if unique_ordered:
            return len(unique_ordered), None
    return len(ingested), None


def _shared_tax_config_path(repo_root: Path, client_id: str) -> Path:
    return repo_root / "clients" / client_id / "config" / "yayoi_tax_config.json"


def _discover_non_template_clients(repo_root: Path) -> List[tuple[str, Path]]:
    clients_dir = repo_root / "clients"
    if not clients_dir.exists():
        return []
    found: List[tuple[str, Path]] = []
    for client_dir in sorted(clients_dir.iterdir(), key=lambda p: p.name):
        if not client_dir.is_dir() or client_dir.name == "TEMPLATE":
            continue
        found.append((client_dir.name, client_dir))
    return found


def _fallback_load_yayoi_tax_postprocess_config(repo_root: Path, client_id: str):
    cfg_path = _shared_tax_config_path(repo_root, client_id)
    if not cfg_path.exists():
        return {
            "enabled": False,
            "bookkeeping_mode": "tax_excluded",
            "rounding_mode": "floor",
        }

    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to parse yayoi_tax_config.json: {cfg_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"yayoi_tax_config.json must be a JSON object: {cfg_path}")

    allowed_keys = {"schema", "version", "enabled", "bookkeeping_mode", "rounding_mode"}
    actual_keys = set(raw.keys())
    missing_keys = sorted(allowed_keys - actual_keys)
    extra_keys = sorted(actual_keys - allowed_keys)
    if missing_keys:
        raise ValueError(f"yayoi_tax_config.json missing required keys: {', '.join(missing_keys)}: {cfg_path}")
    if extra_keys:
        raise ValueError(f"yayoi_tax_config.json contains unsupported keys: {', '.join(extra_keys)}: {cfg_path}")

    schema = str(raw.get("schema") or "").strip()
    if schema != _YAYOI_TAX_CONFIG_SCHEMA:
        raise ValueError(f"yayoi_tax_config.json schema must be {_YAYOI_TAX_CONFIG_SCHEMA!r}: {cfg_path}")
    version = str(raw.get("version") or "").strip()
    if version != _YAYOI_TAX_CONFIG_VERSION:
        raise ValueError(f"yayoi_tax_config.json version must be {_YAYOI_TAX_CONFIG_VERSION!r}: {cfg_path}")

    enabled = raw.get("enabled")
    if not isinstance(enabled, bool):
        raise ValueError(f"yayoi_tax_config.json enabled must be a boolean: {cfg_path}")

    bookkeeping_mode = str(raw.get("bookkeeping_mode") or "").strip()
    if bookkeeping_mode not in _SUPPORTED_TAX_BOOKKEEPING_MODES:
        supported = ", ".join(sorted(_SUPPORTED_TAX_BOOKKEEPING_MODES))
        raise ValueError(f"yayoi_tax_config.json bookkeeping_mode must be one of [{supported}]: {cfg_path}")

    rounding_mode = str(raw.get("rounding_mode") or "").strip()
    if rounding_mode not in _SUPPORTED_TAX_ROUNDING_MODES:
        supported = ", ".join(sorted(_SUPPORTED_TAX_ROUNDING_MODES))
        raise ValueError(f"yayoi_tax_config.json rounding_mode must be one of [{supported}]: {cfg_path}")

    return {
        "enabled": enabled,
        "bookkeeping_mode": bookkeeping_mode,
        "rounding_mode": rounding_mode,
    }


def _load_yayoi_tax_postprocess_config(repo_root: Path, client_id: str):
    try:
        from belle.tax_postprocess import load_yayoi_tax_postprocess_config
    except ImportError:
        return _fallback_load_yayoi_tax_postprocess_config(repo_root, client_id)
    return load_yayoi_tax_postprocess_config(repo_root, client_id)


def _format_shared_tax_config_state(client_id: str, config_obj) -> str:
    if isinstance(config_obj, dict):
        enabled = bool(config_obj.get("enabled"))
        bookkeeping_mode = str(config_obj.get("bookkeeping_mode") or "")
        rounding_mode = str(config_obj.get("rounding_mode") or "")
    else:
        enabled = bool(getattr(config_obj, "enabled"))
        bookkeeping_mode = str(getattr(config_obj, "bookkeeping_mode"))
        rounding_mode = str(getattr(config_obj, "rounding_mode"))
    return (
        f"{client_id}(enabled={enabled}, bookkeeping_mode={bookkeeping_mode}, "
        f"rounding_mode={rounding_mode})"
    )


def _validate_shared_tax_bootstrap_policy(client_id: str, config_obj) -> tuple[bool, str]:
    if isinstance(config_obj, dict):
        enabled = bool(config_obj.get("enabled"))
        bookkeeping_mode = str(config_obj.get("bookkeeping_mode") or "")
        rounding_mode = str(config_obj.get("rounding_mode") or "")
    else:
        enabled = bool(getattr(config_obj, "enabled"))
        bookkeeping_mode = str(getattr(config_obj, "bookkeeping_mode"))
        rounding_mode = str(getattr(config_obj, "rounding_mode"))

    state = _format_shared_tax_config_state(client_id, config_obj)
    expected_enabled = bookkeeping_mode == "tax_excluded"
    issues: List[str] = []
    if bookkeeping_mode == "tax_excluded" and enabled is not True:
        issues.append("expected enabled=true for bookkeeping_mode=tax_excluded")
    if bookkeeping_mode == "tax_included" and enabled is not False:
        issues.append("expected enabled=false for bookkeeping_mode=tax_included")
    if rounding_mode != "floor":
        issues.append("expected rounding_mode=floor")

    expected_summary = (
        f"expected(enabled={expected_enabled}, bookkeeping_mode={bookkeeping_mode}, rounding_mode=floor)"
    )
    if issues:
        return False, f"present_inconsistent: {state}; {expected_summary}; issues=" + ", ".join(issues)
    return True, f"valid_mode_consistent: {state}; {expected_summary}"


def _load_json_object(path: Path, *, label: str) -> dict:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"{label} parse_error: {path}: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return obj


def _is_int_like(value) -> bool:
    try:
        int(value)
    except Exception:
        return False
    return True


def _is_float_like(value) -> bool:
    try:
        float(value)
    except Exception:
        return False
    return True


def _validate_threshold_routes(
    section_obj,
    *,
    section_name: str,
    route_names: Sequence[str],
) -> List[str]:
    issues: List[str] = []
    if not isinstance(section_obj, dict):
        return [f"{section_name}=missing_or_invalid"]
    for route_name in route_names:
        route_obj = section_obj.get(route_name)
        if not isinstance(route_obj, dict):
            issues.append(f"{section_name}.{route_name}=missing_or_invalid")
            continue
        if "min_count" not in route_obj or not _is_int_like(route_obj.get("min_count")):
            issues.append(f"{section_name}.{route_name}.min_count=missing_or_invalid")
        if "min_p_majority" not in route_obj or not _is_float_like(route_obj.get("min_p_majority")):
            issues.append(f"{section_name}.{route_name}.min_p_majority=missing_or_invalid")
    return issues


def _validate_float_key_section(
    section_obj,
    *,
    section_name: str,
    keys: Sequence[str],
) -> List[str]:
    issues: List[str] = []
    if not isinstance(section_obj, dict):
        return [f"{section_name}=missing_or_invalid"]
    for key in keys:
        if key not in section_obj or not _is_float_like(section_obj.get(key)):
            issues.append(f"{section_name}.{key}=missing_or_invalid")
    return issues


def _validate_receipt_runtime_tax_sections(config_path: Path) -> tuple[bool, str]:
    try:
        obj = _load_json_object(config_path, label="receipt replacer config")
    except Exception as exc:
        return False, str(exc)

    issues = _validate_threshold_routes(
        obj.get("tax_division_thresholds"),
        section_name="tax_division_thresholds",
        route_names=_RECEIPT_TAX_THRESHOLD_ROUTES,
    )
    issues.extend(
        _validate_float_key_section(
            obj.get("tax_division_confidence"),
            section_name="tax_division_confidence",
            keys=_RECEIPT_TAX_CONFIDENCE_KEYS,
        )
    )
    if issues:
        return False, f"{config_path.name}: " + "; ".join(issues)
    return (
        True,
        f"{config_path.name}: tax_division_thresholds/routes={len(_RECEIPT_TAX_THRESHOLD_ROUTES)}, "
        f"tax_division_confidence/keys={len(_RECEIPT_TAX_CONFIDENCE_KEYS)}",
    )


def _validate_credit_card_template_config_sections(config_path: Path) -> tuple[bool, str]:
    try:
        obj = _load_json_object(config_path, label="credit_card_line_config")
    except Exception as exc:
        return False, str(exc)

    issues: List[str] = []

    raw_placeholder_names = obj.get("target_payable_placeholder_names")
    if not isinstance(raw_placeholder_names, list):
        issues.append("target_payable_placeholder_names is required and must be a list of non-blank strings")
    else:
        normalized_placeholder_names = [str(value or "").strip() for value in raw_placeholder_names]
        if not any(normalized_placeholder_names):
            issues.append("target_payable_placeholder_names must contain at least one non-blank value")

    teacher_extraction = obj.get("teacher_extraction")
    canonical_payable_thresholds = (
        teacher_extraction.get("canonical_payable_thresholds")
        if isinstance(teacher_extraction, dict)
        else None
    )
    if not isinstance(canonical_payable_thresholds, dict):
        issues.append("teacher_extraction.canonical_payable_thresholds is required and must be an object")
    else:
        min_count = canonical_payable_thresholds.get("min_count")
        if not _is_int_like(min_count) or int(min_count) < 1:
            issues.append("teacher_extraction.canonical_payable_thresholds.min_count must be an integer >= 1")
        min_p_majority = canonical_payable_thresholds.get("min_p_majority")
        if not _is_float_like(min_p_majority):
            issues.append("teacher_extraction.canonical_payable_thresholds.min_p_majority must be > 0 and <= 1")
        else:
            parsed_min_p_majority = float(min_p_majority)
            if parsed_min_p_majority <= 0.0 or parsed_min_p_majority > 1.0:
                issues.append("teacher_extraction.canonical_payable_thresholds.min_p_majority must be > 0 and <= 1")

    issues.extend(
        _validate_threshold_routes(
        obj.get("tax_division_thresholds"),
        section_name="tax_division_thresholds",
        route_names=_CC_TAX_THRESHOLD_ROUTES,
        )
    )
    if issues:
        return False, f"{config_path.name}: " + "; ".join(issues)
    return (
        True,
        f"{config_path.name}: target_payable_placeholder_names=ok; "
        "teacher_extraction.canonical_payable_thresholds=ok; "
        f"tax_division_thresholds/routes={len(_CC_TAX_THRESHOLD_ROUTES)}",
    )


def _discover_clients_with_line(repo_root: Path, line_id: str) -> List[tuple[str, Path]]:
    found: List[tuple[str, Path]] = []
    for client_id, client_dir in _discover_non_template_clients(repo_root):
        line_root = client_dir / "lines" / line_id
        if line_root.exists():
            found.append((client_id, line_root))
    return found


def _discover_receipt_override_targets(repo_root: Path) -> List[tuple[str, Path, str]]:
    found: List[tuple[str, Path, str]] = []
    for client_id, client_dir in _discover_non_template_clients(repo_root):
        line_root = client_dir / "lines" / "receipt"
        if line_root.exists():
            found.append((client_id, line_root / "config" / "category_overrides.json", "line"))
            continue
        legacy_path = client_dir / "config" / "category_overrides.json"
        if legacy_path.exists():
            found.append((client_id, legacy_path, "legacy"))
    return found


def _load_lexicon_category_keys(repo_root: Path) -> List[str]:
    lexicon_path = repo_root / "lexicon" / "lexicon.json"
    if not lexicon_path.exists():
        return []
    try:
        obj = _load_json_object(lexicon_path, label="lexicon")
    except Exception:
        return []
    raw_categories = obj.get("categories")
    if not isinstance(raw_categories, list):
        return []
    keys: List[str] = []
    for raw_row in raw_categories:
        if not isinstance(raw_row, dict):
            continue
        key = str(raw_row.get("key") or "").strip()
        if key:
            keys.append(key)
    return sorted(set(keys))


def _validate_category_overrides_contract(
    repo_root: Path,
    path: Path,
    *,
    lexicon_category_keys: Sequence[str],
    layout_label: str,
) -> tuple[str, str]:
    rel_path = path.relative_to(repo_root).as_posix()
    if not path.exists():
        return "missing_optional", f"optional_missing: layout={layout_label}; path={rel_path}"

    raw_bytes = path.read_bytes()
    has_bom = raw_bytes.startswith(_UTF8_BOM)
    parse_bytes = raw_bytes[len(_UTF8_BOM) :] if has_bom else raw_bytes
    notes: List[str] = []
    invalid_reasons: List[str] = []
    if has_bom:
        notes.append("utf8_bom_present")

    try:
        decoded = parse_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        invalid_reasons.append(f"invalid_utf8@{exc.start}:{exc.end}")
        return "invalid", f"invalid_present: layout={layout_label}; path={rel_path}; " + "; ".join(invalid_reasons)

    try:
        obj = json.loads(decoded)
    except json.JSONDecodeError as exc:
        invalid_reasons.append(f"invalid_json@line={exc.lineno},col={exc.colno}")
        return "invalid", f"invalid_present: layout={layout_label}; path={rel_path}; " + "; ".join(invalid_reasons)

    if not isinstance(obj, dict):
        invalid_reasons.append(f"top_level_invalid:{type(obj).__name__}")
        return "invalid", f"invalid_present: layout={layout_label}; path={rel_path}; " + "; ".join(invalid_reasons)

    schema = str(obj.get("schema") or "").strip()
    if schema != _CATEGORY_OVERRIDES_SCHEMA_V2:
        invalid_reasons.append(
            f"schema_invalid: expected={_CATEGORY_OVERRIDES_SCHEMA_V2} actual={schema or '(empty)'}"
        )
        return "invalid", f"invalid_present: layout={layout_label}; path={rel_path}; " + "; ".join(invalid_reasons)

    overrides = obj.get("overrides")
    if not isinstance(overrides, dict):
        invalid_reasons.append(f"overrides_invalid:{type(overrides).__name__}")
        return "invalid", f"invalid_present: layout={layout_label}; path={rel_path}; " + "; ".join(invalid_reasons)

    expected_keys = {str(k) for k in lexicon_category_keys}
    actual_keys = {str(k) for k in overrides.keys()}
    keys_to_validate = sorted(expected_keys or actual_keys)

    if expected_keys:
        missing_keys = sorted(expected_keys - actual_keys)
        extra_keys = sorted(actual_keys - expected_keys)
        if missing_keys:
            notes.append(f"missing_keys={len(missing_keys)}")
        if extra_keys:
            notes.append(f"extra_keys={len(extra_keys)}")

    applied_count = 0
    row_invalid = 0
    row_missing_keys = 0
    row_extra_keys = 0
    row_value_invalid = 0
    for key in keys_to_validate:
        row = overrides.get(key)
        if not isinstance(row, dict):
            row_invalid += 1
            continue

        row_keys = {str(k) for k in row.keys()}
        missing_required = sorted({"target_account", "target_tax_division"} - row_keys)
        extra = sorted(row_keys - {"target_account", "target_tax_division"})
        if missing_required:
            row_missing_keys += 1
        if extra:
            row_extra_keys += 1

        target_account = row.get("target_account")
        target_tax_division = row.get("target_tax_division")
        if not isinstance(target_account, str) or not target_account.strip():
            row_value_invalid += 1
            continue
        if not isinstance(target_tax_division, str):
            row_value_invalid += 1
            continue
        if missing_required:
            row_value_invalid += 1
            continue
        applied_count += 1

    if row_invalid:
        invalid_reasons.append(f"row_invalid={row_invalid}")
    if row_missing_keys:
        invalid_reasons.append(f"row_missing_target_keys={row_missing_keys}")
    if row_extra_keys:
        invalid_reasons.append(f"row_extra_keys={row_extra_keys}")
    if row_value_invalid:
        invalid_reasons.append(f"row_value_invalid={row_value_invalid}")

    summary = (
        f"layout={layout_label}; path={rel_path}; applied={applied_count}/{len(keys_to_validate)}"
    )
    if invalid_reasons:
        return "invalid", f"invalid_present: {summary}; " + "; ".join(invalid_reasons)
    if notes:
        summary += "; notes=" + ",".join(notes)
    return "valid", f"valid_present: {summary}"


def _discover_bank_line_clients(repo_root: Path) -> List[tuple[str, Path]]:
    clients_dir = repo_root / "clients"
    if not clients_dir.exists():
        return []
    found: List[tuple[str, Path]] = []
    for client_dir in sorted(clients_dir.iterdir(), key=lambda p: p.name):
        if not client_dir.is_dir() or client_dir.name == "TEMPLATE":
            continue
        line_root = client_dir / "lines" / "bank_statement"
        if line_root.exists():
            found.append((client_dir.name, line_root))
    return found


def _detect_bank_forbidden_residue(line_root: Path) -> List[str]:
    forbidden_roots = [
        line_root / "inputs" / "ledger_ref",
        line_root / "artifacts" / "ingest" / "ledger_ref",
    ]
    hits: List[str] = []
    for forbidden_root in forbidden_roots:
        if forbidden_root.exists():
            hits.append(f"{forbidden_root.relative_to(line_root).as_posix()}/**")
    return hits


def _make_table(rows: Sequence[CheckResult]) -> List[str]:
    lines = [
        "| Check | Pass/Fail | Evidence |",
        "|---|---|---|",
    ]
    for row in rows:
        status = "PASS" if row.passed else "FAIL"
        lines.append(
            f"| {_escape_cell(row.check_id + ' ' + row.label)} | {status} | {_escape_cell(row.evidence)} |"
        )
    return lines


def _build_risks(hard_rows: Sequence[CheckResult], soft_rows: Sequence[CheckResult]) -> List[Risk]:
    risks: List[Risk] = []
    for row in hard_rows:
        if not row.passed:
            risks.append(
                Risk(
                    severity="High",
                    check_id=row.check_id,
                    title=row.label,
                    remediation=row.remediation,
                )
            )
    for row in soft_rows:
        if not row.passed:
            risks.append(
                Risk(
                    severity="Medium",
                    check_id=row.check_id,
                    title=row.label,
                    remediation=row.remediation,
                )
            )
    severity_order = {"High": 2, "Medium": 1, "Low": 0}
    risks.sort(key=lambda r: severity_order.get(r.severity, 0), reverse=True)
    return risks[:10]


def _default_next_steps(go: bool, risks: Sequence[Risk]) -> List[str]:
    if not go:
        return [
            "Hard checks failed. Fix the failing items in Section 2 before running any production-facing workflow.",
            "Re-run `python .agents/skills/system-diagnose/scripts/system_diagnose.py` and confirm Go status.",
            "Once Go, continue with explicitly requested skills only.",
        ]
    if risks:
        return [
            "Address warning-level items in Section 3 to reduce operational drift.",
            "Re-run system diagnosis after warning fixes to maintain a clean baseline.",
            "Proceed with explicit-invocation workflows after warnings are acknowledged.",
        ]
    return [
        "No blocking issues detected. Keep the current environment baseline.",
        "Run this diagnostic after tooling changes or repository upgrades.",
        "Proceed with explicit-invocation workflows.",
    ]


def _extract_embedded_report(text: str) -> tuple[str | None, str]:
    start = text.find(_REPORT_BEGIN_MARKER)
    if start < 0:
        return None, text.strip()
    end = text.find(_REPORT_END_MARKER, start + len(_REPORT_BEGIN_MARKER))
    if end < 0:
        return None, text.strip()
    body = text[start + len(_REPORT_BEGIN_MARKER) : end].strip("\n")
    cleaned = (text[:start] + text[end + len(_REPORT_END_MARKER) :]).strip()
    return (body + "\n") if body else "", cleaned


def _normalize_line_report_for_combined(report_content: str) -> str:
    lines = report_content.splitlines()
    if lines and lines[0].strip() == "# System Diagnose Report":
        lines = lines[1:]
        if lines and lines[0].strip() == "":
            lines = lines[1:]
    body = "\n".join(lines).rstrip()
    return (body + "\n") if body else ""


def _run_all_lines_mode(repo_root: Path) -> int:
    audit_time = _utc_now()
    line_results: List[tuple[str, int, str, str]] = []
    script_path = Path(__file__).resolve()

    print("[INFO] running all-line diagnosis: receipt, bank_statement, credit_card_statement")
    for line_id in _SUPPORTED_LINE_IDS:
        child_env = os.environ.copy()
        child_env["PYTHONUTF8"] = "1"
        child_env["PYTHONIOENCODING"] = "utf-8"
        proc = subprocess.run(
            [sys.executable, "-X", "utf8", str(script_path), "--line", line_id, "--render-only"],
            cwd=str(repo_root),
            env=child_env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        combined_output = (proc.stdout or "").strip()
        if proc.stderr:
            combined_output = (combined_output + "\n" + proc.stderr.strip()).strip()
        line_report, combined_output = _extract_embedded_report(combined_output)
        if line_report is None:
            line_report = "\n".join(
                [
                    "# System Diagnose Report",
                    "",
                    "## 0) Internal capture warning",
                    f"- Line ID: {line_id}",
                    "- Render-only markdown capture failed in all-mode child execution.",
                    "",
                ]
            )
        line_results.append((line_id, proc.returncode, combined_output, line_report))

    overall_go = all(return_code == 0 for _, return_code, _, _ in line_results)
    go_text = "GO" if overall_go else "NO-GO"

    summary_lines: List[str] = []
    summary_lines.append("# System Diagnose Report")
    summary_lines.append("")
    summary_lines.append("## 1) Executive Summary")
    summary_lines.append(f"- Audit time (UTC): {_utc_iso(audit_time)}")
    summary_lines.append("- Line ID: all")
    summary_lines.append(f"- Go/No-Go: {go_text}")
    summary_lines.append("")
    summary_lines.append("## 2) Per-line summary")
    summary_lines.append("| Line | Result | Notes |")
    summary_lines.append("|---|---|---|")
    for line_id, return_code, _, _ in line_results:
        result = "GO" if return_code == 0 else "NO-GO"
        summary_lines.append(f"| {line_id} | {result} |  |")

    summary_lines.append("")
    summary_lines.append("## 3) Per-line diagnostic reports")
    for line_id, _, _, line_report in line_results:
        summary_lines.append("")
        summary_lines.append(f"## {line_id}")
        summary_lines.append("")
        body = _normalize_line_report_for_combined(line_report)
        if body:
            summary_lines.append(body.rstrip())
        else:
            summary_lines.append("(empty report)")

    summary_lines.append("")
    summary_lines.append("## 4) Child execution outputs (trimmed)")
    for line_id, return_code, combined_output, _ in line_results:
        summary_lines.append("")
        summary_lines.append(f"### {line_id}")
        summary_lines.append(f"- Exit code: {return_code}")
        summary_lines.append("```text")
        summary_lines.append(_trim_text(combined_output, max_chars=6000))
        summary_lines.append("```")

    summary_content = "\n".join(summary_lines).rstrip() + "\n"
    summary_sha8 = hashlib.sha256(summary_content.encode("utf-8")).hexdigest()[:8]
    summary_name = f"system_diagnose_{_utc_compact(audit_time)}_{summary_sha8}.md"
    export_dir = repo_root / "exports" / "system_diagnose"
    export_dir.mkdir(parents=True, exist_ok=True)
    summary_path = export_dir / summary_name
    summary_path.write_text(summary_content, encoding="utf-8", newline="\n")

    latest_tmp = export_dir / "LATEST.txt.tmp"
    latest_file = export_dir / "LATEST.txt"
    latest_tmp.write_text(f"{summary_name}\n", encoding="utf-8", newline="\n")
    latest_tmp.replace(latest_file)

    print("Line summary:")
    for line_id, return_code, _, _ in line_results:
        result = "GO" if return_code == 0 else "NO-GO"
        print(f"- {line_id}: {result}")
    print(f"Overall: {go_text}")
    print(f"Report: {summary_path}")

    return 0 if overall_go else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--line",
        default="all",
        choices=list(_SUPPORTED_LINE_IDS_WITH_ALL),
        help="Document processing line_id (receipt, bank_statement, credit_card_statement, all)",
    )
    parser.add_argument(
        "--render-only",
        action="store_true",
        help="Render report to stdout between markers (for internal all-mode capture).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[4]
    if args.line == "all":
        return _run_all_lines_mode(repo_root)

    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        return 2
    if not is_line_implemented(line_id):
        print(f"[ERROR] line is unimplemented: {line_id}")
        return 2

    audit_time = _utc_now()
    provisioned_dirs = _ensure_required_dirs(repo_root, line_id)
    command_logs: Dict[str, CommandResult] = {}
    hard_checks: List[CheckResult] = []
    soft_checks: List[CheckResult] = []

    def run_and_store(
        check_id: str,
        command: str,
        timeout_sec: int = 30,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        if env is None:
            res = _run_command(command=command, cwd=repo_root, timeout_sec=timeout_sec)
        else:
            try:
                res = _run_command(command=command, cwd=repo_root, timeout_sec=timeout_sec, env=env)
            except TypeError as exc:
                if "unexpected keyword argument 'env'" not in str(exc):
                    raise
                # Keep backward compatibility with tests that patch _run_command without env.
                res = _run_command(command=command, cwd=repo_root, timeout_sec=timeout_sec)
        command_logs[check_id] = res
        return res

    def add_hard(check_id: str, label: str, passed: bool, evidence: str, remediation: str) -> None:
        hard_checks.append(
            CheckResult(
                check_id=check_id,
                label=label,
                passed=passed,
                evidence=evidence,
                remediation=remediation,
                hard=True,
            )
        )

    def add_soft(check_id: str, label: str, passed: bool, evidence: str, remediation: str) -> None:
        soft_checks.append(
            CheckResult(
                check_id=check_id,
                label=label,
                passed=passed,
                evidence=evidence,
                remediation=remediation,
                hard=False,
            )
        )

    # A) Repo / git
    a1 = run_and_store("A1", "git rev-parse --is-inside-work-tree")
    add_hard(
        "A1",
        "git rev-parse --is-inside-work-tree == true",
        a1.returncode == 0 and a1.stdout.strip().lower() == "true",
        _result_evidence(a1),
        "Run inside the repository root with a valid git working tree.",
    )

    a2 = run_and_store("A2", "git rev-parse HEAD")
    head_commit = a2.stdout.strip() if a2.returncode == 0 else "unknown"
    add_hard(
        "A2",
        "git rev-parse HEAD succeeds",
        a2.returncode == 0 and bool(head_commit),
        _result_evidence(a2),
        "Repair repository metadata or checkout a valid commit.",
    )

    a3 = run_and_store("A3", "git status --porcelain=v1 -uall")
    add_hard(
        "A3",
        "git status --porcelain=v1 -uall succeeds",
        a3.returncode == 0,
        _result_evidence(a3),
        "Ensure git executable and repository state are accessible.",
    )
    repo_dirty = a3.returncode == 0 and bool(a3.stdout.strip())
    dirty_paths = _parse_porcelain_paths(a3.stdout) if a3.returncode == 0 else []
    dirty_snippet_lines = a3.stdout.splitlines()[:30] if repo_dirty else []
    cleanliness_state = "Dirty" if repo_dirty else "Clean"
    cleanliness_evidence = f"state: {cleanliness_state}; dirty paths: {len(dirty_paths)}"
    if dirty_snippet_lines:
        cleanliness_evidence += "\n" + "\n".join(dirty_snippet_lines)
    add_soft(
        "S5",
        "Repo cleanliness (git status)",
        a3.returncode == 0 and not repo_dirty,
        cleanliness_evidence if a3.returncode == 0 else _result_evidence(a3),
        "\u4f5c\u696d\u30c4\u30ea\u30fc\u3092\u30af\u30ea\u30fc\u30f3\u306b\u623b\u3057\u3066\u304f\u3060\u3055\u3044\u3002"
        "\u5019\u88dc: \u5909\u66f4\u3092\u7834\u68c4 `git restore -SW .` / "
        "\u4e00\u6642\u9000\u907f `git stash -u` / "
        "\u30b3\u30df\u30c3\u30c8 `git add ...; git commit ...`\u3002",
    )

    a4 = run_and_store("A4", "git --version")
    add_hard(
        "A4",
        "git --version succeeds",
        a4.returncode == 0,
        _result_evidence(a4),
        "Install git and ensure it is available on PATH.",
    )

    # B) Python
    b1 = run_and_store("B1", "python --version")
    version_text = (b1.stdout.strip() or b1.stderr.strip()).strip()
    version_match = re.search(r"(\d+)\.(\d+)", version_text)
    parsed_version = (
        (int(version_match.group(1)), int(version_match.group(2))) if version_match else None
    )
    add_hard(
        "B1",
        "python --version succeeds",
        b1.returncode == 0 and bool(version_text),
        _result_evidence(b1),
        "Install Python and ensure `python` resolves correctly on PATH.",
    )

    add_hard(
        "B2",
        "Python version >= 3.10",
        bool(parsed_version) and parsed_version >= (3, 10),
        f"parsed version: {parsed_version if parsed_version else 'unavailable'} from `{version_text or '(empty)'}`",
        "Install Python 3.10+ and make it the active `python` interpreter.",
    )

    b3 = run_and_store(
        "B3",
        'python -c "import sys; print(sys.executable); print(sys.version)"',
    )
    b3_lines = [line for line in b3.stdout.splitlines() if line.strip()]
    add_hard(
        "B3",
        'python -c "import sys; print(sys.executable); print(sys.version)" succeeds',
        b3.returncode == 0 and len(b3_lines) >= 2,
        _result_evidence(b3),
        "Fix python runtime resolution and executable integrity.",
    )

    # C) Required files / dirs
    required_paths = [
        ("C4", "spec/FILE_LAYOUT.md exists", repo_root / "spec" / "FILE_LAYOUT.md", False),
        ("C5", "spec/REPLACER_SPEC.md exists", repo_root / "spec" / "REPLACER_SPEC.md", False),
        ("C6", "spec/CLIENT_CACHE_SPEC.md exists", repo_root / "spec" / "CLIENT_CACHE_SPEC.md", False),
        (
            "C7",
            "spec/LEXICON_PENDING_SPEC.md exists",
            repo_root / "spec" / "LEXICON_PENDING_SPEC.md",
            False,
        ),
        (
            "C8",
            "spec/CATEGORY_OVERRIDES_SPEC.md exists",
            repo_root / "spec" / "CATEGORY_OVERRIDES_SPEC.md",
            False,
        ),
        ("C9", ".agents/skills exists", repo_root / ".agents" / "skills", True),
        (
            "C10",
            ".agents/skills/yayoi-replacer exists",
            repo_root / ".agents" / "skills" / "yayoi-replacer",
            True,
        ),
        (
            "C11",
            ".agents/skills/client-register exists",
            repo_root / ".agents" / "skills" / "client-register",
            True,
        ),
        (
            "C12",
            ".agents/skills/client-cache-builder exists",
            repo_root / ".agents" / "skills" / "client-cache-builder",
            True,
        ),
        (
            "C13",
            ".agents/skills/lexicon-apply exists",
            repo_root / ".agents" / "skills" / "lexicon-apply",
            True,
        ),
        (
            "C14",
            ".agents/skills/lexicon-extract exists",
            repo_root / ".agents" / "skills" / "lexicon-extract",
            True,
        ),
        (
            "C15",
            ".agents/skills/export-lexicon-review-pack exists",
            repo_root / ".agents" / "skills" / "export-lexicon-review-pack",
            True,
        ),
    ]
    if line_id == "receipt":
        required_paths = [
            (
                "C1",
                "lexicon/lexicon.json exists",
                repo_root / "lexicon" / "lexicon.json",
                False,
            ),
            (
                "C2A",
                "defaults/receipt/category_defaults_tax_excluded.json exists",
                repo_root / tracked_category_defaults_relpaths("receipt")[0],
                False,
            ),
            (
                "C2B",
                "defaults/receipt/category_defaults_tax_included.json exists",
                repo_root / tracked_category_defaults_relpaths("receipt")[1],
                False,
            ),
        ] + required_paths
    if line_id == "bank_statement":
        bank_template_root = repo_root / "clients" / "TEMPLATE" / "lines" / "bank_statement"
        required_paths = required_paths + [
            (
                "C21",
                "clients/TEMPLATE/lines/bank_statement/inputs/training/ocr_kari_shiwake directory exists",
                bank_template_root / "inputs" / "training" / "ocr_kari_shiwake",
                True,
            ),
            (
                "C22",
                "clients/TEMPLATE/lines/bank_statement/inputs/training/reference_yayoi directory exists",
                bank_template_root / "inputs" / "training" / "reference_yayoi",
                True,
            ),
            (
                "C23",
                "clients/TEMPLATE/lines/bank_statement/inputs/kari_shiwake directory exists",
                bank_template_root / "inputs" / "kari_shiwake",
                True,
            ),
            (
                "C24",
                "clients/TEMPLATE/lines/bank_statement/artifacts/ingest/training_ocr directory exists",
                bank_template_root / "artifacts" / "ingest" / "training_ocr",
                True,
            ),
            (
                "C25",
                "clients/TEMPLATE/lines/bank_statement/artifacts/ingest/training_reference directory exists",
                bank_template_root / "artifacts" / "ingest" / "training_reference",
                True,
            ),
            (
                "C26",
                "clients/TEMPLATE/lines/bank_statement/artifacts/ingest/kari_shiwake directory exists",
                bank_template_root / "artifacts" / "ingest" / "kari_shiwake",
                True,
            ),
            (
                "C27",
                "clients/TEMPLATE/lines/bank_statement/config/bank_line_config.json exists",
                bank_template_root / "config" / "bank_line_config.json",
                False,
            ),
            (
                "C28",
                "belle/build_bank_cache.py exists",
                repo_root / "belle" / "build_bank_cache.py",
                False,
            ),
            (
                "C29",
                "belle/bank_replacer.py exists",
                repo_root / "belle" / "bank_replacer.py",
                False,
            ),
            (
                "C30",
                "belle/bank_cache.py exists",
                repo_root / "belle" / "bank_cache.py",
                False,
            ),
            (
                "C31",
                "belle/bank_pairing.py exists",
                repo_root / "belle" / "bank_pairing.py",
                False,
            ),
        ]
    if line_id == "credit_card_statement":
        card_template_root = repo_root / "clients" / "TEMPLATE" / "lines" / "credit_card_statement"
        required_paths = required_paths + [
            (
                "C39",
                "lexicon/lexicon.json exists",
                repo_root / "lexicon" / "lexicon.json",
                False,
            ),
            (
                "C40A",
                "defaults/credit_card_statement/category_defaults_tax_excluded.json exists",
                repo_root / tracked_category_defaults_relpaths("credit_card_statement")[0],
                False,
            ),
            (
                "C40B",
                "defaults/credit_card_statement/category_defaults_tax_included.json exists",
                repo_root / tracked_category_defaults_relpaths("credit_card_statement")[1],
                False,
            ),
            (
                "C32",
                "clients/TEMPLATE/lines/credit_card_statement directory exists",
                card_template_root,
                True,
            ),
            (
                "C33",
                "clients/TEMPLATE/lines/credit_card_statement/inputs/kari_shiwake directory exists",
                card_template_root / "inputs" / "kari_shiwake",
                True,
            ),
            (
                "C34",
                "clients/TEMPLATE/lines/credit_card_statement/inputs/ledger_ref directory exists",
                card_template_root / "inputs" / "ledger_ref",
                True,
            ),
            (
                "C35",
                "clients/TEMPLATE/lines/credit_card_statement/artifacts/ingest/kari_shiwake directory exists",
                card_template_root / "artifacts" / "ingest" / "kari_shiwake",
                True,
            ),
            (
                "C36",
                "clients/TEMPLATE/lines/credit_card_statement/artifacts/ingest/ledger_ref directory exists",
                card_template_root / "artifacts" / "ingest" / "ledger_ref",
                True,
            ),
            (
                "C37",
                "clients/TEMPLATE/lines/credit_card_statement/outputs/runs directory exists",
                card_template_root / "outputs" / "runs",
                True,
            ),
            (
                "C38",
                "clients/TEMPLATE/lines/credit_card_statement/artifacts/cache directory exists",
                card_template_root / "artifacts" / "cache",
                True,
            ),
        ]
    for check_id, label, path, expect_dir in required_paths:
        passed = path.is_dir() if expect_dir else path.is_file()
        add_hard(
            check_id,
            label,
            passed,
            f"checked path: {path.relative_to(repo_root)}",
            "Restore required repository files/directories from source control.",
        )

    template_tax_config_path = _shared_tax_config_path(repo_root, "TEMPLATE")
    add_hard(
        "C41",
        "clients/TEMPLATE/config/yayoi_tax_config.json exists",
        template_tax_config_path.is_file(),
        f"checked path: {template_tax_config_path.relative_to(repo_root).as_posix()}",
        "Restore clients/TEMPLATE/config/yayoi_tax_config.json from source control.",
    )
    if template_tax_config_path.is_file():
        try:
            template_tax_config = _load_yayoi_tax_postprocess_config(repo_root, "TEMPLATE")
            c42_passed = True
            c42_evidence = _format_shared_tax_config_state("TEMPLATE", template_tax_config)
        except Exception as exc:
            c42_passed = False
            c42_evidence = str(exc)
    else:
        c42_passed = False
        c42_evidence = "missing path: clients/TEMPLATE/config/yayoi_tax_config.json"
    add_hard(
        "C42",
        "clients/TEMPLATE/config/yayoi_tax_config.json is valid under the shared tax config contract",
        c42_passed,
        c42_evidence,
        "Fix clients/TEMPLATE/config/yayoi_tax_config.json to match the shared tax config contract.",
    )
    if template_tax_config_path.is_file() and c42_passed:
        c42b_passed, c42b_evidence = _validate_shared_tax_bootstrap_policy("TEMPLATE", template_tax_config)
    elif not template_tax_config_path.is_file():
        c42b_passed = False
        c42b_evidence = "missing_required: path=clients/TEMPLATE/config/yayoi_tax_config.json"
    else:
        c42b_passed = False
        c42b_evidence = "unavailable: validity check failed first"
    add_hard(
        "C42B",
        "clients/TEMPLATE/config/yayoi_tax_config.json matches the bookkeeping-mode bootstrap policy",
        c42b_passed,
        c42b_evidence,
        "Set TEMPLATE shared tax config to the live bootstrap policy: tax_excluded => enabled=true,floor; "
        "tax_included => enabled=false,floor.",
    )

    non_template_clients = _discover_non_template_clients(repo_root)
    missing_shared_tax_configs: List[str] = []
    valid_shared_tax_configs: List[str] = []
    invalid_shared_tax_configs: List[str] = []
    consistent_shared_tax_configs: List[str] = []
    inconsistent_shared_tax_configs: List[str] = []
    for client_id, _ in non_template_clients:
        client_tax_config_path = _shared_tax_config_path(repo_root, client_id)
        if not client_tax_config_path.is_file():
            missing_shared_tax_configs.append(client_id)
            continue
        try:
            client_tax_config = _load_yayoi_tax_postprocess_config(repo_root, client_id)
            valid_shared_tax_configs.append(_format_shared_tax_config_state(client_id, client_tax_config))
            consistent, policy_evidence = _validate_shared_tax_bootstrap_policy(client_id, client_tax_config)
            if consistent:
                consistent_shared_tax_configs.append(policy_evidence)
            else:
                inconsistent_shared_tax_configs.append(policy_evidence)
        except Exception as exc:
            invalid_shared_tax_configs.append(f"{client_id}: {exc}")

    if not non_template_clients:
        c43_passed = True
        c43_evidence = "N/A: no non-TEMPLATE clients found"
    elif invalid_shared_tax_configs:
        c43_passed = False
        c43_parts: List[str] = []
        if valid_shared_tax_configs:
            valid_preview = "; ".join(valid_shared_tax_configs[:10])
            if len(valid_shared_tax_configs) > 10:
                valid_preview += f"; ... (+{len(valid_shared_tax_configs) - 10} more)"
            c43_parts.append(f"valid: {valid_preview}")
        invalid_preview = "; ".join(invalid_shared_tax_configs[:10])
        if len(invalid_shared_tax_configs) > 10:
            invalid_preview += f"; ... (+{len(invalid_shared_tax_configs) - 10} more)"
        c43_parts.append(f"invalid: {invalid_preview}")
        c43_evidence = " | ".join(c43_parts)
    elif valid_shared_tax_configs:
        c43_passed = True
        valid_preview = "; ".join(valid_shared_tax_configs[:10])
        if len(valid_shared_tax_configs) > 10:
            valid_preview += f"; ... (+{len(valid_shared_tax_configs) - 10} more)"
        c43_evidence = f"valid: {valid_preview}"
    else:
        c43_passed = True
        c43_evidence = "N/A: no present shared tax config among non-TEMPLATE clients"
    add_hard(
        "C43",
        "shared Yayoi tax config is valid for non-TEMPLATE clients when present",
        c43_passed,
        c43_evidence,
        "Fix invalid clients/<CLIENT_ID>/config/yayoi_tax_config.json files or restore them from the template.",
    )
    if not non_template_clients:
        c43b_passed = True
        c43b_evidence = "N/A: no non-TEMPLATE clients found"
    elif inconsistent_shared_tax_configs:
        c43b_passed = False
        c43b_parts: List[str] = []
        if consistent_shared_tax_configs:
            consistent_preview = "; ".join(consistent_shared_tax_configs[:10])
            if len(consistent_shared_tax_configs) > 10:
                consistent_preview += f"; ... (+{len(consistent_shared_tax_configs) - 10} more)"
            c43b_parts.append(f"valid_mode_consistent: {consistent_preview}")
        inconsistent_preview = "; ".join(inconsistent_shared_tax_configs[:10])
        if len(inconsistent_shared_tax_configs) > 10:
            inconsistent_preview += f"; ... (+{len(inconsistent_shared_tax_configs) - 10} more)"
        c43b_parts.append(f"present_inconsistent: {inconsistent_preview}")
        if missing_shared_tax_configs:
            missing_preview = ", ".join(missing_shared_tax_configs[:10])
            if len(missing_shared_tax_configs) > 10:
                missing_preview += ", ..."
            c43b_parts.append(f"missing_allowed: {missing_preview}")
        c43b_evidence = " | ".join(c43b_parts)
    elif consistent_shared_tax_configs:
        c43b_passed = True
        consistent_preview = "; ".join(consistent_shared_tax_configs[:10])
        if len(consistent_shared_tax_configs) > 10:
            consistent_preview += f"; ... (+{len(consistent_shared_tax_configs) - 10} more)"
        c43b_parts = [f"valid_mode_consistent: {consistent_preview}"]
        if missing_shared_tax_configs:
            missing_preview = ", ".join(missing_shared_tax_configs[:10])
            if len(missing_shared_tax_configs) > 10:
                missing_preview += ", ..."
            c43b_parts.append(f"missing_allowed: {missing_preview}")
        c43b_evidence = " | ".join(c43b_parts)
    else:
        c43b_passed = True
        if missing_shared_tax_configs:
            missing_preview = ", ".join(missing_shared_tax_configs[:10])
            if len(missing_shared_tax_configs) > 10:
                missing_preview += ", ..."
            c43b_evidence = f"missing_allowed: {missing_preview}"
        else:
            c43b_evidence = "N/A: no present shared tax config among non-TEMPLATE clients"
    add_hard(
        "C43B",
        "shared Yayoi tax config matches the bookkeeping-mode bootstrap policy for non-TEMPLATE clients when present",
        c43b_passed,
        c43b_evidence,
        "Align present clients/<CLIENT_ID>/config/yayoi_tax_config.json files with the live bootstrap policy "
        "(tax_excluded => enabled=true,floor; tax_included => enabled=false,floor).",
    )

    if not non_template_clients:
        s10_passed = True
        s10_evidence = "N/A: no non-TEMPLATE clients found"
    elif not missing_shared_tax_configs:
        s10_passed = True
        s10_evidence = f"present for all {len(non_template_clients)} non-TEMPLATE client(s)"
    else:
        s10_passed = False
        missing_preview = ", ".join(missing_shared_tax_configs[:10])
        if len(missing_shared_tax_configs) > 10:
            missing_preview += ", ..."
        s10_evidence = (
            f"missing shared tax config for {len(missing_shared_tax_configs)} client(s): {missing_preview}"
        )
    add_soft(
        "S10",
        "shared Yayoi tax config presence for non-TEMPLATE clients (warn-only when missing)",
        s10_passed,
        s10_evidence,
        "Provision clients/<CLIENT_ID>/config/yayoi_tax_config.json from clients/TEMPLATE/config/ when needed.",
    )

    if line_id == "receipt":
        detected_cfg, cfg_evidence = _detect_replacer_config(repo_root, line_id)
        add_hard(
            "C3",
            f"rulesets/{line_id}/replacer_config_v1_15.json or current configured replacer config exists",
            detected_cfg is not None and detected_cfg.exists(),
            cfg_evidence if detected_cfg is None else f"{cfg_evidence}; using {detected_cfg.relative_to(repo_root)}",
            "Add the active replacer config back under rulesets/ and align references.",
        )
        if detected_cfg is not None and detected_cfg.exists():
            c44_passed, c44_evidence = _validate_receipt_runtime_tax_sections(detected_cfg)
        else:
            c44_passed = False
            c44_evidence = "active receipt replacer config missing"
        add_hard(
            "C44",
            "active receipt replacer config contains required tax_division_thresholds and tax_division_confidence sections",
            c44_passed,
            c44_evidence,
            "Restore the tracked receipt tax threshold/confidence sections in the active replacer config.",
        )

        receipt_override_targets = _discover_receipt_override_targets(repo_root)
        lexicon_category_keys = _load_lexicon_category_keys(repo_root)
        receipt_override_invalid: List[str] = []
        receipt_override_valid: List[str] = []
        receipt_override_missing: List[str] = []
        for receipt_client_id, override_path, layout_label in receipt_override_targets:
            state, evidence = _validate_category_overrides_contract(
                repo_root,
                override_path,
                lexicon_category_keys=lexicon_category_keys,
                layout_label=layout_label,
            )
            entry = f"{receipt_client_id}: {evidence}"
            if state == "invalid":
                receipt_override_invalid.append(entry)
            elif state == "valid":
                receipt_override_valid.append(entry)
            else:
                receipt_override_missing.append(entry)
        if not receipt_override_targets:
            c45_passed = True
            c45_evidence = "N/A: no receipt category_overrides targets detected"
        elif receipt_override_invalid:
            c45_passed = False
            evidence_parts: List[str] = []
            if receipt_override_valid:
                evidence_parts.append("valid=" + "; ".join(receipt_override_valid[:5]))
            if receipt_override_missing:
                evidence_parts.append("optional_missing=" + "; ".join(receipt_override_missing[:5]))
            evidence_parts.append("invalid=" + "; ".join(receipt_override_invalid[:5]))
            c45_evidence = " | ".join(evidence_parts)
        else:
            c45_passed = True
            evidence_parts = []
            if receipt_override_valid:
                evidence_parts.append("valid=" + "; ".join(receipt_override_valid[:5]))
            if receipt_override_missing:
                evidence_parts.append("optional_missing=" + "; ".join(receipt_override_missing[:5]))
            c45_evidence = " | ".join(evidence_parts) if evidence_parts else "N/A"
        add_hard(
            "C45",
            "receipt category_overrides.json follows the target_account/target_tax_division row contract when present",
            c45_passed,
            c45_evidence,
            "Replace old debit_account/debit_tax_division-shaped receipt override rows with target_account/target_tax_division rows.",
        )

    if line_id == "bank_statement":
        bank_clients = _discover_bank_line_clients(repo_root)
        invalid_line_roots: List[str] = []
        for bank_client_id, line_root in bank_clients:
            if not line_root.is_dir():
                invalid_line_roots.append(f"{bank_client_id}:{line_root}")
        if not bank_clients:
            c16_passed = True
            c16_evidence = "N/A: no clients/<ID>/lines/bank_statement/ directories found"
        elif not invalid_line_roots:
            bank_preview = ", ".join(client_id for client_id, _ in bank_clients[:5])
            if len(bank_clients) > 5:
                bank_preview += ", ..."
            c16_passed = True
            c16_evidence = f"validated {len(bank_clients)} client(s): {bank_preview}"
        else:
            c16_passed = False
            c16_evidence = "invalid line roots: " + "; ".join(invalid_line_roots)
        add_hard(
            "C16",
            "bank_statement line root exists for opted-in clients",
            c16_passed,
            c16_evidence,
            "Create clients/<ID>/lines/bank_statement/ via $client-register and keep it as a directory.",
        )

        missing_ocr_training_dirs: List[str] = []
        empty_ocr_training_dirs: List[str] = []
        for bank_client_id, line_root in bank_clients:
            ocr_dir = line_root / "inputs" / "training" / "ocr_kari_shiwake"
            if not ocr_dir.is_dir():
                missing_ocr_training_dirs.append(f"{bank_client_id}:{ocr_dir.relative_to(line_root).as_posix()}")
                continue
            if len(_iter_non_placeholder_files(ocr_dir)) == 0:
                empty_ocr_training_dirs.append(bank_client_id)
        if not bank_clients:
            c17_passed = True
            c17_evidence = "N/A: no bank_statement clients found"
        elif not missing_ocr_training_dirs:
            c17_passed = True
            c17_evidence = f"inputs/training/ocr_kari_shiwake exists for {len(bank_clients)} client(s)"
        else:
            c17_passed = False
            c17_evidence = "missing dirs: " + "; ".join(missing_ocr_training_dirs[:20])
            if len(missing_ocr_training_dirs) > 20:
                c17_evidence += f"; ... (+{len(missing_ocr_training_dirs) - 20} more)"
        add_hard(
            "C17",
            "bank_statement ocr training directory exists",
            c17_passed,
            c17_evidence,
            "Create clients/<ID>/lines/bank_statement/inputs/training/ocr_kari_shiwake/ for each bank client.",
        )
        if not bank_clients:
            s6_passed = True
            s6_evidence = "N/A: no bank_statement clients found"
        elif not empty_ocr_training_dirs:
            s6_passed = True
            s6_evidence = "all clients have 1+ files in inputs/training/ocr_kari_shiwake"
        else:
            s6_passed = False
            s6_evidence = (
                f"0 files in inputs/training/ocr_kari_shiwake for {len(empty_ocr_training_dirs)} client(s): "
                + ", ".join(empty_ocr_training_dirs[:10])
            )
            if len(empty_ocr_training_dirs) > 10:
                s6_evidence += ", ..."
        add_soft(
            "S6",
            "bank_statement ocr training file count (warn when 0 files)",
            s6_passed,
            s6_evidence,
            "Add OCR training files under inputs/training/ocr_kari_shiwake when available.",
        )

        missing_reference_dirs: List[str] = []
        reference_dir_evidence: List[str] = []
        for bank_client_id, line_root in bank_clients:
            reference_dir = line_root / "inputs" / "training" / "reference_yayoi"
            has_dir = reference_dir.is_dir()
            reference_dir_evidence.append(
                f"{bank_client_id}:{reference_dir.relative_to(line_root).as_posix()}={has_dir}"
            )
            if not has_dir:
                missing_reference_dirs.append(
                    f"{bank_client_id}: missing {reference_dir.relative_to(line_root).as_posix()}"
                )
        if not bank_clients:
            c18_passed = True
            c18_evidence = "N/A: no bank_statement clients found"
        elif not missing_reference_dirs:
            c18_passed = True
            c18_evidence = "; ".join(reference_dir_evidence)
        else:
            c18_passed = False
            c18_evidence = "missing dirs: " + "; ".join(missing_reference_dirs[:20])
            if len(missing_reference_dirs) > 20:
                c18_evidence += f"; ... (+{len(missing_reference_dirs) - 20} more)"
        add_hard(
            "C18",
            "bank_statement teacher reference directory exists",
            c18_passed,
            c18_evidence,
            "Create clients/<ID>/lines/bank_statement/inputs/training/reference_yayoi/ for each bank client.",
        )

        missing_target_dirs: List[str] = []
        target_dir_evidence: List[str] = []
        for bank_client_id, line_root in bank_clients:
            target_dir = line_root / "inputs" / "kari_shiwake"
            has_dir = target_dir.is_dir()
            target_dir_evidence.append(f"{bank_client_id}:{target_dir.relative_to(line_root).as_posix()}={has_dir}")
            if not has_dir:
                missing_target_dirs.append(
                    f"{bank_client_id}: missing {target_dir.relative_to(line_root).as_posix()}"
                )
        if not bank_clients:
            c19_passed = True
            c19_evidence = "N/A: no bank_statement clients found"
        elif not missing_target_dirs:
            c19_passed = True
            c19_evidence = "; ".join(target_dir_evidence)
        else:
            c19_passed = False
            c19_evidence = "missing dirs: " + "; ".join(missing_target_dirs[:20])
            if len(missing_target_dirs) > 20:
                c19_evidence += f"; ... (+{len(missing_target_dirs) - 20} more)"
        add_hard(
            "C19",
            "bank_statement target kari_shiwake directory exists",
            c19_passed,
            c19_evidence,
            "Create clients/<ID>/lines/bank_statement/inputs/kari_shiwake/ for each bank client.",
        )

        missing_configs: List[str] = []
        config_evidence: List[str] = []
        for bank_client_id, line_root in bank_clients:
            config_path = line_root / "config" / "bank_line_config.json"
            config_evidence.append(
                f"{bank_client_id}:{config_path.relative_to(line_root).as_posix()}={config_path.exists()}"
            )
            if not config_path.is_file():
                missing_configs.append(bank_client_id)
        if not bank_clients:
            c20_passed = True
            c20_evidence = "N/A: no bank_statement clients found"
        elif not missing_configs:
            c20_passed = True
            c20_evidence = "; ".join(config_evidence)
        else:
            c20_passed = False
            c20_evidence = f"missing bank_line_config.json for client(s): {', '.join(missing_configs)}"
        add_soft(
            "S9",
            "bank_statement client config/bank_line_config.json presence (warn-only)",
            c20_passed,
            c20_evidence,
            "Run $client-register to provision missing config files, or restore them from template.",
        )

        forbidden_residue_hits: List[str] = []
        for bank_client_id, line_root in bank_clients:
            hits = _detect_bank_forbidden_residue(line_root)
            if hits:
                forbidden_residue_hits.append(f"{bank_client_id}: {', '.join(hits)}")
        if not bank_clients:
            s8_passed = True
            s8_evidence = "N/A: no bank_statement clients found"
        elif not forbidden_residue_hits:
            s8_passed = True
            s8_evidence = "no forbidden bank ledger_ref residue directories detected"
        else:
            s8_passed = False
            s8_evidence = (
                "WARN forbidden bank ledger_ref residue detected (legacy, safe to delete): "
                + "; ".join(forbidden_residue_hits[:20])
            )
            if len(forbidden_residue_hits) > 20:
                s8_evidence += f"; ... (+{len(forbidden_residue_hits) - 20} more)"
        add_soft(
            "S8",
            "bank_statement forbidden ledger_ref residue directories (warn-only)",
            s8_passed,
            s8_evidence,
            "Delete legacy residue directories when safe: inputs/ledger_ref/** and "
            "artifacts/ingest/ledger_ref/**. These are excluded from bank backups, "
            "and bank restore rejects ZIP files containing them.",
        )

        cache_details: List[str] = []
        cache_missing: List[str] = []
        cache_parse_issues: List[str] = []
        for bank_client_id, line_root in bank_clients:
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            if cache_path.exists():
                try:
                    cache_obj = json.loads(cache_path.read_text(encoding="utf-8"))
                    updated_at = str(cache_obj.get("updated_at") or "").strip()
                    if updated_at:
                        cache_details.append(f"{bank_client_id}:{updated_at}")
                    else:
                        cache_details.append(f"{bank_client_id}:(updated_at missing)")
                        cache_parse_issues.append(f"{bank_client_id}:updated_at_missing")
                except Exception as exc:
                    cache_details.append(f"{bank_client_id}:(parse_error:{type(exc).__name__})")
                    cache_parse_issues.append(f"{bank_client_id}:{type(exc).__name__}")
            else:
                cache_missing.append(bank_client_id)
        if not bank_clients:
            s7_passed = True
            s7_evidence = "N/A: no bank_statement clients found"
        elif cache_missing or cache_parse_issues:
            s7_passed = False
            details: List[str] = []
            if cache_missing:
                missing_preview = ", ".join(cache_missing[:10])
                if len(cache_missing) > 10:
                    missing_preview += ", ..."
                details.append(f"missing={missing_preview}")
            if cache_parse_issues:
                parse_preview = ", ".join(cache_parse_issues[:10])
                if len(cache_parse_issues) > 10:
                    parse_preview += ", ..."
                details.append(f"issues={parse_preview}")
            if cache_details:
                details.append("updated_at=" + "; ".join(cache_details[:5]))
            s7_evidence = "; ".join(details)
        else:
            s7_passed = True
            preview = "; ".join(cache_details[:5])
            if len(cache_details) > 5:
                preview += f"; ... (+{len(cache_details) - 5} more)"
            s7_evidence = f"all caches present. updated_at: {preview}"
        add_soft(
            "S7",
            "bank_statement cache file presence and last update time",
            s7_passed,
            s7_evidence,
            "Run $client-cache-builder --line bank_statement --client <CLIENT_ID> for clients with missing cache.",
        )
    if line_id == "credit_card_statement":
        card_template_config_path = (
            repo_root
            / "clients"
            / "TEMPLATE"
            / "lines"
            / "credit_card_statement"
            / "config"
            / "credit_card_line_config.json"
        )
        add_hard(
            "C46",
            "clients/TEMPLATE/lines/credit_card_statement/config/credit_card_line_config.json exists",
            card_template_config_path.is_file(),
            f"checked path: {card_template_config_path.relative_to(repo_root).as_posix()}",
            "Restore the tracked TEMPLATE credit_card_line_config.json from source control.",
        )
        if card_template_config_path.is_file():
            c47_passed, c47_evidence = _validate_credit_card_template_config_sections(card_template_config_path)
        else:
            c47_passed = False
            c47_evidence = "missing path: clients/TEMPLATE/lines/credit_card_statement/config/credit_card_line_config.json"
        add_hard(
            "C47",
            "clients/TEMPLATE/lines/credit_card_statement/config/credit_card_line_config.json contains required credit-card v2 payable/canonical/tax config sections",
            c47_passed,
            c47_evidence,
            "Restore the TEMPLATE credit-card config contract: target_payable_placeholder_names, "
            "teacher_extraction.canonical_payable_thresholds, and tax_division_thresholds.",
        )

        credit_card_override_targets = [
            (client_id, line_root / "config" / "category_overrides.json", "line")
            for client_id, line_root in _discover_clients_with_line(repo_root, "credit_card_statement")
        ]
        lexicon_category_keys = _load_lexicon_category_keys(repo_root)
        card_override_invalid: List[str] = []
        card_override_valid: List[str] = []
        card_override_missing: List[str] = []
        for card_client_id, override_path, layout_label in credit_card_override_targets:
            state, evidence = _validate_category_overrides_contract(
                repo_root,
                override_path,
                lexicon_category_keys=lexicon_category_keys,
                layout_label=layout_label,
            )
            entry = f"{card_client_id}: {evidence}"
            if state == "invalid":
                card_override_invalid.append(entry)
            elif state == "valid":
                card_override_valid.append(entry)
            else:
                card_override_missing.append(entry)
        if not credit_card_override_targets:
            c48_passed = True
            c48_evidence = "N/A: no credit_card_statement category_overrides targets detected"
        elif card_override_invalid:
            c48_passed = False
            evidence_parts = []
            if card_override_valid:
                evidence_parts.append("valid=" + "; ".join(card_override_valid[:5]))
            if card_override_missing:
                evidence_parts.append("optional_missing=" + "; ".join(card_override_missing[:5]))
            evidence_parts.append("invalid=" + "; ".join(card_override_invalid[:5]))
            c48_evidence = " | ".join(evidence_parts)
        else:
            c48_passed = True
            evidence_parts = []
            if card_override_valid:
                evidence_parts.append("valid=" + "; ".join(card_override_valid[:5]))
            if card_override_missing:
                evidence_parts.append("optional_missing=" + "; ".join(card_override_missing[:5]))
            c48_evidence = " | ".join(evidence_parts) if evidence_parts else "N/A"
        add_hard(
            "C48",
            "credit_card_statement category_overrides.json follows the target_account/target_tax_division row contract when present",
            c48_passed,
            c48_evidence,
            "Replace old debit_account/debit_tax_division-shaped credit-card override rows with target_account/target_tax_division rows.",
        )
    # D) BOM / compilation / tests
    d1 = run_and_store("D1", "python tools/bom_guard.py --check")
    bom_ok = d1.returncode == 0 and bool(re.search(r"UTF-8 BOM files:\s*0\b", d1.stdout + d1.stderr))
    add_hard(
        "D1",
        "python tools/bom_guard.py --check returns 0 and reports 0 BOM",
        bom_ok,
        _result_evidence(d1),
        "Remove BOM bytes via `python tools/bom_guard.py --fix` and re-check.",
    )

    d2 = run_and_store("D2", "python -m compileall belle tools .agents/skills tests", timeout_sec=120)
    add_hard(
        "D2",
        "python -m compileall belle tools .agents/skills tests returns 0",
        d2.returncode == 0,
        _result_evidence(d2),
        "Fix syntax/import issues surfaced by compileall before proceeding.",
    )

    d3_env = os.environ.copy()
    d3_env.pop(_REPORT_RENDER_ONLY_ENV, None)
    d3 = run_and_store("D3", "python tools/run_tests.py", timeout_sec=180, env=d3_env)
    add_hard(
        "D3",
        "python tools/run_tests.py returns 0",
        d3.returncode == 0,
        _result_evidence(d3),
        "Fix failing tests or test execution environment and re-run.",
    )

    # E) Encoding capability
    e1 = run_and_store("E1", 'python -c "import codecs; codecs.lookup(\'cp932\'); print(\'cp932 OK\')"')
    add_hard(
        "E1",
        "python cp932 lookup succeeds",
        e1.returncode == 0 and "cp932 OK" in (e1.stdout + e1.stderr),
        _result_evidence(e1),
        "Install/fix codec support in the active Python runtime.",
    )

    # F) Write permissions (create+delete tiny file)
    write_probe_targets: List[tuple[str, Path]] = [
        ("F2", Path("exports")),
        ("F3", Path("clients") / "TEMPLATE" / "lines" / line_id / "artifacts" / "ingest"),
    ]
    if line_id == "receipt":
        write_probe_targets = [("F1", Path("lexicon") / line_id / "pending" / "locks")] + write_probe_targets
    for check_id, rel in write_probe_targets:
        ok, message = _probe_write_delete(repo_root / rel)
        add_hard(
            check_id,
            f"create+delete tiny file in {rel.as_posix()}",
            ok,
            message,
            f"Grant write/delete permission for {rel.as_posix()} and retry.",
        )

    # Soft checks
    s1 = run_and_store("S1", "py -0p")
    py_launchers = [line for line in s1.stdout.splitlines() if line.strip()]
    add_soft(
        "S1",
        "py -0p available and lists installed python(s)",
        s1.returncode == 0 and len(py_launchers) > 0,
        _result_evidence(s1),
        "Install/repair Python Launcher (`py`) if this workflow depends on it.",
    )

    s2 = run_and_store("S2", "git config --get core.hooksPath")
    hooks_path = s2.stdout.strip()
    add_soft(
        "S2",
        "git core.hooksPath == .githooks",
        s2.returncode == 0 and hooks_path == ".githooks",
        _result_evidence(s2),
        "Run `git config core.hooksPath .githooks` to enable repository hooks.",
    )

    is_windows = platform.system().lower().startswith("windows")
    if is_windows:
        s3 = run_and_store("S3", "where.exe python")
        locations = [line.strip() for line in s3.stdout.splitlines() if line.strip()]
        has_windows_apps = any("windowsapps" in line.lower() for line in locations)
        passed = not (s3.returncode == 0 and has_windows_apps)
        evidence = _result_evidence(s3)
        if has_windows_apps:
            evidence += "; WindowsApps entry detected"
        add_soft(
            "S3",
            "where.exe python should avoid WindowsApps alias risk",
            passed,
            evidence,
            "Disable App Execution Alias for python, or prioritize real Python path before WindowsApps.",
        )
    else:
        add_soft(
            "S3",
            "where.exe python alias risk check (Windows only)",
            True,
            f"skipped on platform: {platform.system()}",
            "Not applicable.",
        )

    s4 = run_and_store("S4", "python3 --version")
    add_soft(
        "S4",
        "python3 command availability",
        s4.returncode == 0,
        _result_evidence(s4),
        "Optional on Windows. Add `python3` shim/alias only if your team tooling requires it.",
    )

    go = all(row.passed for row in hard_checks)
    go_text = "GO" if go else "NO-GO"
    risks = _build_risks(hard_checks, soft_checks)
    next_steps = _default_next_steps(go, risks)

    report_lines: List[str] = []
    report_lines.append("# System Diagnose Report")
    report_lines.append("")
    report_lines.append("## 1) Executive Summary")
    report_lines.append(f"- Audit time (UTC): {_utc_iso(audit_time)}")
    report_lines.append(f"- Line ID: {line_id}")
    report_lines.append(f"- HEAD commit: {head_commit or 'unknown'}")
    report_lines.append(f"- Go/No-Go: {go_text}")
    report_lines.append(f"- Provisioned dirs (created now): {len(provisioned_dirs)}")
    if provisioned_dirs:
        for rel_path in provisioned_dirs:
            report_lines.append(f"  - {rel_path.as_posix()}")
    report_lines.append("")
    report_lines.append("## 2) Hard checks")
    report_lines.extend(_make_table(hard_checks))
    report_lines.append("")
    report_lines.append("## 3) Soft checks")
    report_lines.extend(_make_table(soft_checks))
    if a3.returncode == 0 and repo_dirty:
        report_lines.append("")
        report_lines.append("### Repo Cleanliness Remediation (JA)")
        report_lines.append("- 作業ツリーをクリーンに戻す候補:")
        report_lines.append("1. 変更を破棄: `git restore -SW .`")
        report_lines.append("2. 一時退避: `git stash -u`")
        report_lines.append("3. コミット: `git add ...; git commit ...`")
    report_lines.append("")
    report_lines.append("## 4) Top risks (top 10; severity + remediation)")
    if risks:
        for idx, risk in enumerate(risks, start=1):
            report_lines.append(
                f"{idx}. **{risk.severity}** - `{risk.check_id}` {risk.title} | Remediation: {risk.remediation}"
            )
    else:
        report_lines.append("1. **Low** - No material risks detected in this run.")
    report_lines.append("")
    report_lines.append("## 5) Next steps (ordered)")
    for idx, step in enumerate(next_steps, start=1):
        report_lines.append(f"{idx}. {step}")
    report_lines.append("")
    report_lines.append("## 6) Appendix: raw command outputs (trimmed)")
    for key in sorted(command_logs.keys()):
        res = command_logs[key]
        report_lines.append("")
        report_lines.append(f"### {key} `{res.command}`")
        report_lines.append(f"- Exit code: {res.returncode if res.returncode is not None else 'N/A'}")
        report_lines.append(f"- Timed out: {'yes' if res.timed_out else 'no'}")
        report_lines.append(f"- Duration sec: {res.duration_sec:.3f}")
        if res.error:
            report_lines.append(f"- Error: {res.error}")
        report_lines.append("- stdout:")
        report_lines.append("```text")
        report_lines.append(_trim_text(res.stdout))
        report_lines.append("```")
        report_lines.append("- stderr:")
        report_lines.append("```text")
        report_lines.append(_trim_text(res.stderr))
        report_lines.append("```")

    report_content = "\n".join(report_lines).rstrip() + "\n"
    report_sha8 = hashlib.sha256(report_content.encode("utf-8")).hexdigest()[:8]
    report_name = f"system_diagnose_{_utc_compact(audit_time)}_{report_sha8}.md"
    render_only = args.render_only
    report_path: Path | None = None
    if not render_only:
        export_dir = repo_root / "exports" / "system_diagnose"
        export_dir.mkdir(parents=True, exist_ok=True)
        report_path = export_dir / report_name
        report_path.write_text(report_content, encoding="utf-8", newline="\n")

        latest_tmp = export_dir / "LATEST.txt.tmp"
        latest_file = export_dir / "LATEST.txt"
        latest_tmp.write_text(f"{report_name}\n", encoding="utf-8", newline="\n")
        latest_tmp.replace(latest_file)
    print(f"判定: {go_text}")
    if a3.returncode == 0 and repo_dirty:
        print("[WARN] 作業ツリーに未コミットの変更があります（dirty）")
        for path in dirty_paths[:10]:
            print(f"- {path}")
    if risks:
        print("主なリスク:")
        for idx in range(2):
            if idx < len(risks):
                risk = risks[idx]
                print(f"{idx + 1}. [{risk.severity}] {risk.check_id} {risk.title}")
            else:
                print(f"{idx + 1}. 重大なリスクは検出されませんでした。")
    else:
        print("主なリスク:")
        print("1. 重大なリスクは検出されませんでした。")
        print("2. 重大なリスクは検出されませんでした。")
    if not go:
        next_step_ja = "Hardチェックの失敗を解消し、再診断してください。"
    elif risks:
        next_step_ja = "Softチェックの警告を解消し、再診断してください。"
    else:
        next_step_ja = "現在の状態を維持し、必要時に再診断してください。"
    print(f"次の一手: {next_step_ja}")
    if render_only:
        print(_REPORT_BEGIN_MARKER)
        print(report_content, end="")
        print(_REPORT_END_MARKER)
    else:
        assert report_path is not None
        print(f"レポート: {report_path}")
    return 0 if go else 1


if __name__ == "__main__":
    raise SystemExit(main())

