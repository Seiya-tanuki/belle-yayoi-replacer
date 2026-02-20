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

from belle.lines import is_line_implemented, validate_line_id


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


def _run_command(command: str, cwd: Path, timeout_sec: int = 30) -> CommandResult:
    started = time.perf_counter()
    try:
        proc = subprocess.run(
            command,
            cwd=str(cwd),
            shell=True,
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
        Path("lexicon") / line_id / "pending" / "locks",
    ]
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--line", default="receipt", help="Document processing line_id")
    args = parser.parse_args()

    try:
        line_id = validate_line_id(args.line)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        return 2
    if not is_line_implemented(line_id):
        print(f"[ERROR] line is unimplemented: {line_id}")
        return 2

    repo_root = Path(__file__).resolve().parents[4]
    audit_time = _utc_now()
    provisioned_dirs = _ensure_required_dirs(repo_root, line_id)
    command_logs: Dict[str, CommandResult] = {}
    hard_checks: List[CheckResult] = []
    soft_checks: List[CheckResult] = []

    def run_and_store(check_id: str, command: str, timeout_sec: int = 30) -> CommandResult:
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
        (
            "C1",
            f"lexicon/{line_id}/lexicon.json exists",
            repo_root / "lexicon" / line_id / "lexicon.json",
            False,
        ),
        (
            "C2",
            f"defaults/{line_id}/category_defaults.json exists",
            repo_root / "defaults" / line_id / "category_defaults.json",
            False,
        ),
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
    for check_id, label, path, expect_dir in required_paths:
        passed = path.is_dir() if expect_dir else path.is_file()
        add_hard(
            check_id,
            label,
            passed,
            f"checked path: {path.relative_to(repo_root)}",
            "Restore required repository files/directories from source control.",
        )

    detected_cfg, cfg_evidence = _detect_replacer_config(repo_root, line_id)
    add_hard(
        "C3",
        f"rulesets/{line_id}/replacer_config_v1_15.json or current configured replacer config exists",
        detected_cfg is not None and detected_cfg.exists(),
        cfg_evidence if detected_cfg is None else f"{cfg_evidence}; using {detected_cfg.relative_to(repo_root)}",
        "Add the active replacer config back under rulesets/ and align references.",
    )

    if line_id == "bank_statement":
        bank_clients = _discover_bank_line_clients(repo_root)
        if bank_clients:
            bank_preview = ", ".join(client_id for client_id, _ in bank_clients[:5])
            if len(bank_clients) > 5:
                bank_preview += ", ..."
            c16_passed = True
            c16_evidence = f"found {len(bank_clients)} client(s): {bank_preview}"
        else:
            c16_passed = False
            c16_evidence = "no clients/<ID>/lines/bank_statement directory found"
        add_hard(
            "C16",
            "bank_statement client line roots exist",
            c16_passed,
            c16_evidence,
            "Create clients/<ID>/lines/bank_statement via $client-register or by applying the standard line layout.",
        )

        required_bank_dirs = [
            Path("inputs/training/ocr_kari_shiwake"),
            Path("inputs/training/reference_yayoi"),
            Path("inputs/kari_shiwake"),
            Path("artifacts/cache"),
        ]
        missing_bank_dirs: List[str] = []
        for bank_client_id, line_root in bank_clients:
            for rel in required_bank_dirs:
                target = line_root / rel
                if not target.is_dir():
                    missing_bank_dirs.append(f"{bank_client_id}:{rel.as_posix()}")
        if bank_clients and not missing_bank_dirs:
            c17_passed = True
            c17_evidence = f"all required bank directories present across {len(bank_clients)} client(s)"
        elif not bank_clients:
            c17_passed = False
            c17_evidence = "not validated because no bank_statement client line root exists"
        else:
            c17_passed = False
            c17_evidence = "missing dirs: " + "; ".join(missing_bank_dirs[:20])
            if len(missing_bank_dirs) > 20:
                c17_evidence += f"; ... (+{len(missing_bank_dirs) - 20} more)"
        add_hard(
            "C17",
            "bank_statement required directories exist per client",
            c17_passed,
            c17_evidence,
            "For each client, create inputs/training/ocr_kari_shiwake, "
            "inputs/training/reference_yayoi, inputs/kari_shiwake, and artifacts/cache under lines/bank_statement.",
        )

        reference_rule_failures: List[str] = []
        reference_rule_evidence: List[str] = []
        for bank_client_id, line_root in bank_clients:
            reference_dir = line_root / "inputs" / "training" / "reference_yayoi"
            inbox_files = [
                p
                for p in _iter_non_placeholder_files(reference_dir)
                if p.suffix.lower() in {".csv", ".txt"}
            ]
            ingested_manifest = line_root / "artifacts" / "ingest" / "training_reference_ingested.json"
            ingested_count, manifest_issue = _count_ingested_entries(ingested_manifest)
            inbox_count = len(inbox_files)
            rule_ok = ingested_count == 1 or inbox_count >= 1
            detail = f"{bank_client_id}(ingested={ingested_count}, inbox={inbox_count})"
            if manifest_issue and ingested_count == 0:
                detail += f"[{manifest_issue}]"
            reference_rule_evidence.append(detail)
            if not rule_ok:
                reference_rule_failures.append(
                    f"{bank_client_id}: ingested={ingested_count}, inbox={inbox_count}"
                )
        if bank_clients and not reference_rule_failures:
            c18_passed = True
            c18_evidence = "; ".join(reference_rule_evidence)
        elif not bank_clients:
            c18_passed = False
            c18_evidence = "not validated because no bank_statement client line root exists"
        else:
            c18_passed = False
            c18_evidence = "reference rule failed for: " + "; ".join(reference_rule_failures)
        add_hard(
            "C18",
            "bank_statement teacher reference rule (exactly one ingested OR at least one inbox file)",
            c18_passed,
            c18_evidence,
            "Place at least one teacher file under "
            "clients/<ID>/lines/bank_statement/inputs/training/reference_yayoi/ "
            "or ensure artifacts/ingest/training_reference_ingested.json has exactly one ingested file.",
        )

        cache_details: List[str] = []
        cache_missing: List[str] = []
        for bank_client_id, line_root in bank_clients:
            cache_path = line_root / "artifacts" / "cache" / "client_cache.json"
            if cache_path.exists():
                mtime_iso = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc).isoformat()
                cache_details.append(f"{bank_client_id}:{mtime_iso}")
            else:
                cache_missing.append(bank_client_id)
        if not bank_clients:
            s6_passed = True
            s6_evidence = "no bank_statement clients found (cache check skipped)"
        elif cache_missing:
            s6_passed = False
            missing_preview = ", ".join(cache_missing[:10])
            if len(cache_missing) > 10:
                missing_preview += ", ..."
            s6_evidence = (
                f"missing cache for {len(cache_missing)} client(s): {missing_preview}; "
                f"existing={len(cache_details)}"
            )
        else:
            s6_passed = True
            preview = "; ".join(cache_details[:5])
            if len(cache_details) > 5:
                preview += f"; ... (+{len(cache_details) - 5} more)"
            s6_evidence = f"all caches exist. updated_at(utc): {preview}"
        add_soft(
            "S6",
            "bank_statement cache file presence and last update time",
            s6_passed,
            s6_evidence,
            "Run $client-cache-builder --line bank_statement --client <CLIENT_ID> for clients with missing cache.",
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

    d3 = run_and_store("D3", "python -m unittest discover -s tests -v", timeout_sec=180)
    add_hard(
        "D3",
        "python -m unittest discover -s tests -v returns 0",
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
    for check_id, rel in [
        ("F1", Path("lexicon") / line_id / "pending" / "locks"),
        ("F2", Path("exports")),
        ("F3", Path("clients") / "TEMPLATE" / "lines" / line_id / "artifacts" / "ingest"),
    ]:
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
    print(f"レポート: {report_path}")
    return 0 if go else 1


if __name__ == "__main__":
    raise SystemExit(main())

