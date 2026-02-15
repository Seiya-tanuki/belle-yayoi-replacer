---
name: system-diagnose
description: Run comprehensive environment/system readiness diagnostics and export a Markdown report under exports/system_diagnose/. Explicit invocation only.
---

# system-diagnose

Runs hard/soft readiness checks for this repository and environment, then writes a timestamped Markdown report.

## Output
- `exports/system_diagnose/system_diagnose_<UTC_TS>_<SHA8>.md`
- `exports/system_diagnose/LATEST.txt`

## Notes
- This skill is explicit invocation only.
- The diagnostic run is read-only for tracked files and writes artifacts only under `exports/system_diagnose/`.

## Execution
```bash
python .agents/skills/system-diagnose/scripts/system_diagnose.py
```
