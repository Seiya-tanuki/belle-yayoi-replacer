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
- The diagnostic run is read-only for tracked files.
- It may provision required runtime directories (`exports/*`, `exports/backups/`, `lexicon/receipt/pending/locks/`) via safe `mkdir -p`.
- Report artifacts are written under `exports/system_diagnose/`.
- Default behavior checks all lines (`receipt`, `bank_statement`, `credit_card_statement`) in one run.
- `--line` is optional and narrows diagnostics to a single line.
- Diagnostics now cover the shared tax config at `clients/<CLIENT_ID>/config/yayoi_tax_config.json`.
- `clients/TEMPLATE/config/yayoi_tax_config.json` is checked as a hard repository/template integrity requirement.
- For existing non-`TEMPLATE` clients, missing shared tax config is warn-only.
- For existing non-`TEMPLATE` clients, present-but-invalid shared tax config is treated as a failure.
- For `--line bank_statement`, diagnose focuses on repository integrity and TEMPLATE structure (tracked assets/placeholders).
- For `--line credit_card_statement`, diagnose validates repository/template integrity for the line; runtime replacement contracts (Contract A / strict-stop) are enforced by `$yayoi-replacer`.
- Presence of client-specific training/target input files is validated by runtime skills (`$client-cache-builder`, `$yayoi-replacer`), not by system-diagnose.

## Execution
```bash
python .agents/skills/system-diagnose/scripts/system_diagnose.py
python .agents/skills/system-diagnose/scripts/system_diagnose.py --line bank_statement
```
