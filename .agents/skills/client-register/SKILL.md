---
name: client-register
description: Register a new client directory by copying clients/TEMPLATE to clients/<CLIENT_ID> with strict Windows-safe name validation. Explicit invocation only.
---

# client-register

Creates a new client workspace from the template.

## Preconditions
1. Explicitly invoke `$client-register`.
2. Run in repo root.

## What this skill does
1. Validates user input and canonicalizes to a Windows-safe `CLIENT_ID`.
2. Copies `clients/TEMPLATE/` to `clients/<CLIENT_ID>/`.
3. Provisions lines based on `--line`:
   1. default `--line all`: `receipt`, `bank_statement`, `credit_card_statement`
   2. line-aware `--line <line_id>`: provisions only the selected line directory under `clients/<CLIENT_ID>/lines/`
4. Runs line initialization hooks for selected lines:
   1. Initializes `receipt` `config/category_overrides.json`.
   2. Initializes `credit_card_statement` `config/category_overrides.json`.
   3. Ensures `bank_statement` `config/bank_line_config.json` exists.
5. `credit_card_statement` line is provisioned for runnable flow (Contract A and strict-stop are runtime-enforced).

## Execution
1. All lines (default):
```bash
python .agents/skills/client-register/register_client.py
```
2. Single line:
```bash
python .agents/skills/client-register/register_client.py --line credit_card_statement
```

## Notes
1. Category overrides are generated from shared `lexicon/lexicon.json` category keys and line defaults:
   1. `defaults/receipt/category_defaults.json`
   2. `defaults/credit_card_statement/category_defaults.json`
2. category_overrides generation is best-effort; missing per-category defaults are filled with `global_fallback`.
3. `bank_statement` does not use category_overrides.

## Template contract (must preserve)
1. `clients/TEMPLATE/lines/receipt/config/` exists.
2. `clients/TEMPLATE/lines/receipt/outputs/runs/` exists.
3. `clients/TEMPLATE/lines/receipt/artifacts/cache/` exists.
4. `clients/TEMPLATE/lines/receipt/artifacts/ingest/` exists.
5. `clients/TEMPLATE/lines/receipt/artifacts/telemetry/` exists.
6. `clients/TEMPLATE/lines/bank_statement/config/` exists.
7. `clients/TEMPLATE/lines/credit_card_statement/` exists.
8. `clients/TEMPLATE/config/` may remain as optional shared config root.
9. Use `.gitkeep` files as needed to keep empty directories in git.
