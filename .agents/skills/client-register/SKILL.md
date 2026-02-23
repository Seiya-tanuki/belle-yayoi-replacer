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
3. Always provisions all lines:
   1. `receipt`
   2. `bank_statement`
   3. `credit_card_statement`
4. Always runs line initialization hooks:
   1. Initializes receipt `config/category_overrides.json` (same behavior as before).
   2. Ensures bank `config/bank_line_config.json` exists (same behavior as before).
   3. `credit_card_statement` line is provisioned for runnable flow (Contract A and strict-stop are runtime-enforced).

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

## Execution
```bash
python .agents/skills/client-register/register_client.py
```
