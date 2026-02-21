---
name: yayoi-replacer
description: Replace ONLY debit account (col 5) in Yayoi 25-col CSV using lexicon + client_cache + defaults. Explicit invocation only.
---

# yayoi-replacer

Deterministic replacement skill for Yayoi import CSVs.

## Preconditions
1. `--client <CLIENT_ID>` is required.
2. `--line` choices: `receipt`, `bank_statement`, `credit_card_statement`, `all` (default: `all`).
3. Work under a single client folder.
4. Receipt preferred line layout:
   - `clients/<CLIENT_ID>/lines/receipt/`
5. Receipt legacy fallback (deprecated, auto-detected if line layout missing):
   - `clients/<CLIENT_ID>/`
6. Bank line is line-scoped only:
   - `clients/<CLIENT_ID>/lines/bank_statement/`
7. `bank_statement` requires exactly one teacher reference at runtime:
   - if `artifacts/ingest/training_reference_ingested.json` exists, unique ingested SHA count must be exactly 1
   - otherwise count non-placeholder files under `inputs/training/reference_yayoi/` and require exactly 1
8. `credit_card_statement` is currently unimplemented.

## PLAN semantics (always printed)
1. The skill always performs preflight planning and prints:
   - `[PLAN] client=<CLIENT_ID> line=<...>`
   - one line per selected line with `RUN` / `SKIP` / `FAIL`
2. `SKIP` only when target input count in `inputs/kari_shiwake/` is 0.
3. `FAIL` when:
   - target input count is 2 or more
   - required runtime/config is missing
   - structural invariants are invalid
   - bank teacher reference count is not exactly 1
4. `credit_card_statement` behavior:
   - in `--line all`: `SKIP (unimplemented)`
   - explicit `--line credit_card_statement`: exit 2 with clear unimplemented error

## Confirmation gate
1. If PLAN has any `FAIL`, execution is blocked and exits 1.
2. `--dry-run` prints PLAN and exits:
   - 0 when no `FAIL`
   - 1 when `FAIL` exists
3. If there are `RUN` lines:
   - `--yes`: proceed without prompt
   - interactive TTY: prompt `Proceed with RUN lines? [y/N]`
   - non-interactive without `--yes`: exit 2 with guidance
4. If all selected lines are `SKIP`, exits 0 with `[OK] nothing to do`.

## Runtime behavior (line execution)
1. The skill entrypoint is a dispatcher only.
2. Line runners are separated under `belle/line_runners/`:
   - `receipt.py`
   - `bank_statement.py`
   - `credit_card_statement.py`
3. Receipt and bank execution logic remain unchanged in behavior; only orchestration is refactored.

## Canonical specs
1. `spec/REPLACER_SPEC.md`
2. `spec/CATEGORY_OVERRIDES_SPEC.md`
3. `spec/CLIENT_CACHE_SPEC.md`
4. `spec/LEXICON_PENDING_SPEC.md`
5. `spec/BANK_REPLACER_SPEC.md`

## Execution examples
```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID>
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID> --line receipt --yes
```

```bash
python .agents/skills/yayoi-replacer/scripts/run_yayoi_replacer.py --client <CLIENT_ID> --line all --dry-run
```
