# CATEGORY_DEFAULTS_SPEC (belle.category_defaults.v2)

## Purpose
`defaults/<line_id>/category_defaults.json` provides a shared, side-neutral fallback rule per
lexicon category. It is used when per-client evidence is missing or too weak, especially for
new clients that have no historical journals. The current repository tracks line-specific defaults for:
1. `receipt`
2. `credit_card_statement`

This is deliberately **opinionated** to increase replacement coverage.
Human review remains the source of truth.

## Schema
Top-level keys:
- `schema`: string (`belle.category_defaults.v2`)
- `version`: string
- `created_at`: ISO-8601
- `defaults`: object mapping `category_key` -> default rule
- `global_fallback`: default rule used when no category matched

### Default rule object
- `target_account`: string (non-empty Yayoi account name for the line's target side)
- `target_tax_division`: string (may be empty)
- `confidence`: float (0..1) reported by replacer when this default is used
- `priority`: `"HIGH"|"MED"|"LOW"` recommended review priority when this rule is used
- `reason_code`: string (machine-readable reason)

## Design notes
- Defaults are shared across lines but interpreted by each line's target side:
  - `receipt`: target side is the debit side.
  - `credit_card_statement`: target side is the placeholder side.
- `receipt` may use non-empty `target_tax_division` as a fallback route after learned receipt tax evidence.
- `credit_card_statement` still stores `target_tax_division` only as shared contract data in this phase.
- `target_tax_division` must exist in tracked defaults, but it may be blank.
- Defaults should use commonly available Japanese account names to reduce import risk.
- Defaults are not client-specific; client_cache overrides them when evidence is strong enough.
- Tracked defaults are aligned to the shared `lexicon/lexicon.json` category keyset for each supported line.
- If the client has an allowlist of valid accounts (optional future), defaults should be filtered against it.

