# CATEGORY_DEFAULTS_SPEC (belle.category_defaults.v1)

## Purpose
`defaults/category_defaults.json` provides a **global default debit account** per lexicon category.
It is used when per-client evidence is missing or too weak, especially for **new clients** that
have no historical journals.

This is deliberately **opinionated** to increase replacement coverage.
Human review remains the source of truth.

## Schema
Top-level keys:
- `schema`: string (`belle.category_defaults.v1`)
- `version`: string
- `created_at`: ISO-8601
- `defaults`: object mapping `category_key` -> default rule
- `global_fallback`: default rule used when no category matched

### Default rule object
- `debit_account`: string (Yayoi debit account name)
- `confidence`: float (0..1) reported by replacer when this default is used
- `priority`: `"HIGH"|"MED"|"LOW"` recommended review priority when this rule is used
- `reason_code`: string (machine-readable reason)

## Design notes
- Defaults should use **commonly available** Japanese account names to reduce import risk.
- Defaults are not client-specific; client_cache overrides them when evidence is strong enough.
- If the client has an allowlist of valid accounts (optional future), defaults should be filtered against it.

