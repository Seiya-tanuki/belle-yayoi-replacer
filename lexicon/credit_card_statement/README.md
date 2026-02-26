# credit_card_statement lexicon

This line is implemented.

Canonical lexicon is shared at `lexicon/lexicon.json` (not per-line in this directory).

Usage contract (Phase D):
1. `credit_card_statement` uses lexicon for category fallback only (secondary to merchant-key routing).
2. This line does not own a separate lexicon file.
3. This line does not use `label_queue.csv` autogrow flow.
