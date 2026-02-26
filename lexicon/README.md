# lexicon/

Shared canonical category+terms dictionary lives at `lexicon/lexicon.json`.

Line usage:
1. `receipt`: uses lexicon as primary category inference source.
2. `credit_card_statement`: uses lexicon only as secondary category fallback.
3. `bank_statement`: does not use lexicon category routing.

Pending queue:
1. `label_queue.csv` is receipt-only in Phase D.
2. Path: `lexicon/receipt/pending/`.

See:
1. `spec/LEXICON_SPEC.md`
2. `spec/LEXICON_PENDING_SPEC.md`
