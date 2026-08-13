<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# config/dictionaries

## Purpose
Korean-language reference data for the Bronze → Silver cleansing stage. These files encode
domain knowledge (bank/app brand variants, abusive language, low-signal tokens) that no general NLP
model provides, so cleansing output quality is directly tied to their contents.

## Key Files
| File | Description |
|------|-------------|
| `synonyms.json` | Flat JSON map of surface form → canonical form, mostly banking-app brand variants (`"신한 슈퍼 SOL": "신한슈퍼솔"`). Wired in as `SYNONYMS_PATH` by `scripts/cleanse_reviews.py:30` |
| `profanity.json` | Flat JSON map of term → tag; tags are `[PROFANITY]`, `[STRONG_NEG]`, `[THREAT]`. Wired in as `PROFANITY_PATH` by `scripts/cleanse_reviews.py:31` |
| `stopwords.txt` | 51 low-signal Korean tokens, one per line (`앱`, `어플`, `하다`, …). Currently not read by any code path |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Both JSON files are flat `{string: string}` maps — no nesting, no arrays. The loader assumes that
  shape.
- Keep them UTF-8 without a BOM; the review corpus is Korean and encoding damage is silent.
- Profanity tags are consumed downstream as sentiment signals; inventing a new tag string means
  updating the consuming rules in `src/processing/cleanse.py` and `src/gold/`.
- Synonym canonicalization affects ABSA keyword grouping and therefore `fact_service_aspect_daily`
  rows. A mapping change is an analytics change, not just a text change.
- `stopwords.txt` is referenced only in prose (`src/schemas/parquet/review_preprocessed.py:24`
  describes stopword removal). Confirm the actual consumer before assuming it is live.
- The DB also has `synonyms` / `profanity` / `financial_terms` tables (`src/models/dictionary.py`).
  These files are the file-based path used by the cleansing CLI; keep the two in mind when
  changing either.

### Testing Requirements
`tests/test_cleanse.py` writes its own temporary dictionaries, so edits here are not covered
automatically. Verify with a real cleansing run over a known Bronze partition:

```bash
PYTHONPATH=. uv run python scripts/cleanse_reviews.py --date YYYY-MM-DD
```

### Common Patterns
Longest-match-first replacement is expected — order variants so the most specific surface form is
present (`"신한 슈퍼 SOL"` alongside `"슈퍼솔"`).

## Dependencies

### Internal
`scripts/cleanse_reviews.py`, `src/processing/cleanse.py`, `src/models/dictionary.py`.

### External
None (data files only).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
