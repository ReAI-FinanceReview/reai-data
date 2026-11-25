<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-25T00:00:00Z -->

# src/gold

## Purpose
Gold layer: turns cleansed Silver review text into analytics. Three analyzers run per review in a
fixed order (embedding → ABSA → action analysis) under `GoldOrchestrator`, which owns the
`CLEANED → ANALYZED | SKIPPED | FAILED` state transition. Department assignment then routes `ANALYZED`
reviews to an owning organization as a separate step, and `GoldAggregator` rolls `ANALYZED`
reviews into three fact tables plus one denormalized serving mart consumed by the dashboard
backend.

## Key Files

| File | Description |
|------|-------------|
| `orchestrator.py` | `GoldOrchestrator.run(batch_size, limit, target_date)` → `{total, analyzed, skipped, failed}`. Selects `CLEANED` rows plus `FAILED` rows with `retry_count < 3`, runs the three analyzers, commits per chunk, and records `error_message` (truncated to 2000 chars) on failure. An `AnalysisSkipped` from ABSA becomes the terminal `SKIPPED` with `error_message = skip:<reason>` and no retry increment |
| `skip.py` | `AnalysisSkipped(reason)` and the reason codes `empty` / `non_korean` / `no_aspect` (issue #94). Dependency-free so the orchestrator can catch it without importing analyzer modules eagerly |
| `embedding_generator.py` | `GoldEmbeddingGenerator`: vectorizes `reviews_preprocessed.refined_text` into `review_embeddings` (default model `text-embedding-3-small`) |
| `absa_analyzer.py` | `GoldABSAAnalyzer`: keyword/sentiment/category extraction into `review_aspects`. `S_final = S_base × W_adv`, negation flips to `1.0 - S_final`, range 0.0–1.0. Category resolution is rule-based first, then embedding cosine similarity ≥ 0.8, else unclassified. Extraction runs Okt nouns → surface dictionary → predicate step (Okt stems, `안`/`못` + function verb, `잘 쓰고 있` phrase), each only when the previous one found nothing. `process()` raises `AnalysisSkipped` for empty, Hangul-free, or aspect-less text |
| `action_analyzer.py` | `GoldActionAnalyzer`: `is_attention_required` rules (rating ≥ 4 with sentiment < 0.4, or rating ≤ 2 with sentiment > 0.6) and `is_action_required` via Snorkel labeling functions + `MajorityLabelVoter`, plus a one-sentence LLM summary into `review_action_analysis` |
| `dept_assigner.py` | Department assignment (issue #40): `expand_org_path()` plus two interchangeable `Assigner` implementations — keyword-intersection rules and an LLM assigner (Structured Outputs, `temperature=0`) — both emitting the shared `Assignment` type |
| `assigner_ids.py` | The `ASSIGNER_RULE` / `ASSIGNER_LLM` discriminator constants and `KNOWN_ASSIGNERS`. `reviews_assigned` is unique on `(review_id, assigner)` so rule and LLM results coexist for comparison |
| `dept_eval.py` | Label-based evaluation harness: `load_labels()`, `match_kind()`, `evaluate()`, `format_report()`. Scores stored assignments against a human-labeled CSV |
| `aggregator.py` | `GoldAggregator.run(target_date, retention_days=14)`, `run_range(start_date, end_date)`, `run_all()`. UPSERTs `fact_service_review_daily`, `fact_service_aspect_daily`, `fact_category_radar_scores`, `srv_daily_review_list` |
| `__init__.py` | Package marker only — no re-exports, so import modules directly |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Analyzer order in `_process_one` is a real dependency chain: ABSA's embedding-similarity category
  fallback needs the embedding row to already exist. Do not reorder or parallelize it.
- Each analyzer exposes both `process(session, review_id) -> bool` (orchestrated, shares the caller's
  session/transaction) and `process_batch(batch_size, limit)` (standalone). Add new analyzers with
  both entry points.
- Returning `False` from `process()` is treated as failure — the orchestrator raises, marks the row
  `FAILED`, and increments `retry_count`. Never swallow an analyzer error into a `True` return.
- Raise `AnalysisSkipped` only when a retry would give the same answer (the input itself cannot be
  analyzed). A missing or failing dependency is not such a case: ABSA returns `False` on zero
  aspects when Okt is unavailable or `Okt.nouns` raised, skips the predicate step in both cases
  (a healthy run might have stopped at the noun step), and the predicate step lets Okt errors
  propagate.
- `_MAX_RETRY = 3` in `orchestrator.py:28` is the retry ceiling; rows past it are the review-level
  dead letters queried by `src/pipeline/failures.py`.
- Aggregation must stay idempotent (UPSERT) — Airflow retries and manual backfills re-run the same
  date. `srv_daily_review_list` is range-partitioned by `date`: `GoldAggregator` creates the
  partition for the date it writes (`aggregator.py:298-317`) and, after each run, drops partitions
  older than `retention_days` (default 14, `aggregator.py:272-296`), so a backfill of an older date
  is dropped again by the same run.
- Scheduled runs aggregate a single date; `run_range` exists only for manual repair
  (`docs/superpowers/specs/2026-04-18-aggregate-range-backfill-design.md`).
- ABSA yielding zero aspects must not orphan or drop mart rows — that regression is covered by the
  `absa-orphan-integrity` fixes; keep the tests that pin it. Zero aspects ends in `SKIPPED`, never
  `ANALYZED`.
- New ABSA vocabulary must be justified on reviews outside the cases it is meant to fix: check
  that its polarity matches star ratings on existing `ANALYZED` reviews (the #94 case analysis is
  in PR #118's description). Predicate-step vocabulary never goes into `_SENTIMENT_DICT` /
  `_CATEGORY_KEYWORDS`: those also feed the surface-dictionary step, so an entry there changes
  reviews that already succeed (a test pins this). Where it goes instead: a sentiment stem into
  `_PREDICATE_LEXICON`; a function verb that `안` turns into an outage report into
  `_NEGATED_FUNCTION_VERBS`, which generates its `안X` phrase keyword; an Okt stem mis-analysis
  into `_STEM_ALIASES`. Any other phrase keyword needs an entry in `_PHRASE_KEYWORDS` and code in
  the predicate step that emits it.
- Negation differs by step. Noun and surface-dictionary keywords use the ±3-token window
  (`_has_negation`). A predicate-step keyword is negated when the token right before it is
  `안`/`못`, or when the predicate ends in `-지`/`-진`/`-지는`/`-지도` and the next token is the
  long-form `않다` or `못`+`하다` (`귀찮지 않아요`; not `느려서 못 하겠어요`); both together cancel
  out. A wish, request, or question (`않게`, `않았으면`, `않나요`, `_NOT_A_DENIAL`) is not a
  denial. Phrase keywords in `_PHRASE_KEYWORDS` carry their own polarity and are never flipped.
- Embedding and LLM summary calls require a live `OPENAI_API_KEY`.
- Department assignment is **not** inside `GoldOrchestrator`. It is its own pipeline step
  (`run_dept_assign` / `scripts/assign_dept.py`) that reads `ANALYZED` rows and must leave
  `processing_status` at `ANALYZED`. The mart queries in `aggregator.py` select `ANALYZED`, so
  advancing the status would drop assigned reviews out of the marts, and the enum has no
  `ASSIGNED` member to advance to.

### Testing Requirements
```bash
PYTHONPATH=. uv run pytest \
  tests/test_gold_orchestrator.py tests/test_gold_absa_analyzer.py \
  tests/test_gold_action_analyzer.py tests/test_gold_embedding_generator.py \
  tests/test_gold_aggregator.py tests/test_gold_dept_assigner.py \
  tests/test_backend_datamart_contract.py -q
```
Database-backed tests need the Docker test database; start and connect it as described in
`tests/AGENTS.md` (Testing Requirements).
Mart output must also satisfy `docs/backend-datamart-contract.md`.

### Common Patterns
- Analyzer modules are imported lazily inside `GoldOrchestrator.__init__` to isolate heavy optional
  dependencies (torch/transformers/snorkel/openai) from unrelated pipeline steps.
- The orchestrator commits once per `batch_size` chunk and rolls back the whole batch on an
  unexpected exception.

## Dependencies

### Internal
`src/models/` (`review_master_index`, `review_aspects`, `review_embedding`,
`review_action_analysis`, `fact_*`, `srv_daily_review_list`), `src/utils/db_connector.py`,
`src/utils/logger.py`, `src/processing/cleanse.py` output.

### External
`openai`, `sentence-transformers`, `transformers`, `torch`, `snorkel`, `scikit-learn`, `pgvector`,
`sqlalchemy`.

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
