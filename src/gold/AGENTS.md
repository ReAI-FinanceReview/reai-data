<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# src/gold

## Purpose
Gold layer: turns cleansed Silver review text into analytics. Three analyzers run per review in a
fixed order (embedding → ABSA → action analysis) under `GoldOrchestrator`, which owns the
`CLEANED → ANALYZED | FAILED` state transition. `GoldAggregator` then rolls `ANALYZED` reviews into
three fact tables plus one denormalized serving mart consumed by the dashboard backend.

## Key Files
| File | Description |
|------|-------------|
| `orchestrator.py` | `GoldOrchestrator.run(batch_size, limit, target_date)`. Selects `CLEANED` rows plus `FAILED` rows with `retry_count < 3`, runs the three analyzers, commits per chunk, and records `error_message` (truncated to 2000 chars) on failure |
| `embedding_generator.py` | `GoldEmbeddingGenerator`: vectorizes `reviews_preprocessed.refined_text` into `review_embeddings` (default model `text-embedding-3-small`) |
| `absa_analyzer.py` | `GoldABSAAnalyzer`: keyword/sentiment/category extraction into `review_aspects`. `S_final = S_base × W_adv`, negation flips to `1.0 - S_final`, range 0.0–1.0. Category resolution is rule-based first, then embedding cosine similarity ≥ 0.8, else unclassified |
| `action_analyzer.py` | `GoldActionAnalyzer`: `is_attention_required` rules (rating ≥ 4 with sentiment < 0.4, or rating ≤ 2 with sentiment > 0.6) and `is_action_required` via Snorkel labeling functions + `MajorityLabelVoter`, plus a one-sentence LLM summary into `review_action_analysis` |
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
- `_MAX_RETRY = 3` in `orchestrator.py:26` is the retry ceiling; rows past it are the review-level
  dead letters queried by `src/pipeline/failures.py`.
- Aggregation must stay idempotent (UPSERT) — Airflow retries and manual backfills re-run the same
  date. `srv_daily_review_list` is range-partitioned by `date`; partitions must exist beforehand.
- Scheduled runs aggregate a single date; `run_range` exists only for manual repair
  (`docs/superpowers/specs/2026-04-18-aggregate-range-backfill-design.md`).
- ABSA yielding zero aspects must not orphan or drop mart rows — that regression is covered by the
  `absa-orphan-integrity` fixes; keep the tests that pin it.
- Embedding and LLM summary calls require a live `OPENAI_API_KEY`.

### Testing Requirements
```bash
TEST_DATABASE_URL="postgresql://testuser:testpass@localhost:5433/testdb" PYTHONPATH=. uv run pytest \
  tests/test_gold_orchestrator.py tests/test_gold_absa_analyzer.py \
  tests/test_gold_action_analyzer.py tests/test_gold_embedding_generator.py \
  tests/test_gold_aggregator.py tests/test_backend_datamart_contract.py -q
```
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

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
