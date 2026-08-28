<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# src/loaders

## Purpose
Load stage: the bridge from Bronze Parquet to PostgreSQL. Reads `ingestion_batch` rows in
`PENDING` / `FAILED`, fetches each batch's Parquet object, and inserts the reviews into
`review_master_index` with `processing_status = RAW`, then advances the batch status. This is the
only place where crawled objects become durable DB state.

## Key Files

| File | Description |
|------|-------------|
| `batch_loader.py` | `BatchLoader.load_pending_batches(limit=100)` — batch selection, Parquet read, dedup against existing reviews, `review_master_index` insert, batch status transition, retry/dead-letter handling |
| `__init__.py` | Re-exports `BatchLoader` |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Batch status is the source of truth for ingestion failures: exhausted batches become
  `DEAD_LETTER` and are queried by `src/pipeline/failures.py:fetch_batch_dead_letters`. There is
  deliberately no separate row-level DLQ table (`docs/pipeline-failure-policy.md`).
- Loading must be idempotent — a re-run of the same batch must not duplicate
  `review_master_index` rows. Preserve the existing dedup on platform review identity.
- `PendingRollbackError` is handled explicitly; when adding DB work, keep failures scoped so one bad
  batch does not poison the session for the remaining batches.
- The loader owns only Bronze → index. Text cleansing belongs in `src/processing/`, analytics in
  `src/gold/`.

### Testing Requirements
```bash
TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql://testuser:testpass@localhost:5433/testdb}" \
  PYTHONPATH=. uv run pytest tests/test_batch_loader.py tests/test_bronze_loading.py -q
```
`tests/conftest.py` supplies `db_with_pending_batches` and `db_with_failed_batches`, which write
real Parquet files into a temp Bronze directory — use them rather than stubbing the reader.

### Common Patterns
Raw SQL via `sqlalchemy.text` is used where bulk UPSERT semantics matter; UUIDs and timezone-aware
UTC timestamps everywhere.

## Dependencies

### Internal
`src/models/ingestion_batch.py`, `src/models/review_master_index.py`, `src/models/enums.py`,
`src/utils/db_connector.py`, `src/utils/minio_client.py`, `src/schemas/parquet/app_review.py`.

### External
`sqlalchemy`, `pyarrow`, `boto3`.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
