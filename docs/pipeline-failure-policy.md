# Pipeline Failure Policy

This document defines where pipeline failures are tracked in schema v4.

## Source of Truth by Stage

| Stage | Failure scope | Source of truth |
| --- | --- | --- |
| `crawl` / `load` | Parquet batch load failure | `ingestion_batch` |
| `cleanse` | Review-level cleanse failure | `review_master_index` |
| `gold_analyze` | Review-level downstream analysis failure | `review_master_index` |
| `dept_assign` | Review-level department assignment failure | `reviews_assigned` (`is_failed`, `try_number`) |
| LLM calls | API call audit and model response failure | `review_llm_analysis_logs` |
| `gold_aggregate` | Date/job-level aggregation failure | Logs and `RunResult` |

`review_master_index` is the central review orchestration hub. Review-level
failures that happen after a review has a master index row should be recorded
there with `processing_status = FAILED`, `error_message`, and `retry_count`.

`ingestion_batch` is the batch-level load DLQ. It tracks Parquet files that were
written by crawlers and later consumed by `BatchLoader`.

## Dead-Letter Criteria

Batch-level dead letters:

```sql
SELECT *
FROM ingestion_batch
WHERE status = 'DEAD_LETTER'
ORDER BY updated_at ASC, batch_id;
```

Review-level dead-letter-equivalent records:

```sql
SELECT *
FROM review_master_index
WHERE processing_status = 'FAILED'
  AND retry_count >= 3
ORDER BY review_created_at ASC NULLS LAST, review_id;
```

Department assignment dead letters:

```sql
SELECT review_id, assigner, try_number, assignment_reason, updated_at
FROM reviews_assigned
WHERE is_failed
  AND try_number >= 3
ORDER BY updated_at ASC, review_id;
```

Assignment failures do not move `review_master_index.processing_status`; the review
stays `ANALYZED` because the serving mart loads `ANALYZED` rows only. Nothing
re-processes these rows automatically either — the DAG task is scoped to a single
`ds`, so a failed row is never revisited on a later run. Recovery is
`scripts/assign_dept.py --backfill --reassign`, run by hand. Treat this query as the
manual sweep until an automated re-attempt path exists.

The review-level condition is intentionally called "dead-letter-equivalent"
because `processing_status_type` does not include a separate `DEAD_LETTER`
value. The current operational boundary is `FAILED` plus the retry threshold.

## Skipped Reviews (`SKIPPED`)

`SKIPPED` is not a failure. Gold ABSA is rule-based, and some reviews give it
nothing to analyze: retrying them returns the same result every time (issue #94).
`GoldOrchestrator` records them as the terminal status `SKIPPED` instead of
`FAILED`:

- `error_message = "skip:<reason>"`, where the reason is `empty` (no
  `refined_text`, e.g. an emoji-only review), `non_korean` (letters remain but
  none of them is Hangul once bracketed cleansing placeholders such as
  `[PROFANITY]` are removed), or `no_aspect` (no keyword after noun, dictionary,
  and predicate extraction; this includes text left with only placeholders,
  digits, or symbols)
- `retry_count` is left unchanged, and the orchestrator never selects the row again
- no aspects and no action analysis are written, so the row never reaches the marts,
  same as `FAILED`

`SKIPPED` rows are therefore neither dead letters nor unresolved work:
`fetch_review_dead_letters()` ignores them, and `post_aggregate_validate`'s
`review_state` check reports them as `skipped_count` without failing.
`no_aspect` is only decided after a complete Okt analysis. When Okt is unavailable
or `Okt.nouns` raised, a zero-aspect review stays a retryable `FAILED`.

The `gold` pipeline step fails when it analyzed nothing and at least one review
`FAILED`. A run whose reviews were all `SKIPPED` succeeds. Counting `SKIPPED`
alongside a failure as success would hide an outage, because some rows reach a
skip verdict without the embedding API: the orchestrator embeds before ABSA, and
the embedding step succeeds without a call only for an empty `refined_text` or a
row that already has an embedding (e.g. a retried `FAILED` row). A new review that
still needs an embedding, Korean or not, becomes `FAILED` on such a day. The gate
only catches a run in which nothing was analyzed: a partial outage, where some
reviews were analyzed and others `FAILED`, still succeeds, as it did before #94.

```sql
SELECT error_message, COUNT(*)
FROM review_master_index
WHERE processing_status = 'SKIPPED'
GROUP BY error_message;
```

### Re-opening skipped reviews

`SKIPPED` is terminal for the current rules, not forever. When a rule change can
alter the verdict — translation for `non_korean` (#115), new vocabulary for
`no_aspect` (#117) — re-open the affected rows by hand. Re-running cleansing does
not do this: it promotes only `RAW` rows and `FAILED` rows whose `error_message`
starts with `Cleanse failed:`. The re-opened rows are usually from past dates, and
every downstream step is scoped to one date, so stopping after `gold` leaves them
without a department and out of the marts. The examples use `skip:non_korean`;
substitute the reason being re-opened.

1. Count the rows and list the affected dates. `gold --target-date` selects by the
   UTC day of `review_created_at`.

   ```sql
   SELECT (review_created_at AT TIME ZONE 'UTC')::date AS day, COUNT(*)
   FROM review_master_index
   WHERE processing_status = 'SKIPPED' AND error_message = 'skip:non_korean'
   GROUP BY day ORDER BY day;
   ```

2. Optional, and only when `refined_text` changed after the skip (e.g. after a
   cleansing fix). The embedding generator keeps an existing row, so a stale
   embedding must go first. Skipped rows do have embeddings, because the
   orchestrator embeds before ABSA. Deleting an embedding whose text did not
   change only buys a paid re-embedding, so scope the delete to rows rewritten
   since they were embedded:

   ```sql
   DELETE FROM review_embeddings e
   USING review_master_index m, reviews_preprocessed p
   WHERE e.review_id = m.review_id AND p.review_id = m.review_id
     AND m.processing_status = 'SKIPPED' AND m.error_message = 'skip:non_korean'
     AND e.created_at < p.updated_at;
   ```

3. Move the rows back to `CLEANED`. Reset `retry_count` as well: the row is
   analyzed again under new rules, and failures from before the skip should not
   push it into the dead letters early.

   ```sql
   UPDATE review_master_index
   SET processing_status = 'CLEANED', error_message = NULL, retry_count = 0
   WHERE processing_status = 'SKIPPED' AND error_message = 'skip:non_korean';
   ```

4. For each affected date, run `gold` and department assignment. Without
   `--target-date`, `gold` also sweeps every other `CLEANED` and retryable
   `FAILED` row in the table, each a paid embedding and LLM call.

   ```bash
   PYTHONPATH=. uv run python -m src.pipeline.cli --steps gold,dept_assign --target-date YYYY-MM-DD
   ```

5. Re-aggregate the affected range with an explicit `retention_days`. Do not use
   the CLI `aggregate` step for this: both its `--target-date` and range paths use
   the default `retention_days=14`. After writing, every aggregation drops the
   `srv_daily_review_list` partitions older than the cutoff, affected or not
   (`src/gold/aggregator.py:272-296`). Pick a value that reaches the oldest
   partition you intend to keep, not just the oldest affected date.

   ```bash
   PYTHONPATH=. uv run python -c "
   from datetime import date
   from src.gold.aggregator import GoldAggregator
   GoldAggregator().run_range(date(YYYY, M, D), date(YYYY, M, D), retention_days=N)
   "
   ```

6. Validate each affected date:

   ```bash
   PYTHONPATH=. uv run python -m src.pipeline.cli --steps post_aggregate_validate --target-date YYYY-MM-DD
   ```

## Cleanse Failure Handling

The Bronze-to-Silver cleanse pipeline treats empty review text as `skipped`.
That is not a failure.

If cleansing a specific review row raises an exception, the pipeline records the
failure in `review_master_index` and continues with the remaining rows:

- `processing_status = FAILED`
- `error_message = "Cleanse failed: <ExceptionType>: <message>"`
- `retry_count = retry_count + 1`

Silver write failures and bulk DB status update failures are app/date-level I/O
failures. They are still raised to the caller because they are not safely
attributable to one review row.

## Step-Level Failures

This project does not currently add a generic `pipeline_failures` or
`pipeline_step_failures` table.

That choice is intentional for issue #7:

- `ingestion_batch` already covers batch-level load failures.
- `review_master_index` covers review-level downstream failures.
- `review_llm_analysis_logs` covers LLM call audit failures.
- Adding a generic failure table now would duplicate existing sources of truth.

If Airflow/job-level failure analysis needs DB-backed history later, create a
separate issue for a job-level execution table. That table should not replace
the existing batch and review failure state tables.
