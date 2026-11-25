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
  `refined_text`, e.g. an emoji-only review), `non_korean` (no Hangul once
  cleansing placeholders such as `[PROFANITY]` are removed), or `no_aspect`
  (Korean text with no keyword after noun, dictionary, and predicate extraction)
- `retry_count` is left unchanged, and the orchestrator never selects the row again
- no aspects and no action analysis are written, so the row never reaches the marts,
  same as `FAILED`

`SKIPPED` rows are therefore neither dead letters nor unresolved work:
`fetch_review_dead_letters()` ignores them, and `post_aggregate_validate`'s
`review_state` check reports them as `skipped_count` without failing.
`no_aspect` is only decided after a complete Okt analysis. When Okt is unavailable
or `Okt.nouns` raised, a zero-aspect review stays a retryable `FAILED`.

The `gold` pipeline step fails when it analyzed nothing and at least one review
`FAILED`. A run whose reviews were all `SKIPPED` succeeds, but `SKIPPED` alone
cannot hide an outage: `empty` and `non_korean` are decided from the text, so
they still occur on a day when the embedding API is down.

```sql
SELECT error_message, COUNT(*)
FROM review_master_index
WHERE processing_status = 'SKIPPED'
GROUP BY error_message;
```

### Re-opening skipped reviews

`SKIPPED` is terminal for the current rules, not forever. When a rule change can
alter the verdict — translation for `non_korean` (#115), new vocabulary for
`no_aspect` (#117) — move the affected rows back to `CLEANED` and run the `gold`
step without `--target-date` (or once per affected date). Re-running cleansing does
not do this: it promotes only `RAW` rows and `FAILED` rows whose `error_message`
starts with `Cleanse failed:`.

```sql
BEGIN;
-- Only when refined_text changed since the skip (e.g. after a cleansing fix):
-- the embedding generator keeps an existing row, so a stale one must go first.
DELETE FROM review_embeddings
WHERE review_id IN (
    SELECT review_id FROM review_master_index
    WHERE processing_status = 'SKIPPED' AND error_message = 'skip:non_korean'
);
UPDATE review_master_index
SET processing_status = 'CLEANED', error_message = NULL
WHERE processing_status = 'SKIPPED' AND error_message = 'skip:non_korean';
COMMIT;
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
