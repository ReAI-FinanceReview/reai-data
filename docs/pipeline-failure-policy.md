# Pipeline Failure Policy

This document defines where pipeline failures are tracked in schema v4.

## Source of Truth by Stage

| Stage | Failure scope | Source of truth |
| --- | --- | --- |
| `crawl` / `load` | Parquet batch load failure | `ingestion_batch` |
| `cleanse` | Review-level cleanse failure | `review_master_index` |
| `gold_analyze` | Review-level downstream analysis failure | `review_master_index` |
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

The review-level condition is intentionally called "dead-letter-equivalent"
because `processing_status_type` does not include a separate `DEAD_LETTER`
value. The current operational boundary is `FAILED` plus the retry threshold.

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
