<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# src/pipeline

## Purpose
The orchestration seam. Wraps every stage in a uniform `run_*() -> RunResult` function so the CLI,
Airflow, and tests all drive the same code path; owns argument parsing, sequential step execution
with fail-fast semantics, post-aggregate DB validation, and the operational queries for
retry-exhausted work.

## Key Files

| File | Description |
|------|-------------|
| `steps.py` | `RunResult` dataclass and the step wrappers `run_crawl`, `run_load`, `run_cleanse`, `run_preprocess` (deprecated), `run_extract_features`, `run_action_analysis`, `run_generate_embeddings`, `run_gold`, `run_dept_assign`, `run_aggregate`, `run_post_aggregate_validation`, plus `run_steps()` which stops at the first non-success step |
| `cli.py` | argparse entrypoint: `--steps` (default `crawl,load,cleanse,gold`, mirroring the DAG order), `--batch-size`, `--limit`, `--model-name`, `--config`, `--target-date`. Loads `.env` if present, logs each `RunResult`, returns 1 on the first failure |
| `post_aggregate_validation.py` | `PostAggregateValidator.validate(target_date)` → `ValidationReport` of `ValidationCheck`s with severity `failure` / `warning` / `report` |
| `failures.py` | `fetch_review_dead_letters()` (FAILED with `retry_count >= 3`) and `fetch_batch_dead_letters()` (`ingestion_batch` `DEAD_LETTER`), plus the equivalent raw SQL constants |
| `validation.py` | `make_count_validation(input_count, output_count)` — simple row-count delta metrics |
| `__init__.py` | Re-exports `RunResult` and the crawl/preprocess/features/embeddings/run_steps helpers |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Steps return failures as `RunResult(status="failed", message=...)`; they do not raise. `_handle_step`
  catches everything and logs with `logger.exception`. Preserve that contract — Airflow and the CLI
  map status to the exit code.
- Registering a new step means adding it to the `step_funcs` dict in `run_steps()` **and** to the
  `--steps` help text in `cli.py`. An unrecognized step yields `"unknown step"` and halts the run.
- `post_aggregate_validate` and `validate` are aliases for the same validator. Both require
  `--target-date`; the empty-string fallback (`target_date or ""`) surfaces as a parse failure
  rather than silently validating today.
- Date parsing is centralized in `_parse_date_arg` (`%Y-%m-%d`); `cli.py` additionally validates
  `--target-date` at parse time via `date.fromisoformat`.
- `run_aggregate` rejects `target_date` combined with `start_date`/`end_date`, and requires
  `start_date`/`end_date` together. Keep those guards.
- `run_dept_assign` requires a target date and must keep requiring one. Letting it default would
  turn a single CLI line into a paid LLM sweep over every `ANALYZED` row; the full backfill stays
  reachable only through an explicit `scripts/assign_dept.py --backfill`.
- Heavy modules are imported **inside** each step function so that, for example, `run_crawl` works
  without torch installed. Do not hoist those imports to module scope.
- `run_preprocess` is deprecated: it emits a `DeprecationWarning` and returns failure. Route
  cleansing through `scripts/cleanse_reviews.py`.
- Severity policy in the validator is deliberate — zero new reviews is a `warning`/`report`, while
  broken state transitions, mart gaps, and integrity violations are `failure` and fail the DAG
  (`docs/airflow-continuous-load-readiness.md`).

### Testing Requirements
```bash
TEST_DATABASE_URL="postgresql://testuser:testpass@localhost:5433/testdb" PYTHONPATH=. uv run pytest \
  tests/test_pipeline_steps.py tests/test_cli_parsing.py tests/test_pipeline_cli_validation.py \
  tests/test_pipeline_failures.py tests/test_post_aggregate_validation.py \
  tests/test_pipeline_integration.py -q
```

### Common Patterns
```python
def run_x(...) -> RunResult:
    from src.somewhere import Thing          # lazy import
    return _handle_step("x", lambda: Thing(config_path).do())
```
Validation checks are dataclasses with `as_dict()` so reports serialize straight into
`RunResult.validations` and the CLI's JSON log line.

## Dependencies

### Internal
`src/crawlers/`, `src/loaders/`, `src/gold/`, `src/models/` (enums, `ingestion_batch`,
`review_master_index`), `src/utils/db_connector.py`, `src/utils/logger.py`.

### External
`sqlalchemy`, `python-dotenv`; stdlib `argparse`, `dataclasses`, `json`.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
