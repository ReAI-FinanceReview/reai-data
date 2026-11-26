<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-26T00:00:00Z -->

# src/pipeline

## Purpose
The orchestration seam. Wraps every stage in a uniform `run_*() -> RunResult` function so the CLI,
Airflow, and tests all drive the same code path; owns argument parsing, sequential step execution
with fail-fast semantics, post-aggregate DB validation, and the operational queries for
retry-exhausted work.

## Key Files

| File | Description |
|------|-------------|
| `steps.py` | `RunResult` dataclass and the step wrappers `run_crawl` (`store`: `appstore` / `playstore` / `all`), `run_load`, `run_cleanse`, `run_preprocess` (deprecated), `run_extract_features`, `run_action_analysis`, `run_generate_embeddings`, `run_gold`, `run_dept_assign`, `run_aggregate`, `run_post_aggregate_validation`, plus `run_steps()` which stops at the first non-success step |
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
- `run_crawl(config_path, store="all")` runs `UnifiedCrawler` over the stores in
  `CRAWL_STORE_CHOICES[store]` and fails when any store's `StoreCrawlSummary.failure` is set (its
  crawler raised, the App Store empty-feed cutoff, every Play app empty); the message names each
  failed store (`crawl failed: playstore: ...`). With `all` both stores run before the step
  decides. `validations` holds each store's `as_dict()` (per-app outcomes and the `warnings` list
  of apps that are empty, have a gap, or failed); `input_count` / `output_count` are reviews
  fetched / new. An unknown `store` fails without crawling. The `crawl` entry of `run_steps` (and
  so the CLI's `--steps crawl`) keeps the default `all`.
- Registering a new step means adding it to the `step_funcs` dict in `run_steps()` **and** to the
  `--steps` help text in `cli.py`. An unrecognized step yields `"unknown step"` and halts the run.
- `post_aggregate_validate` and `validate` are aliases for the same validator. Both require
  `--target-date`; the empty-string fallback (`target_date or ""`) surfaces as a parse failure
  rather than silently validating today.
- Date parsing is centralized in `_parse_date_arg` (`%Y-%m-%d`); `cli.py` additionally validates
  `--target-date` at parse time via `date.fromisoformat`.
- `run_aggregate` rejects `target_date` combined with `start_date`/`end_date`, and requires
  `start_date`/`end_date` together. Keep those guards.
- `run_steps` requires a date for `dept_assign`: its entry in `step_funcs` passes
  `target_date or ""`, so a CLI run without `--target-date` fails to parse instead of
  becoming a paid LLM sweep over every `ANALYZED` row. Keep that guard in `run_steps`, not in
  `run_dept_assign`: `run_dept_assign(target_date=None)` is the backfill path that
  `scripts/assign_dept.py --backfill` calls (`scripts/assign_dept.py:54-60`).
- Heavy modules are imported **inside** each step function so that, for example, `run_crawl` works
  without torch installed. Do not hoist those imports to module scope.
- `run_preprocess` is deprecated: it emits a `DeprecationWarning` and returns failure. Route
  cleansing through `scripts/cleanse_reviews.py`.
- Severity policy in the validator is deliberate — zero new reviews is a `warning`/`report`, while
  broken state transitions, mart gaps, and integrity violations are `failure` and fail the DAG
  (`docs/airflow-continuous-load-readiness.md`).

### Testing Requirements
```bash
PYTHONPATH=. uv run pytest \
  tests/test_pipeline_steps.py tests/test_cli_parsing.py tests/test_pipeline_cli_validation.py \
  tests/test_pipeline_failures.py tests/test_post_aggregate_validation.py \
  tests/test_pipeline_integration.py tests/test_crawl_outcome.py -q
```
`run_crawl` is covered in `tests/test_crawl_outcome.py` (store selection, per-store failure, the
`crawl` entry of `run_steps` and the CLI) and `tests/test_appstore_rss.py` (App Store outcomes).

Database-backed tests need the Docker test database; start and connect it as described in
`tests/AGENTS.md` (Testing Requirements).

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

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
