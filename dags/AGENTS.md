<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-27T00:00:00Z -->

# dags

## Purpose
Airflow DAG definitions. Holds the single daily production DAG that chains the pipeline stages
by shelling out to the repository's own scripts and step functions, so the DAG contains scheduling
and timeout policy only — never analytics logic.

## Key Files

| File | Description |
|------|-------------|
| `financial_review_pipeline.py` | DAG `financial_review_etl_pipeline`: `@daily`, `catchup=False`, 3 retries / 5 min, tags `finance, etl, reviews, nlp` |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Task chain and timeouts: `crawl_appstore` (2h) and `crawl_playstore` (2h) → `load_reviews` (30m)
  → `cleanse_reviews` (1h) → `gold_analyze` (3h) → `dept_assign` (1h) → `gold_aggregate` (1h) →
  `post_aggregate_validate` (15m). Both crawl tasks also feed the leaf `crawl_watcher`.
- The crawl is one task per store (`scripts/crawl_reviews.py --store appstore|playstore`, issue
  #127), so each store fails and retries (3 × 5 min) on its own. A store's task fails when its
  crawler raises or reports a store-level failure (App Store: `appstore.max_consecutive_empty_feeds`
  apps in a row read no review; Play: every app returned zero); one empty app is only a warning in
  the task log.
- `load_reviews` has `trigger_rule="all_done"`: it waits until both crawl tasks are finished,
  retries included, and runs even when one (or both) failed, loading whatever PENDING batches
  exist, including what a crawl stopped by the cutoff uploaded. A retry re-reads what the failed
  attempt uploaded (those reviews are not in `review_master_index` until `load_reviews`), so the
  day's Bronze partition can hold a review twice under different `review_id`s; that is safe because
  `BatchLoader` loads it once and `cleanse_reviews` keeps only the loaded copy
  (`ReviewCleaningPipeline.run`, see `src/processing/AGENTS.md`). `none_failed_min_one_success`
  would mark it `upstream_failed` as soon as one crawl failed, and `one_success` would start it
  before the other store's crawl and its retries are done.
- Airflow decides the DAG run state from the leaf tasks only, so with `all_done` a failed crawl
  followed by a successful `post_aggregate_validate` would leave the run green. `crawl_watcher`
  (the watcher pattern from the Airflow best-practices guide: `trigger_rule="one_failed"`,
  `retries=0`, `exit 1`) is a leaf downstream of both crawl tasks that fails when either crawl
  failed and is skipped otherwise, so the run stays failed while the other store's data still
  flows through. Checked with Airflow 2.10.5 `dag.test()` on this DAG's tasks with stub commands:
  App Store crawl failing gives `load_reviews` and the rest `success`, `crawl_watcher` `failed`,
  run `failed`; both crawls succeeding gives `crawl_watcher` `skipped`, run `success`.
- `dept_assign` overrides the DAG-level 3 retries with `retries=1` (and `crawl_watcher` with
  `retries=0`, since retrying it changes nothing). The step
  keeps its own per-row `try_number` ceiling, so a fourth attempt would find zero eligible rows and
  exit successfully — a persistent failure would report green.
- All tasks are `BashOperator`s built from `PROJECT_ROOT` and `PYTHON_BIN` environment variables
  (defaults: the DAG file's parent-of-parent, and `${PROJECT_ROOT}/.venv/bin/python`). Keep paths
  derived from those variables — no absolute paths.
- `gold_analyze` and `gold_aggregate` inline `python -c` calls into `src.pipeline.steps` and map
  `RunResult.status` to the exit code; `post_aggregate_validate` uses
  `python -m src.pipeline.cli --steps post_aggregate_validate --target-date {{ ds }}`.
- `gold_aggregate` intentionally aggregates a **single** logical date (`target_date='{{ ds }}'`).
  Range backfill (`run_aggregate(start_date=..., end_date=...)`) is a manual repair tool only —
  see `docs/superpowers/specs/2026-04-18-aggregate-range-backfill-design.md`.
- DAG success requires the DB validation task, not just task exit codes; the contract is in
  `docs/airflow-continuous-load-readiness.md`.
- The file carries `# pyright: reportMissingImports=false` because `airflow` is not a project
  dependency in `pyproject.toml`; it resolves only in the Airflow environment.

### Testing Requirements
`tests/test_airflow_dag_validation_wiring.py` and `tests/test_airflow_readiness_docs.py` parse this
file as text and assert the wiring/documentation contract — they do not import Airflow. The
wiring test covers the two crawl tasks, `load_reviews`' trigger rule and the `crawl_watcher` leaf.
Run:

```bash
PYTHONPATH=. uv run pytest tests/test_airflow_dag_validation_wiring.py tests/test_airflow_readiness_docs.py -q
```

### Common Patterns
Jinja `{{ ds }}` supplies the logical date to every date-scoped task; each bash command starts with
`cd {PROJECT_ROOT} && PYTHONPATH=.`.

## Dependencies

### Internal
`scripts/crawl_reviews.py`, `scripts/load_reviews.py`, `scripts/cleanse_reviews.py`,
`src.pipeline.steps`, `src.pipeline.cli`.

### External
`apache-airflow` (provided by the Airflow deployment, not by `pyproject.toml`).

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
