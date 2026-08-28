<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# dags

## Purpose
Airflow DAG definitions. Holds the single daily production DAG that chains the seven pipeline stages
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
- Task chain and timeouts: `crawl_reviews` (2h) → `load_reviews` (30m) → `cleanse_reviews` (1h) →
  `gold_analyze` (3h) → `dept_assign` (1h) → `gold_aggregate` (1h) → `post_aggregate_validate` (15m).
- `dept_assign` is the one task that overrides the DAG-level 3 retries with `retries=1`. The step
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
file as text and assert the wiring/documentation contract — they do not import Airflow. Run:

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

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
