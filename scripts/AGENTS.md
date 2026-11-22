<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-22T00:00:00Z -->

# scripts

## Purpose
Thin executable entrypoints. Every script inserts the repository root into `sys.path` and then
delegates to `src.pipeline.steps` (or `src.bootstrap_db` / `src.processing.cleanse`). They exist so
Airflow `BashOperator` tasks, CI jobs, and humans share the same code paths as the step CLI. No
business logic belongs here.

## Key Files

| File | Description |
|------|-------------|
| `run_pipeline.py` | Unified entrypoint; delegates to `src.pipeline.cli.main` (`--steps`, `--target-date`, ...) |
| `crawl_reviews.py` | Crawl step only → Bronze Parquet in MinIO + `ingestion_batch` PENDING rows |
| `load_reviews.py` | Load step only → pending Parquet batches into `review_master_index` (RAW) |
| `cleanse_reviews.py` | Bronze → Silver cleansing CLI; `--date YYYY-MM-DD`, wires `config/dictionaries/{synonyms,profanity}.json` |
| `assign_dept.py` | Department assignment step only; `--date YYYY-MM-DD` for one day, `--backfill` for the explicit full sweep, `--assigner rule\|llm`, `--limit` |
| `eval_assignment.py` | Scores rows already in `reviews_assigned` against a human-labeled CSV; assigns nothing itself. `--labels`, `--json` |
| `preprocess_reviews.py` | Deprecated preprocess step; `run_preprocess` now warns and returns failure |
| `extract_features.py` | ABSA feature extraction step only (Gold) |
| `generate_embeddings.py` | Embedding generation step only (Gold); optional model name argument |
| `bootstrap_db.py` | Local DB reset/seed wrapper around `src.bootstrap_db.main` |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Always invoke with the repo root as CWD: `PYTHONPATH=. uv run python scripts/<name>.py`.
- Scripts must stay thin. New behavior goes into `src/pipeline/steps.py` or the owning module, and
  the script only parses arguments and maps the `RunResult` status to an exit code.
- `dags/financial_review_pipeline.py` invokes `crawl_reviews.py`, `load_reviews.py`,
  `cleanse_reviews.py`, and `assign_dept.py` by path — renaming or moving a script breaks the DAG.
- Dictionary paths are defined once, as `SYNONYMS_PATH` / `PROFANITY_PATH` in
  `src/processing/cleanse.py:24-28`. `cleanse_reviews.py` imports them and uses `_PROJECT_ROOT`
  only to put the repo on `sys.path`. Add a new dictionary path there as well, so the script and
  the pipeline cleanse step cannot diverge.

### Testing Requirements
No dedicated per-script tests; coverage comes from the step layer
(`tests/test_pipeline_steps.py`, `tests/test_cli_parsing.py`, `tests/test_cleanse.py`) and from
`tests/test_bootstrap_db.py` / `tests/test_local_dev_setup.py` for the bootstrap path. After
changing a script, run the corresponding step test plus a manual smoke run against the local stack.

### Common Patterns
```python
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
from src.pipeline.steps import run_x  # noqa: E402
```
`main()` returns `0` on success / `1` on failure and is invoked via `raise SystemExit(main())`.

## Dependencies

### Internal
`src.pipeline.steps`, `src.pipeline.cli`, `src.processing.cleanse`, `src.bootstrap_db`,
`config/dictionaries/`.

### External
`python-dotenv`: `.env` is loaded when a `DatabaseConnector` resolves its URL
(`src/utils/db_connector.py:16-22`), and `cleanse_reviews.py` also calls `load_dotenv()` on import;
`crawl_reviews.py` imports it but never calls it. Plus whatever the delegated step imports.

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
