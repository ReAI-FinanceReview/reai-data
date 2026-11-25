<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-11-25T00:00:00Z -->

# reai-data

## Purpose
Korean financial-app review ETL and NLP analytics pipeline. It crawls App Store / Play Store
reviews for 63 banking apps, lands them as Bronze Parquet in MinIO/S3, cleanses them to Silver,
enriches them in the Gold layer (embeddings → ABSA → actionability/LLM summary), and aggregates
into PostgreSQL fact tables plus a denormalized serving mart consumed by a dashboard backend.
The medallion flow is orchestrated either by the Airflow DAG in `dags/` or by the step CLI
(`src/pipeline/cli.py`, `scripts/run_pipeline.py`). Storage is hybrid: bulk review text lives in
Parquet, while state, index, and analytics tables live in PostgreSQL (pgvector).

## Key Files

| File | Description |
|------|-------------|
| `pyproject.toml` | Project metadata and dependencies; requires Python >= 3.12, dev group is `pytest` |
| `uv.lock` | Locked dependency graph; install with `uv sync` (CI uses `uv sync --frozen`) |
| `.python-version` | Pins the interpreter to 3.12 |
| `alembic.ini` | Alembic config (`script_location = alembic`, `prepend_sys_path = .`, UTC) |
| `docker-compose.yml` | Local infra: PostgreSQL `pgvector/pgvector:pg17`, MinIO, bucket bootstrap; ports and env setup are in `docs/local-development.md` |
| `docker-compose.test.yml` | Test infra: `test-postgres` on `${TEST_POSTGRES_PORT:-5433}`/testdb, `test-minio` on 9002/9003 |
| `.env.example` | Full environment template (DATABASE_URL, MINIO_*, OPENAI_API_KEY, ENABLE_PARQUET_WRITE) |
| `.env.local.example` | Minimal local-stack env matching `docker-compose.yml` |
| `reai.vuerd.json` | ERD source (vuerd format) for the physical schema |
| `.gitignore` | Ignores `.env`, `data/`, `logs/` among others, and blanket-ignores `*.md` and `*.sql` (see below) |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `src/` | All application code: crawlers, processing, gold analytics, loaders, models, pipeline (see `src/AGENTS.md`) |
| `scripts/` | Thin CLI entrypoints invoked by Airflow and by humans (see `scripts/AGENTS.md`) |
| `dags/` | Airflow DAG definition for the daily ETL run (see `dags/AGENTS.md`) |
| `tests/` | pytest suite; requires a real PostgreSQL database (see `tests/AGENTS.md`) |
| `sql/` | Immutable schema snapshots and seed reference data (see `sql/AGENTS.md`) |
| `alembic/` | Versioned schema migrations on top of the schema v4 baseline (see `alembic/AGENTS.md`) |
| `config/` | YAML runtime config, app-id lists, and NLP dictionaries (see `config/AGENTS.md`) |
| `docs/` | Contracts, runbooks, policies, and plan/spec history (see `docs/AGENTS.md`) |
| `.github/` | GitHub Actions CI (see `.github/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- Run everything from the repo root with `PYTHONPATH=.` and `uv run` — `src.*` imports assume the
  root is on `sys.path` (`scripts/*.py` insert it themselves, the Airflow DAG exports it).
- `sql/schema_v4.sql` is the **immutable** Alembic baseline. Schema changes go into a new Alembic
  revision under `alembic/versions/`, never into the baseline file. See `docs/schema-management.md`.
- `pyproject.toml` declares `readme = "README.md"`, but no `README.md` exists at the root; packaging
  commands that resolve the readme will fail until one is added.
- `.gitignore` blanket-ignores `*.md` and `*.sql`. Only `README.md`, `AGENTS.md`, and the SQL file
  names it lists are negated; the other tracked `.md`/`.sql` files stay tracked only because ignore
  rules never apply to a file that is already tracked. A new doc, plan, spec, seed file, or
  revision `.sql` is therefore left out of `git add .` without warning — add it with `git add -f`
  (or add a negation) and confirm with `git status` before committing.
- Never read or write `.env`; use `.env.example` / `.env.local.example` as the source of truth for
  which variables exist.
- `OPENAI_API_KEY` is needed by the Gold analyze step (embedding generation fails without it,
  `src/gold/embedding_generator.py:155-158`; the action-analysis LLM summary is skipped) and by the
  `llm` department assigner (`src/gold/dept_assigner.py:600-603`). Crawl, load, cleanse, and
  aggregate do not use it.

### Testing Requirements
The suite tests against real PostgreSQL — SQLite substitutes and mocked databases are not accepted
(see `tests/conftest.py`). Start the Docker test database and run pytest as `tests/AGENTS.md`
(Testing Requirements) describes; that file holds the one canonical command, and the other
`AGENTS.md` files only name the modules to pass to it.

Markers registered in `tests/conftest.py:662`: `slow`, `integration`, `requires_db`.
CI (`.github/workflows/bootstrap-db.yml`) runs only the bootstrap-focused subset
(`tests/test_ci_workflows.py tests/test_bootstrap_db.py tests/test_local_dev_setup.py
tests/test_alembic_config.py`) and, in a separate DB-free job, the AGENTS.md guard
(`tests/test_agents_docs.py`).

### Common Patterns
- Every component takes `config_path: str = "config/crawler_config.yml"` and builds its own
  `DatabaseConnector`; pass a config path explicitly in tests.
- Logging is always `from src.utils.logger import get_logger; logger = get_logger(__name__)`.
- Review identity uses UUID v7 (`uuid6.uuid7`) so IDs sort by creation time.
- Pipeline steps return the `RunResult` dataclass (`src/pipeline/steps.py:19`); failures are values,
  not exceptions, and `run_steps` stops at the first non-success step.
- Review lifecycle is tracked by `review_master_index.processing_status`:
  `RAW → CLEANED → ANALYZED`, with `FAILED` + `retry_count < 3` eligible for retry. `SKIPPED`
  is terminal: Gold could not analyze the review (reason in `error_message`, see
  `docs/pipeline-failure-policy.md`), so it is never retried and never reaches the marts.

## Dependencies

### Internal
Root config and compose files are consumed by `src/`, `scripts/`, `dags/`, and `tests/`; the layer
boundary is documented in `docs/backend-datamart-contract.md`.

### External
`sqlalchemy` + `psycopg2-binary` + `pgvector` (PostgreSQL), `alembic` (migrations), `boto3`
(MinIO/S3), `pyarrow` + `pandas` (Parquet), `pydantic` (schema validation), `openai` +
`sentence-transformers` + `transformers` + `torch` (embeddings/LLM), `snorkel` (weak supervision),
`konlpy` + `nltk` + `flashtext` + `emoji` (Korean text processing),
`google-play-scraper` + `requests` (crawling), `uuid6`, `pyyaml`, `python-dotenv`.

### Keeping These Files True

- These files are maintained by hand. No generator lives in this repository, and the facts in
  them were corrected against the code by review, so treat any tool-regenerated version as a
  proposal and review it as a diff. The Generated header and the MANUAL marker are kept for format
  compatibility; the guard below requires the marker to stay the last line, so put a new fact in
  the section it belongs to rather than below the marker.
- `Updated:` in the header is bumped whenever a file's content changes, formatting-only
  edits included. `Generated:` records first authorship and stays fixed.
- `tests/test_agents_docs.py` is the guard: it asserts the `.gitignore` negation still
  wins (and stays out of agent-tool dot-directories such as `.claude/`), the header is present and the MANUAL marker appears once as the last line, each
  parent link points at the nearest `AGENTS.md` above it, every `path:line` citation
  resolves to a file and every line number in it (ranges and lists included) is inside
  that file, every name in a table's first column exists, and every directory with
  tracked files has guidance (root dot-directories other than `.github/` are outside that
  rule, since their AGENTS.md stays ignored). CI runs it on every pull request in the
  "Check AGENTS.md guidance" job, so a code change that strands a citation, renames a file a
  Key Files table names, or adds an undocumented directory fails there. It cannot tell that a still-valid line number now points at
  different code, so a citation edited by hand still deserves a look.
- `tests/fixtures/` is the one directory deliberately without its own file; it is data
  only, and `DIRS_WITHOUT_GUIDANCE` in that test records the exemption.

<!-- MANUAL: hand-maintained; keep this marker as the last line and add notes to their section (root AGENTS.md) -->
