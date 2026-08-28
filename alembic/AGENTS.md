<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# alembic

## Purpose
Versioned schema migration environment. Alembic sits on top of the frozen `sql/schema_v4.sql`
baseline: existing databases are stamped at revision `20260430_0001`, empty databases can be built
by `alembic upgrade head`, and every later schema change is a hand-written revision.

## Key Files

| File | Description |
|------|-------------|
| `env.py` | Migration runner. Loads `.env`, resolves the DB URL, keeps `target_metadata = None` (autogenerate deliberately disabled), configures `compare_type` / `compare_server_default`, and runs offline or online with `NullPool` |

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `versions/` | Revision scripts, starting from the schema v4 baseline (see `versions/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- Database URL resolution order in `env.py:24` is: `-x database_url=…` → `config.attributes["database_url"]`
  (programmatic, used by `src/bootstrap_db.py`) → `DATABASE_URL` env var → `sqlalchemy.url` in
  `alembic.ini`. It raises if none is set — `alembic.ini` deliberately ships no URL.
- `target_metadata` is `None` on purpose: the ORM models in `src/models/` are not yet a lossless
  representation of the baseline, so `alembic revision --autogenerate` and `alembic check` are not
  part of the workflow. Do not wire metadata in without the model/schema alignment work.
- Write migrations by hand and review PostgreSQL-specific details manually: enum changes, pgvector
  indexes, partial indexes, partitioned tables, and backfills.
- Keep a single head. `.github/workflows/bootstrap-db.yml` fails the build when `alembic heads`
  prints more than one line.

### Testing Requirements
```bash
PYTHONPATH=. uv run alembic heads                 # must print exactly one head
PYTHONPATH=. uv run alembic upgrade head          # against a local PostgreSQL
PYTHONPATH=. uv run python scripts/bootstrap_db.py
PYTHONPATH=. uv run alembic current --check-heads
PYTHONPATH=. uv run pytest tests/test_alembic_config.py tests/test_bootstrap_db.py tests/test_ci_workflows.py -q
```
The full checklist lives in `docs/schema-management.md`.

### Common Patterns
Revision files declare `revision` / `down_revision` explicitly with date-ordered IDs
(`YYYYMMDD_NNNN`), and use `op.get_bind()` for raw SQL execution when a statement cannot be
expressed through Alembic operations.

## Dependencies

### Internal
`alembic.ini`, `sql/schema_v4.sql` (baseline source), `src/bootstrap_db.py` (programmatic driver),
`docs/schema-management.md` (policy).

### External
`alembic`, `sqlalchemy`, `python-dotenv`, `psycopg2-binary`.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
