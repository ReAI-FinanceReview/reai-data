<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# alembic/versions

## Purpose
Migration revision scripts. The chain starts at the schema v4 baseline, which replays the frozen
`sql/schema_v4.sql` snapshot so an empty database can be built entirely through Alembic while
existing databases are stamped at the same revision instead of re-running the DDL. Later revisions
carry real schema changes on top of it.

## Key Files

| File | Description |
|------|-------------|
| `20260430_0001_schema_v4_baseline.py` | Revision `20260430_0001`, `down_revision = None`. `upgrade()` executes the sibling `.sql` file through the raw DBAPI cursor; `downgrade()` intentionally raises |
| `20260430_0001_schema_v4_baseline.sql` | The immutable DDL snapshot matching `sql/schema_v4.sql` at the time Alembic was introduced |
| `20260813_0002_reviews_assigned_review_id_unique.py` | Revision `20260813_0002`, `down_revision = 20260430_0001`. Adds the `assigner` discriminator column (default `'rule'` for pre-existing rows) and the `(review_id, assigner)` unique constraint that makes assignment re-runs UPSERT. It deliberately does not delete pre-existing duplicates — the constraint fails instead, leaving the choice to the data owner |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- The baseline pair is frozen. Do not edit either file — a change would desynchronize databases
  already stamped at `20260430_0001`.
- Downgrade past the baseline is unsupported by design; the documented recovery for local databases
  is `PYTHONPATH=. uv run python scripts/bootstrap_db.py`.
- New revisions must set `down_revision` to the current head and keep the single-head invariant that
  CI enforces. Use the `YYYYMMDD_NNNN` id convention.
- `upgrade()` here uses `op.get_bind().connection` with a raw cursor because the baseline SQL
  contains multiple statements including PostgreSQL-specific DDL. Prefer normal `op.*` operations
  for ordinary revisions.
- Autogenerate is disabled (`target_metadata = None` in `alembic/env.py`) — write revisions by hand
  and review enum changes, pgvector indexes, partial indexes, partitions, and backfills yourself.

### Testing Requirements
```bash
PYTHONPATH=. uv run alembic heads                 # exactly one head
PYTHONPATH=. uv run alembic upgrade head
PYTHONPATH=. uv run alembic current --check-heads
PYTHONPATH=. uv run pytest tests/test_alembic_config.py -q
```

### Common Patterns
Module-level `revision` / `down_revision` / `branch_labels` / `depends_on` constants, a docstring
header with revision id and create date, and SQL loaded from a sibling file via
`Path(__file__).with_name(...)` when the DDL is too large to inline.

## Dependencies

### Internal
`alembic/env.py`, `sql/schema_v4.sql`, `src/bootstrap_db.py` (`ALEMBIC_BASELINE_REVISION`).

### External
`alembic`, `psycopg2-binary`, PostgreSQL 17 with the `vector` (pgvector) extension.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
