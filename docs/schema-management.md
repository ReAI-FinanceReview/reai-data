# Schema Management

This project now uses Alembic for versioned PostgreSQL schema changes while preserving the existing local bootstrap workflow.

## Baseline strategy

sql/schema_v4.sql is the immutable Alembic baseline snapshot. The first Alembic revision is `20260430_0001`, named `schema_v4_baseline`, and its SQL snapshot matches `sql/schema_v4.sql` at the time Alembic was introduced.

Existing databases that were already created from `sql/schema_v4.sql` should not run the baseline DDL again. Mark them as versioned with:

```bash
PYTHONPATH=. uv run alembic stamp 20260430_0001
```

Empty databases can be created through Alembic with:

```bash
PYTHONPATH=. uv run alembic upgrade head
```

This creates the structural schema only. It does not populate the required local reference catalog rows. Those rows come from `sql/app_service_data.sql`, `sql/apps_data.sql`, and `sql/app_metadata_data.sql`; load those seed files or run `scripts/bootstrap_db.py` when the database must be usable for local workflows.

Local development can continue using:

```bash
PYTHONPATH=. uv run python scripts/bootstrap_db.py
```

The bootstrap command resets the local `public` schema, applies `sql/schema_v4.sql`, loads required seed SQL, stamps revision `20260430_0001`, and applies any later Alembic revisions to `head`.

## Future schema changes

New schema changes should be made as Alembic revisions instead of direct edits to `sql/schema_v4.sql`.

Use this workflow:

```bash
PYTHONPATH=. uv run alembic current --check-heads
PYTHONPATH=. uv run alembic heads
PYTHONPATH=. uv run alembic revision -m "describe schema change"
PYTHONPATH=. uv run alembic upgrade head
```

Write and review migration operations before committing them. PostgreSQL-specific details such as enum changes, pgvector indexes, partial indexes, partitioned tables, and data backfills must be checked manually.

Autogenerate and `alembic check` are intentionally not part of the default workflow yet because the current SQLAlchemy model metadata is not a lossless representation of the `schema_v4.sql` baseline. Enable those commands only after model/schema alignment is completed in its own reviewed change.

## Reference data ownership

Required business reference rows remain in seed SQL:

- `sql/app_service_data.sql`
- `sql/apps_data.sql`
- `sql/app_metadata_data.sql`

Those files are idempotent and are loaded by `scripts/bootstrap_db.py`. They own the current app/service catalog and active app metadata.

Alembic migrations own structural schema changes. A migration may include data movement only when the data change is inseparable from a schema change, such as backfilling a new non-null column or converting enum values. Business catalog refreshes stay in seed SQL.

## Review checklist

Before merging a migration:

- Run `PYTHONPATH=. uv run alembic heads` and confirm there is one head.
- Run `PYTHONPATH=. uv run alembic upgrade head` against a local PostgreSQL database.
- Run `PYTHONPATH=. uv run python scripts/bootstrap_db.py` and confirm local reset plus seed plus migration still works.
- Run `PYTHONPATH=. uv run alembic current --check-heads` and confirm the database is at the latest head.
- Run the focused test suite for schema, bootstrap, and CI workflow coverage.
