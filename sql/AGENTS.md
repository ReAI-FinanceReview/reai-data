<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# sql

## Purpose
Raw PostgreSQL DDL snapshots and seed reference data. `schema_v4.sql` is the current — and
immutable — Alembic baseline; the three `*_data.sql` files own the business catalog (logical
services, physical apps, and their SCD Type 2 metadata) that local and CI databases need before the
pipeline can run. Older schema snapshots are kept for history only.

## Key Files

| File | Description |
|------|-------------|
| `schema_v4.sql` | Current baseline DDL (2026-03-05): Bronze → Silver → Gold → data mart, pgvector, partitioned `srv_daily_review_list`. Applied by `src/bootstrap_db.py`, mirrored by Alembic revision `20260430_0001` |
| `app_service_data.sql` | Seed: 39 logical service masters, UUIDs `01960000-{NNNN}-7000-8000-…`. Load order 1 |
| `apps_data.sql` | Seed: 63 physical apps (37 App Store + 26 Play Store), UUIDs `01960001-{NNNN}-…`. Load order 2 |
| `app_metadata_data.sql` | Seed: 63 active SCD Type 2 rows linking apps ↔ services with group/bank classification. Load order 3 |
| `organizations_data.sql` | Organization hierarchy rows generated from `organization.csv`; not part of the bootstrap load order |
| `schema_v3.sql` | Historical schema snapshot (2026-02-16); not referenced by code |
| `schema_v2.sql` | Historical schema snapshot (2026-02-04); not referenced by code |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- **Do not edit `schema_v4.sql`.** It is the frozen Alembic baseline; its byte-content is mirrored
  in `alembic/versions/20260430_0001_schema_v4_baseline.sql`. New DDL goes into a fresh Alembic
  revision (`docs/schema-management.md`).
- The bootstrap load order is fixed in `src/bootstrap_db.py:20` (`SQL_FILE_ORDER`) because of FK
  parentage: `app_service` → `apps` → `app_metadata`. Adding a seed file means updating that tuple
  and, if the row count matters, `build_verification_queries()` (expects 39 / 63 / 63).
- Seed files must stay idempotent — `scripts/bootstrap_db.py` re-applies them after a schema reset.
- Migrations own structural change; seed SQL owns business catalog refreshes. Data movement belongs
  in a migration only when it is inseparable from a schema change.
- `schema_v2.sql`, `schema_v3.sql`, and `organizations_data.sql` are unreferenced by code and docs.
  Two of them are still named in `.gitignore` (`!schema_v3.sql`, `!organizations_data.sql`), which is
  what keeps them tracked against the blanket `*.sql` rule — do not prune those negations while
  treating the files as history. Do not delete them as part of an unrelated change.

### Testing Requirements
`tests/test_database_schema.py` asserts the objects `schema_v4.sql` creates, and
`tests/conftest.py` applies this file to build the test database. `tests/test_bootstrap_db.py`
covers the seed-order and verification-count contract.

```bash
TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql://testuser:testpass@localhost:5433/testdb}" \
  PYTHONPATH=. uv run pytest tests/test_database_schema.py tests/test_bootstrap_db.py -q
```

### Common Patterns
Every file opens with a banner comment stating purpose, date, row counts, and load order. Seed rows
use deterministic UUID v7-shaped identifiers so fixtures and expectations stay stable.

## Dependencies

### Internal
`src/bootstrap_db.py`, `alembic/versions/`, `tests/conftest.py`, `src/models/` (ORM mirror).

### External
PostgreSQL 17 with the `vector` (pgvector) extension. `ltree` is **not** used — it was removed in
issue #32, and `organizations.org_id` is plain `TEXT` with hyphen-separated levels (`1`, `1-1`, `10-5`).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
