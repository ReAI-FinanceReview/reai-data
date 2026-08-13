<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# docs

## Purpose
Human- and agent-facing documentation for contracts, runbooks, and policy. These files are not
decorative: several are asserted by tests (`tests/test_airflow_readiness_docs.py`,
`tests/test_backend_datamart_serving_readiness.py`), so they are part of the delivery contract.

## Key Files
| File | Description |
|------|-------------|
| `backend-datamart-contract.md` | Physical table contract for the four backend-facing Gold tables; the authoritative interface for dashboard consumers |
| `airflow-continuous-load-readiness.md` | Defines DAG success as task success **plus** the `post_aggregate_validate` DB check, and the severity policy behind it |
| `backend-datamart-serving-readiness.md` | Runbook for the manual release proof of mart serving readiness |
| `local-development.md` | Local stack setup: docker compose, `.env.local.example`, bootstrap, minimum ETL flow, verification queries |
| `schema-management.md` | Alembic baseline strategy, migration workflow, reference-data ownership, pre-merge checklist |
| `pipeline-failure-policy.md` | Where failures are recorded per stage in schema v4 (batch DLQ vs review-level FAILED) |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `evidence/` | Release evidence artifacts for readiness claims (see `evidence/AGENTS.md`) |
| `superpowers/` | Implementation plans and design specs from agent-driven work (see `superpowers/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- `airflow-continuous-load-readiness.md` and the serving-readiness docs are parsed by tests; edit
  their headings and stated guarantees only alongside the corresponding code and test change.
- When behavior changes, update the owning doc in the same change: schema → `schema-management.md`,
  DAG contract → `airflow-continuous-load-readiness.md`, mart columns →
  `backend-datamart-contract.md`, failure handling → `pipeline-failure-policy.md`.
- Docs here are written in English; commands are always shown rooted at the repo with
  `PYTHONPATH=. uv run …`.
- Evidence documents record real runs. Do not fabricate or pre-fill row counts, timestamps, or
  crawl results.

### Testing Requirements
```bash
PYTHONPATH=. uv run pytest tests/test_airflow_readiness_docs.py -q
TEST_DATABASE_URL="postgresql://testuser:testpass@localhost:5433/testdb" \
  PYTHONPATH=. uv run pytest tests/test_backend_datamart_serving_readiness.py -q
```

### Common Patterns
Each doc opens with an H1 title, then a "Contract boundary" or "Purpose"-style section that states
scope before details. Commands are fenced bash blocks; SQL evidence shapes are fenced sql blocks.

## Dependencies

### Internal
Describes `src/pipeline/post_aggregate_validation.py`, `src/gold/aggregator.py`,
`src/bootstrap_db.py`, `alembic/`, `dags/financial_review_pipeline.py`, and the compose files.

### External
None (Markdown only).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
