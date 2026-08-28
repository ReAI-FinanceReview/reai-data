<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-28T00:00:00Z -->

# .github/workflows

## Purpose
GitHub Actions definitions. The single workflow proves that a database can be built two ways — from
an empty schema through Alembic, and through the local bootstrap script — and that both end at the
same Alembic head.

## Key Files

| File | Description |
|------|-------------|
| `bootstrap-db.yml` | Job `bootstrap-db` on `pull_request`, `push: main`, and `workflow_dispatch`. Runs a `pgvector/pgvector:pg17` service (SHA-pinned) on 5432, Python 3.12 + `uv sync --frozen`, then: single-head check → drop/recreate `public` + `alembic upgrade head` → `alembic current --check-heads` → `scripts/bootstrap_db.py` → head check again → focused pytest subset |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- `permissions: contents: read` and SHA-pinned third-party actions are deliberate supply-chain
  choices; keep both when editing or adding a workflow.
- The single-head assertion is shell-based (`alembic heads | wc -l` must equal 1). Adding a
  migration branch will fail CI here first.
- Both `DATABASE_URL` and `TEST_DATABASE_URL` point at the same CI service database, and the
  workflow drops the `public` schema — never point these at anything shared.
- CI runs only the bootstrap-focused tests (`test_ci_workflows.py`, `test_bootstrap_db.py`,
  `test_local_dev_setup.py`, `test_alembic_config.py`). The Gold/mart suites are not covered here;
  run them locally against the Docker test stack.
- `tests/test_ci_workflows.py` asserts the contents of this file — update the test in the same
  change.

### Testing Requirements
```bash
PYTHONPATH=. uv run pytest tests/test_ci_workflows.py -q
```

### Common Patterns
`"on":` is quoted to avoid the YAML boolean pitfall; every step is named; heredoc Python
(`uv run python - <<'PY'`) is used for inline DB setup instead of an extra script file.

## Dependencies

### Internal
`scripts/bootstrap_db.py`, `alembic/`, `pyproject.toml` + `uv.lock`, the four focused test modules.

### External
`actions/checkout`, `actions/setup-python`, `astral-sh/setup-uv`, the `pgvector/pgvector:pg17`
service image.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
