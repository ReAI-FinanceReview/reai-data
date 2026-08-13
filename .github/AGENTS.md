<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# .github

## Purpose
GitHub platform configuration. Currently holds only the CI workflow definitions that validate the
database bootstrap and migration path on every pull request and on pushes to `main`.

## Key Files
None at this level.

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `workflows/` | GitHub Actions workflow definitions (see `workflows/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- There is no issue/PR template, CODEOWNERS, or Dependabot config here yet; adding one is a
  deliberate change, not a side effect.
- Workflow content is asserted by `tests/test_ci_workflows.py`, so CI changes need a matching test
  update.

### Testing Requirements
```bash
PYTHONPATH=. uv run pytest tests/test_ci_workflows.py -q
```

### Common Patterns
Third-party actions are pinned to full commit SHAs rather than tags.

## Dependencies

### Internal
`tests/test_ci_workflows.py`, `scripts/bootstrap_db.py`, `alembic/`.

### External
GitHub Actions runners and the `pgvector/pgvector:pg17` service image.

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
