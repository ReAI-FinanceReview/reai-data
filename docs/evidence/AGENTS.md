<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# docs/evidence

## Purpose
Release evidence artifacts. Holds the recorded proof for readiness claims that automated PR checks
cannot make on their own, because they depend on live store responses, real credentials, or
production-like data.

## Key Files
None at this level — evidence lives in per-claim subdirectories.

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `backend-datamart-serving-readiness/` | Evidence path for backend datamart serving readiness (see `backend-datamart-serving-readiness/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- Evidence is a record of what actually ran. Never generate, estimate, or pre-fill counts,
  timestamps, or command output here.
- One subdirectory per readiness claim, each with a `README.md` explaining what the automated checks
  cover and what must be proven manually.
- The runbook that defines what to record is `docs/backend-datamart-serving-readiness.md`.

### Testing Requirements
`tests/test_backend_datamart_serving_readiness.py` checks the readiness contract; the evidence files
themselves are reviewed by humans at release time.

### Common Patterns
Each evidence README states the boundary between automated proof and manual proof before listing
what must be captured.

## Dependencies

### Internal
`docs/backend-datamart-serving-readiness.md`, `docker-compose.test.yml`, `src/gold/aggregator.py`.

### External
None (Markdown only).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
