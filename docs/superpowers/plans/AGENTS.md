<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# docs/superpowers/plans

## Purpose
Task-by-task implementation plans written before a change was executed. Each plan states a goal and
breaks it into checkbox steps an agent or engineer can work through in order.

## Key Files
| File | Description |
|------|-------------|
| `2026-04-18-aggregate-range-backfill.md` | Plan to restore single-date scheduled aggregation and add an explicit date-range backfill utility for manual repair. Implemented as `GoldAggregator.run_range()` |
| `2026-04-18-local-dev-compose.md` | Plan to add the local PostgreSQL + MinIO compose stack, env template, and usage docs. Implemented as `docker-compose.yml`, `.env.local.example`, `docs/local-development.md` |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- Both plans correspond to work that is already merged. Unchecked boxes are historical state, not a
  live to-do list — confirm intent before acting on one.
- The paired design rationale for the backfill plan lives in
  `../specs/2026-04-18-aggregate-range-backfill-design.md`.
- New plans follow `YYYY-MM-DD-<slug>.md` and keep the slug consistent with any matching spec.

### Testing Requirements
None directly. Validate against the implementing code and its tests:
`tests/test_gold_aggregator.py` for the backfill plan, `tests/test_local_dev_setup.py` for the
compose plan.

### Common Patterns
Header note naming the required sub-skill, a bolded **Goal**, then ordered `- [ ]` steps small enough
to verify individually.

## Dependencies

### Internal
`src/gold/aggregator.py`, `src/pipeline/steps.py:run_aggregate`, `docker-compose.yml`,
`docs/local-development.md`.

### External
None (Markdown only).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
