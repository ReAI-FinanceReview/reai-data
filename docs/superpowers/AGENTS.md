<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# docs/superpowers

## Purpose
Agent-authored planning artifacts. Holds the design specs and task-by-task implementation plans
produced before a change was made, kept as a record of intent and trade-offs behind features that
are already merged.

## Key Files
None at this level.

## Subdirectories

| Directory | Purpose |
|-----------|---------|
| `plans/` | Date-stamped implementation plans with checkbox task lists (see `plans/AGENTS.md`) |
| `specs/` | Date-stamped design specs stating goal and approach (see `specs/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- These are historical records of completed work, not a live backlog. Do not "finish" unchecked
  boxes in an old plan without confirming the work is still wanted.
- Naming convention is `YYYY-MM-DD-<slug>.md`, with a spec and a plan sharing the slug when both
  exist.
- Design decisions recorded here are still binding where code implements them — for example, the
  aggregate design fixes `gold_aggregate` to a single logical date with range backfill as a manual
  tool only.

### Testing Requirements
None directly. Verify against the code the document describes and the corresponding tests
(`tests/test_gold_aggregator.py`, `tests/test_local_dev_setup.py`).

### Common Patterns
Plans open with an agentic-worker sub-skill note, a **Goal**, and checkbox (`- [ ]`) steps; specs
open with a **Goal** and a **Design** bullet list.

## Dependencies

### Internal
`src/gold/aggregator.py`, `dags/financial_review_pipeline.py`, `docker-compose.yml`,
`docs/local-development.md`.

### External
None (Markdown only).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
