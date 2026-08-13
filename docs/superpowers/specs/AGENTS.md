<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-08-10T08:45:46Z | Updated: 2026-08-10T08:45:46Z -->

# docs/superpowers/specs

## Purpose
Design specs stating the goal and chosen approach for a change before implementation. Shorter than
the plans in `../plans/`: a spec explains *why* the design is what it is, the plan enumerates the
steps.

## Key Files
| File | Description |
|------|-------------|
| `2026-04-18-aggregate-range-backfill-design.md` | Restores `gold_aggregate` to `target_date='{{ ds }}'` so scheduled runs aggregate only the logical execution date, and keeps a bounded manual backfill utility for exceptional repair |

## Subdirectories
None.

## For AI Agents

### Working In This Directory
- This design is still binding on the code: the Airflow task must aggregate a single date, and
  `GoldAggregator.run_range()` stays a manual repair tool. Do not make range backfill the scheduled
  default without superseding this spec.
- Specs are paired with a plan of the same slug in `../plans/` where one exists.
- Naming convention: `YYYY-MM-DD-<slug>-design.md`.

### Testing Requirements
None directly. The behavior it constrains is covered by `tests/test_gold_aggregator.py` and
`tests/test_airflow_dag_validation_wiring.py`.

### Common Patterns
A bolded **Goal** line followed by a **Design** bullet list naming the concrete code paths affected.

## Dependencies

### Internal
`dags/financial_review_pipeline.py`, `src/gold/aggregator.py`, `src/pipeline/steps.py`.

### External
None (Markdown only).

<!-- MANUAL: Notes added below this line are preserved on regeneration -->
