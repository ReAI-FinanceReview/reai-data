# Aggregate Range Backfill Design

**Goal:** Restore the daily aggregate path as the default Airflow behavior and keep a bounded manual backfill utility for exceptional aggregate repair.

**Design:**
- `gold_aggregate` in Airflow goes back to `target_date='{{ ds }}'`, so scheduled runs aggregate only the logical execution date.
- `run_aggregate()` accepts either `target_date` or an explicit `start_date`/`end_date` pair for manual backfills.
- `GoldAggregator` adds `run_range(start_date, end_date)` and reuses the existing per-date commit flow, but only for `ANALYZED` dates inside the requested bounds.
- `run_all()` remains available as an explicit full backfill helper, but it is no longer the implicit default path.

**Why this approach:**
- The team expects aggregate repair to be rare, so always scanning all analyzed dates is unnecessary operational cost.
- Explicit range parameters make exceptional repair obvious and auditable.
- No schema or state-machine changes are needed.
